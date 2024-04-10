/*
** 2024-03-18
**
** Copyright 2024 the libSQL authors
**
** Permission is hereby granted, free of charge, to any person obtaining a copy of
** this software and associated documentation files (the "Software"), to deal in
** the Software without restriction, including without limitation the rights to
** use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
** the Software, and to permit persons to whom the Software is furnished to do so,
** subject to the following conditions:
**
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
** FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
** COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
** IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
** CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
**
******************************************************************************
**
** libSQL vector search.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#define MAX_VECTOR_SZ 16000
#define MAX_FLOAT_CHAR_SZ  1024

/**************************************************************************
** Utility routines for serializing and deserializing
**************************************************************************/

static inline unsigned serializeU32(unsigned char *mem, u32 num){
  mem[0] = num & 0xFF;
  mem[1] = (num >> 8) & 0xFF;
  mem[2] = (num >> 16) & 0xFF;
  mem[3] = (num >> 24) & 0xFF;
  return sizeof(u32);
}

static inline u32 deserializeU32(const unsigned char *mem){
  return (mem[3] << 24) | (mem[2] << 16) | (mem[1] << 8) | mem[0];
}

static inline unsigned serializeF32(unsigned char *mem, float num){
  u32 *p = (u32 *)&num;
  mem[0] = *p & 0xFF;
  mem[1] = (*p >> 8) & 0xFF;
  mem[2] = (*p >> 16) & 0xFF;
  mem[3] = (*p >> 24) & 0xFF;
  return sizeof(float);
}

static inline float deserializeF32(const unsigned char *mem){
  u32 p = 0;
  p |= (u32)mem[0];
  p |= (u32)mem[1] << 8;
  p |= (u32)mem[2] << 16;
  p |= (u32)mem[3] << 24;
  return *(float *)&p;
}

/**************************************************************************
** Utility routines for dealing with Vector objects
**************************************************************************/

/* Initialize the Vector object
*/
static int vectorInit(Vector *p, sqlite3_context *pCtx){
  p->data = contextMalloc(pCtx, MAX_VECTOR_SZ);
  if( p->data==0 ){
    sqlite3_result_error_nomem(pCtx);
    return -1;
  }
  p->len = 0;
  return 0;
}

/* Deinitialize the Vector object
*/
static void vectorDeinit(Vector *p){
  sqlite3_free(p->data);
}

/*
** Initialize the Vector object from blob
**
** Note that that the vector object points to the blob so if
** you free the blob, the vector becomes invalid.
**/
static void vectorInitFromBlob(Vector *p, const unsigned char *blob){
  p->len = deserializeU32(blob);
  blob += sizeof(u32);
  p->data = (void*)blob;
}

static float vectorDistanceCos(Vector *v1, Vector *v2){
  float dot = 0, norm1 = 0, norm2 = 0;
  float *e1 = v1->data;
  float *e2 = v2->data;
  int i;
  assert( v1->len == v2->len );
  for(i = 0; i < v1->len; i++){
    dot += e1[i]*e2[i];
    norm1 += e1[i]*e1[i];
    norm2 += e2[i]*e2[i];
  }
  return 1.0 - (dot / sqrt(norm1 * norm2));
}

static size_t vectorParseText(
  sqlite3_context *context,
  sqlite3_value *arg,
  Vector *v
){
  char elBuf[MAX_FLOAT_CHAR_SZ];
  const unsigned char *zStr;
  float *elems = v->data;
  char zErr[128];
  int bufidx = 0;
  int vecidx = 0;
  double el;

  if( sqlite3_value_type(arg)!=SQLITE_TEXT ){
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: not a text type");
    goto error;
  }

  memset(elBuf, 0, sizeof(elBuf));

  zStr = sqlite3_value_text(arg);

  while (zStr && sqlite3Isspace(*zStr))
    zStr++;

  if( zStr==0 ) return 0;

  if (*zStr != '[') {
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: doesn't start with ']':");
    goto error;
  }
  zStr++;

  while (zStr != NULL && *zStr != '\0' && *zStr != ']') {
    char this = *zStr++;
    if (sqlite3Isspace(this)) {
      continue;
    }
    if (this != ',' && this != ']') {
      elBuf[bufidx++] = this;
      if (bufidx > MAX_FLOAT_CHAR_SZ) {
        char zErr[MAX_FLOAT_CHAR_SZ+100];
        sqlite3_snprintf(sizeof(zErr), zErr, "float too big while parsing vector: %s...", elBuf);
        return -1;
      }
    } else {
      if (sqlite3AtoF(elBuf, &el, bufidx, SQLITE_UTF8) <= 0) {
        sqlite3_snprintf(sizeof(zErr), zErr, "invalid number: %s...", elBuf);
        return -1;
      }
      bufidx = 0;
      memset(elBuf, 0, sizeof(elBuf));
      elems[vecidx++] = el;
      if (vecidx >= MAX_VECTOR_SZ) {
        sqlite3_snprintf(sizeof(zErr), zErr, "vector is larger than the maximum: (%d)", MAX_VECTOR_SZ);
        return -1;
      }
    }
  }
  if (bufidx != 0) {
    if (sqlite3AtoF(elBuf, &el, bufidx, SQLITE_UTF8) <= 0) {
      sqlite3_snprintf(sizeof(zErr), zErr, "invalid number: %s...", elBuf);
        return -1;
    }
    elems[vecidx++] = el;
    if (vecidx >= MAX_VECTOR_SZ) {
      sqlite3_snprintf(sizeof(zErr), zErr, "vector is larger than the maximum: (%d)", MAX_VECTOR_SZ);
        return -1;
    }
  }
  if (zStr && *zStr!= ']') {
    sqlite3_snprintf(sizeof(zErr), zErr, "malformed vector, doesn't end with ']'");
    return -1;
  }
  v->len = vecidx;
  return vecidx;
error:
  sqlite3_result_error(context, zErr, -1);
  return -1;
}

static size_t vectorParseBlob(
  sqlite3_context *context,
  sqlite3_value *arg,
  Vector *v
){
  const unsigned char *blob;
  float *elems = v->data;
  char zErr[128];
  unsigned i;
  size_t len;

  if( sqlite3_value_type(arg)!=SQLITE_BLOB ){
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: not a blob type");
    goto error;
  }

  blob = sqlite3_value_blob(arg);
  if( !blob ) {
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: zero length");
    goto error;
  }
  len = deserializeU32(blob);
  if (len > MAX_VECTOR_SZ) {
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: too large: %d", len);
    goto error;
  }
  blob += sizeof(u32);
  for(i = 0; i < len; i++){
    if( !blob ){
      sqlite3_snprintf(sizeof(zErr), zErr, "malformed blob");
      goto error;
    }
    elems[i] = deserializeF32(blob);
    blob += sizeof(float);
  }
  v->len = len;
  return len;
error:
  sqlite3_result_error(context, zErr, -1);
  return -1;
}

static size_t vectorParse(
  sqlite3_context *context,
  sqlite3_value *arg,
  Vector *v
){
  switch( sqlite3_value_type(arg) ){
    case SQLITE_BLOB:
      return vectorParseBlob(context, arg, v);
      break;
    case SQLITE_TEXT:
      return vectorParseText(context, arg, v);
    default:
      sqlite3_result_error(context, "invalid vector: not a text or blob type", -1);
      return -1;
  }
}

static void vectorSerialize(
  sqlite3_context *context,
  Vector *v
){
  float *elems = v->data;
  unsigned char *blob;
  unsigned char *blobPtr;
  unsigned int blobSz;

  blobSz = sizeof(u32) + v->len * sizeof(float);
  blob = contextMalloc(context, blobSz);

  if( blob ){
    unsigned i;

    blobPtr = blob;
    blobPtr += serializeU32(blobPtr, v->len);

    for (i = 0; i < v->len; i++) {
      blobPtr += serializeF32(blobPtr, elems[i]);
    }
    sqlite3_result_blob(context, (char*)blob, blobSz, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

static inline int isInteger(float num){
  return num == (u64)num;
}

static inline unsigned formatF32(float num, char *str){
  char tmp[32];
  if (isInteger(num)) {
    return snprintf(tmp, 32, "%lld", (u64)num);
  } else {
    return snprintf(tmp, 32, "%.6e", num);
  }
}

void vectorDump(Vector *pVec){
  float *elems = pVec->data;
  unsigned i;
  for(i = 0; i < pVec->len; i++){
    printf("%f ", elems[i]);
  }
  printf("\n");
}

static void vectorDeserialize(
  sqlite3_context *context,
  Vector *v
){
  float *elems = v->data;
  unsigned bufSz;
  unsigned bufIdx = 0;
  char *z;

  bufSz = 2 + v->len * 33;
  z = contextMalloc(context, bufSz);

  if( z ){
    unsigned i;

    z[bufIdx++]= '[';
    for (i = 0; i < v->len; i++) { 
      char tmp[12];
      unsigned bytes = formatF32(elems[i], tmp);
      memcpy(&z[bufIdx], tmp, bytes);
      bufIdx += strlen(tmp);
      z[bufIdx++] = ',';
    }
    bufIdx--;
    z[bufIdx++] = ']';

    sqlite3_result_text(context, z, bufIdx, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

/**************************************************************************
** Vector index cursor implementations
****************************************************************************/

/*
** A VectorIdxCursor is a special cursor to perform vector index lookups.
 */
struct VectorIdxCursor {
  sqlite3 *db;          /* Database connection */
  DiskAnnIndex *index;   /* DiskANN index on disk */
};

int vectorIndexCreate(Index *pIdx){
  printf("STUB: vectorIndexCreate: %s\n", pIdx->zName);
  return 0;
}

int vectorIndexInsert(
  VectorIdxCursor *pCur,
  const BtreePayload *pX
){
  struct sqlite3_value *rowid;
  struct sqlite3_value *vec;
  UnpackedRecord r;
  r.aMem = pX->aMem;
  r.nField = pX->nMem;
  assert( r.nField == 2 );
  vec = r.aMem + 0;
  assert( sqlite3_value_type(vec) == SQLITE_BLOB );
  rowid = r.aMem + 1;
  assert( sqlite3_value_type(rowid) == SQLITE_INTEGER );
  Vector v;
  vectorInitFromBlob(&v, sqlite3_value_blob(vec));
  diskAnnInsert(pCur->index, &v, sqlite3_value_int64(rowid));
  return 0;
}

int vectorIndexCursorInit(sqlite3 *db, VdbeCursor *pCsr, const char *zIndexName){
  VectorIdxCursor *pCur;
  char zIndexFile[SQLITE_MAX_PATHLEN];
  const char *zDbPath;
  int rc;

  // TODO: We're taking the filename of the currently selected
  // database (think attach). I think it's what we want to do,
  // but let's verify.
  zDbPath = sqlite3_db_filename(db, db->aDb[pCsr->iDb].zDbSName);

  // TODO: We may want to use a name that is unique to the _index_.
  sqlite3_snprintf(sizeof(zIndexFile), zIndexFile, "%s-vectoridx-%s", zDbPath, zIndexName);

  // TODO: Where do we deallocate this?
  pCur = sqlite3DbMallocZero(db, sizeof(VectorIdxCursor));
  if( pCur == 0 ){
    return SQLITE_NOMEM_BKPT;
  }
  rc = diskAnnOpenIndex(db, zIndexFile, &pCur->index);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pCur->db = db;
  pCsr->uc.pVecIdx = pCur;
  return SQLITE_OK;
}

/**************************************************************************
** SQL function implementations
****************************************************************************/

/*
** Implementation of vector(X) function.
*/
static void vectorFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Vector vec;
  if( argc < 1 ){
    return;
  }
  if( vectorInit(&vec, context)<0 ){
    return;
  }
  if( vectorParse(context, argv[0], &vec)>0 ) {
    vectorSerialize(context, &vec);
  }
  vectorDeinit(&vec);
}

/*
** Implementation of vector_extract(X) function.
*/
static void vectorExtractFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zStr;
  Vector vec;
  char zErr[128];
  unsigned i;

  if( argc < 1 ){
    return;
  }
  if( vectorInit(&vec, context)<0 ){
    return;
  }
  if( vectorParse(context, argv[0], &vec)<0 ){
    goto out_free;
  }
  vectorDeserialize(context, &vec);
out_free:
  vectorDeinit(&vec);
}

/*
** Implementation of vector_distance_cos(X, Y) function.
*/
static void vectorDistanceCosFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Vector vec1, vec2;
  if( argc < 2 ) {
    return;
  }
  if( vectorInit(&vec1, context)<0 ){
    return;
  }
  if( vectorInit(&vec2, context)<0 ){
    goto out_free_vec1;
  }
  if( vectorParse(context, argv[0], &vec1)<0 ){
    goto out_free_vec2;
  }
  if( vectorParse(context, argv[1], &vec2)<0){
    goto out_free_vec2;
  }
  if( vec1.len != vec2.len ){
    sqlite3_result_error(context, "vectors must have the same length", -1);
    goto out_free_vec2;
  }
  sqlite3_result_double(context, vectorDistanceCos(&vec1, &vec2));
out_free_vec2:
  vectorDeinit(&vec2);
out_free_vec1:
  vectorDeinit(&vec1);
}

/*
** Register vector functions.
*/
void sqlite3RegisterVectorFunctions(void){
 static FuncDef aVectorFuncs[] = {
    VECTOR_FUNCTION(vector_distance_cos,  2, 0, 0, vectorDistanceCosFunc),

    FUNCTION(vector,         1, 0, 0, vectorFunc),
    FUNCTION(vector_extract, 1, 0, 0, vectorExtractFunc),
  };
  sqlite3InsertBuiltinFuncs(aVectorFuncs, ArraySize(aVectorFuncs));
}
#endif /* !defined(SQLITE_OMIT_VECTOR) */
