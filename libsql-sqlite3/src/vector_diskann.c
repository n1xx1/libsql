/*
** 2024-03-23
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
** DiskANN for SQLite/libSQL.
**
** The algorithm is described in the following publications:
**
**   Suhas Jayaram Subramanya et al (2019). DiskANN: Fast Accurate Billion-point
**   Nearest Neighbor Search on a Single Node. In NeurIPS 2019.
**
**   Aditi Singh et al (2021). FreshDiskANN: A Fast and Accurate Graph-Based ANN
**   Index for Streaming Similarity Search. ArXiv.
**
**   Yu Pan et al (2023). LM-DiskANN: Low Memory Footprint in Disk-Native
**   Dynamic Graph-Based ANN Indexing. In IEEE BIGDATA 2023.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

/* Objects */
typedef struct DiskAnnHeader DiskAnnHeader;
typedef struct SearchContext SearchContext;
typedef struct VectorNode VectorNode;
typedef struct Metadata Metadata;

/**
** The block size in bytes.
**/
#define DISKANN_BLOCK_SIZE 4096

/**
** The bit shift to get the block size in bytes.
**/
#define DISKANN_BLOCK_SIZE_SHIFT 9

struct DiskAnnHeader {
  i64 nMagic;                        /* Magic number */
  unsigned short nBlockSize;         /* Block size */
  unsigned short nVectorType;        /* Vector type */
  unsigned short nVectorDims;        /* Number of vector dimensions */
  unsigned short similarityFunction; /* Similarity function */
  i64 entryVectorOffset;             /* Offset to random offset to use to start search */
  i64 firstFreeOffset;               /* First free offset */
};

struct DiskAnnIndex {
  sqlite3_file  *pFd;             /* File descriptor */
  DiskAnnHeader header;           /* Header */
  i64 nFileSize;                  /* File size */
};

struct VectorNode {
  Vector vec;
  u64 id;
  u64 offset;
  int visited;                    /* Is this node visited? */
  VectorNode *pNext;              /* Next node in the visited list */
};

struct Metadata {
  u64 id;
  u64 offset;
};

/**************************************************************************
** Utility routines for managing vector nodes
**************************************************************************/

static VectorNode *vectorNodeNew(void){
  VectorNode *pNode;
  pNode = sqlite3_malloc(sizeof(VectorNode));
  if( pNode ){
    pNode->visited = 0;
    pNode->pNext = NULL;
  }
  return pNode;
}

static void vectorNodeFree(VectorNode *pNode){
  sqlite3_free(pNode->vec.data);
  sqlite3_free(pNode);
}

/**************************************************************************
** Utility routines for parsing the index file
**************************************************************************/

#define VECTOR_METADATA_SIZE    sizeof(u64)
#define NEIGHBOUR_METADATA_SIZE sizeof(Metadata)

static unsigned int blockSize(DiskAnnIndex *pIndex){
  return pIndex->header.nBlockSize << DISKANN_BLOCK_SIZE_SHIFT;
}

static unsigned int vectorSize(DiskAnnIndex *pIndex){
  assert( pIndex->header.nVectorType == VECTOR_TYPE_F32 );
  return sizeof(u32) + pIndex->header.nVectorDims * sizeof(float);
}

static int neighbourMetadataOffset(DiskAnnIndex *pIndex){
  unsigned int nNeighbourVectorSize;
  unsigned int maxNeighbours;
  unsigned int nVectorSize;
  unsigned int nBlockSize;
  nBlockSize = blockSize(pIndex);
  nVectorSize = vectorSize(pIndex);
  nNeighbourVectorSize = vectorSize(pIndex);
  maxNeighbours = (nBlockSize - nVectorSize - VECTOR_METADATA_SIZE) / (nNeighbourVectorSize + NEIGHBOUR_METADATA_SIZE);
  return nVectorSize + VECTOR_METADATA_SIZE + maxNeighbours * (nNeighbourVectorSize); 
}

static int diskAnnReadHeader(
  sqlite3_file *pFd,
  DiskAnnHeader *pHeader
){
  int rc;
  // TODO: endianess
  rc = sqlite3OsRead(pFd, pHeader, sizeof(DiskAnnHeader), 0);
  assert( rc!=SQLITE_IOERR_SHORT_READ );
  return rc;
}

static int diskAnnWriteHeader(
  sqlite3_file *pFd,
  DiskAnnHeader *pHeader
){
  int rc;
  // TODO: endianess
  rc = sqlite3OsWrite(pFd, pHeader, sizeof(DiskAnnHeader), 0);
  return rc;
}

static VectorNode *diskAnnReadVector(
  DiskAnnIndex *pIndex,
  u64 offset
){
  unsigned char blockData[DISKANN_BLOCK_SIZE];
  VectorNode *pNode;
  int i = 0;
  int rc;
  if( offset==0 ){
    return NULL;
  }
  assert( offset < pIndex->nFileSize );
  rc = sqlite3OsRead(pIndex->pFd, blockData, DISKANN_BLOCK_SIZE, offset);
  if( rc != SQLITE_OK ){
    return NULL;
  }
  pNode = vectorNodeNew();
  if( pNode==NULL ){
    return NULL;
  }
  pNode->vec.type = pIndex->header.nVectorType;
  pNode->vec.len = pIndex->header.nVectorDims;
  pNode->vec.data = sqlite3_malloc(pNode->vec.len * sizeof(float));
  if( pNode->vec.data == NULL ){
    vectorNodeFree(pNode);
    return NULL;
  }
  i = vectorDeserializeFromBlob(&pNode->vec, blockData, DISKANN_BLOCK_SIZE);
  pNode->id = (u64) blockData[i+0]
    | (u64) blockData[i+1] << 8
    | (u64) blockData[i+2] << 16
    | (u64) blockData[i+3] << 24
    | (u64) blockData[i+4] << 32
    | (u64) blockData[i+5] << 40
    | (u64) blockData[i+6] << 48
    | (u64) blockData[i+7] << 56;
  i += 8;
  pNode->offset = offset;
  return pNode;
}

static int diskAnnWriteVector(
  DiskAnnIndex *pIndex,
  Vector *pVec,
  u64 id,
  Vector **aNeighbours,
  Metadata *aNeighbourMetadata,
  int nNeighbours,
  u64 offset,
  u64 nBlockSize
){
  char blockData[DISKANN_BLOCK_SIZE]; // TODO: dynamic allocation
  int rc = SQLITE_OK;
  int off = 0;
  memset(blockData, 0, DISKANN_BLOCK_SIZE);
  off = vectorSerializeToBlob(pVec, (unsigned char*)blockData, DISKANN_BLOCK_SIZE);
  /* ID */
  blockData[off++] = id;
  blockData[off++] = id >> 8;
  blockData[off++] = id >> 16;
  blockData[off++] = id >> 24;
  blockData[off++] = id >> 32;
  blockData[off++] = id >> 40;
  blockData[off++] = id >> 48;
  blockData[off++] = id >> 56;
  /* nNeighbours */
  blockData[off++] = nNeighbours;
  blockData[off++] = nNeighbours >> 8;
  for (int i = 0; i < nNeighbours; i++) {
    off += vectorSerializeToBlob(aNeighbours[i], (unsigned char*)&blockData[off], DISKANN_BLOCK_SIZE);
  }
  off = neighbourMetadataOffset(pIndex);
  for( int i = 0; i < nNeighbours; i++ ){
    blockData[off++] = aNeighbourMetadata[i].id;
    blockData[off++] = aNeighbourMetadata[i].id >> 8;
    blockData[off++] = aNeighbourMetadata[i].id >> 16;
    blockData[off++] = aNeighbourMetadata[i].id >> 24;
    blockData[off++] = aNeighbourMetadata[i].id >> 32;
    blockData[off++] = aNeighbourMetadata[i].id >> 40;
    blockData[off++] = aNeighbourMetadata[i].id >> 48;
    blockData[off++] = aNeighbourMetadata[i].id >> 56;
    blockData[off++] = aNeighbourMetadata[i].offset;
    blockData[off++] = aNeighbourMetadata[i].offset >> 8;
    blockData[off++] = aNeighbourMetadata[i].offset >> 16;
    blockData[off++] = aNeighbourMetadata[i].offset >> 24;
    blockData[off++] = aNeighbourMetadata[i].offset >> 32;
    blockData[off++] = aNeighbourMetadata[i].offset >> 40;
    blockData[off++] = aNeighbourMetadata[i].offset >> 48;
    blockData[off++] = aNeighbourMetadata[i].offset >> 56;
  }
  rc = sqlite3OsWrite(pIndex->pFd, blockData, nBlockSize, pIndex->nFileSize);
  if( rc != SQLITE_OK ){
    return rc;
  }
  return rc;
}

/**************************************************************************
** DiskANN search
**************************************************************************/

struct SearchContext {
  Vector *pQuery;
  VectorNode **aCandidates;
  unsigned int nCandidates;
  unsigned int maxCandidates;
  VectorNode *visitedList;
  unsigned int nUnvisited;
  int k;
};

static void initSearchContext(SearchContext *pCtx, Vector* pQuery, unsigned int maxCandidates){
  pCtx->pQuery = pQuery;
  pCtx->aCandidates = sqlite3_malloc(maxCandidates * sizeof(VectorNode));
  pCtx->nCandidates = 0;
  pCtx->maxCandidates = maxCandidates;
  pCtx->visitedList = NULL;
  pCtx->nUnvisited = 0;
}

static void deinitSearchContext(SearchContext *pCtx){
  for( VectorNode *pNode = pCtx->visitedList; pNode!=NULL; pNode = pNode->pNext ){
    vectorNodeFree(pNode);
  }
  sqlite3_free(pCtx->aCandidates);
}

static void addCandidate(SearchContext *pCtx, VectorNode *pNode){
  pCtx->aCandidates[pCtx->nCandidates++] = pNode;
  pCtx->nUnvisited++;
}

static VectorNode* findClosestCandidate(SearchContext *pCtx){
  VectorNode *pNode = NULL;
  for (int i = 0; i < pCtx->nCandidates; i++) {
    if( !pCtx->aCandidates[i]->visited ){
      if( pNode==NULL || vectorDistanceCos(pCtx->pQuery, &pCtx->aCandidates[i]->vec) < vectorDistanceCos(pCtx->pQuery, &pNode->vec) ){
        pNode = pCtx->aCandidates[i];
      }
    }
  }
  return pNode;
}

static void markAsVisited(SearchContext *pCtx, VectorNode *pNode){
  pNode->visited = 1;
  assert(pCtx->nUnvisited > 0);
  pCtx->nUnvisited--;
  pNode->pNext = pCtx->visitedList;
  pCtx->visitedList = pNode;
}

static int hasUnvisitedCandidates(SearchContext *pCtx){
  return pCtx->nUnvisited > 0;
}

int diskAnnSearch(
  DiskAnnIndex *pIndex,
  SearchContext *pCtx
){
  VectorNode *start;

  start = diskAnnReadVector(pIndex, pIndex->header.entryVectorOffset);
  if( start==NULL ){
    return 0;
  }
  addCandidate(pCtx, start);
  while( hasUnvisitedCandidates(pCtx) ){
    VectorNode *candidate = findClosestCandidate(pCtx);
    markAsVisited(pCtx, candidate);
    // TODO: add candidate neighbours to candidates, trimming if necessary
  }
  // TODO: return k top candidates and visited ndoes
  return 0;
}

/**************************************************************************
** DiskANN insertion
**************************************************************************/

// TODO: fix hard-coded limit
#define MAX_NEIGHBOURS 10

int diskAnnInsert(
  DiskAnnIndex *pIndex,
  Vector *pVec,
  i64 id
){
  unsigned int nNeighbours = 0;
  unsigned int nBlockSize;
  Vector *aNeighbours[MAX_NEIGHBOURS]; 
  Metadata aNeighbourMetadata[MAX_NEIGHBOURS];
  VectorNode *pNode;
  SearchContext ctx;
  u64 offset;

  pNode = vectorNodeNew();
  if( pNode==NULL ){
    return SQLITE_NOMEM;
  }
  initSearchContext(&ctx, pVec, 10); // TODO: Fix hard-coded L
  diskAnnSearch(pIndex, &ctx);
  for( VectorNode *pNeighbour = ctx.visitedList; pNeighbour!=NULL; pNeighbour = pNeighbour->pNext ){
    aNeighbours[nNeighbours] = &pNeighbour->vec;
    aNeighbourMetadata[nNeighbours].id = pNeighbour->id;
    aNeighbourMetadata[nNeighbours].offset = pNeighbour->offset;
    nNeighbours++;
  }
  // TODO: prune p 
  for( VectorNode* pNeighbour = ctx.visitedList; pNeighbour!=NULL; pNeighbour = pNeighbour->pNext ){
    // TODO: add p as pNode neigbour
    // TODO: prune pNode
  }

  nBlockSize = blockSize(pIndex);
  offset = pIndex->nFileSize;
  pIndex->nFileSize += diskAnnWriteVector(pIndex, pVec, id, aNeighbours, aNeighbourMetadata, nNeighbours, offset, nBlockSize);

  deinitSearchContext(&ctx);

  if( pIndex->header.entryVectorOffset == 0 ){
    // TODO: We actually want the entry to be random, but let's start with the first one.
    pIndex->header.entryVectorOffset = offset;
    diskAnnWriteHeader(pIndex->pFd, &pIndex->header);
  }
  return SQLITE_OK;
}

/**************************************************************************
** DiskANN index file management
**************************************************************************/

static int diskAnnOpenIndexFile(
  sqlite3 *db,
  const char *zName,
  sqlite3_file **ppFd
){
  int rc;
  rc = sqlite3OsOpenMalloc(db->pVfs, zName, ppFd,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, &rc
  );
  // TODO: close the file and free the memory
  return rc;
}

int diskAnnOpenIndex(
  sqlite3 *db,                    /* Database connection */
  const char *zName,              /* Index name */
  DiskAnnIndex **ppIndex          /* OUT: Index */
){
  DiskAnnIndex *pIndex;
  int rc = SQLITE_OK;
  /* Allocate memory */
  pIndex = sqlite3_malloc(sizeof(DiskAnnIndex));
  if( pIndex == NULL ){
    rc = SQLITE_NOMEM;
    goto err_free;
  }
  /* Open index file */
  rc = diskAnnOpenIndexFile(db, zName, &pIndex->pFd);
  if( rc != SQLITE_OK ){
    goto err_free;
  }
  /* Probe file size */
  rc = sqlite3OsFileSize(pIndex->pFd, &pIndex->nFileSize);
  if( rc != SQLITE_OK ){
    goto err_free;
  }
  if( pIndex->nFileSize == 0 ){
    /* Initialize header */
    pIndex->header.nMagic = 0x4e4e416b736944; /* 'DiskANN' */
    pIndex->header.nBlockSize = DISKANN_BLOCK_SIZE >> DISKANN_BLOCK_SIZE_SHIFT;
    pIndex->header.nVectorType = VECTOR_TYPE_F32;
    pIndex->header.nVectorDims = 3; // FIXME: take from vector column type in schema?
    pIndex->header.similarityFunction = 0;
    rc = diskAnnWriteHeader(pIndex->pFd, &pIndex->header);
    if( rc != SQLITE_OK ){
      goto err_free;
    }
    pIndex->nFileSize = blockSize(pIndex);
  } else {
    /* Read header */
    rc = diskAnnReadHeader(pIndex->pFd, &pIndex->header);
    if( rc != SQLITE_OK ){
      goto err_free;
    }
  }
  *ppIndex = pIndex;
  return SQLITE_OK;
err_free:
  sqlite3_free(pIndex);
  return rc;
}

void diskAnnCloseIndex(DiskAnnIndex *pIndex){
  sqlite3OsClose(pIndex->pFd);
  sqlite3_free(pIndex);
}
#endif /* !defined(SQLITE_OMIT_VECTOR) */
