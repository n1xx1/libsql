#ifndef _VECTOR_H
#define _VECTOR_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Objects */
typedef struct Vector Vector;
typedef struct DiskAnnIndex DiskAnnIndex;

#define VECTOR_TYPE_F32 0

/* An instance of this object represents a vector.
*/
struct Vector {
  u32 type;
  u32 len;
  void *data;
};

void vectorDump(Vector *v);

void vectorF32Dump(Vector *v);
void vectorF32Deserialize(sqlite3_context *,Vector *v);
void vectorF32Serialize(sqlite3_context *,Vector *v);
void vectorF32InitFromBlob(Vector *, const unsigned char *);
size_t vectorF3ParseBlob(sqlite3_context *, sqlite3_value *, Vector *);
float vectorF32DistanceCos(Vector *, Vector *);

int diskAnnOpenIndex(sqlite3 *, const char *zName, DiskAnnIndex **);
int diskAnnInsert(DiskAnnIndex *, Vector *v, i64);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_H */
