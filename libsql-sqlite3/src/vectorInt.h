#ifndef _VECTOR_H
#define _VECTOR_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Objects */
typedef struct Vector Vector;
typedef struct DiskAnnIndex DiskAnnIndex;

/* An instance of this object represents a vector.
*/
struct Vector {
  void *data;
  size_t len;
};

void vectorDump(Vector *v);

int diskAnnOpenIndex(sqlite3 *, const char *zName, DiskAnnIndex **);
int diskAnnInsert(DiskAnnIndex *, Vector *v, i64);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_H */
