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

#include "vector.h"

/* Objects */
typedef struct DiskAnnIndex DiskAnnIndex;
typedef struct DiskAnnHeader DiskAnnHeader;
typedef struct DiskAnnVectorBlock DiskAnnVectorBlock;
typedef struct DiskAnnVector DiskAnnVector;
typedef struct DiskAnnMetadata DiskAnnMetadata;
typedef struct DiskAnnFreeBlock DiskAnnFreeBlock;

struct DiskAnnHeader {
  i64 nMagic;           /* Magic number */
  i64 firstVectorBlock; /* First vector block */
  i64 firstFreeBlock;   /* First free block */
};

struct DiskAnnVectorBlock {
  i64 rowid;                      /* Rowid */
  unsigned short nBlockSize;      /* Block size */
  unsigned short nNeighbours;     /* Number of neighbours */
  unsigned short nVectorDims;     /* Number of vector dimensions */
  unsigned short nVectorType;     /* Vector type */
  DiskAnnVector  *pNeighbours;    /* Next vector block */
  DiskAnnMetadata *pMetadata;     /* Metadata */
};

struct DiskAnnVector {
    void *pData;                    /* Data */
};

struct DiskAnnMetadata {
  sqlite_uint64 offset;           /* Offset */
  sqlite_uint64 rowid;            /* Size */
};

struct DiskAnnIndex {
  sqlite3_file  *pFd;             /* File descriptor */
  DiskAnnHeader header;           /* Header */
  i64 nFileSize;                  /* File size */
};

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
  assert( rc!=SQLITE_IOERR_SHORT_WRITE );
  return rc;
}

int diskAnnInsert(
  DiskAnnIndex *pIndex,
  const void *vec,
  i64 rowid
){
  char blockData[4096];
  int nBlockSize = 4096;
  int rc = SQLITE_OK;
  int i = 0;
  memset(blockData, 0, nBlockSize); // TODO: eliminate this
  /* rowid */
  blockData[i++] = rowid;
  blockData[i++] = rowid >> 8;
  blockData[i++] = rowid >> 16;
  blockData[i++] = rowid >> 24;
  blockData[i++] = rowid >> 32;
  blockData[i++] = rowid >> 40;
  blockData[i++] = rowid >> 48;
  blockData[i++] = rowid >> 56;
  /* nBlockSize */
  blockData[i++] = nBlockSize;
  blockData[i++] = nBlockSize >> 8;
  /* nNeighbours */
  blockData[i++] = 0x00;
  blockData[i++] = 0x00;
  /* nVectorDims */
  blockData[i++] = 0;
  blockData[i++] = 0;
  /* nVectorType */
  blockData[i++] = 0x00;
  blockData[i++] = 0x00;
  rc = sqlite3OsWrite(pIndex->pFd, blockData, nBlockSize, pIndex->nFileSize);
  if( rc != SQLITE_OK ){
    return rc;
  }
  pIndex->nFileSize += nBlockSize;
  return rc;
}

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
    pIndex->header.firstVectorBlock = 0;
    pIndex->header.firstFreeBlock = 0;
    rc = diskAnnWriteHeader(pIndex->pFd, &pIndex->header);
    if( rc != SQLITE_OK ){
      goto err_free;
    }
    pIndex->nFileSize = sizeof(DiskAnnHeader);
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
#endif /* !defined(SQLITE_OMIT_VECTOR) */
