#ifndef STORAGE_LEVELDB_INCLUDE_TXDB_H_
#define STORAGE_LEVELDB_INCLUDE_TXDB_H_

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include <string>
#include "port/port_posix.h"

namespace leveldb {

class TXDB {
 public:

  TXDB() { }
   ~TXDB(){ };

  virtual Status Put(const Slice& key,
                     const Slice& value, uint64_t seq) = 0;

//  virtual Status PutBatch(const Slice& key,
  //                   const Slice& value, uint64_t seq) = 0;

  virtual Status Get(const Slice& key,
                     Slice* value, uint64_t seq) = 0;
  
  virtual Status Delete(const Slice& key, uint64_t seq) = 0; 

  virtual Status GetMaxSeq(const Slice& key, uint64_t* seq) = 0;

};

}
#endif
