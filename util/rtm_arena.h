/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA for speedy distributed
 *  in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS), Shanghai Jiao Tong University
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/drtm.html
 *
 */


#ifndef STORAGE_LEVELDB_UTIL_RTM_ARENA_H_
#define STORAGE_LEVELDB_UTIL_RTM_ARENA_H_

#include <cstddef>
#include <vector>
#include <assert.h>
#include <stdint.h>
#include <immintrin.h>

class RTMArena {
 public:
  RTMArena();
  ~RTMArena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (including space allocated but not yet used for user
  // allocations).
  size_t MemoryUsage() const {
    return blocks_memory_ + blocks_.capacity() * sizeof(char*);
  }


  void  AllocateFallback();

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;
  uint64_t cachelineaddr;
  int cacheset[64];

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_;

  // No copying allowed
  RTMArena(const RTMArena&);
  void operator=(const RTMArena&);
};

inline char* RTMArena::AllocateAligned(size_t bytes) {
  //printf("Alloca size %d\n", bytes);
  //TODO: align to cache line
  const int align = 64;// sizeof(void *);
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory

    while(_xtest())
      _xabort(0xf0);

    result = AllocateFallback(bytes);
  }

#if CACHSIM
  if( cachelineaddr != (uint64_t)result >> 6) {
    cachelineaddr = (uint64_t)result >> 6;
    int index = (int)(((uint64_t)result>>6)&0x3f);
    cacheset[index]++;
  }
#endif

  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}


inline char* RTMArena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  char *result = NULL;
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
  } else {
    result =  AllocateFallback(bytes);
  }
  return result;
}

#endif  // STORAGE_LEVELDB_UTIL_RTM_ARENA_H_
