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


#include "util/rtm_arena.h"
#include <assert.h>
#include <stdio.h>
#include <immintrin.h>


#define CACHSIM 0

static const int kBlockSize = 16*4096;

RTMArena::RTMArena() {
  blocks_memory_ = 0;
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;

#if CACHSIM
  for(int i = 0; i < 64; i ++)
    cacheset[i] = 0;
  cachelineaddr = 0;
#endif
}

RTMArena::~RTMArena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    //printf("Free %lx\n", blocks_[i]);
    delete[] blocks_[i];
  }

#if CACHSIM
  for(int i = 0; i < 64; i ++)
    printf("cacheset[%d] %d ", i, cacheset[i]);
  printf("\n");
#endif
}

char* RTMArena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

void RTMArena::AllocateFallback() {

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  for(int i = 0; i < kBlockSize; i+=4096)
    alloc_ptr_[i] = 0;

  alloc_bytes_remaining_ = kBlockSize;
}

char* RTMArena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  //printf("Allocate %lx\n", result);
  blocks_memory_ += block_bytes;
  blocks_.push_back(result);
  return result;
}
