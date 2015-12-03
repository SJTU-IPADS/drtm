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

#ifndef RAWHASH_H
#define RAWHASH_H

#include <stdlib.h>
#include <iostream>

#include "util/rtmScope.h"
#include "util/rtm.h"
#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"

#define HASH_LOCK 0
#define RHASHLENGTH 1024*1024

using namespace leveldb;

namespace drtm {

class RawHashTable {

 public:
  struct RawHashNode {
    //The first field should be next
    RawHashNode* next;
    uint64_t key;
    void* value;
  };

  struct RawHead{
    RawHashNode *h;
    //char padding[56];
  };

  char padding[64];
  int length;
  char padding0[64];
  RawHead *lists;
  char padding1[64];
  RTMProfile prof;
  char padding2[64];
  SpinLock rtmlock;
  char padding3[64];
  port::SpinLock slock;
  char padding4[64];
  static __thread RawHashNode *dummynode_;
  char padding5[64];


  RawHashTable(){
    length = RHASHLENGTH;
    lists = new RawHead[length];
    for (int i=0; i<length; i++)
      lists[i].h = NULL;

  }

  ~RawHashTable(){
    //		printf("=========HashTable========\n");
    //		PrintStore();
    prof.reportAbortStatus();
  }

  inline void ThreadLocalInit() {
    if(dummynode_ == NULL) {
      dummynode_ = new RawHashNode();
    }
  }


  static inline uint64_t MurmurHash64A (uint64_t key, unsigned int seed )  {

    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (8 * m);
    const uint64_t * data = &key;
    const uint64_t * end = data + 1;

    while(data != end)  {
      uint64_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(8 & 7)   {
    case 7: h ^= uint64_t(data2[6]) << 48;
    case 6: h ^= uint64_t(data2[5]) << 40;
    case 5: h ^= uint64_t(data2[4]) << 32;
    case 4: h ^= uint64_t(data2[3]) << 24;
    case 3: h ^= uint64_t(data2[2]) << 16;
    case 2: h ^= uint64_t(data2[1]) << 8;
    case 1: h ^= uint64_t(data2[0]);
      h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
  }

  static inline uint64_t GetHash(uint64_t key) {
    return MurmurHash64A(key, 0xdeadbeef) & (RHASHLENGTH - 1);
    //return key % RHASHLENGTH ;
  }


  void Insert(uint64_t key, void* val) {
    ThreadLocalInit();
    //		dummynode_ = new RawHashNode();
    //Is this thread safe? be called in rtm
    uint64_t hash = GetHash(key);
    assert(hash < RHASHLENGTH);
    RawHashNode* head = lists[hash].h;
    if (head == NULL) {
      lists[hash].h = dummynode_;
      lists[hash].h->next = NULL;
    }
    else {
      RawHashNode* prev = head;
      RawHashNode* cur = prev->next;

      while(cur != NULL && cur->key < key) {
	prev = cur;
	cur = cur->next;
      }

      if(cur != NULL && cur->key == key){
	cur->value = val;
	return;
      }


      prev->next = dummynode_;
      dummynode_->next = cur;
    }
    dummynode_->key = key;
    dummynode_->value = val;
    dummynode_ = NULL;
    //printf("Insert hash %ld head %lx \n", hash, lists[hash].h);
    return;
  }

  void* Get(uint64_t key) {

    uint64_t hash = GetHash(key);

    RawHashNode* cur = lists[hash].h;
    //printf("Get hash %ld head %lx \n", hash, cur);

    while(cur != NULL && cur->key < key) {

      cur = cur->next;
    }

    if(cur != NULL && cur->key == key)
      return cur->value;

    return NULL;
  }


  void* Delete(uint64_t key) {
    uint64_t hash = GetHash(key);


    RawHashNode* head = lists[hash].h;
    //printf("hash %ld head %lx\n", hash, head);
    if (head == NULL) {
      return NULL;
    }
    if (head->key == key) {
      void* res = head->value;
      lists[hash].h = lists[hash].h->next;
      return res;
    }
    else {
      RawHashNode* prev = head;
      RawHashNode* cur = prev->next;

      while(cur != NULL && cur->key < key) {
	prev = cur;
	cur = cur->next;
      }

      if(cur != NULL && cur->key == key){
	void* res = cur->value;
	prev->next = cur->next;
	//delete cur;
	return res;
      }

    }
    return NULL;
  }
};
}
#endif
