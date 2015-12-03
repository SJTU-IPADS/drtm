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

#ifndef RDMA_31_CUCKOOHASH_H
#define RDMA_31_CUCKOOHASH_H

#include <stdlib.h>
#include <iostream>
#include <vector>

//paddings for RDMA,may not be needed

#define SLOT_PER_BUCKET 1
#define MAX_TRY 500
using std::vector;

class Rdma_3_1_CuckooHash {

 public:
  struct RdmaArrayNode {
    uint64_t key;
    uint64_t index;
    bool valid;
  };
  char * array;
  uint64_t length;
  uint64_t entrysize;
  uint64_t bucketlength;
  uint64_t bucketsize;
  uint64_t size;//total
  uint64_t data_offset;
  uint64_t free_ptr;
  RdmaArrayNode * header;
  Rdma_3_1_CuckooHash(uint64_t esize,uint64_t len,char* arr){
    entrysize = (((esize-1)>>3)+1) <<3; ;
    bucketsize = sizeof(RdmaArrayNode) * SLOT_PER_BUCKET;
    length = len ;
    bucketlength = length / SLOT_PER_BUCKET;
    array=arr;
    data_offset = bucketlength * bucketsize;
    free_ptr=0;
    size = data_offset+ entrysize * length ;

    header = (RdmaArrayNode *)array;
  }

  ~Rdma_3_1_CuckooHash(){
  }


  static inline uint64_t MurmurHash64A (uint64_t key, unsigned int seed )  {

    const uint64_t m = 0xc6a4a7935bd1e995;
    const uint64_t r = 47;
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

  inline uint64_t GetHash(uint64_t key) {
    return MurmurHash64A(key, 0xdeadbeef) % bucketlength;
  }

  inline uint64_t GetHash3(uint64_t key) {
    return key % bucketlength ;
  }
  inline uint64_t GetHash2(uint64_t key) {
    uint32_t a = key;
    a = (a+0x7ed55d16) + (a<<12);
    a = (a^0xc761c23c) ^ (a>>19);
    a = (a+0x165667b1) + (a<<5);
    a = (a+0xd3a2646c) ^ (a<<9);
    a = (a+0xfd7046c5) + (a<<3);
    a = (a^0xb55a4f09) ^ (a>>16);

    return (a) % bucketlength ;
  }

  uint64_t get_dataloc(uint64_t index){
    return data_offset+entrysize * index;
  }
  void find_path(uint64_t start_pos,vector<uint64_t>& pos_vec){
    uint64_t depth=0;
    uint64_t kick_pos=start_pos;
    while(depth<MAX_TRY){
      RdmaArrayNode * node =&header[kick_pos];
      uint64_t kick_key=node->key;
      pos_vec.push_back(kick_pos);

      uint64_t p[3];
      p[0]=GetHash(kick_key);
      p[1]=GetHash2(kick_key);
      p[2]=GetHash3(kick_key);
      depth++;
      for(uint64_t slot=0;slot<3;slot++){
	if(p[slot]==(kick_pos/SLOT_PER_BUCKET)){
	  p[slot]=p[2];
	  continue;
	}
	for(uint64_t i=0;i<SLOT_PER_BUCKET;i++){
	  node =&header[p[slot]*SLOT_PER_BUCKET+i];
	  if(node->valid==false){
	    // find a empty slot
	    pos_vec.push_back(p[slot]*SLOT_PER_BUCKET+i);
	    return ;
	  }
	}
      }
      kick_pos=p[rand()%2]*SLOT_PER_BUCKET+ rand()%SLOT_PER_BUCKET;
    }
    pos_vec.clear();
    //        assert(false);
  }

  void Insert(uint64_t key, void* val) {
    uint64_t p[3];
    p[0]=GetHash(key);
    p[1]=GetHash2(key);
    p[2]=GetHash3(key);
    for(uint64_t slot=0;slot<3;slot++){
      for(uint64_t i=0;i<SLOT_PER_BUCKET;i++){
	RdmaArrayNode * node = &header[p[slot]*SLOT_PER_BUCKET+i];
	if(node->valid==false){
	  node->valid=true;
	  node->key=key;
	  node->index=free_ptr;
	  memcpy((void*)(array+get_dataloc(free_ptr)),val,entrysize);
	  free_ptr ++;
	  return ;
	}
      }
    }

    //// didn't find empty slot at first
    uint64_t kick_pos=(p[rand()%3])*SLOT_PER_BUCKET+ rand()%SLOT_PER_BUCKET;
    vector<uint64_t> pos_vec;
    find_path(kick_pos,pos_vec);
    if(pos_vec.size()==0){
      printf("fail when inserting %d\n", key);
      assert(false);
    }
    uint64_t pointer=pos_vec.size()-1;
    RdmaArrayNode * node = &header[pos_vec[pointer]];
    assert(node->valid==false);
    node->valid=true;
    while(pointer>0){
      RdmaArrayNode * prev =&header[pos_vec[pointer-1]];
      node->key=prev->key;
      node->index=prev->index;
      //            memcpy((void*)(node+1),(void*)(prev+1),entrysize);
      node=prev;
      pointer--;
    }
    node->key=key;
    node->index=free_ptr;
    memcpy((void*)(array+get_dataloc(free_ptr)),val,entrysize);
    free_ptr ++;
    return;
  }


  uint64_t* Get(uint64_t key) {
    uint64_t p[3];
    p[0]=GetHash(key);
    p[1]=GetHash2(key);
    p[2]=GetHash3(key);
    for(uint64_t slot=0;slot<3;slot++){
      for(uint64_t i=0;i<SLOT_PER_BUCKET;i++){
	char* bucket_addr= array+p[slot]*bucketsize;
	RdmaArrayNode * node =&header[p[slot]*SLOT_PER_BUCKET+i];
	if(node->valid==true && node->key==key){
	  return (uint64_t*)(array+get_dataloc(node->index));
	}
      }
    }
    assert(false);
    return NULL;
  }

  uint64_t read(uint64_t key) {
    uint64_t p[3];
    p[0]=GetHash(key);
    p[1]=GetHash2(key);
    p[2]=GetHash3(key);
    for(uint64_t slot=0;slot<3;slot++){
      for(uint64_t i=0;i<SLOT_PER_BUCKET;i++){
	char* bucket_addr= array+p[slot]*bucketsize;
	RdmaArrayNode * node =&header[p[slot]*SLOT_PER_BUCKET+i];
	if(node->valid==true && node->key==key){
	  return (slot+1);
	}
      }
    }
    printf("%d fail\n",key );
    assert(false);
    return NULL;
  }

  void* Delete(uint64_t key) {
    // TODO
    return NULL;
  }
};

#endif
