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

/*
 *  Table definations   - Hao Qian
 *  Table definations   - XingDa 
 */


#ifndef RAWTABLES_H
#define RAWTABLES_H

#include <stdlib.h>

#include "raw_bplustree.h"
#include "rawstore.h"
#include "raw_uint64bplustree.h"
#include "db/snapshotmanage.h"

#include "util/rwlock.h"
#include "raw_hashtable.h"
#include "rdma_chainhash.h"
#include "rdma_hashext.h"
#include "rdma_cuckoohash.h"

#define NONE 0
#define BTREE 1
#define HASH 2
#define CUCKOO 6
#define SKIPLIST 3
#define IBTREE 4
#define SBTREE 5

//FIMME: Hand code data structure for different db tables
#define WARE 0
#define DIST 1
#define CUST 2
#define HIST 3
#define NEWO 4
#define ORDE 5
#define ORLI 6
#define ITEM 7
#define STOC 8
#define ORDER_INDEX 9
#define CUST_INDEX 10

#define USING_CHAIN_HASH 0
#define USING_HASH_EXT 1


#define USING_CACHE 1


//bench specifced options

#define ACCT 0
#define SAV  1
#define CHECK 2

#define ACCT_IDX 3

#define BENCH_TPCC  0
#define BENCH_BANK  1

extern size_t total_partition;
extern size_t current_partition;
extern size_t nthreads;

class RAWTables {

  struct TableSchema {

    bool versioned;
    int klen;
    int vlen; //We didn't support varible length of value

    int commu_len;
    int versioned_len;  //include commu_len
    int noversion_len;
  };


 public:
  SSManage* ssman_;
  //	DelayProcessor *dp_;
  BPlusTree btrees[ORDER_INDEX + 1];
  uint64_t rdma_off_mapping[ORDER_INDEX + 1];
  bool     rdma_table[ORDER_INDEX + 1];

  RawUint64BPlusTree cusIndex;
  TableSchema schemas[11];
  char padding1[64]; //The Spinlock has default padding
  volatile uint64_t DECounter1;
  char padding2[64];
  volatile uint64_t DECounter2;
  char padding3[64];
  SpinLock *payment_lock;
  SpinLock *bank_lock;
  //SpinLock payment_lock;
  //Counter for neworder transactions
  //char paddind3[64];
  SpinLock *neworder_lock;
  SpinLock *cache_lock;
  SpinLock neworder_lock1;
  volatile uint64_t NOCounter1;
  SpinLock neworder_lock2;
  //char paddind4[64];
  volatile uint64_t NOCounter2;
  char paddind[64];
  drtm::RawHashTable** ol_cache;
  //	drtm::RawHashTable** local_customer;

  //	DelayStore* NPendingStore;
  //	DelayStore* DPendingStore;

  //	DelayQueue** delayQueues;
  int thrnum;
  pthread_rwlock_t *rwLock;

  //  drtm::RdmaArray * rdmastore;
  char *start_rdma;
  char *end_rdma;
  uint64_t  rdma_size;
  //	drtm::RdmaChainHash *table_stock;
  drtm::RdmaChainHash  *rdmachainhash[ORDER_INDEX + 1];
  drtm::RdmaCuckooHash *rdmacuckoohash[ORDER_INDEX + 1];
  drtm::RdmaHashExt    *rdmahashext[ORDER_INDEX + 1];

  int rdmatablesize[ORDER_INDEX + 1];
  drtm::RdmaChainHash *rdmaremotecache[ORDER_INDEX + 1];

  RAWTables(int thrs,int bench = BENCH_TPCC){
    //init locks
    int lock_size = nthreads * total_partition;//TODO
    payment_lock  = new SpinLock[lock_size];
    neworder_lock = new SpinLock[lock_size];
    cache_lock    = new SpinLock[lock_size];
    bank_lock     = new SpinLock[1];

    NOCounter1 = 0;
    NOCounter2 = 0;
    DECounter1 = 0;
    DECounter2 = 0;

    InitSSManage(thrs);
    thrnum = thrs;
    //	  delayQueues= new DelayQueue*[thrs];
    //		local_customer = new drtm::RawHashTable*[thrs];
    /*
      for (int i =0; i<thrs; i++) {
      //local_customer[i] = new drtm::RawHashTable();
      delayQueues[i] = NULL;
      }*/
    //	  NPendingStore = new DelayStore();
    //	  DPendingStore = new DelayStore();
    ol_cache = new drtm::RawHashTable*[lock_size];

    for (int i=0;i < lock_size;i++)
      ol_cache[i] = new drtm::RawHashTable();

    //
    rdma_size = 1024 * 1024 * 1024;
    rdma_size = rdma_size * 4; //4G
    start_rdma = (char *)malloc(rdma_size);
    end_rdma = start_rdma;


    for(size_t i = 0;i <= ORDER_INDEX;++i)
      rdma_table[i] = false;

    switch(bench) {
    case BENCH_TPCC:
      fprintf(stdout,"init tpcc tables\n");
      rdma_table[WARE] = true;
      rdma_table[DIST] = true;
      rdma_table[STOC] = true;
      //FIXME hard coded
      rdmatablesize[STOC]=1024*1024*4;//1024*1024*8;
      rdmatablesize[WARE]=1024*10;
      rdmatablesize[DIST]=1024*100;

      break;
    case BENCH_BANK:
      fprintf(stdout,"init bank tables\n");
      rdma_table[CHECK] = true;
      rdma_table[SAV]   = true;

      rdmatablesize[SAV] = 100000 * 8 * 2;//todo,not hard coded
      rdmatablesize[CHECK] = 100000 * 8 * 2;
      break;
    default:
      fprintf(stderr,"unknown benchmarns\n");
      assert(false);
    }

  }

  ~RAWTables(){};

  void AddSchema(int tableid, int kl,
		 int commu_len, int versioned_len, int noversion_len, bool noInsert)
  {
    schemas[tableid].klen = kl;
    schemas[tableid].vlen = versioned_len + noversion_len;
    schemas[tableid].versioned = (versioned_len > 0);
    schemas[tableid].versioned_len = versioned_len;
    schemas[tableid].commu_len = commu_len;
    schemas[tableid].noversion_len = noversion_len;

    if(rdma_table[tableid]) {
      rdma_off_mapping[tableid] = end_rdma - start_rdma;
      rdma_off_mapping[tableid] = end_rdma - start_rdma;

#if USING_CHAIN_HASH
      rdmachainhash[tableid] = new drtm::RdmaChainHash(schemas[tableid].vlen + 64,rdmatablesize[tableid],end_rdma);
      end_rdma += rdmachainhash[tableid]->size;
#elif USING_HASH_EXT
      rdmahashext[tableid] = new drtm::RdmaHashExt(schemas[tableid].vlen + 64,rdmatablesize[tableid],end_rdma);
      end_rdma += rdmahashext[tableid]->size;
#else
      rdmacuckoohash[tableid] = new drtm::RdmaCuckooHash(schemas[tableid].vlen + 64,rdmatablesize[tableid],end_rdma);
      end_rdma += rdmacuckoohash[tableid]->size;
#endif
      char * cache=new char [sizeof(uint64_t)*4*rdmatablesize[tableid]];
      rdmaremotecache[tableid] = new drtm::RdmaChainHash(sizeof(uint64_t),rdmatablesize[tableid],cache);
    }

    if (tableid != CUST_INDEX) btrees[tableid].getWithRTM = !noInsert;
  }

  __attribute__((always_inline)) uint64_t* Get(int tabid, uint64_t key)
  {
    assert(tabid <= CUST_INDEX);

    //	asm __volatile__("lfence");
    if(rdma_table[tabid])
#if USING_CHAIN_HASH
      return rdmachainhash[tabid]->Get(key);
#elif USING_HASH_EXT
    return rdmahashext[tabid]->Get(key);
#else
    return rdmacuckoohash[tabid]->Get(key);
#endif

    if(tabid == CUST_INDEX)
      return cusIndex.Get(key);
    else
      return btrees[tabid].Get(key);
  }


  __attribute__((always_inline)) uint64_t* Delete(int tabid, uint64_t key)
  {
    assert(tabid <= CUST_INDEX);
    if(tabid == CUST_INDEX)
      return cusIndex.Delete(key);
    else
      return btrees[tabid].Delete(key);
  }

  inline void Put(int tabid, uint64_t key, uint64_t *value)
  {
    assert(tabid <= CUST_INDEX);

    if(rdma_table[tabid]){

#if USING_CHAIN_HASH
      rdmachainhash[tabid]->Insert(key,value);
#elif USING_HASH_EXT
      rdmahashext[tabid]->Insert(key,value);
#else
      rdmacuckoohash[tabid]->Insert(key,value);
#endif
      return ;
    }
    //			return rdmastore->Insert(key,value);

    if(tabid == CUST_INDEX)
      cusIndex.Put(key, value);
    else
      btrees[tabid].Put(key, value);
  }


  inline RAWStore::Iterator* GetIterator(int tabid)
  {

    assert(tabid <= CUST_INDEX);
    if(tabid == CUST_INDEX)
      return cusIndex.GetIterator();
    else
      return btrees[tabid].GetIterator();
  }

  void Sync(){}

  void InitSSManage(int thr_num)
  {
    rwLock = new pthread_rwlock_t();
    pthread_rwlock_init(rwLock, NULL);
    ssman_ = new SSManage(thr_num);
    ssman_->rwLock = rwLock;

    ssman_->rawtable = this;
    //	dp_ = new DelayProcessor(this);
  }

  void ThreadLocalInit(int tid){
    ssman_->RegisterThread(tid);

  }

  uint64_t GetMinDelayQueueSN() {
    return 0;
  }

  uint64_t GetMinDelaySN() {
    return 0;
  }


  uint64_t GetMinDDelaySN() {
    //		return DPendingStore->minSeq;
    return 0;
  }

  uint64_t GetMinNDelaySN() {
    //		return NPendingStore->minSeq;
    return 0;
  }
};


#endif
