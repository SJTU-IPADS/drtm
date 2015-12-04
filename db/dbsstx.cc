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
 *  Transactional layer of DrTM - XingDa
 *  TimeStamp  methods          - Yanzhe 
 *
 */

#include <string>
#include "dbsstx.h"
#include <utility>

#include <iostream>

namespace drtm {


  bool EXPIRED(uint64_t lease_time) {
    if (lease_time == 0)
      return true;
    else if (lease_time == UINT_MAX)
      return false;
    else
      return timestamp > lease_time + DELTA;
  }


  bool VALID(uint64_t lease_time) {
    if (lease_time == 0)
      return false;
    else if (lease_time == UINT_MAX)
      return true;
    else
      return timestamp < lease_time - DELTA;
  }

  bool _rw_item_cmp(DBSSTX::rwset_item a,DBSSTX::rwset_item b ) {
    if(a.tableid < b.tableid) {
      return true;
    }else if(a.tableid == b.tableid)
      return a.key < b.key;
    return false;
  }

  DBSSTX::DBSSTX(RAWTables* store,RdmaResource *r,int t_id)
  {
    txdb_ = store;
    rdma = r;
    thread_id = t_id;
    /*
    cache_miss=0;
    cache_hit=0;
    remote_ops=0;
    rdma_travel=0;
    rdma_read=0;
    remote_cas=0;
    local_cas=0;*/
    rw_set.reserve(15);
    lastsn = txdb_->ssman_->GetLocalSS();
  }

  DBSSTX::DBSSTX(RAWTables* store)
  {
    txdb_ = store;
    lastsn = txdb_->ssman_->GetLocalSS();
  }



  DBSSTX::~DBSSTX()

  {
    //TODO
  }

  void DBSSTX::SSReadLock(){
    //txdb_->rwLock->StartRead();
    pthread_rwlock_rdlock(txdb_->rwLock);
  }

  void DBSSTX::SSReadUnLock(){
    //txdb_->rwLock->EndRead();
    pthread_rwlock_unlock(txdb_->rwLock);
  }


  uint64_t DBSSTX::GetSS() {

    localsn = txdb_->ssman_->GetLocalSS();
    return localsn;
  }

  uint64_t DBSSTX::GetMySS() {
    return txdb_->ssman_->GetMySS();
  }


  void DBSSTX::GetSSDelayP() {
    register uint64_t dqsn = txdb_->GetMinDDelaySN();
    register uint64_t managersn = txdb_->ssman_->GetLocalSS();
    if (managersn < dqsn ) localsn = managersn;
    else localsn = dqsn;
  }

  void DBSSTX::GetSSDelayN() {
    register uint64_t dqsn = txdb_->GetMinNDelaySN();
    register uint64_t managersn = txdb_->ssman_->GetLocalSS();
    if (managersn < dqsn ) localsn = managersn;
    else localsn = dqsn;
  }


  void DBSSTX::UpdateLocalSS() {
    txdb_->ssman_->UpdateLocalSS(localsn);
  }




  void DBSSTX::Begin(bool ro)
  {
    readonly = ro;
    emptySSLen = 0;
    localsn = -1;
  }

  bool DBSSTX::Abort()
  {
    return false;
  }

  bool DBSSTX::End()
  {
    return true;
  }




  void DBSSTX::Add(int tableid, uint64_t key, uint64_t* val)
  {
    register int v_len = txdb_->schemas[tableid].vlen;
    char* value = new char[META_LENGTH+v_len];

    memcpy(value+VALUE_OFFSET, val, v_len);
    assert(localsn != -1);
    *(uint64_t *)value = localsn;
    *(uint64_t *)((uint64_t)value+TIME_OFFSET) = 0;
    txdb_->Put(tableid, key, (uint64_t *)value);
  }


  void DBSSTX::Delete(int tableid, uint64_t key)
  {
    txdb_->Delete(tableid,key);
  }


  bool DBSSTX::GetLoc(int tableid, uint64_t key, uint64_t** val){
    uint64_t* value = txdb_->Get(tableid, key);
    if (value == NULL)
      return false;
    else {
      *val = value;
      return true;
    }
  }

  uint64_t DBSSTX::GetLocalLoc(int tableid,uint64_t key) {
    rwset_item item;
    item.tableid = tableid;
    item.key = key;
    item.pid = current_partition;

    bool hit=false;
    if(!hit){
#if USING_CHAIN_HASH
      chain_travel(item);
#elif USING_HASH_EXT
      hashext_travel(item);
#else
      cuckoo_travel(item);
#endif
    }
    return item.loc;
  }

  uint64_t DBSSTX::GetRdmaLoc(int tableid,uint64_t key,int pid) {
    rwset_item item;
    item.tableid = tableid;
    item.key = key;
    item.pid = pid;

    bool hit=false;
#if USING_CACHE
    if(item.pid != current_partition){
      uint64_t * loc_cache_prt=txdb_->rdmaremotecache[item.tableid]->Concurrent_get(item.key);
      if(loc_cache_prt!=NULL){
        item.loc = *loc_cache_prt;
	//        cache_hit++;
        hit=true;
      }
    }
#endif
    if(!hit){
#if USING_CHAIN_HASH
      chain_travel(item);
#elif USING_HASH_EXT
      hashext_travel(item);
#else
      cuckoo_travel(item);
#endif
    }

#if USING_CACHE

    if(hit && item.pid != current_partition){
      txdb_->rdmaremotecache[item.tableid]->Concurrent_insert(item.key,&item.loc);
      //      cache_miss++;
    }
#endif
    return item.loc;
  }

  void DBSSTX::AddToLocalReadOnlySet(int _tableid,uint64_t _key,uint64_t *loc) {
    rwset_item item;
    item.tableid = _tableid;
    item.key = _key;
    item.pid = current_partition;
    item.ro = true;

    readonly_set.push_back(item);
  }

  void DBSSTX::AddToRemoteReadSet(int _tableid, uint64_t _key, int _pid, uint64_t *addr) {
    rwset_item item;
    item.tableid = _tableid;
    item.key = _key;
    item.pid = _pid;
    item.addr= addr;
    item.ro = true;

    rw_set.push_back(item);
  }

  void DBSSTX::AddToLocalReadSet(int _tableid, uint64_t _key, int _pid, uint64_t *addr) {
    assert(_pid == current_partition);
    rwset_item item;
    item.tableid = _tableid;
    item.key = _key;
    item.pid = _pid;
    item.addr= addr;
    item.ro = true;

    rw_set.push_back(item);
  }

  void DBSSTX::AddToRemoteWriteSet(int _tableid,uint64_t _key,int _pid,uint64_t *addr){
    //      remote_ops++;

    rwset_item item;
    item.tableid = _tableid;
    item.key = _key;
    item.pid = _pid;
    item.addr= addr;
    item.ro = false;

    rw_set.push_back(item);
  }

  void DBSSTX::AddToLocalWriteSet(int _tableid,uint64_t _key,int _pid,uint64_t *addr){
    assert(_pid == current_partition);
    rwset_item item;
    item.tableid = _tableid;
    item.key = _key;
    item.pid = _pid;
    item.addr= addr;
    item.ro = false;

    rw_set.push_back(item);
  }

  void DBSSTX::chain_travel(rwset_item &item) {
    if(item.pid == current_partition){ // local travel
      uint64_t index;
      uint64_t loc;
      uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id);
      index = txdb_->rdmachainhash[item.tableid]->GetHash(item.key);
      while(true){
        loc = index * txdb_->rdmachainhash[item.tableid]->slotsize;
        RdmaChainHash::RdmaArrayNode* node=(RdmaChainHash::RdmaArrayNode*)(txdb_->rdmachainhash[item.tableid]->array + loc);
        uint64_t tmpkey=node->key;
        if(node->key==item.key){
          loc+=sizeof(RdmaChainHash::RdmaArrayNode);
          break;
        } else if(node->next == 0){
          assert(false);
        } else {
          index = node->next;
        }
      }
      item.loc=loc + txdb_->rdma_off_mapping[item.tableid];
    } else {
      uint64_t index;
      uint64_t loc;
      uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id);
      index = txdb_->rdmachainhash[item.tableid]->GetHash(item.key);
      while(true){
	loc = index * txdb_->rdmachainhash[item.tableid]->slotsize + txdb_->rdma_off_mapping[item.tableid] ;
        rdma->RdmaRead(thread_id,item.pid,(char *)local_buffer,sizeof(RdmaChainHash::RdmaArrayNode),loc  );

	//        rdma_travel++;
        RdmaChainHash::RdmaArrayNode* node =  (RdmaChainHash::RdmaArrayNode *) local_buffer;
        if(node->key==item.key){
          loc += sizeof(RdmaChainHash::RdmaArrayNode);
          break;
        } else if(node->next==0){
          assert(false);
        } else {
          index = node->next;
        }
      }
      item.loc=loc;
    }
  }


  void DBSSTX::hashext_travel(rwset_item &item) {
    if(item.pid == current_partition){ // local travel
      uint64_t index;
      index = txdb_->rdmahashext[item.tableid]->GetHash(item.key);
      uint64_t loc = txdb_->rdmahashext[item.tableid]->getHeaderNode_loc(index);
      RdmaHashExt::HeaderNode* node=(RdmaHashExt::HeaderNode*)(txdb_->rdmahashext[item.tableid]->array + loc);
      while(true){
	for(int i=0;i<CLUSTER_H;i++){
          if(node->keys[i]==item.key && node->indexes[i]!=0){
            loc = txdb_->rdmahashext[item.tableid]->getDataNode_loc(node->indexes[i]);
            loc+=sizeof(RdmaHashExt::DataNode);
            item.loc=loc+ txdb_->rdma_off_mapping[item.tableid];
            return ;
          }
	}
	if(node->next !=NULL){
	  loc = txdb_->rdmahashext[item.tableid]->getHeaderNode_loc(node->next);
	  node = (RdmaHashExt::HeaderNode *) (txdb_->rdmahashext[item.tableid]->array + loc);
	}
	else{
	  assert(false);
	}
      }
    } else {
      uint64_t index;
      uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id);
      index = txdb_->rdmahashext[item.tableid]->GetHash(item.key);
      uint64_t loc = txdb_->rdmahashext[item.tableid]->getHeaderNode_loc(index)+txdb_->rdma_off_mapping[item.tableid];
      rdma->RdmaRead(thread_id,item.pid,(char *)local_buffer,sizeof(RdmaHashExt::HeaderNode),loc);
      RdmaHashExt::HeaderNode* node= (RdmaHashExt::HeaderNode*)local_buffer;
      while(true){
	for(int i=0;i<CLUSTER_H;i++){
          if(node->keys[i]==item.key && node->indexes[i]!=0){
            loc = txdb_->rdmahashext[item.tableid]->getDataNode_loc(node->indexes[i]);
            loc+= sizeof(RdmaHashExt::DataNode);
            item.loc=loc+ txdb_->rdma_off_mapping[item.tableid];
            return ;
          }
	}
	if(node->next !=NULL){
	  loc = txdb_->rdmahashext[item.tableid]->getHeaderNode_loc(node->next) + txdb_->rdma_off_mapping[item.tableid];
	  rdma->RdmaRead(thread_id,item.pid,(char *)local_buffer,sizeof(RdmaHashExt::HeaderNode),loc);
	  node= (RdmaHashExt::HeaderNode*)local_buffer;
	}
	else{
	  fprintf(stderr,"acct: %lld err @ %d\n",item.key,item.pid);
	  assert(false);
	  exit(-1);
	}
      }
    }
  }

  void DBSSTX::cuckoo_travel(rwset_item &item) {
    if(item.pid == current_partition){ // local travel

      uint64_t index[2];
      uint64_t loc;
      uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id);
      index[0] = txdb_->rdmacuckoohash[item.tableid]->GetHash(item.key);
      index[1] = txdb_->rdmacuckoohash[item.tableid]->GetHash2(item.key);
      bool success=false;
      for(int i=0;i<2;i++){
	for(int j=0;j<SLOT_PER_BUCKET;j++){
	  RdmaCuckooHash::RdmaArrayNode* node = (txdb_->rdmacuckoohash[item.tableid]->header +  index[i]*SLOT_PER_BUCKET + j);
	  if(node->key==item.key){
	    loc= txdb_->rdmacuckoohash[item.tableid]->get_dataloc(node->index);
	    success = true;
	    break;
	  }
	}
	if(success)
	  break;
      }
      assert(success);
      item.loc=loc+ txdb_->rdma_off_mapping[item.tableid] ;
    } else {
      uint64_t index[2];
      uint64_t loc;
      uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id);
      index[0] = txdb_->rdmacuckoohash[item.tableid]->GetHash(item.key);
      index[1] = txdb_->rdmacuckoohash[item.tableid]->GetHash2(item.key);
      bool success=false;
      for(int i=0;i<2;i++){
	uint64_t read_length=txdb_->rdmacuckoohash[item.tableid]->bucketsize;
	loc = index[i] * read_length  + txdb_->rdma_off_mapping[item.tableid] ;
	rdma->RdmaRead(thread_id,item.pid,(char *)local_buffer,read_length,loc);
	//      rdma_travel++;
	for(int j=0;j<SLOT_PER_BUCKET;j++){
	  RdmaCuckooHash::RdmaArrayNode* node =  ((RdmaCuckooHash::RdmaArrayNode *) local_buffer) +j;
	  if(node->key==item.key){
          loc  = txdb_->rdmacuckoohash[item.tableid]->get_dataloc(node->index);
          loc += txdb_->rdma_off_mapping[item.tableid];
          success=true;
          break;
	  }
	}
	if(success)
	  break;
      }
      assert(success);
      item.loc=loc;
    }
  }

  void DBSSTX::ReleaseAllLocal(){

    for(int i = 0;i < rw_set.size();++i){
      if(!rw_set[i].ro && rw_set[i].pid==current_partition){
	Release(rw_set[i]);
      }
    }

  }

  void DBSSTX::RdmaFetchAdd ( uint64_t off,int pid,uint64_t value) {
    uint64_t *local_buffer = (uint64_t*)rdma->GetMsgAddr(thread_id,2);
    rdma->RdmaFetchAdd(thread_id,pid,(char *)local_buffer,value,off);
  }

  bool DBSSTX::AllLeasesAreValid() {
      for (int i = 0; i < rw_set.size(); ++i) {
          if (rw_set[i].ro && rw_set[i].pid != current_partition) {
              uint64_t *value = rw_set[i].addr;
              uint64_t lease = *(uint64_t *)((uint64_t)value + TIME_OFFSET);
              if (!VALID(lease))
		return false;
          }
      }
      return true;
  }

  bool DBSSTX::AllLocalLeasesValid() {
    for (int i = 0;i < readonly_set.size();++i) {
      assert(readonly_set[i].ro);
      uint64_t *value = readonly_set[i].addr;
      uint64_t lease = *(uint64_t *)((uint64_t)value + TIME_OFFSET);

      //      fprintf(stderr,"t: %d key: %lld lease %x\n",readonly_set[i].tableid,readonly_set[i].key,lease);

      if (!VALID(lease)) {
	return false;
      }

    }
    return true;
  }

  void DBSSTX::PrefetchAllRemote(uint64_t endtime) {
      sort(rw_set.begin(), rw_set.end(), _rw_item_cmp);
      for (int i = 0; i < rw_set.size(); ++i) {
	if (!rw_set[i].ro && rw_set[i].pid!=current_partition)
	  Lock(rw_set[i]);
	else if (rw_set[i].ro && rw_set[i].pid != current_partition)
	  GetLease(rw_set[i], endtime);
      }
  }

  void DBSSTX::GetLocalLease(int tableid,uint64_t key,uint64_t *loc, uint64_t endtime) {
    rwset_item item;

    item.pid = current_partition;
    item.key = key;
    item.tableid = tableid;
    item.ro = true;
    item.addr = loc;

    GetLease(item, endtime);
    readonly_set.push_back(item);
  }


  void DBSSTX::GetLease(rwset_item &item, uint64_t endtime) {

    int pid = item.pid;
    int tableid = item.tableid;
    uint64_t key = item.key;

    bool hit=false;
#if USING_CACHE
    if(item.pid != current_partition){
      uint64_t * loc_cache_prt=txdb_->rdmaremotecache[item.tableid]->Concurrent_get(item.key);
      if(loc_cache_prt!=NULL){
        item.loc = *loc_cache_prt;
	//        cache_hit++;
        hit=true;
      }
    }
#endif
    if(!hit){
#if USING_CHAIN_HASH
      chain_travel(item);
#elif USING_HASH_EXT
      hashext_travel(item);
#else
      cuckoo_travel(item);
#endif
    }

#if USING_CACHE

    if(hit && item.pid != current_partition){
      txdb_->rdmaremotecache[item.tableid]->Concurrent_insert(item.key,&item.loc);
      //      cache_miss++;
    }
#endif

    uint64_t index;
    uint64_t loc = item.loc;
    uint64_t *local_buffer = (uint64_t *) rdma->GetMsgAddr(thread_id);

    int count = 0;
    uint64_t init_flag = 0;
    while (1) {
      count++;
      if (count == 50000) {
	printf("GetLease %d, %d %lld\n", thread_id, key, *local_buffer);
      }
      if (count == 500000) {
	//to avoid deads
	exit(0);
      }
      rdma->RdmaCmpSwap(thread_id,pid,(char *)local_buffer, init_flag, endtime,sizeof(uint64_t),loc + TIME_OFFSET);
      uint64_t ret_flag = *local_buffer;
      if (ret_flag == init_flag) {
	// successfully get the lease
	break;
      } else if ((ret_flag >> 63) & 0x1) {
	// retry
      } else {
	if (VALID(ret_flag)) {
	  break;
	} else {
	  init_flag = ret_flag;
	}
      }
    }

    if (pid != current_partition) {
      int length = txdb_->schemas[item.tableid].vlen + VALUE_OFFSET;
      rdma->RdmaRead(thread_id,pid,(char *)local_buffer, length, loc);
      //          rdma_read++;
      memcpy((char *)item.addr,(char *)local_buffer,length);
    }
    return;
  }

  void DBSSTX::Lock(rwset_item &item) {
    int pid = item.pid;
    int tableid = item.tableid;
    uint64_t key = item.key;


    bool hit=false;
#if USING_CACHE
    if(item.pid != current_partition){
      uint64_t * loc_cache_prt=txdb_->rdmaremotecache[item.tableid]->Concurrent_get(item.key);
      if(loc_cache_prt!=NULL){
        item.loc = *loc_cache_prt;
	//        cache_hit++;
        hit=true;
      }
    }
#endif
    if(!hit){
#if USING_CHAIN_HASH
    chain_travel(item);
#elif USING_HASH_EXT
    hashext_travel(item);
#else
    cuckoo_travel(item);
#endif
    }

#if USING_CACHE

    if(hit && item.pid != current_partition){
      txdb_->rdmaremotecache[item.tableid]->Concurrent_insert(item.key,&item.loc);
      //      cache_miss++;
    }
#endif

    uint64_t index;
    uint64_t loc  = item.loc;
    uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id);
    int ret;

    int count = 0;
    uint64_t init_flag = 0;
    while(1){
      count++;
      if(count==50000){
	//magic number to avoid dead loop
        printf("Lock: %d,%d %lld, %llu %d\n",thread_id,key,*local_buffer, init_flag,pid);
        //PrintAllLocks();
      }
      if(count==500000){
        exit(0);
      }
      int ret;
      ret = rdma->RdmaCmpSwap(thread_id,pid,(char *)local_buffer, init_flag, (1UL << 63),sizeof(uint64_t),loc + TIME_OFFSET);
      assert(ret == 0);
      /*
      if(pid != current_partition)
        remote_cas++;
      else
        local_cas++;
      */
      uint64_t ret_flag = *local_buffer;
      if (ret_flag == init_flag) {
          // successfully get the lock
          break;
      } else if ((ret_flag >> 63) & 0x1) {
          // someone is holding the write lock
          // suppose to abort in protocol
          // spin here
      } else {
          // it is a read lease
          if (VALID(ret_flag)) {
              // retry
          } else {
              init_flag = ret_flag;
          }
      }
    }

    //if remote,read the value
    if(pid != current_partition) {
      int length = txdb_->schemas[item.tableid].vlen;
      ret = rdma->RdmaRead(thread_id,pid,(char *)local_buffer + VALUE_OFFSET,length,loc + VALUE_OFFSET);
      assert(ret == 0);
      //      rdma_read++;
      //!!Donot write the meta data of the item!this is important
      memcpy((char *)item.addr + VALUE_OFFSET,(char *)local_buffer + VALUE_OFFSET,length);
    }
    return;
  }

  void DBSSTX::Release(rwset_item &item,bool flag) {

    int tableid  = item.tableid;
    int pid      = item.pid;
    uint64_t key = item.key;
    int ret;

    //    uint64_t loc = RdmaArray::GetHash(key) * txdb_->rdmastore->slotsize + sizeof(RdmaArray::RdmaArrayNode);
    uint64_t loc = item.loc;
    uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id,0);


    if(flag && pid != current_partition) {
      //write back
      int length = txdb_->schemas[item.tableid].vlen;
      memcpy((char *)local_buffer + VALUE_OFFSET,(char *)item.addr + VALUE_OFFSET,length);
      //      assert(rdma->RdmaWrite(thread_id,pid,(char *)local_buffer + VALUE_OFFSET,length,loc + VALUE_OFFSET) == 0);
      uint64_t *lock_buffer = (uint64_t *)rdma->GetMsgAddr(thread_id,1);
      normal_op_req reqs[2];
      reqs[0].opcode = IBV_WR_RDMA_WRITE;
      reqs[0].local_buf = (char *)local_buffer + VALUE_OFFSET;
      reqs[0].size    = length;
      reqs[0].remote_offset = loc + VALUE_OFFSET;

      reqs[1].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
      reqs[1].local_buf = (char *)lock_buffer;
      reqs[1].size = sizeof(uint64_t);
      reqs[1].remote_offset = loc + TIME_OFFSET;
      reqs[1].compare_and_add = (1UL << 63);
      reqs[1].swap = 0;

      ret = rdma->RdmaOps(thread_id,pid,reqs,2);
      assert(ret == 0);

    }else {
      //release lock
      ret = rdma->RdmaCmpSwap(thread_id,pid,(char *)local_buffer,(1UL << 63),0,sizeof(uint64_t),loc + TIME_OFFSET);
      assert(ret == 0);
      if(*local_buffer != (1UL << 63))
	fprintf(stderr,"key: %lld val: %ulld\n",key,*local_buffer);
      assert(*local_buffer == (1UL << 63));
    }
    return;
  }

  void DBSSTX::ReleaseAllRemote() {
    for(int i = 0;i < rw_set.size();++i){
      if(!rw_set[i].ro && rw_set[i].pid!=current_partition)
              Release(rw_set[i],release_flag);
    }
  }

  void DBSSTX::RemoteWriteBack() {

    this->release_flag = true;//TODO ,maybe need refine some code
    this->ReleaseAllRemote();
    this->release_flag = false;
    this->ClearRwset();

  }

  void DBSSTX::Fallback_LockAll(SpinLock** sl, int numOfLocks, uint64_t endtime) {
    // release all locks
    int i = 0, j = 0;

    ReleaseAllRemote();

    int count = 0;

    while (true) {

      if(!VALID(endtime))
	endtime = timestamp + DEFAULT_INTERVAL;
      count++;
      if (count == 500000) {
	printf("Fallback GetLease\n");
	assert(false);
	exit(0);
      }

      // reacquire the read lease if necessary
      // record the oldest lease at the same time
      uint64_t oldest_lease = UINT_MAX;
      for (int i = 0; i < rw_set.size(); ++i) {
	if (rw_set[i].ro) {
	  uint64_t *value = rw_set[i].addr;
	  uint64_t lease = *(uint64_t *)((uint64_t)value + TIME_OFFSET);
	  if (!VALID(lease)) {
	    GetLease(rw_set[i], endtime);
	    lease = *(uint64_t *)((uint64_t)value + TIME_OFFSET);
	  }
	  if (lease < oldest_lease) oldest_lease = lease;
	} else {
	  Lock(rw_set[i]);
	}
      }

      // recheck all the leases are valid
      if (VALID(oldest_lease))
	break;
      else {
	for (int i = 0; i < rw_set.size(); ++i) {
	  if (!rw_set[i].ro)
	    Release(rw_set[i],release_flag);
	}
      }

    }

    // lock local global locks
    if(sl != NULL) {
      for(int i = 0;i < numOfLocks;++i) {
	sl[i]->Lock();
      }
    }
  }

  void DBSSTX::ClearRwset(){
    rw_set.clear();
  }

  void DBSSTX::LocalLockSpin(char *loc) {
    while(!__sync_bool_compare_and_swap((uint64_t *)(loc + TIME_OFFSET),0,1UL<<63)) {
    }
  }

  void DBSSTX::LocalReleaseSpin(char *loc) {
    __sync_bool_compare_and_swap((uint64_t *)(loc + TIME_OFFSET),1UL << 63,0);
  }

  bool DBSSTX::Get(int tableid, uint64_t key, uint64_t** val, bool update ,int *_status) {
    return GetAt(tableid, key, val, update, NULL, _status);
  }


  bool DBSSTX::GetRemote(int tableid,uint64_t key,std::string *,Network_Node *node,int _pid,uint64_t timestamp) {
    //TODO
    return false;
  }

  bool DBSSTX::LockLocal(rwset_item item) {

    uint64_t *value;
    if(item.addr == NULL) {
      if(!GetLoc(item.tableid,item.key,&value) )
	assert(false);

    }else
      value = item.addr;

    uint64_t origin = 0UL << 63;
    uint64_t target = 1 + current_partition;

    return __sync_bool_compare_and_swap( (uint64_t *)((uint64_t)value + TIME_OFFSET),origin,target);
  }

  bool DBSSTX::ReleaseLocal(rwset_item item) {
    uint64_t *value;
    if(item.addr != NULL)
      value = item.addr;
    else
      assert(GetLoc(item.tableid,item.key,&value));

    uint64_t origin = 1 + current_partition;
    uint64_t target = 0UL << 63;

    //!!TODO assume that loc will be the original offset
    if(__sync_bool_compare_and_swap( (uint64_t *)((uint64_t)value + TIME_OFFSET),origin,target) ) {
      return true;
    }else {
      assert(false);
    }
    return false;
  }

  bool DBSSTX::ReleaseLockRemote(rwset_item item,Network_Node *node) {

    msg_struct unlock_msg;
    unlock_msg.tableid = item.tableid;
    unlock_msg.key = item.key;
    unlock_msg.optype=RELEASE_REQ;

    std::string temp((char*)(&unlock_msg),sizeof(unlock_msg));

    int length = ((item.addr == NULL)? 0:(txdb_->schemas[item.tableid].vlen + META_LENGTH));
    if(length)
      temp+=std::string((char *)item.addr,length);

    node->Send(item.pid,nthreads,temp);
    temp = node->Recv().substr(2);

    if(temp[0] == 0) {
      assert(false) ;//!!must release a held lock!
      return false;
    }

    return true;
  }

  std::string DBSSTX::HandleMsg(std::string &req){

    assert(req.size() >= sizeof(msg_struct));

    std::string res;
    const char *ptr = req.c_str();
    msg_struct msg;
    memcpy(&msg,ptr,sizeof(msg_struct));


    char *res_buf = NULL;
    /*
    uint64_t *loc;
    if(!GetLoc(msg.tableid,msg.key,&loc))
      assert(false);//should not happen!
    */
    //    int length  = txdb_->schemas[msg.tableid].vlen + META_LENGTH;

    if ( msg.optype == WRITE_REQ) {
      //now we only handle the write requests
      /*
      res_buf = new char[1 + length];

      uint64_t origin = 0UL << 63;
      uint64_t target = 1UL << 63;

      //!!TODO assume that loc will be the original offset
      //bug?
      if(__sync_bool_compare_and_swap( (uint64_t *)((uint64_t)loc + TIME_OFFSET),origin,target) ) {
		res_buf[0] = 1;
		memcpy(res_buf + 1,(char *)((uint64_t)loc),length);
      }else {
		res_buf[0] = 0;
      }
      res = std::string(res_buf,length + 1);
      delete res_buf;
      //endif
      */

    }else if(msg.optype == RELEASE_REQ) {
      /*
      //write back the message
      if(req.size() == 64)
		memcpy((char *)((uint64_t)loc + VALUE_OFFSET),ptr + 64 + VALUE_OFFSET,length - META_LENGTH);//idle release

      	uint64_t origin = 1UL << 63;
      	uint64_t target = 0UL << 63;

      //!!TODO assume that loc will be the original offset
      if(__sync_bool_compare_and_swap( (uint64_t *)((uint64_t)loc + TIME_OFFSET),origin,target) ) {
		char r_c = 1;
		res = std::string(&r_c,1);
      }else {
		assert(false);
		} */
      //end else handle release req
    }
    return res;
  }

  //status is used for detect lock and read lease
  bool DBSSTX::GetAt(int tableid, uint64_t key, uint64_t** val, bool update, uint64_t *loc,int *_status)
  {

    uint64_t* value;
    if (loc != NULL)
      value = loc;
    else {
      value = txdb_->Get(tableid, key);
      if (value == NULL) {
      return false;
      }
    }

    // now we check the data flag
    uint64_t flag = *(uint64_t *)((uint64_t)value + TIME_OFFSET);
    if ((flag >> 63) & 0x1) {
      if(_status) *_status = LOCK;
    } else {
      if (VALID(flag)) {
	if (_status) *_status = READ_LEASE;
      } else if (EXPIRED(flag)) {
	if (_status) *_status = IS_EXPIRED;
      } else {
	if (_status) *_status = NONE;
      }
    }

    uint64_t * res = NULL;
    if(update) {
      res = (uint64_t *)((uint64_t)value + VALUE_OFFSET);

    }else {
      //	  register char deleted  = *(char *)((uint64_t)value+DEL_OFFSET);
      //	  if (deleted) return false;

      *val = (uint64_t *)((uint64_t)value + VALUE_OFFSET);
      return true;
    }
    if(!res)
      return false;
    *val = res;
    return true;
  }

  void DBSSTX::Redo(uint64_t *value, int32_t delta, bool after) {
    uint64_t* last = NULL;
    //fix me:hard-coding
    int length = txdb_->schemas[DIST].versioned_len + 17;
    char *dummy = new char[length];
    // RAWRTMTX::Begin();
    RTMScope rtm(NULL);
    uint64_t *node = (uint64_t *)((uint64_t)value - 17 - 4); //size of int32_t

    while (node !=NULL && *node >= localsn) {
      *(int32_t *)((uint64_t)node + 17) = delta + *(int32_t *)((uint64_t)node + 17);
      last = node;
      node = *(uint64_t **)((uint64_t)node+8);
    }

    if (last == NULL) {
      memcpy(dummy,node,length);
      *(uint64_t *)node = localsn;
      *(uint64_t **)((uint64_t)node+8) = (uint64_t *)dummy;
      *(int32_t *)((uint64_t)node + 17) = delta + *(int32_t *)((uint64_t)dummy+ 17);
    }

    else if (*last > localsn) {
      memcpy(dummy, node, length);
      *(uint64_t *)dummy = localsn;
      *(uint64_t **)((uint64_t)dummy+8) = node;
      *(int32_t *)((uint64_t)dummy + 17) = delta + *(int32_t *)((uint64_t)node + 17);
      *(uint64_t **)((uint64_t)last+8)= (uint64_t *)dummy;
    }
    else delete dummy;
    // RAWRTMTX::End();

  }

  void DBSSTX::Redo(uint64_t *value, float delta, bool after) {

    uint64_t* last = NULL;
    //fix me:hard-coding
    int length = txdb_->schemas[CUST].versioned_len + 17;
    char *dummy = new char[length];
    // RAWRTMTX::Begin();
    RTMScope rtm(NULL);
    uint64_t *node = (uint64_t *)((uint64_t)value - 17 - sizeof(float));

    while (node !=NULL && *node >= localsn) {
      *(float *)((uint64_t)node + 17) = delta + *(float *)((uint64_t)node + 17);
      last = node;
      node = *(uint64_t **)((uint64_t)node+8);
    }


    if (last == NULL) {
      memcpy(dummy,node,length);
      *(uint64_t *)node = localsn;
      *(uint64_t **)((uint64_t)node+8) = (uint64_t *)dummy;
      *(float *)((uint64_t)node + 17) = delta + *(float *)((uint64_t)dummy+ 17);
    }

    else if (*last > localsn) {
      memcpy(dummy, node, length);
      *(uint64_t *)dummy = localsn;
      *(uint64_t **)((uint64_t)dummy+8) = node;
      *(float *)((uint64_t)dummy + 17) = delta + *(float *)((uint64_t)node + 17);
      *(uint64_t **)((uint64_t)last+8)= (uint64_t *)dummy;
    }
    else delete dummy;

    //  RAWRTMTX::End();
  }


  uint64_t* DBSSTX::check(int tableid ,uint64_t key ){
    uint64_t* value = txdb_->Get(tableid, key);
	printf("check\n");
	printf("ss %d\n", *value);
	//	while (value!=NULL) {
	//		printf("key %lx ss %ld v %d\n", key, *value, *(int32_t *)((uint64_t)value + 17));
	//		value = *(uint64_t **)((uint64_t)value+8);
	//	}
	printf("check End\n");
 	return NULL;

  }
  //  Memstore::MemNode* DBSSTX::GetNodeCopy(Memstore::MemNode* node)
  //  {

  //    return NULL;
  //  }



  DBSSTX::Iterator* DBSSTX::GetIterator(int tableid)
  {
    return new DBSSTX::Iterator(this, tableid, false);
  }


  RAWStore::Iterator *DBSSTX::GetRawIterator(int tableid)
  {
    return txdb_->GetIterator(tableid);
  }






  DBSSTX::Iterator::Iterator(DBSSTX* sstx, int tableid, bool update)
  {
    sstx_ = sstx;
    iter_ = sstx_->txdb_->GetIterator(tableid);
    tableid_ = tableid;
    copyupdate = update;
    versioned = sstx_->txdb_->schemas[tableid].versioned;
    val_ = NULL;
    key_ = 0;
  }



  inline uint64_t* DBSSTX::Iterator::GetWithSnapshot(uint64_t* node)
  {


    //XXX: we use RTM with a global fb lock to protect the operations on old version list
    uint64_t* next = node;


    int length;
    char *dummy;

    RTMScope rtm(NULL);
    if (sstx_->readonly) {
      while (next != NULL) {
	if (*next <= sstx_->localsn) {
	  //				register char deleted  = *(char *)((uint64_t)next+DEL_OFFSET);
	  //				if (deleted) return NULL;
	  return next;
	}
	//next = *(uint64_t **)((uint64_t)next+OLDV_OFFSEST);
      }
      return NULL;
    }

    //	register char deleted  = *(char *)((uint64_t)next+DEL_OFFSET);
    //	if (deleted) return NULL;



    if (copyupdate) {

      if (*next < sstx_->localsn) {
	length = sstx_->txdb_->schemas[tableid_].vlen + META_LENGTH;
	dummy = new char[length];
	memcpy(dummy, next, length);

	*next = sstx_->localsn;

	//set old version
	//			*(uint64_t **)((uint64_t)next+OLDV_OFFSEST) = (uint64_t *)dummy;

      }
    }

    return next;
  }




  bool DBSSTX::Iterator::Valid()
  {
    return val_ != NULL;
  }


  uint64_t DBSSTX::Iterator::Key()
  {
    return iter_->Key();
  }

  uint64_t* DBSSTX::Iterator::Value()
  {
    //	if (!sstx_->readonly)
    return (uint64_t *)((uint64_t)val_+VALUE_OFFSET);
  }

  void DBSSTX::Iterator::Next()
  {
    //	RTMScope rtm(NULL);
    //printf("Seek key %ld\n", iter_->Key());
    iter_->Next();
    //printf("key %ld\n", iter_->Key());
    //	if (iter_->Key() != 1650000017 && iter_->Key() != 3150000017) exit(0);
    while(iter_->Valid()) {


      register uint64_t* temp;
#ifdef _DB_RDMA
      temp =  iter_->Value();
#else
      temp = GetWithSnapshot(iter_->Value());
#endif
      if(temp != NULL ) {
	val_ = temp;
	//		if (key_ == iter_->Key()) printf("dbsstx %lx\n", key_);
	key_ = iter_->Key();
	return;
      }
      //	bool b = (key_ == 3150000016);
      iter_->Next();
      //	if (b && iter_->Key() == 3150000016) printf("yoy\n");
    }

    val_ = NULL;

  }

  bool DBSSTX::Iterator::Next(uint64_t bound)
  {
    iter_->Next();

    while(iter_->Valid()) {
      if (iter_->Key() >= bound) return false;

      register uint64_t* temp;
#ifdef _DB_RDMA
      temp = iter_->Value();
#else
      temp = GetWithSnapshot(iter_->Value());
#endif

      if(temp != NULL ) {
	val_ = temp;
	//	if (key_ == iter_->Key()) printf("dbsstx %lx\n", key_);
	key_ = iter_->Key();
	return true;
      }
      //	bool b = (key_ == 3150000016);
      iter_->Next();
      //	if (b && iter_->Key() == 3150000016) printf("yoy\n");
    }

    val_ = NULL;
    return true;
  }


  void DBSSTX::Iterator::Prev()
  {

    while(iter_->Valid()) {

      iter_->Prev();
      if (!iter_->Valid()) break;
      register uint64_t* temp;
#ifdef _DB_RDMA
      temp = iter_->Value();
#else
      temp = GetWithSnapshot(iter_->Value());
#endif

      if(temp != NULL ) {
	val_ = temp;
	return;
      }

    }
    val_ = NULL;
  }

  void DBSSTX::Iterator::Seek(uint64_t key)
  {
    iter_->Seek(key);

    while(iter_->Valid()) {
      register uint64_t* temp;

#ifdef _DB_RDMA
      temp = iter_->Value();
#else
      temp = GetWithSnapshot(iter_->Value());
#endif

      if(temp != NULL ) {
	key_ = iter_->Key();
	val_ = temp;
	return;
      }

      iter_->Next();
    }

    val_ = NULL;
  }

  // Position at the first entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  void DBSSTX::Iterator::SeekToFirst()
  {
    iter_->SeekToFirst();
    while(iter_->Valid()) {

      register uint64_t* temp;

#ifdef _DB_RDMA
      temp = iter_->Value();
#else
      temp = GetWithSnapshot(iter_->Value());
#endif

      if(temp != NULL ) {
	val_ = temp;
	return;
      }

      iter_->Next();
    }
    val_ = NULL;

  }

  // Position at the last entry in list.
  // Final state of iterator is Valid() iff list is not empty.
  void DBSSTX::Iterator::SeekToLast()
  {
    //TODO
    assert(0);
  }

  //end namespace
};
