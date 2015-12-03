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


#ifndef DRTM_DB_DBSSTX_H
#define DRTM_DB_DBSSTX_H

#include <string>
#include <limits.h>

#include "memstore/rawtables.h"
#include "db/network_node.h"
#include "memstore/rdma_resource.h"


#define VALUE_OFFSET 8
#define TIME_OFFSET  0
#define META_LENGTH  8


#define RELEASE_REQ 2
#define WRITE_REQ  1
#define READ_REQ   0

// flag status
#define NONE 0
#define LOCK 1
#define READ_LEASE 2
#define IS_EXPIRED 3
#include <map>
#include <vector>


#define _DB_RDMA

#define DEFAULT_INTERVAL 400000   // 0.4ms
#define DELTA 200000 // 0.2ms


#define LOG_SIZE (10 * 1024) //10k for log size


typedef struct msg_struct {
  int tableid;
  int optype;
  uint64_t key;
} msg_struct;

extern uint64_t timestamp;

namespace drtm {


  class DBSSTX {

  public:

    DBSSTX(RAWTables* tables);
    DBSSTX(RAWTables* tables,RdmaResource *rdma,int t_id);

    ~DBSSTX();

    uint64_t GetSS();
    uint64_t GetMySS(); //return thread's ss
    void GetSSDelayP();
    void GetSSDelayN();
    void SSReadLock();
    void SSReadUnLock();
    void UpdateLocalSS();
    void Begin(bool readonly);
    bool Abort();
    bool End();


    //Copy value
    void Add(int tableid, uint64_t key, uint64_t* val);
    bool GetLoc(int tableid, uint64_t key, uint64_t** val);
    bool Get(int tableid, uint64_t key, uint64_t** val, bool copyupdate,int *_status = NULL);
    bool GetAt(int tableid, uint64_t key, uint64_t** val, bool copyupdate, uint64_t *loc,int *status = NULL);
    void Delete(int tableid, uint64_t key);

    bool GetRemote(int tableid,uint64_t key,std::string *,Network_Node *node,int pid,uint64_t timestamp);
    // track RW set
    typedef struct rwset_item{
      int tableid;
      uint64_t key;
      uint64_t loc;
      uint64_t* addr;
      int pid;
      bool ro;
    } rwset_item;
    typedef std::map<std::pair<int, uint64_t>, rwset_item> rw_set_type;

    std::vector<rwset_item > rw_set;
    std::vector<rwset_item > readonly_set;

    bool release_flag = false;//flag when can free all the remote lock,so it will write back and free memory space

    void DBSSTX::chain_travel(rwset_item &item);
    void DBSSTX::hashext_travel(rwset_item &item);
    void DBSSTX::cuckoo_travel(rwset_item &item);

    //void PrintAllLocks();
    //Rdma methods
    void PrefetchAllRemote(uint64_t endtime);
    void ReleaseAllRemote();
    void Fallback_LockAll(SpinLock** sl, int numOfLocks, uint64_t endtime);
    void RemoteWriteBack();

    bool LockRemote(rwset_item item,Network_Node *node);
    bool ReleaseLockRemote(rwset_item item,Network_Node *node);
    bool LockLocal(rwset_item item);
    bool ReleaseLocal(rwset_item item);

    uint64_t GetLocalLoc(int tableid,uint64_t key);
    uint64_t GetRdmaLoc(int tableid,uint64_t key,int pid);

    void AddToRemoteWriteSet(int tableid,uint64_t key,int pid,uint64_t *loc = NULL);
    void AddToLocalWriteSet(int tableid,uint64_t key,int pid,uint64_t *loc = NULL);
    void AddToRemoteReadSet(int tableid,uint64_t key,int pid,uint64_t *loc = NULL);
    void AddToLocalReadSet(int tableid,uint64_t key,int pid,uint64_t *loc = NULL);

    void AddToLocalReadOnlySet(int tableid,uint64_t key,uint64_t *loc = NULL);

    void ReleaseAllLocal();
    void RdmaFetchAdd ( uint64_t off,int pid,uint64_t value);
    //The general lock operation
    void Lock(rwset_item &item);
    void Release(rwset_item &item,bool flag = false);
    void LocalLockSpin(char *loc);
    void LocalReleaseSpin(char *loc);


    void _LocalLock(rwset_item &item);
    void _LocalRelease(rwset_item &item);


    void GetLease(rwset_item &item, uint64_t endtime);

    bool AllLeasesAreValid();

    void GetLocalLease(int tableid,uint64_t key,uint64_t *loc,uint64_t endtime);

    bool AllLocalLeasesValid();

    void ClearRwset();

    std::string HandleMsg(std::string &req);

    RAWStore::Iterator *GetRawIterator(int tableid);

    class Iterator {

    public:
      explicit Iterator(DBSSTX* rotx, int tableid, bool update);

    bool Valid();

    uint64_t Key();

    uint64_t* Value();

    void Next();
    // <bound
    bool Next(uint64_t bound);

    void Prev();
    void Seek(uint64_t key);

    void SeekToFirst();

    void SeekToLast();

    void SetUpdate()
    {copyupdate = true;}
    inline uint64_t* GetWithSnapshot(uint64_t* mn);

  private:

    DBSSTX* sstx_;
    bool copyupdate;
    bool versioned;
    uint64_t key_;
    RAWStore::Iterator *iter_;
    uint64_t *val_;
    int tableid_;

    };


    DBSSTX::Iterator *GetIterator(int tableid);

  public:
    RAWTables *txdb_;
    RdmaResource *rdma;
    int thread_id;//TODO!!init
    uint64_t localsn;
    uint64_t lastsn; //to check whether another epoch has passed

    //  inline Memstore::MemNode* GetNodeCopy(Memstore::MemNode* org);


    uint64_t * check(int tableid ,uint64_t key);

    void Redo(uint64_t *value, int32_t delta, bool after);
    void Redo(uint64_t *value, float delta, bool after);
  private:
    bool readonly;
    uint64_t *emptySS[20];
    int emptySSLen;

    //methods for logging


  };

}  // namespace drtm

#endif  //
