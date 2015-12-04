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
 *  Bank transactions,settings  - XingDa 
 * 
 */


/**
 * An implementation of smallbank based on:
 * https://github.com/apavlo/h-store/tree/master/src/benchmarks/edu/brown/benchmark/smallbank/
 */


#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>
#include <time.h>
#include <sys/time.h>
#include <sys/timex.h>
#include <stdio.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <algorithm>
#include <set>
#include <vector>
#include "time.h"

#include "bench.h"
#include "bank.h"

//#include "db/dbtx.h"
#include "db/dbsstx.h"
//#include "db/dbrotx.h"
//#include "db/dbtables.h"
#include "util/rtm.h"
#include "port/atomic.h"

#include "db/network_node.h"
#include "memstore/rdma_resource.h"

using namespace std;
using namespace util;
using namespace drtm;

//#define LOCKING //for test readonly lease vs locks

#include <set>

extern size_t total_partition;
extern size_t current_partition;

/* ---small bank benchmark  constants----------- */


#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE    15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT  25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK      15

#define NUM_ACCOUNTS 100000
#define NUM_HOT      4000

#define MIN_BALANCE 10000
#define MAX_BALANCE 50000

#define MULTI_SITE 1
#define TX_HOT     90



/* --------------------------------------------- */

extern uint64_t timestamp;

static inline ALWAYS_INLINE size_t
NumAcctsLocal()
{
  return (size_t) scale_factor * NUM_ACCOUNTS;
}

static inline ALWAYS_INLINE size_t
TotalAccts() {
  return NumAcctsLocal() * total_partition;
}

static inline ALWAYS_INLINE uint64_t
GetStartAcct() {
  return current_partition * NumAcctsLocal();
}

static inline ALWAYS_INLINE uint64_t
GetEndAcct() {
  return (current_partition + 1) * NumAcctsLocal() - 1;
}

static inline ALWAYS_INLINE uint64_t
AllAcct() {
  return total_partition * NumAcctsLocal();
}

static inline ALWAYS_INLINE int
_CheckBetweenInclusive(int v, int lower, int upper)
{
  INVARIANT(v >= lower);
  INVARIANT(v <= upper);
  return v;
}


static inline ALWAYS_INLINE int
_RandomNumber(fast_random &r, int min, int max)
{
  return _CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
}

static inline ALWAYS_INLINE int
custid_to_pid(uint64_t id) {
  return (id / NumAcctsLocal());
}

static inline ALWAYS_INLINE int
_HotSpotRandomNumber(fast_random &r) {
  if( _RandomNumber(r,0,100) < 50) {
    return _RandomNumber(r,0,NUM_HOT);
  }else {
    return _RandomNumber(r,0 + NUM_HOT,NUM_ACCOUNTS);
  }
}


static inline ALWAYS_INLINE void
_HotSpotOneAcct(fast_random &r,uint64_t *acct) {

  static const int total_accts = NUM_ACCOUNTS * scale_factor;
  static const int total_hot   = NUM_HOT * scale_factor;

  if( _RandomNumber(r,0,100) < TX_HOT) {
    *acct = GetStartAcct()  + _RandomNumber(r,0,total_hot-1);
  }else {
    *acct = GetStartAcct()  + _RandomNumber(r,total_hot,total_accts - 1);
  }
}

static inline ALWAYS_INLINE void
_HotSpotTwoAcct(fast_random &r,uint64_t *acct0,uint64_t *acct1) {

  static const int total_accts = NUM_ACCOUNTS * scale_factor;
  static const int total_hot   = NUM_HOT * scale_factor;

  bool is_dtxn = ((total_partition > 1) && _RandomNumber(r,0,100) < MULTI_SITE);
  if( _RandomNumber(r,0,100) < TX_HOT) {
    *acct0 = GetStartAcct()  + _RandomNumber(r,0,total_hot-1);
    int mac_id,acct_id;
    if(is_dtxn) {
      do {
	mac_id = _RandomNumber(r,0,total_partition - 1);
      }while(mac_id == current_partition);

      acct_id = _RandomNumber(r,0,total_hot-1);
      *acct1  = acct_id + mac_id * total_accts;
    }else {
      //local
      do {
	*acct1 = GetStartAcct()  + _RandomNumber(r,0,total_hot-1);
      }while(*acct1 == *acct0);
    }
    //end hot
  }else {

    *acct0 = GetStartAcct()  + _RandomNumber(r,total_hot,total_accts-1);
    int mac_id,acct_id;
    if(is_dtxn) {
      do {
	mac_id = _RandomNumber(r,0,total_partition - 1);
      }while(mac_id == current_partition);

      acct_id = _RandomNumber(r,total_hot,total_accts-1);
      *acct1  = acct_id + mac_id * total_accts;
    }else {
      //local
      do {
	*acct1 = GetStartAcct()  + _RandomNumber(r,total_hot,total_accts-1);
      }while(*acct1 == *acct0);
    }
    //end no hot
  }
}

static int g_send_payment_remote_id_pct = 1;
static int g_remote_pct = 0;
static unsigned g_txn_workload_mix[] =
  { FREQUENCY_SEND_PAYMENT,
    FREQUENCY_DEPOSIT_CHECKING ,
    FREQUENCY_TRANSACT_SAVINGS,
    FREQUENCY_WRITE_CHECK,
    FREQUENCY_AMALGAMATE,
    FREQUENCY_BALANCE
  };

static RTMProfile balance_prof;
static RTMProfile readonly_prof;

class listener : public bench_worker {
public:
  DBSSTX sstx;
  Network_Node* node;
  RdmaResource* rdma;
  RAWTables *store;
  int live_worker_num;
  double full_transaction_thr;

  //Note that the listener will set the tpcc link(aka node) for rdma
  listener(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
	       RAWTables *store,RdmaResource *r)
  : bench_worker(worker_id, true, seed, db, barrier_a, barrier_b),
    sstx(store),
    store(store)
  {

    node = new Network_Node(current_partition,worker_id-8,config_file);
    rdma = r;
    rdma -> node = node;
    rdma->Servicing();

    sleep(10);
    live_worker_num = total_partition;

    if (verbose) {
      cerr << "listener : worker id " << worker_id << endl;
    }
  }

  void Coordinate(){

    for(int i = 0;i < total_partition;++i) {
      if(i == current_partition )
	continue;
      node->Send(i,nthreads,"done");

    }

    for(int i = 0;i < total_partition - 1;++i)
      node->Recv();

    sleep(5);

  }

  void run() {
    int recv_counter = 0;
    int recv_interval = 1000;
    struct ntptimeval tv;
    while(true) {

      if (recv_counter >= recv_interval) {
	std::string msg = node->tryRecv();
	if (msg != "") {
	  recv_interval = 100;
	  int pid=msg[0];
	  int nid=msg[1];
	  std::string real_msg=msg.substr(2);
	  fprintf(stdout,"recv real %d %s \n",pid,real_msg.c_str());
	  if(current_partition == 0) {
	    //calculate the throughput
	    std::istringstream istr;
	    istr.str(real_msg);

	    double temp;
	    istr >> temp;
	    istr >> temp;
	    full_transaction_thr += temp;
	  }

	  live_worker_num--;
	  if(live_worker_num == 0) {
	    if(current_partition == 0) {
	      fprintf(stdout,"avg full thr: %f\n",full_transaction_thr / total_partition);
	      fprintf(stdout,"full thr %f\n",full_transaction_thr);
	    }
	    break;
	  }

	} else {
	  recv_counter = 0;
	}
      }

      // update timestamp
      this_thread::sleep_for(chrono::milliseconds(1));
      if (ntp_gettime(&tv) == 0)
	timestamp = ((tv.time.tv_sec << 32) + tv.time.tv_usec) >> 1;
      recv_counter++;
    }

    delete node;
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    return w;
  }
};


class bank_worker : public bench_worker {
public:
  DBSSTX sstx;
  Network_Node* node;
  RAWTables *store;

  void run() {
    bench_worker::run();
  }

  //  DelayQueue *delayQueue;
  // resp for [warehouse_id_start, warehouse_id_end)
  bank_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              RAWTables *store,RdmaResource *rdma)

    : bench_worker(worker_id, true, seed, db,
		   barrier_a, barrier_b),
      sstx(store,rdma,worker_id - 8),
      store(store)
  {
    //    fprintf(stdout,"worked id: %d\n",worker_id);
    fprintf(stdout,"bank worker id: %d\n",sstx.thread_id);
    node = new Network_Node(current_partition,worker_id - 8,config_file);

    obj_key0.reserve(2 * CACHELINE_SIZE);
    obj_key1.reserve(2 * CACHELINE_SIZE);
    obj_v.reserve(2 * CACHELINE_SIZE);
  }

  bool processDelayed() {
  	store->ssman_->UpdateLocalSS(-1);
  	return true;
  }

  virtual workload_desc_vec

  get_workload() const
  {
    workload_desc_vec w;
    unsigned m = 0;

    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);
    if (g_txn_workload_mix[0])
      w.push_back(workload_desc("SendPayment", double(g_txn_workload_mix[0])/100.0, TxnSendPayment));
    if (g_txn_workload_mix[1])
      w.push_back(workload_desc("DepositChecking",double(g_txn_workload_mix[1])/100.0,TxnDepositChecking));
    if (g_txn_workload_mix[2])
      w.push_back(workload_desc("TrasactSaving",double(g_txn_workload_mix[2])/100.0,TxnTransactSavings));
    if (g_txn_workload_mix[3])
      w.push_back(workload_desc("WriteCheck",double(g_txn_workload_mix[3])/100.0,TxnWriteCheck));
    if (g_txn_workload_mix[4])
      w.push_back(workload_desc("Amalgamate",double(g_txn_workload_mix[4])/100.0,TxnAmalganate));
    if (g_txn_workload_mix[5])
      w.push_back(workload_desc("balance",double(g_txn_workload_mix[5])/100.0,TxnBalance));

    return w;
  }


  //only distributed one
  txn_result txn_send_payment() ;

  static txn_result
  TxnSendPayment(bench_worker *w)
  {
    txn_result r =  static_cast<bank_worker *>(w)->txn_send_payment();
    return r;
  }


  txn_result txn_deposit_checking();

  static txn_result
  TxnDepositChecking(bench_worker *w)
  {
    txn_result r = static_cast<bank_worker *>(w) -> txn_deposit_checking();
    return r;
  }

  txn_result txn_transact_savings();

  static txn_result
  TxnTransactSavings(bench_worker *w)
  {
    txn_result r = static_cast<bank_worker *>(w)->txn_transact_savings();

    return r;

  }

  txn_result txn_write_check();

  static txn_result
  TxnWriteCheck(bench_worker *w)
  {
    txn_result r = static_cast<bank_worker *>(w)->txn_write_check();
    return r;

  }

  txn_result txn_amalgamate() ;

  //helper function
  txn_result _txn_amalgamate_local_0(uint64_t,uint64_t);
  txn_result _txn_amalgamate_local_1(uint64_t,uint64_t);

  static txn_result
  TxnAmalganate(bench_worker *w)
  {
    txn_result r = static_cast<bank_worker *>(w)->txn_amalgamate();
    return r;
  }

  txn_result txn_balance();

  static txn_result
  TxnBalance(bench_worker *w)
  {
    txn_result r =  static_cast<bank_worker *>(w)->txn_balance2();
    return r;
  }
  //the distributed version of txnbalance
  txn_result txn_balance2() ;
  txn_result txn_balance3() ;


protected:

  virtual void
  on_run_setup() OVERRIDE
  {
    printf("%ld wid %d\n", pthread_self(), worker_id);
    //    delayQueue = new DelayQueue();
    //    store->delayQueues[worker_id - 8] = delayQueue;
    store->ThreadLocalInit(worker_id - 8);
    printf("Start ss %ld\n", store->ssman_->curSS);
    if (!pin_cpus)
      return;

  }


  void ending(std::string msg) {
    for(int pid = 0;pid < total_partition;pid++){
      node->Send(pid,nthreads,msg);
    }
  }

private:
  std::string obj_key0;
  std::string obj_key1;
  std::string obj_v;


};


class bank_acct_loader : public bench_loader {

  RAWTables  *store;
public:
  bank_acct_loader(unsigned long seed,
		       abstract_db *db,
		       RAWTables *s)

    :bench_loader(seed,db),
     store(s)
  {
  }
protected :
  virtual void
  load() {
    printf("load account table\n");

    char acct_name[32];
    const char *acctNameFormat = "%lld 32 d";

    char *wrapper_acct = new char[META_LENGTH + sizeof(account::value)];
    char *wrapper_saving = new char[META_LENGTH + sizeof(savings::value)];
    char *wrapper_check  = new char[META_LENGTH + sizeof(checking::value)];

    fprintf(stdout,"loadint .. from %d to %d\n",GetStartAcct(),GetEndAcct());
    for(uint64_t i = GetStartAcct();i <= GetEndAcct();++i){
      sprintf(acct_name,acctNameFormat,i);

      memset(wrapper_acct, 0, META_LENGTH);
      memset(wrapper_saving, 0, META_LENGTH);
      memset(wrapper_check,  0, META_LENGTH);

      account::value *a = (account::value*)(wrapper_acct + META_LENGTH);
      a->a_name.assign(std::string(acct_name) );
      store->Put(ACCT,i,(uint64_t *)wrapper_acct);

      float balance_c = (float)_RandomNumber(r, MIN_BALANCE, MAX_BALANCE);
      float balance_s = (float)_RandomNumber(r, MIN_BALANCE, MAX_BALANCE);

      savings::value *s = (savings::value *)(wrapper_saving + META_LENGTH);
      s->s_balance = balance_s;
      store->Put(SAV,i,(uint64_t *)wrapper_saving);

      checking::value *c = (checking::value *)(wrapper_check + META_LENGTH);
      c->c_balance = balance_c;
      store->Put(CHECK,i,(uint64_t *)wrapper_check);
    }
  }



};

class bank_bench_runner : public bench_runner {
public:
  RAWTables *store;

  bank_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    printf("Init bank tables\n");

    store = new RAWTables(nthreads,BENCH_BANK);

    int floatsize = sizeof(float);

    store->AddSchema(ACCT,sizeof(uint64_t),0,0,sizeof(account::value),true);
    store->AddSchema(SAV,sizeof(uint64_t),0,0,sizeof(savings::value),true);
    store->AddSchema(CHECK,sizeof(uint64_t),0,0,sizeof(checking::value),true);

    store->AddSchema(ACCT_IDX,sizeof(uint64_t),0,0,16,true);

  }

protected:
  bool check_consistency() {
    return true;
  }

  virtual void sync_log() {

    store->Sync();
    printf("========================= Balance RTM Profile=======================\n");
    balance_prof.reportAbortStatus();


    uint64_t aborts = balance_prof.abortCounts;
    printf("aborts %ld\n", aborts);
  }

  virtual void final_check() {
    fprintf(stdout,"final check bank\n");

  }

  virtual void initPut() {

  }

  virtual vector<bench_loader *>
  make_loaders() {

    vector<bench_loader *> ret;
    ret.push_back(new bank_acct_loader(9324,db,store));

    return ret;
  }

  virtual void init_rdma() {
    rdma = new RdmaResource(store->start_rdma,store->rdma_size,512,store->end_rdma);
  }

  virtual vector<bench_worker *>
  make_workers(){
    //    fprintf(stdout,"start make workers\n");
    const int blockstart = 8;

    fast_random r(23984543);
    vector<bench_worker *> ret;


    bench_worker *l = new listener(
				   blockstart + nthreads,r.next(),db,
				   NULL,NULL,store,rdma);

    fprintf(stdout,"connecting...\n");
    rdma->Connect();
    fprintf(stdout,"rdma connection done\n");
    ((listener *)l)->Coordinate();
    fprintf(stdout,"coordination done\n");

    for(size_t i = 0;i < nthreads;++i) {
      ret.push_back(
		    new bank_worker( blockstart + i,r.next(), db, &barrier_a, &barrier_b,
				     store, rdma));

    }
    ret.push_back(l);
    //    l->start();
    return ret;
  }

};

void
bank_do_test( int argc, char **argv)
{

  abstract_db *db = NULL;
  bank_bench_runner r(db);

  bool did_spec_remote_pct = false;
  optind = 1;
  while (1) {
    static struct option long_options[] =
      {
	{"new-order-remote-item-pct"            , required_argument , 0                                     , 'r'} ,
	{"workload-mix"                         , required_argument , 0                                     , 'w'} ,
	{0, 0, 0, 0}
      };
    int option_index = 0;
    int c = getopt_long(argc, argv, "r:", long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 'r':
      g_remote_pct = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(g_remote_pct >= 0 && g_remote_pct <= 100);
      did_spec_remote_pct = true;
      break;

    case 'w':
      {
        const vector<string> toks = split(optarg, ',');
        ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
        unsigned s = 0;
        for (size_t i = 0; i < toks.size(); i++) {
          unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
          ALWAYS_ASSERT(p >= 0 && p <= 100);
          s += p;
          g_txn_workload_mix[i] = p;
        }
        ALWAYS_ASSERT(s == 100);
      }
      break;

    case '?':
      /* getopt_long already printed an error message. */
      fprintf(stderr,"unknown options\n");
      exit(1);

    default:
      fprintf(stderr,"unknown options %d\n",c);
      abort();
    }
  }

  if (verbose) {
    cerr << "  remote_pct    : " << g_remote_pct << endl;
    cerr << "  workload_mix                 : " <<
      format_list(g_txn_workload_mix,
                  g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
  }

  r.run();
}

bank_worker::txn_result
bank_worker::txn_balance2() {

  ssize_t ret = 0;

  int pid = current_partition;
  uint64_t id;
  _HotSpotOneAcct(r,&id);

  bool target_remote = false;
  uint64_t *s_loc,*c_loc;
  if(target_remote) {
    s_loc = (uint64_t *)(new char[META_LENGTH + sizeof(savings::value)]);
    c_loc = (uint64_t *)(new char[META_LENGTH + sizeof(checking::value)]);
    sstx.AddToRemoteReadSet(SAV,id,pid,s_loc);
    sstx.AddToRemoteReadSet(CHECK,id,pid,c_loc);
  }else {
    bool found = false;
    found = sstx.GetLoc(SAV,id,&s_loc);
    assert(found);
    found = sstx.GetLoc(CHECK,id,&c_loc);
    assert(found);
    sstx.AddToLocalReadSet(SAV,id,current_partition,s_loc);
    sstx.AddToLocalReadSet(CHECK,id,current_partition,c_loc);
    //sstx.AddToLocalWriteSet(SAV,id,current_partition,s_loc);
    //sstx.AddToLocalWriteSet(CHECK,id,current_partition,c_loc);
  }
    //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];



  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //  RTMTX::RdmaBegin(NULL,0,&balance_prof);
  RTMTX::RdmaBegin(NULL,0,NULL);

  if(!_xtest())  {
    endtime = timestamp + DEFAULT_INTERVAL;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

  checking::value *check = (checking::value  *)((uint64_t)c_loc + VALUE_OFFSET);
  savings::value   *save = (savings::value   *)((uint64_t)s_loc + VALUE_OFFSET);

  double total = save->s_balance + check->c_balance;

  if(target_remote && _xtest() && !sstx.AllLeasesAreValid()) {
    _xabort(0x73);
  }
  if(!_xtest()) {
    sstx.ReleaseAllLocal();
  }
  RTMTX::End(NULL,0);

  sstx.ClearRwset();

  bool b = sstx.End();
  return txn_result(b, ret);
}


bank_worker::txn_result
bank_worker::txn_balance3() {

  ssize_t ret = 0;

  int pid = current_partition;
  uint64_t id;
  _HotSpotOneAcct(r,&id);

  bool target_remote = false;
  uint64_t *s_loc,*c_loc;
  if(target_remote) {
    s_loc = (uint64_t *)(new char[META_LENGTH + sizeof(savings::value)]);
    c_loc = (uint64_t *)(new char[META_LENGTH + sizeof(checking::value)]);
    sstx.AddToRemoteReadSet(SAV,id,pid,s_loc);
    sstx.AddToRemoteReadSet(CHECK,id,pid,c_loc);

    uint64_t endtime = timestamp + DEFAULT_INTERVAL;
    sstx.PrefetchAllRemote(endtime);

    //  RTMTX::RdmaBegin(NULL,0,&balance_prof);
    RTMTX::RdmaBegin(NULL,0,NULL);

    if(!_xtest())  {
      endtime = timestamp + DEFAULT_INTERVAL;
      sstx.Fallback_LockAll(NULL,0,endtime);
    }

    checking::value *check = (checking::value  *)((uint64_t)c_loc + VALUE_OFFSET);
    savings::value   *save = (savings::value   *)((uint64_t)s_loc + VALUE_OFFSET);

    double total = save->s_balance + check->c_balance;

    if(target_remote && _xtest() && !sstx.AllLeasesAreValid()) {
      _xabort(0x73);
    }
    if(!_xtest()) {
      sstx.ReleaseAllLocal();
    }
    RTMTX::End(NULL,0);

    sstx.ClearRwset();

    bool b = sstx.End();
    return txn_result(b, ret);

    // end remote case, maybe it is no possible
  }else {
    bool found = false;
    found = sstx.GetLoc(SAV,id,&s_loc);
    assert(found);
    found = sstx.GetLoc(CHECK,id,&c_loc);
    assert(found);

      int counter = 1;
      double total[2];

      while(1) {

	if(counter > 500000) {
	  //magic number to avoid stuck
	  fprintf(stderr,"txn_stock_level stuck %d\n",counter);
	  assert(false);
	}


	uint64_t endtime = timestamp + 1000000;//1us

	for(int time = 0; time < 2;time++) {
	  total[time] = 0;

	  RTMTX::RdmaBegin(NULL,0,NULL);
	  checking::value *check = (checking::value  *)((uint64_t)c_loc + VALUE_OFFSET);

	  if(!_xtest())  {
	    sstx.GetLocalLease(CHECK,id,c_loc,endtime);
	  }
	  total[time] += check->c_balance;

	  RTMTX::End(NULL,0);

	  RTMTX::RdmaBegin(NULL,0,NULL);
	  savings::value   *save = (savings::value   *)((uint64_t)s_loc + VALUE_OFFSET);

	  if(!_xtest())  {
	    sstx.GetLocalLease(SAV,id,s_loc,endtime);
	  }
	  total[time] += save->s_balance;

	  RTMTX::End(NULL,0);
	  //end 2 round
	}

	if(total[0] == total[1])
	  break;

	counter ++;
      }

      bool b = sstx.End();
      return txn_result(b, ret + (int)(total[0]));

  }

  bool b = sstx.End();
  return txn_result(b, ret);
}


bank_worker::txn_result
bank_worker::txn_balance() {

  ssize_t ret = 0;

  //  uint64_t acct_id = _RandomNumber(r,GetStartAcct(),GetEndAcct());
  uint64_t acct_id = _HotSpotRandomNumber(r) + GetStartAcct();

  uint64_t *c_loc,*s_loc;

  bool found = false;

  found = sstx.GetLoc(SAV,acct_id,&s_loc);
  assert(found);
  found = sstx.GetLoc(CHECK,acct_id,&c_loc);
  assert(found);

  //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];

  sstx.AddToLocalReadSet(SAV,acct_id,current_partition,s_loc);
  sstx.AddToLocalReadSet(CHECK,acct_id,current_partition,c_loc);

  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //RTMTX::RdmaBegin(NULL,0,&balance_prof);
  RTMTX::RdmaBegin(NULL,0,NULL);

  if(!_xtest())  {
    endtime = timestamp + DEFAULT_INTERVAL;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

  checking::value *check = (checking::value  *)((uint64_t)c_loc + VALUE_OFFSET);
  savings::value   *save = (savings::value   *)((uint64_t)s_loc + VALUE_OFFSET);

  double total = save->s_balance + check->c_balance;

  RTMTX::End(NULL,0);

  sstx.ClearRwset();

  bool b = sstx.End();
  return txn_result(b, ret);

}


bank_worker::txn_result
bank_worker::txn_send_payment() {

  ssize_t ret = 0;

  uint64_t send_id,target_id;
  _HotSpotTwoAcct(r,&send_id,&target_id);
  const float amount = 5.0;
  //  const float amount = 0;
  int pid = custid_to_pid(target_id);

  bool  target_remote = (current_partition != pid);
  bool found = false;

  uint64_t *sc_value ;
  uint64_t *tc_value ;

  found = sstx.GetLoc(CHECK,send_id,&sc_value);
  assert(found);
  sstx.AddToLocalWriteSet(CHECK,send_id,current_partition,sc_value);

  if(!target_remote) {
    found = sstx.GetLoc(CHECK,target_id,&tc_value);
    assert(found);
    sstx.AddToLocalWriteSet(CHECK,target_id,current_partition,tc_value);

  }else {
    tc_value = (uint64_t *)(new char[ sstx.txdb_->schemas[CHECK].vlen + META_LENGTH]);
    sstx.AddToRemoteWriteSet(CHECK,target_id,pid,tc_value);
  }


  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  bool aborting  = false;

  //!!!for testing
  //  float previous_c,after_c;

  //RTMTX::RdmaBegin(NULL,0,&balance_prof,&aborting);
  RTMTX::RdmaBegin(NULL,0,NULL,&aborting);

  if ( !_xtest() && !aborting) {
    endtime = timestamp + DEFAULT_INTERVAL;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

  if(_xtest() ) {
    //in fallback,we need to check the lock
    int status = NONE;
    found = sstx.GetAt(CHECK, 0, &sc_value, true, sc_value,&status);

    //read write case
    if(status == LOCK || status == READ_LEASE)
      _xabort(0x73);

    found  = sstx.GetAt(CHECK,0,&tc_value,true,tc_value,&status);
    if(!target_remote && (status == LOCK || status == READ_LEASE))
      _xabort(0x73);

    checking::value *send_check = (checking::value *)sc_value;

    if(send_check->c_balance < amount)
      _xabort(0x93);

    send_check->c_balance -= amount;

    checking::value *target_check = (checking::value *)tc_value;
    target_check->c_balance += amount;

  }else {
    //fallback space,because we sometimes need to abort the txn

    found  = sstx.GetAt(CHECK,0,&sc_value,true,sc_value);
    found  = sstx.GetAt(CHECK,0,&tc_value,true,tc_value);

    if(!aborting) {

      checking::value *send_check = (checking::value *)sc_value;
      checking::value *target_check = (checking::value *)tc_value;

      if(send_check->c_balance < amount) {
	//pass

      }else {

	send_check->c_balance -= amount;
	target_check->c_balance += amount;
	//end else
      }
    }
    //end else

  }

  if( !_xtest() && !aborting) {
    sstx.ReleaseAllLocal();
  }

  RTMTX::End(NULL, 0);

  sstx.release_flag = true;//TODO ,maybe need refine some code
  sstx.ReleaseAllRemote();
  sstx.release_flag = false;
  sstx.ClearRwset();//!!

  if(aborting)
    return txn_result(sstx.Abort(), ret);

  bool b = sstx.End();
  return txn_result(b, ret);

}


bank_worker::txn_result
bank_worker::txn_deposit_checking() {

  ssize_t ret = 0;

  uint64_t acct_id;
  _HotSpotOneAcct(r,&acct_id);
  float amount = 1.3 ;// from original code

  //almost the same like send payment

  bool found = false;

  uint64_t *loc;

  found = sstx.GetLoc(CHECK,acct_id,&loc);
  assert(found );

  sstx.AddToLocalWriteSet(CHECK,acct_id,current_partition,loc);

  //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];

  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //RTMTX::RdmaBegin(NULL,0,&balance_prof);
  RTMTX::RdmaBegin(NULL,0,NULL);

  if ( !_xtest()) {
    //    sstx.Fallback_LockAll(node);
    endtime = timestamp + DEFAULT_INTERVAL;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

  int status = NONE;
  found = sstx.GetAt(CHECK, 0, &loc, true, loc ,&status);

  if(_xtest() ) {
    if(status == LOCK || status == READ_LEASE)
      _xabort(0x73);
  }

  checking::value *check = (checking::value *)loc;
  check->c_balance += amount;

  if ( !_xtest()) {
      sstx.ReleaseAllLocal();
  }

  RTMTX::End(NULL,0);
  sstx.ClearRwset();

  return txn_result(true, ret);
}


bank_worker::txn_result
bank_worker::txn_transact_savings()
{
  ssize_t ret = 0;

  uint64_t acct_id;
  _HotSpotOneAcct(r,&acct_id);
  float amount   = 20.20; //from original code

  uint64_t *s_loc;

  bool found;

  found = sstx.GetLoc(SAV,acct_id,&s_loc);
  assert(found);
  sstx.AddToLocalWriteSet(SAV,acct_id,current_partition,s_loc);

  //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];
  bool aborting = false;

  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //RTMTX::RdmaBegin(NULL,0,&balance_prof,&aborting);
  RTMTX::RdmaBegin(NULL,0,NULL,&aborting);
  if(aborting) {

  }else {
    if ( !_xtest()) {
      endtime = timestamp + DEFAULT_INTERVAL;
      sstx.Fallback_LockAll(NULL,0,endtime);
    }

    int status = NONE;
    found = sstx.GetAt(SAV,0,&s_loc,true,s_loc,&status);

    savings::value  * save  = (savings::value *) (s_loc);

    if( (save->s_balance + amount ) < 0)
      _xabort(0x93);

    save->s_balance += amount;    //i think there is an error in h-store's code

    if ( !_xtest()) {
      sstx.ReleaseAllLocal();
    }
  }
  RTMTX::End(NULL,0);
  sstx.ClearRwset();

  if(aborting)
    return txn_result(sstx.Abort(), ret);

  bool b = sstx.End();
  return txn_result(b, ret);

}


bank_worker::txn_result
bank_worker::txn_write_check()
{
  ssize_t ret = 0;

  uint64_t acct_id;
  _HotSpotOneAcct(r,&acct_id);

  float amount     = 5.0; //from original code

  uint64_t *s_loc,*c_loc;

  bool found;

  found = sstx.GetLoc(SAV,acct_id,&s_loc);
  assert(found);

  found = sstx.GetLoc(CHECK,acct_id,&c_loc);
  assert(found);

  sstx.AddToLocalReadSet(SAV,acct_id,current_partition,s_loc);
  sstx.AddToLocalWriteSet(CHECK,acct_id,current_partition,c_loc);


  //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];
  bool aborting = false;

  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //RTMTX::RdmaBegin(NULL,0,&balance_prof,&aborting);
  RTMTX::RdmaBegin(NULL,0,NULL,&aborting);

  if ( !_xtest()) {
    endtime = timestamp + DEFAULT_INTERVAL;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

   int status = NONE;
   found = sstx.GetAt(CHECK,0,&c_loc,true,c_loc,&status);

   if(_xtest()) {

     if(status == LOCK) {
       //only need to check the check_obj lock
       _xabort(0x73);
     }
   }

   found = sstx.GetAt(SAV,0,&s_loc,true,s_loc,&status);

   if(_xtest()){
     if(status == LOCK || status == READ_LEASE) {
       _xabort(73);
     }
   }

   savings::value  * save  = (savings::value *) (s_loc);
   checking::value * check = (checking::value *)(c_loc);

   double total = save->s_balance + check->c_balance;

   if(total < amount) {
     check->c_balance -= (amount - 1);
   }else  {
     check->c_balance -= amount;
   }

  if ( !_xtest()) {
    sstx.ReleaseAllLocal();
  }

  RTMTX::End(NULL,0);
  sstx.ClearRwset();

  if(aborting)
    return txn_result(sstx.Abort(), ret);

  bool b = sstx.End();
  //  fprintf(stdout,"w done\n");
  return txn_result(b, ret);
}

bank_worker::txn_result
bank_worker::txn_amalgamate() {

  uint64_t acct_id0,acct_id1;
  _HotSpotTwoAcct(r,&acct_id0,&acct_id1);
  if(_RandomNumber(r,1,100) <= 50) {
    return _txn_amalgamate_local_0(acct_id0,acct_id1);
  }else {
    return _txn_amalgamate_local_1(acct_id0,acct_id1);
  }
}


bank_worker::txn_result
bank_worker::_txn_amalgamate_local_0(uint64_t local_id,uint64_t remote_id)
{

  //acct_id0 is local,so acct_id1 has a probability to be remote
  ssize_t ret = 0;
  int pid;//The machine id

  //now suppose that is a single machine txn
  uint64_t acct_id0 = local_id;

  uint64_t acct_id1 = remote_id;
  bool  target_remote = false;

  pid = custid_to_pid(acct_id1);
  target_remote = (pid != current_partition);

  uint64_t *s_0,*c_0,*s_1,*c_1;

  bool found;

  found = sstx.GetLoc(SAV,acct_id0,&s_0);
  assert(found);
  found = sstx.GetLoc(CHECK,acct_id0,&c_0);

#ifdef LOCKING
  sstx.AddToLocalWriteSet(SAV,acct_id0,current_partition,s_0);
#else
  sstx.AddToLocalReadSet(SAV,acct_id0,current_partition,s_0);
#endif
  sstx.AddToLocalWriteSet(CHECK,acct_id0,current_partition,c_0);

  if(!target_remote) {

    found = sstx.GetLoc(CHECK,acct_id1,&c_1);
    assert(found);
    found = sstx.GetLoc(SAV,acct_id1,&s_1);

    sstx.AddToLocalWriteSet(SAV,acct_id1,current_partition,s_1);
#ifdef LOCKING
    sstx.AddToLocalWriteSet(CHECK,acct_id1,current_partition,c_1);
#else
    sstx.AddToLocalReadSet(CHECK,acct_id1,current_partition,c_1);
#endif

  }else {
    s_1 = (uint64_t *)(new char[sstx.txdb_->schemas[SAV].vlen + META_LENGTH]);
    c_1 = (uint64_t *)(new char[sstx.txdb_->schemas[CHECK].vlen + META_LENGTH]);


    sstx.AddToRemoteWriteSet(SAV,acct_id1,pid,s_1);
#ifdef LOCKING
    sstx.AddToRemoteWriteSet(CHECK,acct_id1,pid,s_1);
#else
    sstx.AddToRemoteReadSet(CHECK,acct_id1,pid,c_1);
#endif
  }


  //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];
  bool aborting = false;

  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //RTMTX::RdmaBegin(NULL,0,&balance_prof,&aborting);
  RTMTX::RdmaBegin(NULL,0,NULL,&aborting);

  //the logic is wierd,i think

  if(!_xtest())  {
    //    endtime = timestamp + 1000000;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

  int s_status0 = NONE,c_status0 = NONE,s_status1 = NONE,c_status1 = NONE;
  found = sstx.GetAt(SAV,0,&s_0,true,s_0,&s_status0);
  found = sstx.GetAt(CHECK,0,&c_0,true,c_0,&c_status0);
  found = sstx.GetAt(SAV,0,&s_1,true,s_1,&s_status1);
  found = sstx.GetAt(CHECK,0,&c_1,true,c_1,&c_status1);

  if(_xtest()) {
    if(s_status0 == LOCK || c_status0 == LOCK || c_status0 == READ_LEASE)
      _xabort(0x73);
    if(!target_remote) {
      //
      if(s_status1 == LOCK || s_status1 == READ_LEASE || c_status1 == LOCK)
	_xabort(0x73);

    }
  }

  savings::value *save0 = (savings::value *)(s_0);
  checking::value *check0 = (checking::value *)(c_0);

  savings::value *save1 = (savings::value *)(s_1);
  checking::value *check1 = (checking::value *)(c_1);

  double total = save0->s_balance + check1->c_balance;

  check0->c_balance = 0;
  save1->s_balance += total;

  if(target_remote && _xtest() && !sstx.AllLeasesAreValid())
    _xabort(0x73);

  if(!_xtest()) {
    sstx.ReleaseAllLocal();
  }

  RTMTX::End(NULL,0);

  sstx.release_flag = true;//TODO ,maybe need refine some code
  sstx.ReleaseAllRemote();
  sstx.release_flag = false;
  sstx.ClearRwset();//!!

  bool b = sstx.End();
  return txn_result(b, ret);
}


bank_worker::txn_result
bank_worker::_txn_amalgamate_local_1(uint64_t local_id,uint64_t remote_id)
{
  //acct_id1 is local,so acct_id0 must be remote
  ssize_t ret = 0;
  int pid;

  uint64_t acct_id0 = remote_id,acct_id1 = local_id;

  bool  target_remote = false;

  pid = custid_to_pid(acct_id0);
  target_remote = (pid != current_partition);

  uint64_t *s_0,*c_0,*s_1,*c_1;

  bool found;

  found = sstx.GetLoc(SAV,acct_id1,&s_1);
  assert(found);
  found = sstx.GetLoc(CHECK,acct_id1,&c_1);
  assert(found);
#ifdef LOCKING
  sstx.AddToLocalWriteSet(SAV,acct_id1,current_partition,s_1);
#else
  sstx.AddToLocalReadSet(SAV,acct_id1,current_partition,s_1);
#endif
  sstx.AddToLocalWriteSet(CHECK,acct_id1,current_partition,c_1);

  if(target_remote) {

    s_0 = (uint64_t *)(new char[sstx.txdb_->schemas[SAV].vlen + META_LENGTH]);
    c_0 = (uint64_t *)(new char[sstx.txdb_->schemas[CHECK].vlen + META_LENGTH]);

#ifdef LOCKING
    sstx.AddToRemoteWriteSet(SAV,acct_id0,pid,s_0);
#else
    sstx.AddToRemoteReadSet(SAV,acct_id0,pid,s_0);
#endif

    sstx.AddToRemoteWriteSet(CHECK,acct_id0,pid,c_0);

  }else {

    found = sstx.GetLoc(SAV,acct_id0,&s_0);
    assert(found);
    found = sstx.GetLoc(CHECK,acct_id0,&c_0);
    assert(found);

#ifdef LOCKING
    sstx.AddToLocalWriteSet(SAV,acct_id0,current_partition,s_0);
#else
    sstx.AddToLocalReadSet(SAV,acct_id0,current_partition,s_0);
#endif
    sstx.AddToLocalWriteSet(CHECK,acct_id0,current_partition,c_0);
  }

  //  SpinLock** spinlocks= new SpinLock*[1];
  //  spinlocks[0] = &store->bank_lock[0];
  bool aborting = false;

  uint64_t endtime = timestamp + DEFAULT_INTERVAL;
  sstx.PrefetchAllRemote(endtime);

  //RTMTX::RdmaBegin(NULL,0,&balance_prof,&aborting);
  RTMTX::RdmaBegin(NULL,0,NULL,&aborting);

  //the logic is wierd,i think
  if(!_xtest())  {
    //    endtime = timestamp + 1000000;
    sstx.Fallback_LockAll(NULL,0,endtime);
  }

  int s_status0 = NONE,c_status0 = NONE,s_status1 = NONE,c_status1 = NONE;
  found = sstx.GetAt(SAV,0,&s_0,true,s_0,&s_status0);
  found = sstx.GetAt(CHECK,0,&c_0,true,c_0,&c_status0);
  found = sstx.GetAt(SAV,0,&s_1,true,s_1,&s_status1);
  found = sstx.GetAt(CHECK,0,&c_1,true,c_1,&c_status1);

  if(_xtest()) {
    if(s_status1 == LOCK || s_status1 == READ_LEASE || c_status1 == LOCK )
      _xabort(0x73);
    if(!target_remote) {
      //
      if(s_status0 == LOCK || c_status0 == LOCK || c_status0 == READ_LEASE)
	_xabort(0x73);
    }
  }

  savings::value *save0 = (savings::value *)(s_0);
  checking::value *check0 = (checking::value *)(c_0);

  savings::value *save1 = (savings::value *)(s_1);
  checking::value *check1 = (checking::value *)(c_1);

  double total = save0->s_balance + check1->c_balance;

  check0->c_balance = 0;
  save1->s_balance += total;

  if(target_remote && _xtest() && !sstx.AllLeasesAreValid())
    _xabort(0x73);

  if(!_xtest()) {
    sstx.ReleaseAllLocal();
  }

  RTMTX::End(NULL,0);

  sstx.release_flag = true;//TODO ,maybe need refine some code
  sstx.ReleaseAllRemote();
  sstx.release_flag = false;
  sstx.ClearRwset();//!!

  bool b = sstx.End();
  return txn_result(b, ret);

}
