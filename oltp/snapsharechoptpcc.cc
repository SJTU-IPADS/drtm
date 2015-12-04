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
 *  TPCC chopping impl                        - Hao Qian
 *  TPCC chopping,DrTM method, bench  setting - XingDa 
 *  Timestamp sync                            - Yanzhe 
 * 
 */

/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

//final

#include <iostream>
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

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <algorithm>
#include <set>
#include <vector>
#include "time.h"

#include "bench.h"
#include "tpcc.h"

#include "db/dbsstx.h"
#include "util/rtm.h"
#include "port/atomic.h"

#include "db/network_node.h"
#include "memstore/rdma_resource.h"

using namespace std;
using namespace util;
//using namespace leveldb;
using namespace drtm;

#include <set>
#include <sstream>
#include <iostream>
#define SEPERATE 0
#define SLDBTX	0
#define CHECKTPCC 0
#define WRITE_RATIO 10

#define CHECK_FLAG 1

//whether to count the cross warehouse & cross partition times
#define RATIO

#ifdef RATIO

uint64_t *cross_warehouse;
uint64_t *cross_partition;

#endif

#define READ_ONLY_FLAG


extern uint64_t timestamp;

// stock temp structs
struct _stock_entry{
  uint64_t s_key;
  int s_index;
  int pid;
  _stock_entry(int i,uint64_t key,int p) {
    s_key = key;
    s_index = i;
    pid = p;
  }
};


int log_off = 0;
int log_size = 160000;

bool __stock_comp (_stock_entry i,_stock_entry j) { return (i.s_key < j.s_key); }


#if SHORTKEY
static inline ALWAYS_INLINE int64_t makeDistrictKey(int32_t w_id, int32_t d_id) {
  int32_t did = d_id + (w_id * 10);
  int64_t id = static_cast<int64_t>(did);
  return id;
}

static inline ALWAYS_INLINE int64_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t id =  static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(c_id);
  return id;
}

static inline ALWAYS_INLINE int64_t makeHistoryKey(int32_t h_c_id, int32_t h_c_d_id, int32_t h_c_w_id, int32_t h_d_id, int32_t h_w_id) {
  int32_t cid = (h_c_w_id * 10 + h_c_d_id) * 3000 + h_c_id;
  int32_t did = h_d_id + (h_w_id * 10);
  int64_t id = static_cast<int64_t>(cid) << 20 | static_cast<int64_t>(did);
  return id;
}

static inline ALWAYS_INLINE int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
  return id;
}

static inline ALWAYS_INLINE int64_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
  return id;
}

static inline ALWAYS_INLINE int64_t makeOrderIndex(int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_id) {
  int32_t upper_id = (w_id * 10 + d_id) * 3000 + c_id;
  int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
  return id;
}

static inline ALWAYS_INLINE int64_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
  int32_t upper_id = w_id * 10 + d_id;
  int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
  int64_t olid = oid * 15 + number;
  int64_t id = static_cast<int64_t>(olid);
  return id;
}

static inline ALWAYS_INLINE int64_t makeStockKey(int32_t w_id, int32_t s_id) {
  int32_t sid = s_id + (w_id * 100000);
  int64_t id = static_cast<int64_t>(sid);
  return id;
}

static void convertString(char *newstring, const char *oldstring, int size) {
  for (int i=0; i<8; i++)
    if (i < size)
      newstring[7 -i] = oldstring[i];
    else newstring[7 -i] = '\0';

  for (int i=8; i<16; i++)
    if (i < size)
      newstring[23 -i] = oldstring[i];
    else newstring[23 -i] = '\0';
}

static bool compareCustomerIndex(uint64_t key, uint64_t bound){
  uint64_t *k = (uint64_t *)key;
  uint64_t *b = (uint64_t *)bound;
  for (int i=0; i<5; i++) {
    if (k[i] > b[i]) return false;
    if (k[i] < b[i]) return true;
  }
  return true;
}

static uint64_t makeCustomerIndex(int32_t w_id, int32_t d_id, string s_last, string s_first) {
  uint64_t *seckey = new uint64_t[5];
  int32_t did = d_id + (w_id * 10);
  seckey[0] = did;
  convertString((char *)(&seckey[1]), s_last.data(), s_last.size());
  convertString((char *)(&seckey[3]), s_first.data(), s_last.size());
  return (uint64_t)seckey;
}

#endif


static inline ALWAYS_INLINE size_t
NumWarehouses()
{
  return (size_t) scale_factor *total_partition;
}

// config constants

static constexpr inline ALWAYS_INLINE size_t
NumItems()
{
  return 100000;
}

static constexpr inline ALWAYS_INLINE size_t
NumDistrictsPerWarehouse()
{
  return 10;
}

static constexpr inline ALWAYS_INLINE size_t
NumCustomersPerDistrict()
{
  return 3000;
}

// T must implement lock()/unlock(). Both must *not* throw exceptions
template <typename T>
class scoped_multilock {
public:
  inline scoped_multilock()
    : did_lock(false)
  {
  }

  inline ~scoped_multilock()
  {
    if (did_lock)
      for (auto &t : locks)
        t->unlock();
  }

  inline void
  enq(T &t)
  {
    ALWAYS_ASSERT(!did_lock);
    locks.emplace_back(&t);
  }

  inline void
  multilock()
  {
    ALWAYS_ASSERT(!did_lock);
    if (locks.size() > 1)
      sort(locks.begin(), locks.end());
#ifdef CHECK_INVARIANTS
    if (set<T *>(locks.begin(), locks.end()).size() != locks.size()) {
      for (auto &t : locks)
        cerr << "lock: " << hexify(t) << endl;
      INVARIANT(false && "duplicate locks found");
    }
#endif
    for (auto &t : locks)
      t->lock();
    did_lock = true;
  }

private:
  bool did_lock;
  typename util::vec<T *, 64>::type locks;
};

// like a lock_guard, but has the option of not acquiring
template <typename T>
class scoped_lock_guard {
public:
  inline scoped_lock_guard(T &l)
    : l(&l)
  {
    this->l->lock();
  }

  inline scoped_lock_guard(T *l)
    : l(l)
  {
    if (this->l)
      this->l->lock();
  }

  inline ~scoped_lock_guard()
  {
    if (l)
      l->unlock();
  }

private:
  T *l;
};

// configuration flags
static int g_disable_xpartition_txn = 0;
static int g_disable_read_only_scans = 0;
static int g_enable_partition_locks = 0;
static int g_enable_separate_tree_per_partition = 0;
static int g_new_order_remote_item_pct = 1; //ratio
static int g_new_order_fast_id_gen = 0;
static int g_uniform_item_dist = 0;
static unsigned g_txn_workload_mix[] = { 45, 43, 4, 4, 4 }; // default TPC-C workload mix

//static aligned_padded_elem<spinlock> *g_partition_locks = nullptr;
static aligned_padded_elem<atomic<uint64_t>> *g_district_ids = nullptr;

// maps a wid => partition id
static inline ALWAYS_INLINE unsigned int
PartitionId(unsigned int wid)
{
  INVARIANT(wid >= 1 && wid <= NumWarehouses());
  wid -= 1; // 0-idx
  if (NumWarehouses() <= nthreads)
    // more workers than partitions, so its easy
    return wid;
  const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
  const unsigned partid = wid / nwhse_per_partition;
  if (partid >= nthreads)
    return nthreads - 1;
  return partid;
}

static inline atomic<uint64_t> &
NewOrderIdHolder(unsigned warehouse, unsigned district)
{
  INVARIANT(warehouse >= 1 && warehouse <= NumWarehouses());
  INVARIANT(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
    (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t
FastNewOrderIdGen(unsigned warehouse, unsigned district)
{
  return NewOrderIdHolder(warehouse, district).fetch_add(1, memory_order_acq_rel);
}

struct checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static inline ALWAYS_INLINE void
  SanityCheckCustomer(const customer::key *k, const customer::value *v)
  {

#if !SHORTKEY
    INVARIANT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    INVARIANT(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    INVARIANT(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
#endif
    INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
    INVARIANT(v->c_middle == "OE");
  }

  static inline ALWAYS_INLINE void
  SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
#endif
    INVARIANT(v->w_state.size() == 2);
    INVARIANT(v->w_zip == "123456789");
  }

  static inline ALWAYS_INLINE void
  SanityCheckDistrict(const district::key *k, const district::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    INVARIANT(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
#endif
    INVARIANT(v->d_next_o_id >= 3001);
    INVARIANT(v->d_state.size() == 2);
    INVARIANT(v->d_zip == "123456789");
  }

  static inline ALWAYS_INLINE void
  SanityCheckItem(const item::key *k, const item::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
#endif
    INVARIANT(v->i_price >= 1.0 && v->i_price <= 100.0);
  }

  static inline ALWAYS_INLINE void
  SanityCheckStock(const stock::key *k, const stock::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    INVARIANT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckNewOrder(const new_order::key *k, const new_order::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    INVARIANT(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckOOrder(const oorder::key *k, const oorder::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    INVARIANT(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
#endif
    INVARIANT(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    INVARIANT(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    INVARIANT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static inline ALWAYS_INLINE void
  SanityCheckOrderLine(const order_line::key *k, const order_line::value *v)
  {
#if !SHORTKEY
    INVARIANT(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    INVARIANT(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    INVARIANT(k->ol_number >= 1 && k->ol_number <= 15);
#endif
    INVARIANT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
  }

};


struct _dummy {}; // exists so we can inherit from it, so we can use a macro in
                  // an init list...

class tpcc_worker_mixin : private _dummy {


public:
	RAWTables *store;
  tpcc_worker_mixin(RAWTables *s) :
    _dummy() // so hacky...
  {
    ALWAYS_ASSERT(NumWarehouses() >= 1);
	store = s;
  }
#if 0
#undef DEFN_TBL_INIT_X

protected:

#define DEFN_TBL_ACCESSOR_X(name) \
private:  \
  vector<abstract_ordered_index *> tbl_ ## name ## _vec; \
protected: \
  inline ALWAYS_INLINE abstract_ordered_index * \
  tbl_ ## name (unsigned int wid) \
  { \
    INVARIANT(wid >= 1 && wid <= NumWarehouses()); \
    INVARIANT(tbl_ ## name ## _vec.size() == NumWarehouses()); \
    return tbl_ ## name ## _vec[wid - 1];		       \
  }

  TPCC_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X
#endif



  // only TPCC loaders need to call this- workers are automatically
  // pinned by their worker id (which corresponds to warehouse id
  // in TPCC)
  //
  // pins the *calling* thread
  static void
  PinToWarehouseId(unsigned int wid)
  {
    const unsigned int partid = PartitionId(wid);
    ALWAYS_ASSERT(partid < nthreads);
    const unsigned int pinid  = partid;
  }

public:

  static inline uint32_t
  GetCurrentTimeMillis()
  {
    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number
    static __thread uint32_t tl_hack = 0;
    return ++tl_hack;
  }

  // utils for generating random #s and strings

  static inline ALWAYS_INLINE int
  CheckBetweenInclusive(int v, int lower, int upper)
  {
    INVARIANT(v >= lower);
    INVARIANT(v <= upper);
    return v;
  }

  static inline ALWAYS_INLINE int
  RandomNumber(fast_random &r, int min, int max)
  {
    return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
  }

  static inline ALWAYS_INLINE int
  NonUniformRandom(fast_random &r, int A, int C, int min, int max)
  {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  static inline ALWAYS_INLINE int
  GetItemId(fast_random &r)
  {
    return CheckBetweenInclusive(
				 g_uniform_item_dist ?
				 RandomNumber(r, 1, NumItems()) :
				 NonUniformRandom(r, 8191, 7911, 1, NumItems()),
				 1, NumItems());
  }

  static inline ALWAYS_INLINE int
  GetCustomerId(fast_random &r)
  {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1, NumCustomersPerDistrict());
  }

  // pick a number between [start, end)
  static inline ALWAYS_INLINE unsigned
  PickWarehouseId(fast_random &r, unsigned start, unsigned end)
  {
    INVARIANT(start < end);
    const unsigned diff = end - start;
    if (diff == 1)
      return start;
    return (r.next() % diff) + start;
  }

  static string NameTokens[];

  // all tokens are at most 5 chars long
  static const size_t CustomerLastNameMaxSize = 5 * 3;

  static inline size_t
  GetCustomerLastName(uint8_t *buf, fast_random &r, int num)
  {
    const string &s0 = NameTokens[num / 100];
    const string &s1 = NameTokens[(num / 10) % 10];
    const string &s2 = NameTokens[num % 10];
    uint8_t *const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    NDB_MEMCPY(buf, s0.data(), s0_sz); buf += s0_sz;
    NDB_MEMCPY(buf, s1.data(), s1_sz); buf += s1_sz;
    NDB_MEMCPY(buf, s2.data(), s2_sz); buf += s2_sz;
    return buf - begin;
  }

  static inline ALWAYS_INLINE size_t
  GetCustomerLastName(char *buf, fast_random &r, int num)
  {
    return GetCustomerLastName((uint8_t *) buf, r, num);
  }

  static inline string
  GetCustomerLastName(fast_random &r, int num)
  {
    string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
    return ret;
  }

  static inline ALWAYS_INLINE string
  GetNonUniformCustomerLastNameLoad(fast_random &r)
  {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
  }

  static inline ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r)
  {
    return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  static inline ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(char *buf, fast_random &r)
  {
    return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
  }

  static inline ALWAYS_INLINE string
  GetNonUniformCustomerLastNameRun(fast_random &r)
  {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline string
  RandomStr(fast_random &r, uint len)
  {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint i = 0;
    string buf(len - 1, 0);
    while (i < (len - 1)) {
      const char c = (char) r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
  static inline string
  RandomNStr(fast_random &r, uint len)
  {
    const char base = '0';
    string buf(len, 0);
    for (uint i = 0; i < len; i++)
      buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }
};

string tpcc_worker_mixin::NameTokens[] =
  {
    string("BAR"),
    string("OUGHT"),
    string("ABLE"),
    string("PRI"),
    string("PRES"),
    string("ESE"),
    string("ANTI"),
    string("CALLY"),
    string("ATION"),
    string("EING"),
  };

//STATIC_COUNTER_DECL(scopedperf::tsc_ctr, tpcc_txn, tpcc_txn_cg)

struct DeliveryPEntry
{
  RAWTables* store;
  uint64_t localsn;
  uint64_t* cus[10];
  float amounts[10];
};




struct NewOrderPEntry {
  RAWTables* store;
  int32_t warehouse_id;
  int32_t districtID;
  int32_t my_next_o_id;
  uint64_t localsn;
  uint numItem;
  uint64_t* stocks[15];
  uint quantity[15];
  uint remote[15];
};


uint32_t total_remote_ops;
uint32_t total_rdma_travel ;
uint32_t total_rdma_read  ;
uint32_t total_remote_cas ;
uint32_t total_local_cas  ;

uint32_t total_execution = 0;
uint32_t method_called = 0;


class tpcc_listener : public bench_worker, public tpcc_worker_mixin {

public:

  DBSSTX sstx;
  Network_Node* node;
  RdmaResource* rdma;
  int live_worker_num;
  double full_transaction_thr;
  double neworder_transaction_thr;

  tpcc_listener(unsigned int worker_id,
		unsigned long seed, abstract_db *db,
		spin_barrier *barrier_a, spin_barrier *barrier_b,
		uint warehouse_id_start, uint warehouse_id_end, RAWTables *store,RdmaResource *r)
  : bench_worker(worker_id, true, seed, db, barrier_a, barrier_b),
    tpcc_worker_mixin( store),
    sstx(store)
  {

    total_remote_ops  =0;
    total_rdma_travel =0;
    total_rdma_read   =0;
    total_remote_cas  =0;
    total_local_cas   =0;

    node = new Network_Node(current_partition,worker_id-8,config_file);
    rdma = r;
    rdma->node = node;

    live_worker_num = total_partition;

    rdma->Servicing();

    sleep(10);//wait for peers to start

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
    int recv_interval = 10000;
    struct ntptimeval tv;

    while(true) {

      if (recv_counter >= recv_interval) {
	std::string msg=node->tryRecv();
	if (msg != "") {

	  recv_interval = 100;
	  int pid=msg[0];
	  int nid=msg[1];
	  std::string real_msg=msg.substr(2);

	  fprintf(stdout,"recv real %s from %d\n",real_msg.c_str(),pid);
	  if(current_partition == 0) {

	    //calculate the throughput
	    std::istringstream istr;
	    istr.str(real_msg);

	    double temp;
	    istr>> temp;
	    neworder_transaction_thr += temp;
	    istr>> temp;
	    full_transaction_thr += temp;
	  }
	  live_worker_num--;
	  if(live_worker_num == 0) {
	    if(current_partition == 0) {
	      //current partition will server as the master
	      fprintf(stdout,"avg full thr: %f\n",full_transaction_thr / total_partition);
	      fprintf(stdout,"avg neworder thr: %f\n",neworder_transaction_thr / total_partition);
	      fprintf(stdout,"full thr %f\n",full_transaction_thr);
	      fprintf(stdout,"full neworder: %f\n",neworder_transaction_thr);
#ifdef RATIO
	      uint64_t total_cross_warehouse = 0;
	      uint64_t total_cross_partition = 0;
	      for(int i = 0;i < nthreads;++i) {
		total_cross_warehouse += cross_warehouse[i];
		total_cross_partition += cross_partition[i];
		//end for
	      }

	      fprintf(stdout,"cross warehouse: %lld \n",total_cross_warehouse);
	      fprintf(stdout,"cross partition: %lld \n",total_cross_partition);

#endif
	    }
	    break;
	  }
	} else {
	  recv_counter = 0;
	}
      }

      // update timestamp
      this_thread::sleep_for(chrono::microseconds(100));
      int ret ;
      if ((ret = ntp_gettime(&tv)) == 0) {
	timestamp = (tv.time.tv_sec * 1000000000 + tv.time.tv_usec) >> 1;
      } else {
	switch(ret) {
	case EFAULT:
	  fprintf(stderr,"efault\n");
	  break;
	case EOVERFLOW:
	  fprintf(stderr,"eoverflow\n");
	  break;
	default:
	  fprintf(stderr,"gettime ret: %d\n",ret);
	}
	assert(false);
      }
      recv_counter++;
    }
    printf("TOTAL remote:%d,travel:%d,read:%d,r_cas:%d,l_cas:%d\n",
	   total_remote_ops,total_rdma_travel,total_rdma_read,total_remote_cas,total_local_cas);
    delete node;
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    return w;
  }
};


class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
public:
  DBSSTX sstx;
  Network_Node* node;

  void ending(std::string msg) {
    for(int pid = 0;pid < total_partition;pid++){
      node->Send(pid,nthreads,msg);
    }
  }
  void run(){
    bench_worker::run();
  }

  //RTM profile for different txns
  static RTMProfile district_prof;
  static RTMProfile stock_prof;
  static RTMProfile customerP_prof;
  static RTMProfile customerD_prof;
  static RTMProfile neworder_prof;

  static RTMProfile readonly_prof;
  static RTMProfile payment_con_prof;
  static RTMProfile payment_con_prof1;


  //  DelayQueue *delayQueue;
  // resp for [warehouse_id_start, warehouse_id_end)
  tpcc_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint warehouse_id_start, uint warehouse_id_end, RAWTables *store,RdmaResource *rdma)
    : bench_worker(worker_id, true, seed, db,
		   barrier_a, barrier_b),
      tpcc_worker_mixin( store),
      warehouse_id_start(warehouse_id_start),
      warehouse_id_end(warehouse_id_end),
      sstx(store,rdma,worker_id - 8)
  {
    //    fprintf(stdout,"worked id: %d\n",worker_id);
    fprintf(stdout,"worker id: %d\n",sstx.thread_id);
    node=new Network_Node(current_partition,worker_id-8,config_file);
    secs = 0;
    INVARIANT(warehouse_id_start >= 1);
    INVARIANT(warehouse_id_start <= NumWarehouses());
    INVARIANT(warehouse_id_end > warehouse_id_start);
    INVARIANT(warehouse_id_end <= (NumWarehouses() + 1));
    NDB_MEMSET(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
    if (verbose) {
      cerr << "tpcc: worker id " << worker_id
	   << " => warehouses [" << warehouse_id_start
	   << ", " << warehouse_id_end << ")"
	   << endl;
    }
    obj_key0.reserve(2 * CACHELINE_SIZE);
    obj_key1.reserve(2 * CACHELINE_SIZE);
    obj_v.reserve(2 * CACHELINE_SIZE);
  }

  bool processDelayed() {
    store->ssman_->UpdateLocalSS(-1);
    return true;
  }

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;
  bool check_consistency();
  bool txn_delivery_customer(void *input, uint64_t counter);
  bool txn_delivery_customer(void *input, uint64_t counter, uint64_t localsn);
  txn_result txn_new_order();

  static txn_result
  TxnNewOrder(bench_worker *w)
  {
    txn_result r =  static_cast<tpcc_worker *>(w)->txn_new_order();
    return r;
  }

  txn_result txn_delivery();

  static txn_result
  TxnDelivery(bench_worker *w)
  {

    txn_result r =	static_cast<tpcc_worker *>(w)->txn_delivery();
    return r;
  }

  txn_result txn_payment();

  static txn_result
  TxnPayment(bench_worker *w)
  {

    txn_result r =	static_cast<tpcc_worker *>(w)->txn_payment();
    return r;
  }

  txn_result txn_order_status();

  static txn_result
  TxnOrderStatus(bench_worker *w)
  {
    txn_result r =	static_cast<tpcc_worker *>(w)->txn_order_status();
    return r;
  }

  txn_result txn_stock_level();
  txn_result txn_stock_level_rtm(); //Real RO

  static txn_result
  TxnStockLevel(bench_worker *w)
  {
    txn_result r =	static_cast<tpcc_worker *>(w)->txn_stock_level_rtm();
    return r;
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
      w.push_back(workload_desc("NewOrder", double(g_txn_workload_mix[0])/100.0, TxnNewOrder));
    if (g_txn_workload_mix[1])
      w.push_back(workload_desc("Payment", double(g_txn_workload_mix[1])/100.0, TxnPayment));
    if (g_txn_workload_mix[2])
      w.push_back(workload_desc("Delivery", double(g_txn_workload_mix[2])/100.0, TxnDelivery));
    if (g_txn_workload_mix[3])
      w.push_back(workload_desc("OrderStatus", double(g_txn_workload_mix[3])/100.0, TxnOrderStatus));
    if (g_txn_workload_mix[4])
      w.push_back(workload_desc("StockLevel", double(g_txn_workload_mix[4])/100.0, TxnStockLevel));
    return w;
  }

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



private:
  const uint warehouse_id_start;
  const uint warehouse_id_end;
  int32_t last_no_o_ids[10]; // XXX(stephentu): hack

  // some scratch buffer space
  string obj_key0;
  string obj_key1;
  string obj_v;
};

RTMProfile tpcc_worker::district_prof;
RTMProfile tpcc_worker::stock_prof;
RTMProfile tpcc_worker::customerP_prof;
RTMProfile tpcc_worker::customerD_prof;
RTMProfile tpcc_worker::neworder_prof;
RTMProfile tpcc_worker::readonly_prof;
RTMProfile tpcc_worker::payment_con_prof;
RTMProfile tpcc_worker::payment_con_prof1;


class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_warehouse_loader(unsigned long seed,
                        abstract_db *db,
                        RAWTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store)
  {}

protected:
  virtual void
  load()
  {
    string obj_buf;
    printf("load warehouse\n");
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    try {
      vector<warehouse::value> warehouses;
      for (uint i = get_start_wid(); i <= get_end_wid(); i++) {

        const warehouse::key k(i);
        const string w_name = RandomStr(r, RandomNumber(r, 6, 10));
        const string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_city = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_state = RandomStr(r, 3);
        const string w_zip = "123456789";

	char *wrapper = new char[META_LENGTH+sizeof(warehouse::value)];
	memset(wrapper, 0, META_LENGTH);
	warehouse::value *v = (warehouse::value *)(wrapper + META_LENGTH);
	v->w_ytd = 300000 * 100;
	v->w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;
	v->w_name.assign(w_name);
	v->w_street_1.assign(w_street_1);
	v->w_street_2.assign(w_street_2);
	v->w_city.assign(w_city);
	v->w_state.assign(w_state);
        v->w_zip.assign(w_zip);

        checker::SanityCheckWarehouse(&k, v);
        const size_t sz = Size(*v);
        warehouse_total_sz += sz;
        n_warehouses++;

	store->Put(WARE, i, (uint64_t *)wrapper);

	if (Encode(k).size() !=8) cerr << Encode(k).size() << endl;
        warehouses.push_back(*v);
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading warehouse" << endl;
      cerr << "[INFO]   * average warehouse record length: "
           << (double(warehouse_total_sz)/double(n_warehouses)) << " bytes" << endl;
    }
  }
};

class tpcc_item_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_item_loader(unsigned long seed,
                   abstract_db *db,
		   RAWTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store)
  {}

protected:
  virtual void
  load()
  {
    string obj_buf;
    uint64_t total_sz = 0;
    try {
      for (uint i = 1; i <= NumItems(); i++) {
        const item::key k(i);
	char *wrapper = new char[META_LENGTH+sizeof(item::value)];
	memset(wrapper, 0, META_LENGTH);
        item::value *v = (item::value *)(wrapper + META_LENGTH);;
        const string i_name = RandomStr(r, RandomNumber(r, 14, 24));
        v->i_name.assign(i_name);
        v->i_price = (float) RandomNumber(r, 100, 10000) / 100.0;
        const int len = RandomNumber(r, 26, 50);
        if (RandomNumber(r, 1, 100) > 10) {
          const string i_data = RandomStr(r, len);
          v->i_data.assign(i_data);
        } else {
          const int startOriginal = RandomNumber(r, 2, (len - 8));
          const string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
          v->i_data.assign(i_data);
        }
        v->i_im_id = RandomNumber(r, 1, 10000);

        checker::SanityCheckItem(&k, v);
        const size_t sz = Size(*v);
        total_sz += sz;


	store->Put(ITEM, i, (uint64_t *)wrapper);
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading item" << endl;
      cerr << "[INFO]   * average item record length: "
           << (double(total_sz)/double(NumItems())) << " bytes" << endl;
    }
  }
};

class tpcc_stock_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_stock_loader(unsigned long seed,
                    abstract_db *db,
                    ssize_t warehouse_id,
		    RAWTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin(store),
      warehouse_id(warehouse_id)
  {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

protected:
  virtual void
  load()
  {
    string obj_buf, obj_buf1;

    uint64_t stock_total_sz = 0, n_stocks = 0;
    const uint w_start = get_start_wid();
    const uint w_end   = get_end_wid();

    for (uint w = w_start; w <= w_end; w++) {
      const size_t batchsize =
	NumItems() ;
      const size_t nbatches = (batchsize > NumItems()) ? 1 : (NumItems() / batchsize);

      if (pin_cpus)
        PinToWarehouseId(w);

      for (uint b = 0; b < nbatches;) {
	//        scoped_str_arena s_arena(arena);
        try {
          const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
          for (uint i = (b * batchsize + 1); i <= iend; i++) {
	    uint64_t key = makeStockKey(w, i);
#if SHORTKEY
            const stock::key k(makeStockKey(w, i));
#else
	    const stock::key k(w,i);

#endif
	    //            const stock_data::key k_data(w, i);

	    char *wrapper = new char[META_LENGTH+sizeof(stock::value)];
	    memset(wrapper, 0, META_LENGTH);
	    stock::value *v = (stock::value *)(wrapper + META_LENGTH);
	    v->s_quantity = RandomNumber(r, 10, 100);
	    v->s_ytd = 0;
	    v->s_order_cnt = 0;
	    v->s_remote_cnt = 0;

            const int len = RandomNumber(r, 26, 50);
            if (RandomNumber(r, 1, 100) > 10) {
              const string s_data = RandomStr(r, len);
            } else {
              const int startOriginal = RandomNumber(r, 2, (len - 8));
              const string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
	      //			  v->s_data.assign(s_data);
	      //			  v_data.s_data.assign(s_data);
            }
	    /*
				v_data.s_dist_01.assign(RandomStr(r, 24));
				v_data.s_dist_02.assign(RandomStr(r, 24));
				v_data.s_dist_03.assign(RandomStr(r, 24));
				v_data.s_dist_04.assign(RandomStr(r, 24));
				v_data.s_dist_05.assign(RandomStr(r, 24));
				v_data.s_dist_06.assign(RandomStr(r, 24));
				v_data.s_dist_07.assign(RandomStr(r, 24));
				v_data.s_dist_08.assign(RandomStr(r, 24));
				v_data.s_dist_09.assign(RandomStr(r, 24));
				v_data.s_dist_10.assign(RandomStr(r, 24));


				v->s_dist_01.assign(RandomStr(r, 24));
				v->s_dist_02.assign(RandomStr(r, 24));
				v->s_dist_03.assign(RandomStr(r, 24));
				v->s_dist_04.assign(RandomStr(r, 24));
				v->s_dist_05.assign(RandomStr(r, 24));
				v->s_dist_06.assign(RandomStr(r, 24));
				v->s_dist_07.assign(RandomStr(r, 24));
				v->s_dist_08.assign(RandomStr(r, 24));
				v->s_dist_09.assign(RandomStr(r, 24));
				v->s_dist_10.assign(RandomStr(r, 24));
	    */



            checker::SanityCheckStock(&k, v);
            const size_t sz = Size(*v);
            stock_total_sz += sz;
            n_stocks++;


	    store->Put(STOC, key, (uint64_t *)wrapper);
          }
            b++;
        } catch (abstract_db::abstract_abort_exception &ex) {
          ALWAYS_ASSERT(warehouse_id != -1);
          if (verbose)
            cerr << "[WARNING] stock loader loading abort" << endl;
        }
      }
    }

    if (verbose) {
      if (warehouse_id == -1) {
        cerr << "[INFO] finished loading stock" << endl;
        cerr << "[INFO]   * average stock record length: "
             << (double(stock_total_sz)/double(n_stocks)) << " bytes" << endl;
      } else {
        cerr << "[INFO] finished loading stock (w=" << warehouse_id << ")" << endl;
      }
    }
  }

private:
  ssize_t warehouse_id;
};

class tpcc_district_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_district_loader(unsigned long seed,
                       abstract_db *db,
                        RAWTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store)
  {}

protected:
  virtual void
  load()
  {
    string obj_buf;

    const ssize_t bsize = -1;
    uint64_t district_total_sz = 0, n_districts = 0;
    try {
      uint cnt = 0;
      for (uint w = get_start_wid(); w <= get_end_wid(); w++) {

        if (pin_cpus)
          PinToWarehouseId(w);
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
	  uint64_t key = makeDistrictKey(w, d);
#if SHORTKEY

	  const district::key k(makeDistrictKey(w, d));
#else
          const district::key k(w, d);
#endif

	  char *wrapper = new char[META_LENGTH+sizeof(district::value)];
	  memset(wrapper, 0, META_LENGTH);
	  district::value *v = (district::value *)(wrapper + META_LENGTH);
	  v->d_ytd = 30000 * 100; //notice i did the scale up
          v->d_tax = (float) (RandomNumber(r, 0, 2000) / 10000.0);
          v->d_next_o_id = 3001;
          v->d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
          v->d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          v->d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          v->d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          v->d_state.assign(RandomStr(r, 3));
          v->d_zip.assign("123456789");

          const size_t sz = Size(*v);
          district_total_sz += sz;
          n_districts++;

	  store->Put(DIST, key, (uint64_t *)wrapper);
        }
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading district" << endl;
      cerr << "[INFO]   * average district record length: "
           << (double(district_total_sz)/double(n_districts)) << " bytes" << endl;
    }
  }
};

class tpcc_customer_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_customer_loader(unsigned long seed,
                       abstract_db *db,
                       ssize_t warehouse_id,
		       RAWTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin(store),
      warehouse_id(warehouse_id)
  {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

protected:
  virtual void
  load()
  {
    string obj_buf;

    // const uint w_start = (warehouse_id == -1) ?
    //   1 : static_cast<uint>(warehouse_id);
    // const uint w_end   = (warehouse_id == -1) ?
    //   NumWarehouses() : static_cast<uint>(warehouse_id);

    const uint w_start = get_start_wid();
    const uint w_end   = get_end_wid();

    const size_t batchsize =

      NumCustomersPerDistrict() ;
    const size_t nbatches =
      (batchsize > NumCustomersPerDistrict()) ?
      1 : (NumCustomersPerDistrict() / batchsize);
    cerr << "num batches: " << nbatches << endl;

    uint64_t total_sz = 0;

    for (uint w = w_start; w <= w_end; w++) {
      if (pin_cpus)
        PinToWarehouseId(w);
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        for (uint batch = 0; batch < nbatches;) {
	  //          scoped_str_arena s_arena(arena);
          const size_t cstart = batch * batchsize;
          const size_t cend = std::min((batch + 1) * batchsize, NumCustomersPerDistrict());
          try {
            for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
              const uint c = cidx0 + 1;
	      uint64_t key = makeCustomerKey(w, d, c);
#if SHORTKEY
              const customer::key k(makeCustomerKey(w, d, c));
#else
	      const customer::key k(w, d, c);

#endif

	      char *wrapper = new char[META_LENGTH+ sizeof(customer::value)];
	      memset(wrapper, 0, META_LENGTH);
              customer::value *v = (customer::value *)(wrapper + META_LENGTH);
              v->c_discount = (float) (RandomNumber(r, 1, 5000) / 10000.0);
              if (RandomNumber(r, 1, 100) <= 10)
                v->c_credit.assign("BC");
              else
                v->c_credit.assign("GC");

              if (c <= 1000)
                v->c_last.assign(GetCustomerLastName(r, c - 1));
              else
                v->c_last.assign(GetNonUniformCustomerLastNameLoad(r));

              v->c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
              v->c_credit_lim = 50000;

              v->c_balance = -10;
	      v->c_balance_1 = 0;
              v->c_ytd_payment = 10;
              v->c_payment_cnt = 1;
              v->c_delivery_cnt = 0;

              v->c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
              v->c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
              v->c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
              v->c_state.assign(RandomStr(r, 3));
              v->c_zip.assign(RandomNStr(r, 4) + "11111");
              v->c_phone.assign(RandomNStr(r, 16));
              v->c_since = GetCurrentTimeMillis();
              v->c_middle.assign("OE");
              v->c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

              const size_t sz = Size(*v);
              total_sz += sz;

	      store->Put(CUST, key, (uint64_t *)wrapper);
              // customer name index

	      uint64_t sec = makeCustomerIndex(w, d,
					       v->c_last.str(true), v->c_first.str(true));
#if SHORTKEY
              const customer_name_idx::key k_idx(w*10+d, v->c_last.str(true), v->c_first.str(true));
#else
	      const customer_name_idx::key k_idx(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));

#endif
              const customer_name_idx::value v_idx(k.c_id);

              // index structure is:
              // (c_w_id, c_d_id, c_last, c_first) -> (c_id)
	      uint64_t* mn = store->Get(CUST_INDEX,sec);
	      if (mn == NULL) {
		char *ciwrap = new char[META_LENGTH + sizeof(uint64_t)*2];
		memset(ciwrap, 0, META_LENGTH);
		uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);
		prikeys[0] = 1; prikeys[1] = key;
		//printf("key %ld\n",key);
		store->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
	      }
	      else {
		printf("ccccc\n");
		uint64_t *value = (uint64_t *)((char *)mn + META_LENGTH);
		int num = value[0];
		char *ciwrap = new char[META_LENGTH + sizeof(uint64_t)*(num+2)];
		memset(ciwrap, 0, META_LENGTH);
		uint64_t *prikeys = (uint64_t *)(ciwrap + META_LENGTH);
		prikeys[0] = num + 1;
		for (int i=1; i<=num; i++)
		  prikeys[i] = value[i];
		prikeys[num+1] = key;
		store->Put(CUST_INDEX, sec, (uint64_t*)ciwrap);
		//delete[] value;
	      }
	      //cerr << Encode(k_idx).size() << endl;
	      char *hwrap = new char[META_LENGTH + sizeof(history::value)];
	      memset(hwrap, 0, META_LENGTH);

	      uint64_t hkey = makeHistoryKey(c,d,w,d,w);
#if SHORTKEY
	      history::key k_hist(makeHistoryKey(c,d,w,d,w));
#else
              history::key k_hist;
              k_hist.h_c_id = c;
              k_hist.h_c_d_id = d;
              k_hist.h_c_w_id = w;
              k_hist.h_d_id = d;
              k_hist.h_w_id = w;
              k_hist.h_date = GetCurrentTimeMillis();
#endif

              history::value *v_hist = (history::value*)(hwrap + META_LENGTH);
              v_hist->h_amount = 10;
              v_hist->h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

	      store->Put(HIST, hkey, (uint64_t *)hwrap);
	      if (Encode(k_hist).size() !=8)cerr << Encode(k_hist).size() << endl;
            }
              batch++;

          } catch (abstract_db::abstract_abort_exception &ex) {
            if (verbose)
              cerr << "[WARNING] customer loader loading abort" << endl;
          }
        }
      }
    }

    if (verbose) {
      if (warehouse_id == -1) {
        cerr << "[INFO] finished loading customer" << endl;
        cerr << "[INFO]   * average customer record length: "
             << (double(total_sz)/double(NumWarehouses()*NumDistrictsPerWarehouse()*NumCustomersPerDistrict()))
             << " bytes " << endl;
      } else {
        cerr << "[INFO] finished loading customer (w=" << warehouse_id << ")" << endl;
      }
    }
  }

private:
  ssize_t warehouse_id;
};

class tpcc_order_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_order_loader(unsigned long seed,
                    abstract_db *db,
                    ssize_t warehouse_id,
		    RAWTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store),
      warehouse_id(warehouse_id)
  {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

protected:
  virtual void
  load()
  {
    string obj_buf;

    uint64_t order_line_total_sz = 0, n_order_lines = 0;
    uint64_t oorder_total_sz = 0, n_oorders = 0;
    uint64_t new_order_total_sz = 0, n_new_orders = 0;

    // const uint w_start = (warehouse_id == -1) ?
    //   1 : static_cast<uint>(warehouse_id);
    // const uint w_end   = (warehouse_id == -1) ?
    //   NumWarehouses() : static_cast<uint>(warehouse_id);
    const uint w_start = get_start_wid();
    const uint w_end   = get_end_wid();

    for (uint w = w_start; w <= w_end; w++) {
      if (pin_cpus)
        PinToWarehouseId(w);
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        set<uint> c_ids_s;
        vector<uint> c_ids;
        while (c_ids.size() != NumCustomersPerDistrict()) {
          const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
          if (c_ids_s.count(x))
            continue;
          c_ids_s.insert(x);
          c_ids.emplace_back(x);
        }
        for (uint c = 1; c <= NumCustomersPerDistrict();) {
	  //          scoped_str_arena s_arena(arena);
          try {
	    uint64_t okey = makeOrderKey(w, d, c);
#if SHORTKEY
            const oorder::key k_oo(makeOrderKey(w, d, c));
#else
	    const oorder::key k_oo(w, d, c);
#endif

	    char *wrapper = new char[META_LENGTH+sizeof(oorder::value)];
	    memset(wrapper, 0 ,META_LENGTH);
            oorder::value *v_oo = (oorder::value *)(wrapper + META_LENGTH);
            v_oo->o_c_id = c_ids[c - 1];
            if (c < 2101)
              v_oo->o_carrier_id = RandomNumber(r, 1, 10);
            else
              v_oo->o_carrier_id = 0;
            v_oo->o_ol_cnt = RandomNumber(r, 5, 15);

            v_oo->o_all_local = 1;
            v_oo->o_entry_d = GetCurrentTimeMillis();
            checker::SanityCheckOOrder(&k_oo, v_oo);
            const size_t sz = Size(*v_oo);
            oorder_total_sz += sz;
            n_oorders++;
	    store->Put(ORDE, okey, (uint64_t *)wrapper);


	    uint64_t sec = makeOrderIndex(w, d, v_oo->o_c_id, c);

	    char *oiwrapper = new char[META_LENGTH+16];
	    memset(oiwrapper, 0 ,META_LENGTH);
	    uint64_t *prikeys = (uint64_t *)(oiwrapper+META_LENGTH);
	    prikeys[0] = 1; prikeys[1] = okey;
	    store->Put(ORDER_INDEX, sec, (uint64_t *)oiwrapper);

#if SHORTKEY
            const oorder_c_id_idx::key k_oo_idx(makeOrderIndex(w, d, v_oo->o_c_id, c));
#else
            const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo->o_c_id, k_oo.o_id);
#endif
            const oorder_c_id_idx::value v_oo_idx(0);

            if (c >= 2101) {
	      uint64_t nokey = makeNewOrderKey(w, d, c);
#if SHORTKEY
              const new_order::key k_no(makeNewOrderKey(w, d, c));
#else
	      const new_order::key k_no(w, d, c);
#endif
	      char* nowrap = new char[META_LENGTH + sizeof(new_order::value)];
	      memset(nowrap, 0, META_LENGTH);
	      new_order::value *v_no = (new_order::value *)(nowrap+META_LENGTH);

              checker::SanityCheckNewOrder(&k_no, v_no);
              const size_t sz = Size(*v_no);
              new_order_total_sz += sz;
              n_new_orders++;
	      store->Put(NEWO, nokey, (uint64_t *)nowrap);
            }

#if 1
            for (uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
	      uint64_t olkey = makeOrderLineKey(w, d, c, l);
#if SHORTKEY
              const order_line::key k_ol(makeOrderLineKey(w, d, c, l));
#else
              const order_line::key k_ol(w, d, c, l);
#endif

	      char *olwrapper = new char[META_LENGTH+sizeof(order_line::value)];
	      memset(olwrapper, 0 ,META_LENGTH);
	      order_line::value *v_ol = (order_line::value *)(olwrapper + META_LENGTH);
              v_ol->ol_i_id = RandomNumber(r, 1, 100000);
              if (c < 2101) {
                v_ol->ol_delivery_d = v_oo->o_entry_d;
                v_ol->ol_amount = 0;
              } else {
                v_ol->ol_delivery_d = 0;
                // random within [0.01 .. 9,999.99]
                v_ol->ol_amount = (float) (RandomNumber(r, 1, 999999) / 100.0);
              }

              v_ol->ol_supply_w_id = w;
              v_ol->ol_quantity = 5;
              // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
              //v_ol.ol_dist_info = RandomStr(r, 24);

              checker::SanityCheckOrderLine(&k_ol, v_ol);
              const size_t sz = Size(*v_ol);
              order_line_total_sz += sz;
              n_order_lines++;
	      store->Put(ORLI, olkey, (uint64_t *)olwrapper);
            }
#endif
              c++;
          } catch (abstract_db::abstract_abort_exception &ex) {
            ALWAYS_ASSERT(warehouse_id != -1);
            if (verbose)
              cerr << "[WARNING] order loader loading abort" << endl;
          }
        }
      }
    }

    if (verbose) {
      if (warehouse_id == -1) {
        cerr << "[INFO] finished loading order" << endl;
        cerr << "[INFO]   * average order_line record length: "
             << (double(order_line_total_sz)/double(n_order_lines)) << " bytes" << endl;
        cerr << "[INFO]   * average oorder record length: "
             << (double(oorder_total_sz)/double(n_oorders)) << " bytes" << endl;
        cerr << "[INFO]   * average new_order record length: "
             << (double(new_order_total_sz)/double(n_new_orders)) << " bytes" << endl;
      } else {
        cerr << "[INFO] finished loading order (w=" << warehouse_id << ")" << endl;
      }
    }
  }

private:
  ssize_t warehouse_id;
};



bool tpcc_worker::check_consistency(){
  fprintf(stdout,"check_consistency\n");
#if CHECK_FLAG
  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);

  DBSSTX tx(store);
  tx.Begin(false);
  uint64_t *w_value;
  tx.Get(WARE, warehouse_id, &w_value, false);
  warehouse::value *v_w = (warehouse::value*)w_value;
  AMOUNT w_ytd = (AMOUNT)(v_w->w_ytd / 100.0);
  AMOUNT d_ytd_total = 0;
  //h_amount_wh = 0;
  fprintf(stdout,"WH %d check\n", warehouse_id);

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    uint64_t d_key = makeDistrictKey(warehouse_id, d);
    uint64_t *d_value;
    tx.Get(DIST, d_key, &d_value, false);
    district::value *v_d = (district::value *)d_value;
    d_ytd_total += (AMOUNT)(v_d->d_ytd / 100.0);
    int32_t d_next_o_id = v_d->d_next_o_id;
    int32_t minn = -1, maxn = -1, countn = 0, mino = -1, maxo = -1, counto = 0;
    int32_t o_ol_cnt_total = 0, ol_cnt = 0; AMOUNT h_amount_d = 0, d_ytd = v_d->d_ytd;
    printf("district %d check start\n", d);

    //neworder check for min order, max order and order count(not deleted)
    {
      uint64_t no_start = makeNewOrderKey(warehouse_id, d, 0);
      uint64_t no_end = makeNewOrderKey(warehouse_id, d+1, 0);
      DBSSTX::Iterator* iter = tx.GetIterator(NEWO);
      iter->Seek(no_start);
      //Get the min no_o_id for the district
      minn = static_cast<int32_t>(iter->Key() << 32 >> 32);
      while (iter->Valid()) {
	//count the neworder entries
	countn++;
	//Get the max no_o_id for the district
	maxn = static_cast<int32_t>(iter->Key() << 32 >> 32);
	if (!iter->Next(no_end)) break;
      }
    }
    {
      uint64_t o_start = makeOrderKey(warehouse_id, d, 1);
      uint64_t o_end = makeOrderKey(warehouse_id, d+1, 1);
      DBSSTX::Iterator* iter = tx.GetIterator(ORDE);
      iter->Seek(o_start);
      mino = static_cast<int32_t>(iter->Key() << 32 >> 32);
      while (iter->Valid()) {
	counto++;
	maxo = static_cast<int32_t>(iter->Key() << 32 >> 32);
	if (!iter->Next(o_end)) break;
      }

      for (int32_t so = mino; so<=maxo; so++) {
	uint64_t o_key = makeOrderKey(warehouse_id, d, so);
	uint64_t *o_value;
	tx.Get(ORDE, o_key, &o_value, false);
	oorder::value* v_oo = (oorder::value *)o_value;
	o_ol_cnt_total += v_oo->o_ol_cnt;

	uint64_t *no_value;
	bool found = tx.Get(NEWO, o_key, &no_value, false);
	if (v_oo->o_carrier_id == 0) {
	  if (!found)
	    printf("Order Should Not be delivered!\n");
	}
	else {
	  if (found)
	    printf("Order Should be delivered!\n");
	}
	int32_t olcount = 0;
	uint64_t ol_start = makeOrderLineKey(warehouse_id, d, so, 1);
	uint64_t ol_end = makeOrderLineKey(warehouse_id, d, so, 15);
	DBSSTX::Iterator* oliter = tx.GetIterator(ORLI);
	oliter->Seek(ol_start);
	while (oliter->Valid()) {
	  if (oliter->Key() <= ol_end) olcount++;
	  else break;
	  oliter->Next();
	}
	if (v_oo->o_ol_cnt!=olcount)
	  printf("Order %d orderline %d\n", v_oo->o_ol_cnt, olcount);
      }
    }

    printf("district %d OL check start\n", d);
    //orderline total count
    //for each orderline non-zero delivery_d, order must have a valid carrier_id and vice versa

    {
      uint64_t ol_start = makeOrderLineKey(warehouse_id, d, 0, 1);
      uint64_t ol_end = makeOrderLineKey(warehouse_id, d+1, 0, 1);
      DBSSTX::Iterator* oliter = tx.GetIterator(ORLI);
      oliter->Seek(ol_start);
      while (oliter->Valid()) {
	if (oliter->Key() < ol_end) ol_cnt++;
	else break;
	oliter->Next();
      }
      for (int32_t so = mino; so<=maxo; so++) {
	for (int32_t ol = 1; ol <=15; ol++) {
	  uint64_t ol_key = makeOrderLineKey(warehouse_id, d, so, ol);
	  uint64_t *ol_value;
	  bool found = tx.Get(ORLI, ol_key, &ol_value, false);
	  if (!found) break;
	  order_line::value *v_ol = (order_line::value *)ol_value;
	  uint64_t o_key = makeOrderKey(warehouse_id, d, so);
	  uint64_t *o_value;
	  tx.Get(ORDE, o_key, &o_value, false);
	  oorder::value* v_oo = (oorder::value *)o_value;
	  if (v_ol->ol_delivery_d == 0) {
	    if (v_oo->o_carrier_id!=0)
	      printf("Order %d OL %d Order Should Not Be Delivered! %d %d\n",
		     so,ol,v_oo->o_carrier_id,v_ol->ol_delivery_d);
	  }
	  else {
	    if (v_oo->o_carrier_id==0)
	      printf("OL %d Order Should Be Delivered! %d %d\n",ol, v_oo->o_carrier_id,v_ol->ol_delivery_d);
	  }
	}
      }
    }
#if 0
    printf("district %d History check start\n", d);
    //total history amount for a district
    {
      uint64_t h_start = makeHistoryKey(0,0,0,d,warehouse_id);
      uint64_t h_end = makeHistoryKey(NumCustomersPerDistrict(),NumDistrictsPerWarehouse(),
				      NumWarehouses(),d,warehouse_id);
      DBSSTX::iterator *iter = tx.GetIterator(HIST);
			iter.Seek(h_start);
			while (iter.Valid()) {
			  if (iter.Key() > h_end) break;
			  history::value *v_h = (history::value *)(iter.Value());
			  h_amount_d += v_h->h_amount;
			}
    }
#endif
    //customer balance/history amount/orderline amount check
    {
      for (int32_t c=1; c<=(int32_t)NumCustomersPerDistrict(); c++) {
	uint64_t c_key = makeCustomerKey(warehouse_id, d, c);
	uint64_t *c_value;
	tx.Get(CUST, c_key, &c_value, false);
	customer::value *v_c = (customer::value *)c_value;

	AMOUNT h_amount_total = 0, ol_amount_total = 0;
#if 0

	for (int wh = 1; wh<=NumWarehouses(); wh++){
	  for (int ds =1; ds <= NumDistrictsPerWarehouse(); ds++){
	    uint64_t h_start = makeHistoryKey(c, d, warehouse_id, ds, wh);
	  }
	}
#endif
	for (int32_t so = mino; so <= maxo; so++) {
	  uint64_t o_key = makeOrderKey(warehouse_id, d, so);
	  uint64_t *o_value;
	  tx.Get(ORDE, o_key, &o_value, false);
	  oorder::value* v_oo = (oorder::value *)o_value;
	  if (v_oo->o_c_id != c) continue;
	  uint64_t ol_start = makeOrderLineKey(warehouse_id,d,so,1);
	  uint64_t ol_end = makeOrderLineKey(warehouse_id,d,so,15);

	  DBSSTX::Iterator* oliter = tx.GetIterator(ORLI);
	  oliter->Seek(ol_start);
	  while (oliter->Valid()) {
	    if (oliter->Key() <= ol_end) {
	      if (v_oo->o_carrier_id!=0)
		ol_amount_total += ((order_line::value *)(oliter->Value()))->ol_amount;
	    }
	    else break;
	    oliter->Next();
	  }
	}
	v_c->c_balance += v_c->c_balance_1;
	v_c->c_balance += v_c->c_balance_2;

	if (fabs(ol_amount_total) <= fabs(100*v_c->c_balance)) {
	  AMOUNT delta = fabs(v_c->c_balance*0.001);
	  if(fabs(v_c->c_balance+v_c->c_ytd_payment-ol_amount_total) > delta)
	    printf("then check balance %f %f %f %f delta %f\n", v_c->c_balance, v_c->c_ytd_payment,
		   v_c->c_balance+v_c->c_ytd_payment,ol_amount_total, delta);
	}
	else {
	  AMOUNT delta = fabs(ol_amount_total*0.001);
	  if(fabs(v_c->c_balance+v_c->c_ytd_payment-ol_amount_total) > delta)
	    printf("ol then check balance %f %f %f %f delta %f\n", v_c->c_balance, v_c->c_ytd_payment,
		   v_c->c_balance+v_c->c_ytd_payment,ol_amount_total, delta);

	}

      }
    }

    printf("district check %d\n", d);
    if(d_next_o_id-1!=maxo)
      printf("check next_o_id %d %d\n", d_next_o_id, maxo);
    INVARIANT(d_next_o_id-1==maxo);
    if(maxn > 0){
      if(d_next_o_id-1!=maxn)
	printf("check maxn %d %d\n", d_next_o_id, maxn);
      INVARIANT(d_next_o_id-1==maxn);
    }
    if(countn > 0){
      if(maxn-minn+1!=countn)
	printf("check countn %d %d %d\n", maxn, minn, countn);
      INVARIANT(maxn-minn+1==countn);
    }
    if(o_ol_cnt_total!=ol_cnt)
      printf("check ol cnt %d %d\n", o_ol_cnt_total, ol_cnt);
    INVARIANT(o_ol_cnt_total==ol_cnt);
    printf("Worker %d District %d WH %d Order:%d - NewOrder:%d = %d\n", worker_id, d, warehouse_id, counto, countn, (counto-countn));
  }

  AMOUNT delta = fabs(w_ytd*0.001);
  if(fabs(w_ytd-d_ytd_total)>=delta)
    printf("check wh ytd %f %f\n", w_ytd, d_ytd_total);
  INVARIANT(fabs(w_ytd-d_ytd_total)<delta);
  tx.End();
  return true;
#endif
}



tpcc_worker::txn_result
tpcc_worker::txn_new_order()
{

  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint districtID = RandomNumber(r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(r, 5, 15);
  uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
  float i_prices[15]; //to buffer the results of the 1 hop

  bool allLocal = true;
  bool warehouses[NumWarehouses()];

#ifdef RATIO
  int cross_warehouse_count = 0;
  int cross_partition_count = 0;
#endif

  for (int i=0; i<NumWarehouses(); i++)
  	warehouses[i] = false;
  warehouses[warehouse_id-1] = true;
  int numW = 1;
  std::set<uint64_t> stock_set;//remove identity stock ids
  for (uint i = 0; i < numItems; i++) {
    bool conflict=false;
    itemIDs[i] = GetItemId(r);
    if (likely(g_disable_xpartition_txn ||
               NumWarehouses() == 1 ||
               RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
      uint64_t s_key = makeStockKey(supplierWarehouseIDs[i] , itemIDs[i]);
      if(stock_set.find(s_key)!=stock_set.end()){
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
    } else {
#ifdef RATIO
      cross_warehouse_count = cross_warehouse_count || 1;
#endif
      do {
        supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
        uint64_t s_key = makeStockKey(supplierWarehouseIDs[i] , itemIDs[i]);
        if(stock_set.find(s_key)!=stock_set.end()){
          conflict=true;
        } else {
          stock_set.insert(s_key);
        }
      } while (supplierWarehouseIDs[i] == warehouse_id);
      if(conflict){
        i--;
        continue;
      }
      allLocal = false;
      if (!warehouses[supplierWarehouseIDs[i]-1]) {
	  	warehouses[supplierWarehouseIDs[i]-1] = true;
		numW++;
      }

    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }

  INVARIANT(!g_disable_xpartition_txn || allLocal);
  SpinLock** spinlocks= new SpinLock*[numW];
  numW = 0;
  for (int i=0; i<NumWarehouses(); i++) {
    if (warehouses[i]) {
      spinlocks[numW] = &store->neworder_lock[i];
      numW++;
    }
  }
  // if (numW > 1) printf("NO LOCK %d\n", numW);
  ssize_t ret = 0;

  bool readonly = false;

  //TX BEGIN
  sstx.Begin(readonly);

  bool found = false;

  //STEP 1 Check ITEM, get i_price (Only first hop can has abort operation) (Original Step 6)
  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_i_id = itemIDs[ol_number - 1];

    uint64_t* i_value;

    //Get Item for read
    found = sstx.Get(ITEM, ol_i_id, &i_value, false);
    if(!found) {
      return txn_result(sstx.Abort(), ret);
    }

    item::value *v_i = (item::value *)i_value;
    checker::SanityCheckItem(NULL, v_i);

    i_prices[ol_number - 1] = v_i->i_price;
  }
  //STEP 3: read w_tax from WAREHOUSE  (Original Step 1)
  uint64_t *w_value;
  found = sstx.Get(WARE, warehouse_id, &w_value, false);
  assert(found);

  warehouse::value *v_w = (warehouse::value *)w_value;
  checker::SanityCheckWarehouse(NULL, v_w);

  //STEP 4: read c_discount, c_last, c_credit from CUSTOMER (Original Step 3)
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);//TODO!! add locks for this later
  uint64_t *c_value;
  found = sstx.Get(CUST, c_key, &c_value, false);
  assert(found);
  customer::value *v_c = (customer::value *)c_value;

  //3 Start First Conflict Hop
  //STEP 2. DIST & NEWO (Original Step 2, 4)

  //Traverse the tree to get record (No protection)
  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
  uint64_t *d_value_loc;
  found = sstx.GetLoc(DIST, d_key, &d_value_loc);
  assert(found);

  sstx.AddToLocalWriteSet(DIST,d_key,current_partition,d_value_loc);

  NewOrderPEntry pentry;// = new NewOrderPEntry();

  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    uint64_t* s_value_loc;
    uint64_t s_key = makeStockKey(ol_supply_w_id, ol_i_id);

    int pid;
    if( (pid = wid_to_pid(ol_supply_w_id)) != current_partition) {
      s_value_loc = (uint64_t *)(new char[ sstx.txdb_->schemas[STOC].vlen + META_LENGTH]);
      sstx.AddToRemoteWriteSet(STOC,s_key,pid,s_value_loc);
#ifdef RATIO
      cross_partition_count = cross_partition_count || 1;
#endif
    }else {
      found = sstx.GetLoc(STOC, s_key, &s_value_loc);
      assert(found);

      sstx.AddToLocalWriteSet(STOC,s_key,pid,s_value_loc);
    }
    pentry.stocks[ol_number - 1] = s_value_loc;
    pentry.quantity[ol_number - 1] = orderQuantities[ol_number - 1];
    pentry.remote[ol_number - 1] = (ol_supply_w_id == warehouse_id) ? 0 : 1;
  }

  drtm::RawHashTable* cache =  store->ol_cache[warehouse_id-1];
  order_line_short::value *ol_shorts = new order_line_short::value[numItems];

  //reallocate the code to find the dist ptr first
  uint64_t *d_value;
  found = sstx.GetAt(DIST, d_key, &d_value, true, d_value_loc);
  assert(found);
  // Protect the dist record update and neworder insertion

  // acquire remote lock
  uint64_t endtime = timestamp + DEFAULT_INTERVAL;//remote timestamps
  sstx.PrefetchAllRemote(endtime);
  //  RTMTX::RdmaBegin(spinlocks,numW,&district_prof);
  RTMTX::RdmaBegin(spinlocks,numW,NULL);

  if ( !_xtest()) {
    //fallback path
    sstx.Fallback_LockAll(spinlocks,numW, endtime);
  }
  //2.1 Get the snapshot number
  sstx.GetSS();

  //2.2 update d_next_o_id of DISTRICT

  district::value *v_d = (district::value *)d_value;
  register const uint32_t my_next_o_id = v_d->d_next_o_id;

  for (uint i = 1; i <= numItems; i++) {
    uint64_t *s_value;
    s_value = pentry.stocks[i- 1];

    if( *(uint64_t *)((uint64_t)s_value + TIME_OFFSET) != 0){
      if (_xtest()) {
	_xabort(0x73);//read locked object
      }
    }

    s_value = (uint64_t *)((uint64_t) s_value  + VALUE_OFFSET);
    stock::value* sv = (stock::value*)s_value;

    const uint remote = pentry.remote[i - 1];
    const uint ol_quantity = pentry.quantity[i - 1];

    if (sv->s_quantity - ol_quantity >= 10)
      sv->s_quantity -= ol_quantity;
    else
      sv->s_quantity += -int32_t(ol_quantity) + 91;

    sv->s_ytd += ol_quantity;
    sv->s_remote_cnt += remote;
  }

  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  const new_order::value v_no;

  //STEP 6 Insert a record into ORDERLINE (Original Step 6.3)
  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {

    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];
    float i_price = i_prices[ol_number - 1];

    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);

    order_line_short::value ol_short;
    ol_shorts[ol_number-1].sn = sstx.localsn;
    ol_shorts[ol_number-1].ol_delivery_d = 0;
    ol_shorts[ol_number-1].ol_amount = float(ol_quantity) * i_price;

    cache->Insert(ol_key, &ol_shorts[ol_number-1]);
  }

  //STEP 5: insert a new record to ORDER
  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0; // seems to be ignored
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();
  sstx.Add(ORDE, o_key, (uint64_t *)(&v_oo));

  sstx.Add(NEWO, no_key, (uint64_t *)(&v_no));
  v_d->d_next_o_id = my_next_o_id + 1;

  if ( !_xtest()) {
    sstx.ReleaseAllLocal();
  }else{

  }

  RTMTX::End(spinlocks, numW);

  /*
  sstx.release_flag = true;//TODO ,maybe need refine some code
  sstx.ReleaseAllRemote();
  sstx.release_flag = false;
  sstx.ClearRwset();*/
  sstx.RemoteWriteBack();

  for (uint ol_number = numItems; ol_number > 0; ol_number--) {

    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];
    float i_price = i_prices[ol_number - 1];
    uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);

    order_line::value v_ol;

    v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_price;
    v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
    v_ol.ol_quantity = int8_t(ol_quantity);
    //    RTMTX::Begin(&store->cache_lock[warehouse_id-1], &stock_prof);
    RTMTX::Begin(&store->cache_lock[warehouse_id-1]);
    order_line_short::value *ol_short = (order_line_short::value *)(cache->Delete(ol_key));
    if (ol_short->sn == sstx.localsn)
      v_ol.ol_delivery_d = ol_short->ol_delivery_d;

    sstx.Add(ORLI, ol_key, (uint64_t *)(&v_ol));
    if (ol_short->sn != sstx.localsn) {
      v_ol.ol_delivery_d = ol_short->ol_delivery_d;
      sstx.localsn = ol_short->sn;
      sstx.Add(ORLI, ol_key, (uint64_t *)(&v_ol));
    }
    RTMTX::End(&store->cache_lock[warehouse_id-1]);
  }

//  }

  uint64_t o_sec = makeOrderIndex(warehouse_id, districtID, customerID, my_next_o_id);
  //XXX: Local version should have no exist seccondary index too, so directly insert
  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;
  sstx.Add(ORDER_INDEX, o_sec, array_dummy);
  sstx.UpdateLocalSS();

  //3 End Second Conflict Hop

#ifdef RATIO
  cross_warehouse[worker_id - 8] += cross_warehouse_count;
  cross_partition[worker_id - 8] += cross_partition_count;
#endif

  bool b = sstx.End();
  return txn_result(b, ret);
}



tpcc_worker::txn_result
tpcc_worker::txn_delivery()
{
  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  assert(NumDistrictsPerWarehouse() == 10);

  int64_t starts[10];
  int64_t ends[10];
  int64_t no_o_ids[10];
  int64_t c_ids[10];
  ssize_t ret = 0;


  //RWTX
  sstx.Begin(false);


   //3 First Conflict Hop Start

   //STEP 1. delete record with minimal o_id from NEWORDER

   //XXX: remove the opt in silo masstree?
  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {

    //	 starts[d - 1] = makeNewOrderKey(warehouse_id, d,1);
    starts[d - 1] = makeNewOrderKey(warehouse_id, d, last_no_o_ids[d - 1]);
    ends[d - 1] = makeNewOrderKey(warehouse_id, d, numeric_limits<int32_t>::max());
    no_o_ids[d - 1] = -1;
  }

  DBSSTX::Iterator* iter = sstx.GetIterator(NEWO);

  //Use RTM to guarantee the conflict hop atomicity
  BPlusTree* notree = &(store->btrees[NEWO]);
  uint64_t myCounter;

  SpinLock** spinlocks = new SpinLock*[2];
  spinlocks[0] = &store->neworder_lock[warehouse_id-1];
  spinlocks[1] = &store->payment_lock[warehouse_id-1];

  drtm::RawHashTable *ol_cache = store->ol_cache[warehouse_id-1];

  //  RTMTX::Begin(&store->neworder_lock[warehouse_id-1], &neworder_prof);
  RTMTX::Begin(&store->neworder_lock[warehouse_id-1]);
  sstx.GetSS();

  register char nested = _xtest();

  for (register uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    iter->Seek(starts[d-1]);

    if (iter->Valid()) {
      if (iter->Key() <= ends[d-1]) {
	no_o_ids[d - 1] = iter->Key();
	//sstx.Delete(NEWO,no_o_ids[d - 1]  );
      }
    }
  }
  for (register uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    /*
  	if (no_o_ids[d - 1] == -1) {
	printf("No order %d\n",d);
	continue;
  	}*/
    if (no_o_ids[d-1] != -1) {
      sstx.Delete(NEWO,no_o_ids[d - 1]  );
      no_o_ids[d - 1] = static_cast<int32_t>(no_o_ids[d - 1] << 32 >> 32);
      last_no_o_ids[d - 1] = no_o_ids[d - 1] + 1; // XXX: update last seen
    }

  }

  RTMTX::End(&store->neworder_lock[warehouse_id-1]);
  float sum_ol_amount[10];

  for (register uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {


    //STEP 2. read o_c_id and update o_carriere_id  of ORDER
    uint64_t o_key = makeOrderKey(warehouse_id, d, no_o_ids[d - 1]);
    uint64_t* o_value;
    sstx.Get(ORDE, o_key, &o_value, true);

    assert(o_value);
    oorder::value *v_oo = (oorder::value *)o_value;

    c_ids[d - 1] = v_oo->o_c_id;
    if(v_oo->o_carrier_id != 0) {
      fprintf(stderr,"id: %d\n",v_oo->o_carrier_id);
    }
    assert(v_oo->o_carrier_id == 0);


    v_oo->o_carrier_id = o_carrier_id;
    int ol_cnt = v_oo->o_ol_cnt;

    //STEP 3. update  ol_delivery_d and record ol_amount of ORDERLINE



    int i=1;

    //    RTMTX::Begin(&store->cache_lock[warehouse_id-1], &stock_prof);
    RTMTX::Begin(&store->cache_lock[warehouse_id-1]);
    for (; i<=ol_cnt; i++) {
      int64_t ol_key = makeOrderLineKey(warehouse_id, d, no_o_ids[d - 1], i);
      order_line_short::value * ol_short = (order_line_short::value *)(ol_cache->Get(ol_key));
      if (ol_short != NULL) {
	//printf("In cache\n");
	sum_ol_amount[d-1] += ol_short->ol_amount;
	ol_short->ol_delivery_d = ts;
	ol_short->sn = sstx.localsn;
      }
      else {

	RTMTX::End(&store->cache_lock[warehouse_id-1]);
	break;
      }
    }


    if (i <= ol_cnt) {
      DBSSTX::Iterator* iter1 = sstx.GetIterator(ORLI);
      iter1->SetUpdate();
      int64_t start = makeOrderLineKey(warehouse_id, d, no_o_ids[d - 1], i);
      int64_t end = makeOrderLineKey(warehouse_id, d, no_o_ids[d - 1],ol_cnt );

      iter1->Seek(start);
      while (iter1->Valid()) {

	int64_t ol_key = iter1->Key();

	if (ol_key > end)
	  break;

	uint64_t *ol_value = iter1->Value();

	order_line::value *v_ol = (order_line::value *)ol_value;

	sum_ol_amount[d-1] += v_ol->ol_amount;

	v_ol->ol_delivery_d = ts;
	iter1->Next();
      }
    }
    else RTMTX::End(&store->cache_lock[warehouse_id-1]);

    //pentry.amounts[d - 1] = sum_ol_amount;
  }

  uint64_t* c_value_loc[10];

  for (register uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {

    uint64_t c_key = makeCustomerKey(warehouse_id, d, c_ids[d - 1]);
    sstx.GetLoc(CUST, c_key, &c_value_loc[d-1]);
  }


  //  RTMTX::Begin(&store->payment_lock[warehouse_id-1], &customerD_prof);
  RTMTX::Begin(&store->payment_lock[warehouse_id-1]);
  for (register uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    uint64_t* c_value;

    customer::value *cv;

    uint64_t c_key = makeCustomerKey(warehouse_id, d, c_ids[d - 1]);
    c_value = (uint64_t *)((uint64_t)c_value_loc[d-1]+VALUE_OFFSET);

    cv = (customer::value *)c_value;

    if(cv->snapshot < sstx.localsn) {
      //transform the previous snapshort's data to current
      //publisher piece
      c_value = (uint64_t *)((uint64_t)c_value_loc[d-1]+VALUE_OFFSET);
      cv = (customer::value *) c_value;
      cv->c_balance_1 += cv->c_balance_2;
      cv->c_balance_2 = 0;
    }
    cv->snapshot = sstx.localsn;

    cv->c_balance_2 += sum_ol_amount[d-1];
    cv->c_delivery_cnt++;
  }
  RTMTX::End(&store->payment_lock[warehouse_id-1]);
  sstx.UpdateLocalSS(); //XXX: update ss here?
  //}



  return txn_result(sstx.End(), ret);

}


tpcc_worker::txn_result
tpcc_worker::txn_payment()
{
  //  fprintf(stdout,"p %d\n",worker_id);
  uint warehouse_id;
  uint districtID;

  uint customerDistrictID, customerWarehouseID;
  //here is the trick
  customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  customerWarehouseID = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);

  if (likely(g_disable_xpartition_txn ||
             NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    districtID = customerDistrictID;
    warehouse_id = customerWarehouseID;
  } else {
    warehouse_id = RandomNumber(r,1,NumWarehouses());
    districtID   = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  }


  const uint64_t addAmount = (uint64_t)(RandomNumber(r,100,500000));
  const float paymentAmount = (float) (addAmount / 100.0);

  const uint32_t ts = GetCurrentTimeMillis();
  INVARIANT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  ssize_t ret = 0;

  //RWTX
  sstx.Begin(false);

  //STEP 3. update record of CUSTORMER
  uint64_t c_key;
  customer::value *v_c;
  if (RandomNumber(r, 1, 100) <= 60) {

    // 3.1 search with C_LAST name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const string zeros(16, 0);
    static const string ones(16, 255);

    string clast;
    clast.assign((const char *) lastname_buf, 16);
    uint64_t c_start = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, zeros);
    uint64_t c_end = makeCustomerIndex(customerWarehouseID, customerDistrictID, clast, ones);

    DBSSTX::Iterator* iter = sstx.GetIterator(CUST_INDEX);

    iter->Seek(c_start);

    uint64_t c_keys[100];
    int j = 0;

    while (iter->Valid()) {

      if (compareCustomerIndex(iter->Key(), c_end)){


	uint64_t *prikeys = iter->Value();

	int num = prikeys[0];

	for (int i=1; i<=num; i++) {
	  c_keys[j] = prikeys[i];
	  j++;
	}

	if (j >= 100) {
	  printf("Payment ERROR: P Array Full\n");
	  exit(0);
	}
      }
      else break;

      iter->Next();

    }

    j = (j+1)/2 - 1;
    c_key = c_keys[j];


    } else {

    // 3.2 search with C_ID
    const uint customerID = GetCustomerId(r);

    c_key = makeCustomerKey(customerWarehouseID,customerDistrictID,customerID);

  }

  uint64_t *c_value_loc;
  bool found = sstx.GetLoc(CUST, c_key, &c_value_loc);

  assert(found);

  uint64_t gsn = sstx.GetSS();
  bool aborting = false;
 HERE:
  uint64_t now = sstx.GetSS();
  sstx.UpdateLocalSS();
  store->ssman_->WaitAll(now);
  //  uint64_t my = store->ssman_->GetMySS();
  //  if (gsn > my){

	  //  }
  uint64_t *c_value;
  //  RTMTX::Begin(&store->payment_lock[customerWarehouseID-1], &customerP_prof);
  aborting = false;
  RTMTX::RdmaBegin(&store->payment_lock[customerWarehouseID - 1],NULL,&aborting);

  if(!_xtest()) {
    //    store->payment_lock[customerWarehouseID - 1].Lock();
  }

  sstx.GetAt(CUST, c_key, &c_value, true, c_value_loc);
  v_c = ( customer::value *)c_value;

  if(v_c->snapshot < sstx.localsn && v_c->c_balance_1 != 0) {
    v_c->c_balance += v_c->c_balance_1;
    v_c->c_balance_1 = 0;
  }
  v_c->c_balance -= paymentAmount;
  v_c->c_ytd_payment += paymentAmount;
  v_c->c_payment_cnt++;

  v_c->snapshot = sstx.localsn;

  if (v_c->c_credit == "BC") {


    char buf[501];

    int d_id = static_cast<int32_t>(c_key >> 32) % 10;
    if (d_id == 0)
      d_id = 10;

    int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
		     static_cast<int32_t>(c_key << 32 >> 32),
		     d_id,
		     (static_cast<int32_t>(c_key >> 32) - d_id)/10,
		     districtID,
		     warehouse_id,
		     paymentAmount,
		     v_c->c_data.c_str());

      v_c->c_data.resize_junk(min(static_cast<size_t>(n), v_c->c_data.max_size()));

      NDB_MEMCPY((void *) v_c->c_data.data(), &buf[0], v_c->c_data.size());
  }


  //recheck
  /*
    if(_xtest() ) {
    goto HERE;
    }else {*/
  if(!_xtest()) {
    //    sstx.LocalReleaseSpin((char *)c_value_loc);
    //    fprintf(stdout,"unlock %d\n",customerWarehouseID - 1 );
  }
  else {
    if (sstx.GetSS() != now) {
      _xabort(0x93);
    }
  }
  RTMTX::End(&store->payment_lock[customerWarehouseID-1]);
  //  RTMTX::End(NULL);

  //STEP 1. update w_ytd of WAREHOUSE
  int pid = wid_to_pid(warehouse_id);

  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

  bool remote = false;
  char *addr_w,*addr_d;
  if(pid != current_partition) {
    remote = true;
    addr_w = new char[sstx.txdb_->schemas[WARE].vlen + META_LENGTH];
    addr_d = new char[sstx.txdb_->schemas[DIST].vlen + META_LENGTH];
  }else {
    sstx.GetLoc(WARE,warehouse_id,(uint64_t **)(&addr_w));
    sstx.GetLoc(DIST,d_key,(uint64_t **)(&addr_d));
  }
  DBSSTX::rwset_item w_item,d_item;

  w_item.tableid = WARE;
  w_item.key     = warehouse_id;
  w_item.pid     = pid;
  w_item.addr    = (uint64_t *)addr_w;
  w_item.ro      = false;

  d_item.tableid = DIST;
  d_item.key     = d_key;
  d_item.pid     = pid;
  d_item.addr    = (uint64_t *)addr_d;
  d_item.ro      = false;

  //warehouse piece
  if(remote)  {
    sstx.Lock(w_item);
    warehouse::value *v_w = (warehouse::value *)((uint64_t)addr_w + META_LENGTH);
    v_w->w_ytd += addAmount;
    sstx.Release(w_item);
  }else {
    //    RTMTX::RdmaBegin(NULL,0,&payment_con_prof);
    RTMTX::RdmaBegin(NULL,0,NULL);
    if(!_xtest()) {
      sstx.Lock(w_item);
    }
    if( *(uint64_t *)((uint64_t)addr_w + TIME_OFFSET) != 0){
      _xabort(0x73);
    }
    warehouse::value *v_w = (warehouse::value *)((uint64_t)addr_w + META_LENGTH);
    v_w->w_ytd += addAmount;

    if(!_xtest())
      sstx.Release(w_item);
    RTMTX::End(NULL,0);
  }

  //district piece

  if(remote)  {
    sstx.Lock(d_item);
    district::value *v_d = (district::value *)((uint64_t )addr_d + META_LENGTH);
    v_d->d_ytd += addAmount;
    sstx.Release(d_item);
  }else {
    //    RTMTX::RdmaBegin(NULL,0,&payment_con_prof1);
    RTMTX::RdmaBegin(NULL,0,NULL);
    if(!_xtest()) {
      sstx.Lock(d_item);
    }

    if( *(uint64_t *)((uint64_t)addr_d + TIME_OFFSET) != 0){
      _xabort(0x73);
    }

    district::value *v_d = (district::value *)((uint64_t )addr_d + META_LENGTH);
    v_d->d_ytd += addAmount;

    if(!_xtest())
      sstx.Release(d_item);
    RTMTX::End(NULL,0);
  }
  //STEP 4. insert a new record into HISTORY
  int d_id = static_cast<int32_t>(c_key >> 32) % 10;
  if (d_id == 0) d_id = 10;
  uint64_t h_key = makeHistoryKey(static_cast<int32_t>(c_key << 32 >> 32),
				  d_id, (static_cast<int32_t>(c_key >> 32)-d_id) / 10,
				  districtID, warehouse_id);
  history::value v_h;
#if SHORTKEY
  v_h.h_date = ts;
#endif
  v_h.h_amount = paymentAmount;
#if 0
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
		   "%.10s    %.10s",
		   v_w->w_name.c_str(),
		   v_d->d_name.c_str());
  v_h.h_data.resize_junk(min(static_cast<size_t>(n), v_h.h_data.max_size()));
#endif
  sstx.Add(HIST, h_key, (uint64_t *)(&v_h));

  return txn_result(sstx.End(), ret);
}




tpcc_worker::txn_result
tpcc_worker::txn_order_status()
{
  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  //Transaction Begin
  //	sstx.Begin(true);

  //	sstx.GetSS();
  //STEP 1. r record from CUSTOMER
  uint64_t c_key;
  customer::value v_c;
  if (RandomNumber(r, 1, 100) <= 60) {

    //1.1 cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const string zeros(16, 0);
    static const string ones(16, 255);

    string clast;
    clast.assign((const char *) lastname_buf, 16);
    uint64_t c_start = makeCustomerIndex(warehouse_id, districtID, clast, zeros);
    uint64_t c_end = makeCustomerIndex(warehouse_id, districtID, clast, ones);

    DBSSTX::Iterator* citer = sstx.GetIterator(CUST_INDEX);

    citer->Seek(c_start);

    uint64_t *c_values[100];
    uint64_t c_keys[100];
    int j = 0;
    while (citer->Valid()) {

      if (compareCustomerIndex(citer->Key(), c_end)) {

	uint64_t *prikeys = citer->Value();

	int num = prikeys[0];

	for (int i=1; i<=num; i++) {
	  c_keys[j] = prikeys[i];
	  j++;
	}

	if (j >= 100) {
	  printf("OS Array Full\n");
	  exit(0);
	}
      }
      else {
	break;
      }

      citer->Next();

    }

    j = (j+1)/2 - 1;
    c_key = c_keys[j];

    uint64_t *c_loc;
    sstx.GetLoc(CUST, c_key, &c_loc);
    v_c = *(customer::value *)((uint64_t)c_loc+VALUE_OFFSET);

  } else {

    //1.2 cust by ID
    const uint customerID = GetCustomerId(r);
    c_key = makeCustomerKey(warehouse_id,districtID,customerID);
    uint64_t *c_value;

    sstx.Get(CUST, c_key, &c_value, false);
    v_c = *(customer::value *)c_value;

  }


  //STEP 2. read record from ORDER
  int32_t o_id = -1;
  int o_ol_cnt[2];

  bool flag = true;

  while(flag) {

    for(size_t time = 0;time < 2;++time) {
      DBSSTX::Iterator* iter = sstx.GetIterator(ORDER_INDEX);

      uint64_t start = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 10000000+ 1);
      uint64_t end = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 1);

      iter->Seek(start);
      if(iter->Valid())
	iter->Prev();
      else printf("ERROR: SeekOut!!!\n");

      if (iter->Valid() && iter->Key() >= end) {

	uint64_t *prikeys = iter->Value();
	o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);
	uint64_t *o_value;

	bool found = sstx.Get(ORDE, prikeys[1], &o_value, false);
	//if (!found) printf("oid %lx\n", prikeys[1]);
	oorder::value *v_ol = (oorder::value *)o_value;
	o_ol_cnt[time] = v_ol->o_ol_cnt;

      }

      //STEP 3. read record from ORDERLINE
      if (o_id != -1) {

	for (int32_t line_number = 1; line_number <= o_ol_cnt[time]; ++line_number) {
	  uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, o_id, line_number);

	  uint64_t *ol_value;
	  bool found = sstx.Get(ORLI, ol_key, &ol_value, false);
	}
      }
      else {
	printf("ERROR: Customer %d No order Found\n", static_cast<int32_t>(c_key << 32 >> 32));
      }
      //end for time 2
      if( o_ol_cnt[0] == o_ol_cnt[1])
	flag = false;

    }
    //end forever loop
  }

  return txn_result(sstx.End(), 0);

}


tpcc_worker::txn_result
tpcc_worker::txn_stock_level()
{

  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  //	uint64_t start_t = rdtsc();
  //STEP 1. read d_next_o_id from DISTRICT
  //	sstx.Begin(true);
  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
  std::vector<int32_t> s_i_ids;

  int counter = 0;
  while(1) {

    if(counter >= 5000) {
      fprintf(stderr,"txn_stock_level stuck %d\n",counter);
      assert(false);
    }

    s_i_ids.clear();
    //sstx.GetSS();
    uint64_t *d_value;
    uint64_t endtime = timestamp + 4000000;
    sstx.GetLoc(DIST, d_key, &d_value);
    sstx.GetLocalLease(DIST,d_key,d_value, endtime);

    district::value *v_d = (district::value *)((char *)d_value + VALUE_OFFSET);
    const uint64_t cur_next_o_id = v_d->d_next_o_id;

    //STEP 2. read record from ORDERLINE
    const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
    uint64_t start = makeOrderLineKey(warehouse_id, districtID, lower, 0);
    uint64_t end   = makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0);
    std::vector<int32_t> ol_i_ids;
    ol_i_ids.reserve(300);


    DBSSTX::Iterator* iter = sstx.GetIterator(ORLI);

    iter->Seek(start);

    while (iter->Valid()) {

      int64_t ol_key = iter->Key();
      if (ol_key >= end) break;

	    uint64_t *ol_value = iter->Value();
	    order_line::value *v_ol = (order_line::value *)ol_value;
	    if(wid_to_pid(v_ol->ol_supply_w_id == current_partition))
	      ol_i_ids.push_back(v_ol->ol_i_id);
	    bool inrange = iter->Next(end);
	    if (!inrange) break;
    }

    std::sort(ol_i_ids.begin(),ol_i_ids.end());
    //	  fprintf(stdout,"sort done %d\n",ol_i_ids.size());

    //STEP 3. read record from STOCK
    s_i_ids.reserve(300);
    for (size_t i = 0; i < ol_i_ids.size(); ++i) {
      int64_t s_key = makeStockKey(warehouse_id, ol_i_ids[i]);

      uint64_t *s_value;
      bool found = sstx.GetLoc(STOC, s_key, &s_value);
      sstx.GetLocalLease(STOC,s_key,s_value,endtime);

      stock::value *v_s = (stock::value *)((char *)s_value + VALUE_OFFSET);
      if (v_s->s_quantity < int(threshold))
	s_i_ids.push_back(ol_i_ids[i]);
    }
    //check whether all the lease is valid
    if(sstx.AllLocalLeasesValid()) {
      sstx.readonly_set.clear();
      break;
    }
    counter ++;
    sstx.readonly_set.clear();
    //	  std::sort(s_i_ids.begin(), s_i_ids.end());
    //end while
  }
  //	sstx.readonly_set.clear();
  int num_distinct = 0;

  int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
  for (size_t i = 0; i < s_i_ids.size(); ++i) {
    if (s_i_ids[i] != last) {
      last = s_i_ids[i];
      num_distinct += 1;
    }
  }
  return txn_result(sstx.End(), 0);

}


tpcc_worker::txn_result
tpcc_worker::txn_stock_level_rtm()
{
  //this is the impl of read-only tx
  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  //STEP 1. read d_next_o_id from DISTRICT
  //	sstx.Begin(true);
  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
  std::vector<int32_t> s_i_ids[2];

#ifdef READ_ONLY_FLAG
  atomic_inc32(&method_called);
#endif
  int counter = 1;
  while(1) {

    if(counter > 500000) {
      //magic number to avoid stuck
      fprintf(stderr,"txn_stock_level stuck %d\n",counter);
      assert(false);
    }

    int num_distinct[2];
    num_distinct[0] = 0;
    num_distinct[1] = 0;
    s_i_ids[0].clear();
    s_i_ids[1].clear();

    uint64_t endtime = timestamp + 1000000;//1us
    for(size_t time = 0;time < 2;++time){

      s_i_ids[time].clear();
      s_i_ids[time].reserve(300);
      //sstx.GetSS();
      uint64_t *d_value;
      sstx.GetLoc(DIST, d_key, &d_value);

      uint64_t cur_next_o_id;
      RTMTX::RdmaBegin(NULL,0,NULL);

      //sstx.GetLocalLease(DIST,d_key,d_value);
      if(!_xtest())  {
	sstx.GetLocalLease(DIST,d_key,d_value,endtime);
      }

      uint64_t *status = (uint64_t *)((char *)d_value + TIME_OFFSET);
      if(*status == (1UL << 63))
	_xabort(0x73);

      district::value *v_d = (district::value *)((char *)d_value + VALUE_OFFSET);
      //FixME ,i think it is save donot lock this item
      //aha,it really need to be locked
      cur_next_o_id = v_d->d_next_o_id;

      RTMTX::End(NULL,0);

      //STEP 2. read record from ORDERLINE
      const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
      uint64_t start = makeOrderLineKey(warehouse_id, districtID, lower, 0);
      uint64_t end   = makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0);
      std::vector<int32_t> ol_i_ids;

      DBSSTX::Iterator* iter = sstx.GetIterator(ORLI);

      iter->Seek(start);

      while (iter->Valid()) {

	int64_t ol_key = iter->Key();
	if (ol_key >= end) break;

	uint64_t *ol_value = iter->Value();
	order_line::value *v_ol = (order_line::value *)ol_value;
	if(wid_to_pid(v_ol->ol_supply_w_id == current_partition))
	  ol_i_ids.push_back(v_ol->ol_i_id);
	bool inrange = iter->Next(end);
	if (!inrange) break;
      }

      std::sort(ol_i_ids.begin(),ol_i_ids.end());
      //	  fprintf(stdout,"sort done %d\n",ol_i_ids.size());

      //STEP 3. read record from STOCK

      for (size_t i = 0; i < ol_i_ids.size(); ++i) {
	int64_t s_key = makeStockKey(warehouse_id, ol_i_ids[i]);

	uint64_t *s_value;
	bool found = sstx.GetLoc(STOC, s_key, &s_value);

	      //	      sstx.GetLocalLease(STOC,s_key,s_value);

	RTMTX::RdmaBegin(NULL,0,NULL);

	if(!_xtest())
	  sstx.GetLocalLease(STOC,s_key,s_value, endtime);


	uint64_t *status = (uint64_t *)((char *)s_value + TIME_OFFSET);
	if(*status == 1UL << 63)
	  _xabort(0x73);

	stock::value *v_s = (stock::value *)((char *)s_value + VALUE_OFFSET);
	if (v_s->s_quantity < int(threshold))
	  s_i_ids[time].push_back(ol_i_ids[i]);

	RTMTX::End(NULL,0);
      }
      //check whether all the lease is valid
      sstx.readonly_set.clear();

      //	    sstx.readonly_set.clear();
      //	  std::sort(s_i_ids.begin(), s_i_ids.end());
      //end while
      //	sstx.readonly_set.clear();

      int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
      for (size_t i = 0; i < s_i_ids[time].size(); ++i) {
	if (s_i_ids[time][i] != last) {
	  last = s_i_ids[time][i];
	  num_distinct[time] += 1;
	}
      }
      //end 2 times
    }
    /*
      if(num_distinct[0] == num_distinct[1])
      break;*/
    bool flag = true;
    if(s_i_ids[0].size() == s_i_ids[1].size()) {
      for(int i = 0;i < s_i_ids[0].size();++i)
	if(s_i_ids[0][i] != s_i_ids[1][i]){
	  flag = false;
	  break;
	}

    }else
      flag = false;
    if(flag){
      assert(num_distinct[0] == num_distinct[1]);
      break;
    }

    counter ++;

    //end while
  }
#ifdef READ_ONLY_FLAG
  atomic_add32(&total_execution,counter);
#endif
  return txn_result(sstx.End(), 0);

}


template <typename T>
static vector<T>
unique_filter(const vector<T> &v)
{
  set<T> seen;
  vector<T> ret;
  for (auto &e : v)
    if (!seen.count(e)) {
      ret.emplace_back(e);
      seen.insert(e);
    }
  return ret;
}

class tpcc_bench_runner : public bench_runner {
public:

  RAWTables* store;

private:

  static bool
  IsTableReadOnly(const char *name)
  {
    return strcmp("item", name) == 0;
  }

  static bool
  IsTableAppendOnly(const char *name)
  {
    return strcmp("history", name) == 0 ||
           strcmp("oorder_c_id_idx", name) == 0;
  }


public:
  tpcc_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    printf("Init Table\n");
    store = new RAWTables(nthreads);

    int floatsize = sizeof(float);
    //Add the schema

    //So that the STOC table will be put to the first

    store->AddSchema(WARE, sizeof(uint64_t), 0, 0, sizeof(warehouse::value), true);
    store->AddSchema(DIST, sizeof(uint64_t), 0, 4, sizeof(district::value)-4, true);
    store->AddSchema(STOC, sizeof(uint64_t), 0, 2, sizeof(stock::value) - 2, true);
    store->AddSchema(CUST, sizeof(uint64_t), 0, floatsize+34, sizeof(customer::value)- floatsize -34, true);
    store->AddSchema(HIST, sizeof(uint64_t), 0, 0, sizeof(history::value), false);
    store->AddSchema(NEWO, sizeof(uint64_t), 0, 0, sizeof(new_order::value), false);
    store->AddSchema(ORDE, sizeof(uint64_t), 0, 9, sizeof(oorder::value) - 9, false);
    store->AddSchema(ORLI, sizeof(uint64_t), 0, sizeof(order_line::value), 0, false);
    store->AddSchema(ITEM, sizeof(uint64_t), 0, 0, sizeof(item::value), true);


    //XXX FIXME: won't serialize sec index currently
    store->AddSchema(CUST_INDEX, sizeof(uint64_t), 0, 0, 16, true); //No tx.add for this table
    store->AddSchema(ORDER_INDEX, sizeof(uint64_t), 0, 16, 0, false);

  }

protected:

  bool check_consistency(){
    //The real function to check consistency
    fprintf(stdout,"start checking consistency\n");
#if 1
    DBSSTX tx(store);
    int warehouses = NumWarehouses();
    int dists = NumDistrictsPerWarehouse();
    for(int i = 1; i<= warehouses; i++){
      //add check y
      if(wid_to_pid(i) != current_partition)  {
	//skipping remote warehousesy
	continue;
      }
      const uint warehouse_id = i; //PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
      uint64_t *w_value = store->Get(WARE,warehouse_id);
      if(!w_value) return false;
      warehouse::value *v_w = (warehouse::value*)((uint64_t)w_value + VALUE_OFFSET);
      int64_t w_ytd = v_w->w_ytd;
      int64_t d_ytd_total = 0;


      for(int j = 1; j <= dists; j++){
	const uint districtID = j;
	int32_t o_id_first = 0;  //MAX(O_ID)
	int32_t o_id_second = 0; //MAX(NO_O_ID)
	int32_t dnext = 0;       //D_NEXT_O_ID
	int32_t num = 0;
	int32_t o_id_min = 0;
	int32_t c = 0;
	int32_t c1 = 0;
	int32_t cid = 10000;

	bool f = false;
	while (!f) {
	  //Consistency 2
	  int64_t d_key = makeDistrictKey(warehouse_id, districtID);
	  uint64_t *d_value = store->Get(DIST,d_key);

	  if(!d_value)
	    return false;
	  district::value *d = (district::value *)((uint64_t)d_value + VALUE_OFFSET);
	  d_ytd_total += d->d_ytd;

	  int32_t o_id;
	  //DBTX::Iterator iter(&rotx, ORDE);
	  RAWStore::Iterator *iter = store->GetIterator(ORDE);
	  uint64_t start = makeOrderKey(warehouse_id, districtID, 10000000 + 1);
	  uint64_t end = makeOrderKey(warehouse_id, districtID, 1);
	  iter->Seek(start);

	  iter->Prev();
	  if (iter->Valid() && iter->Key() >= end) {
	    o_id = static_cast<int32_t>(iter->Key() << 32 >> 32); //max(O_ID)
	    assert(o_id == d->d_next_o_id - 1);
	    o_id_first = o_id;
	    dnext = d->d_next_o_id - 1;
	  }

	  start = makeNewOrderKey(warehouse_id, districtID, 10000000 + 1);
	  end = makeNewOrderKey(warehouse_id, districtID, 1);
	  RAWStore::Iterator *iter1 = store->GetIterator(NEWO); //DBTX::Iterator iter1(&rotx, NEWO);
	  iter1->Seek(start);

	  if(!iter1->Valid())
	    break;//return false;

	  assert(iter1->Valid());

	  iter1->Prev();

	  if (iter1->Valid() && iter1->Key() >= end) {
	    o_id = static_cast<int32_t>(iter1->Key() << 32 >> 32);
	    //assert(o_id == d->d_next_o_id - 1);
	    o_id_second = o_id; //max(NO_O_ID)

	    //Consistency 5 ?it seems that it only checks one order_line
	    uint64_t *o_value = store->Get(ORDE,iter1->Key());
	    //rotx.Get(ORDE, iter1.Key(), &o_value);
	    cid = ((oorder::value *)((uint64_t)o_value + VALUE_OFFSET))->o_carrier_id;
	  }


	  /***** consistrncy 3      ******/
	  iter1->Seek(end);//first new order
	  int32_t min = static_cast<int32_t>(iter1->Key() << 32 >> 32);
	  num = 0;
	  while (iter1->Valid() && iter1->Key() < start) {
	    num++;
	    iter1->Next();
	  }
	  if (o_id - min + 1 != num) printf("o_id %d %d %d",o_id, min, num);
	  assert(o_id - min + 1 == num);
	  o_id_min = o_id - min + 1;
	  fprintf(stdout,"consistency 3 check passes!\n");
	  /***** consistrncy 3 done ******/


	  /***** consistrncy 4      ******/
	  end = makeOrderKey(warehouse_id, districtID, 10000000);
	  start = makeOrderKey(warehouse_id, districtID, 1);
	  iter->Seek(start);
	  c = 0;
	  while (iter->Valid() && iter->Key() <= end) {
	    uint64_t *o_value = iter->Value();

	    oorder::value *o = (oorder::value *)((uint64_t)o_value + VALUE_OFFSET);
	    c += o->o_ol_cnt;
	    iter->Next();
	  }
	  start = makeOrderLineKey(warehouse_id, districtID, 1, 1);
	  end   = makeOrderLineKey(warehouse_id, districtID, 10000000, 15);
	  c1 = 0;
	  RAWStore::Iterator* iter2 = store->GetIterator(ORLI); //DBTX::Iterator iter2(&rotx, ORLI);
	  iter2->Seek(start);
	  while (iter2->Valid() && iter2->Key() <= end) {
	    c1++;
	    iter2->Next();
	  }
	  f = true;
	}
	if(c != c1) {
	  fprintf(stdout,"c: %d c1: %d\n",c,c1);
	}else {
	  fprintf(stdout,"consistency 4 check passes! %d\n",warehouse_id);
	  f = false;
	}
	assert(c == c1);
	//return false;
	/*
	   consistrncy check 2,3,4,5

	*/
	assert(dnext == o_id_first && dnext == o_id_second);
	if(dnext == o_id_first && dnext == o_id_second) {
	  fprintf(stdout,"consistency 2 check passes! %d\n",warehouse_id);
	}else {
	  fprintf(stdout,"f %d s %d next %d\n",o_id_first,o_id_second,dnext);
	  f = false;
	}

	if(o_id_min == num) {
	  fprintf(stdout,"consistrncy 3 check passes! %d\n",warehouse_id);
	}else {
	  fprintf(stdout,"dev %d %d\n",o_id_min,num);
	  f = false;
	}


	//return false;
	if(cid == 10000 || cid == 0) {
	  fprintf(stdout,"consistrncy 5 check passes! %d\n",warehouse_id);
	}else {
	  fprintf(stdout,"cid %d\n",cid);
	  f = false ;
	}
	assert(cid == 10000 || cid ==0);
	//return false;
	if(f) return false;


	//customer balance/history amount/orderline amount check
#if 0
	{
	  int32_t minn = -1, maxn = -1, countn = 0, mino = -1, maxo = -1, counto = 0;
	  uint64_t o_start = makeOrderKey(warehouse_id, j, 1);
	  uint64_t o_end = makeOrderKey(warehouse_id, j+1, 1);

	  DBSSTX::Iterator* iter = tx.GetIterator(NEWO);
	  iter->Seek(o_start);
	  mino = static_cast<int32_t>(iter->Key() << 32 >> 32);
	  while (iter->Valid()) {
	    counto++;
	    maxo = static_cast<int32_t>(iter->Key() << 32 >> 32);
	    if (!iter->Next(o_end)) break;
	  }

	  for (int32_t c=1; c<=(int32_t)NumCustomersPerDistrict(); c++) {
	    uint64_t c_key = makeCustomerKey(warehouse_id, j, c);
	    uint64_t *c_value;
	    tx.Get(CUST, c_key, &c_value, false);
	    customer::value *v_c = (customer::value *)c_value;

	    AMOUNT h_amount_total = 0, ol_amount_total = 0;

	    for (int32_t so = mino; so <= maxo; so++) {
	      uint64_t o_key = makeOrderKey(warehouse_id, j, so);
	      uint64_t *o_value;
	      tx.Get(ORDE, o_key, &o_value, false);
	      oorder::value* v_oo = (oorder::value *)o_value;
	      if (v_oo->o_c_id != c) continue;
	      uint64_t ol_start = makeOrderLineKey(warehouse_id,j,so,1);
	      uint64_t ol_end = makeOrderLineKey(warehouse_id,j,so,15);

	      DBSSTX::Iterator* oliter = tx.GetIterator(ORLI);
	      oliter->Seek(ol_start);
	      while (oliter->Valid()) {
		if (oliter->Key() <= ol_end) {
		  if (v_oo->o_carrier_id != 0)
		    ol_amount_total += ((order_line::value *)(oliter->Value()))->ol_amount;
		}
		else break;
		oliter->Next();
	      }
	    }
	    v_c->c_balance += v_c->c_balance_1;
	    v_c->c_balance += v_c->c_balance_2;

	    if (fabs(ol_amount_total) <= fabs(v_c->c_balance)) {
	      AMOUNT delta = fab1s(v_c->c_balance*0.001);
	      if(fabs(v_c->c_balance+v_c->c_ytd_payment-ol_amount_total) > delta)
		printf("then check balance %f %f %f %f delta %f\n", v_c->c_balance, v_c->c_ytd_payment,
		       v_c->c_balance+v_c->c_ytd_payment,ol_amount_total, delta);
	    }
	    else {
	      AMOUNT delta = fabs(ol_amount_total*0.001);
	      if(fabs(v_c->c_balance+v_c->c_ytd_payment-ol_amount_total) > delta)
		printf("ol then check balance %f %f %f %f delta %f\n", v_c->c_balance, v_c->c_ytd_payment,
		       v_c->c_balance+v_c->c_ytd_payment,ol_amount_total, delta);

	    }

	  }

	  //end consistency check 10,11,12
	}
#endif
	//end district loop
      }

      if(d_ytd_total == w_ytd) {
	//	fprintf(stdout,"consistrncy 1 check passes %d\n",warehouse_id);
	    }else {
	//	fprintf(stdout,"%d %d\n",d_ytd_total,w_ytd);
	return false;
      }
      //end warehouse loop
    }


#endif
    return true;
    //end check consistency
  }

  virtual void initPut() {
    uint64_t *temp = new uint64_t[4];
    temp[0] = 0; temp[1] = 0; temp[2] = 0;
    for (int i=0; i<9; i++) {
      //Fixme: invalid value pointer
      store->Put(i,(uint64_t)1<<60, temp);
    }


    //XXX: add empty record to identify the end of the table./
    store->Put(ORDER_INDEX,(uint64_t)1<<60, temp);
  }
  virtual void final_check(){
    store->ssman_->ReportProfile();
    if(!check_consistency()) {
      printf("Consistency Error in check_consistency\n");
	}else
      printf("Check consistency pass!!!!\n");

  }
  virtual void sync_log() {

    //		RTMTX::localprofile.reportAbortStatus();
    store->Sync();
    printf("NewOrder Counter #1 %ld #2 %ld\n", store->NOCounter1, store->NOCounter2);
    printf("Delivery Counter #1 %ld #2 %ld\n", store->DECounter1, store->DECounter2);

    printf("========================= District RTM Profile=======================\n");
    tpcc_worker::district_prof.reportAbortStatus();
    printf("========================= Stock RTM Profile=======================\n");
    tpcc_worker::stock_prof.reportAbortStatus();
    printf("========================= NewOrder RTM Profile=======================\n");
    tpcc_worker::neworder_prof.reportAbortStatus();
    printf("========================= CustomerD RTM Profile=======================\n");
    tpcc_worker::customerD_prof.reportAbortStatus();
    printf("========================= CustomerP RTM Profile=======================\n");
    tpcc_worker::customerP_prof.reportAbortStatus();
    printf("========================= Readonly RTM Profile=======================\n");
    tpcc_worker::readonly_prof.reportAbortStatus();
    printf("========================= debug    RTM Profile=======================\n");
    tpcc_worker::payment_con_prof.reportAbortStatus();
    printf("========================= debug1   RTM Profile=======================\n");
    tpcc_worker::payment_con_prof1.reportAbortStatus();


    uint64_t aborts = 0;
    aborts += tpcc_worker::district_prof.abortCounts;
    aborts += tpcc_worker::stock_prof.abortCounts;
    aborts += tpcc_worker::neworder_prof.abortCounts;
    aborts += tpcc_worker::customerD_prof.abortCounts;
    aborts += tpcc_worker::customerP_prof.abortCounts;
    //printf("endSS %ld\n", store->ssman_->curSS);
    printf("aborts %ld\n", aborts);

#ifdef READ_ONLY_FLAG
    printf("readonly: %d %d %f\n",total_execution,method_called,(float)total_execution / method_called);
#endif
  }

  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new tpcc_warehouse_loader(9324, db,  store));
    ret.push_back(new tpcc_item_loader(235443, db,  store));
    if (enable_parallel_loading) {
      fast_random r(89785943);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.push_back(new tpcc_stock_loader(r.next(), db,  i, store));
    } else {
      ret.push_back(new tpcc_stock_loader(89785943, db, -1, store));
    }
    ret.push_back(new tpcc_district_loader(129856349, db,  store));
    if (enable_parallel_loading) {
      fast_random r(923587856425);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.push_back(new tpcc_customer_loader(r.next(), db, i, store));
    } else {
      ret.push_back(new tpcc_customer_loader(923587856425, db,  -1, store));
    }
    if (enable_parallel_loading) {
      fast_random r(2343352);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.push_back(new tpcc_order_loader(r.next(), db,  i, store));
    } else {
      ret.push_back(new tpcc_order_loader(2343352, db, -1, store));
    }
    return ret;
  }

  virtual void init_rdma() {
    //    rdma = new RdmaResource(store->rdmastore);
    rdma = new RdmaResource(store->start_rdma,store->rdma_size,512,store->end_rdma);
  }

  virtual vector<bench_worker *>
  make_workers()
  {
 //   const unsigned alignment = coreid::num_cpus_online();
 //   const int blockstart =
 //     coreid::allocate_contiguous_aligned_block(nthreads, alignment);
 //   ALWAYS_ASSERT(blockstart >= 0);
 //   ALWAYS_ASSERT((blockstart % alignment) == 0);
 const int blockstart = 8;
    fast_random r(23984543);
    vector<bench_worker *> ret;

#ifdef RATIO

    cross_warehouse = new uint64_t [nthreads];
    cross_partition = new uint64_t [nthreads];

#endif

    bench_worker * listener = new tpcc_listener(
            blockstart + nthreads,
            r.next(), db,
            &barrier_a, &barrier_b, 1, NumWarehouses()+1, store, rdma);
    fprintf(stdout,"connecting...\n");
    rdma->Connect();
    fprintf(stdout,"connection done\n");
    ((tpcc_listener *)listener)->Coordinate();

      for (size_t i = 0; i < nthreads; i++){
        ret.push_back(
          new tpcc_worker( blockstart + i,r.next(), db, &barrier_a, &barrier_b,
            current_partition*NumWarehousePerPartition()+ i % NumWarehousePerPartition() + 1,
			   current_partition*NumWarehousePerPartition()+ i % NumWarehousePerPartition() + 2, store, rdma));
      }
    ret.push_back(listener);
    return ret;
  }


};

void
tpcc_do_test( int argc, char **argv)
{
  // parse options
 abstract_db *db = NULL;
  optind = 1;
  bool did_spec_remote_pct = false;
  while (1) {
    static struct option long_options[] =
    {
      {"disable-cross-partition-transactions" , no_argument       , &g_disable_xpartition_txn             , 1}   ,
      {"disable-read-only-snapshots"          , no_argument       , &g_disable_read_only_scans            , 1}   ,
      {"enable-partition-locks"               , no_argument       , &g_enable_partition_locks             , 1}   ,
      {"enable-separate-tree-per-partition"   , no_argument       , &g_enable_separate_tree_per_partition , 1}   ,
      {"new-order-remote-item-pct"            , required_argument , 0                                     , 'r'} ,
      {"new-order-fast-id-gen"                , no_argument       , &g_new_order_fast_id_gen              , 1}   ,
      {"uniform-item-dist"                    , no_argument       , &g_uniform_item_dist                  , 1}   ,
      {"workload-mix"                         , required_argument , 0                                     , 'w'} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "r:", long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'r':
      g_new_order_remote_item_pct = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(g_new_order_remote_item_pct >= 0 && g_new_order_remote_item_pct <= 100);
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
      exit(1);

    default:
      abort();
    }
  }

  if (did_spec_remote_pct && g_disable_xpartition_txn) {
    cerr << "WARNING: --new-order-remote-item-pct given with --disable-cross-partition-transactions" << endl;
    cerr << "  --new-order-remote-item-pct will have no effect" << endl;
  }

  if (verbose) {
    cerr << "tpcc settings:" << endl;
    cerr << "  cross_partition_transactions : " << !g_disable_xpartition_txn << endl;
    cerr << "  read_only_snapshots          : " << !g_disable_read_only_scans << endl;
    cerr << "  partition_locks              : " << g_enable_partition_locks << endl;
    cerr << "  separate_tree_per_partition  : " << g_enable_separate_tree_per_partition << endl;
    cerr << "  new_order_remote_item_pct    : " << g_new_order_remote_item_pct << endl;
    cerr << "  new_order_fast_id_gen        : " << g_new_order_fast_id_gen << endl;
    cerr << "  uniform_item_dist            : " << g_uniform_item_dist << endl;
    cerr << "  workload_mix                 : " <<
      format_list(g_txn_workload_mix,
                  g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
  }

  tpcc_bench_runner r(db);

  r.run();
}
