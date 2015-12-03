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

/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */
#include <iostream>
#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>

//#include "txn.h"
//#include "macros.h"
//#include "scopedperf.hh"
//#include "spinlock.h"

#include "bench.h"
#include "tpcc.h"


#include "db/dbtx.h"
#include "db/dbrawtx.h"
#include "db/dbrotx.h"
#include "db/dbtables.h"
#include "util/rtm.h"



using namespace std;
using namespace util;
#define SEPERATE 0
#define SLDBTX	0
#define CHECKTPCC 0
#define SEC_INDEX 1

#define WARE 0
#define DIST 1
#define CUST 2
#define HIST 3
#define NEWO 4
#define ORDE 5
#define ORLI 6
#define ITEM 7
#define STOC 8
#if USESECONDINDEX
#define CUST_INDEX 0
#define ORDER_INDEX 1
#else
#define CUST_INDEX 9
#define ORDER_INDEX 10
#endif



#if 0
#define TPCC_TABLE_LIST(x) \
  x(customer) \
  x(customer_name_idx) \
  x(district) \
  x(history) \
  x(item) \
  x(new_order) \
  x(oorder) \
  x(oorder_c_id_idx) \
  x(order_line) \
  x(stock) \
  x(stock_data) \
  x(warehouse)
#endif

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
#if 0
	for (int i=0; i<16; i++)
		printf("%lx ", oldstring[i]);
	printf("\n");
	for (int i=0; i<16; i++)
		printf("%lx ", newstring[i]);
	printf("\n");
#endif
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
#if 0
	printf("%d %d %s %s \n", w_id, d_id, c_last, c_first);
	for (int i= 0;i<5; i++)
		printf("%lx ",seckey[i]);
	printf("\n");
#endif
	return (uint64_t)seckey;
}

#endif

static inline ALWAYS_INLINE size_t
NumWarehouses()
{
  return (size_t) scale_factor;
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
static int g_new_order_remote_item_pct = 1;
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
#if 0
static inline ALWAYS_INLINE spinlock &
LockForPartition(unsigned int wid)
{
  INVARIANT(g_enable_partition_locks);
  return g_partition_locks[PartitionId(wid)].elem;
}
#endif
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

#if 0
#define DEFN_TBL_INIT_X(name) \
  , tbl_ ## name ## _vec(partitions.at(#name))
#endif

public:
	DBTables *store;
  tpcc_worker_mixin(DBTables *s) :
    _dummy() // so hacky...
#if 0
    TPCC_TABLE_LIST(DEFN_TBL_INIT_X)
#endif
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
    return tbl_ ## name ## _vec[wid - 1]; \
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
#if 0
    if (verbose)
      cerr << "PinToWarehouseId(): coreid=" << coreid::core_id()
           << " pinned to whse=" << wid << " (partid=" << partid << ")"
           << endl;
//    rcu::s_instance.pin_current_thread(pinid);
//    rcu::s_instance.fault_region();
#endif
  }

public:

  static inline uint32_t
  GetCurrentTimeMillis()
  {
    //struct timeval tv;
    //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    //return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number

    static __thread uint32_t tl_hack = 0;
    return tl_hack++;
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

class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
public:
  DBTX tx;
  DBRAWTX rawtx;
  DBROTX rotx;

  //Lock for different tables
  SpinLock *stock_lock;
  SpinLock *customer_lock;
  SpinLock *neworder_lock;
  // resp for [warehouse_id_start, warehouse_id_end)
  tpcc_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint warehouse_id_start, uint warehouse_id_end, DBTables *store, SpinLock *sl,SpinLock *cl, SpinLock *nl)
    : bench_worker(worker_id, true, seed, db,
                    barrier_a, barrier_b),
      tpcc_worker_mixin( store),
      warehouse_id_start(warehouse_id_start),
      warehouse_id_end(warehouse_id_end),
      tx(store),
      rawtx(store),
      rotx(store)
  {
  	stock_lock = sl;
	customer_lock = cl;
	neworder_lock = nl;
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

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;
  bool check_consistency();
  txn_result txn_new_order();

  static txn_result
  TxnNewOrder(bench_worker *w)
  {
//    ANON_REGION("TxnNewOrder:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_new_order();
  }

  txn_result txn_delivery();

  static txn_result
  TxnDelivery(bench_worker *w)
  {
//    ANON_REGION("TxnDelivery:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_delivery();
  }

  txn_result txn_payment();

  static txn_result
  TxnPayment(bench_worker *w)
  {
//    ANON_REGION("TxnPayment:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_payment();
  }

  txn_result txn_order_status();

  static txn_result
  TxnOrderStatus(bench_worker *w)
  {
//    ANON_REGION("TxnOrderStatus:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_order_status();
  }

  txn_result txn_stock_level();

  static txn_result
  TxnStockLevel(bench_worker *w)
  {
//    ANON_REGION("TxnStockLevel:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_stock_level();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    // numbers from sigmod.csail.mit.edu:
    //w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
    //w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
    //w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k ops/sec
    //w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k ops/sec
    //w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k ops/sec
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
	store->ThreadLocalInit(worker_id - 8);

	if (!pin_cpus)
      return;
//    const size_t a = worker_id % coreid::num_cpus_online();
//    const size_t b = a % nthreads;
//    rcu::s_instance.pin_current_thread(b);
//   rcu::s_instance.fault_region();


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

class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_warehouse_loader(unsigned long seed,
                        abstract_db *db,
                        DBTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store)
  {}

protected:
  virtual void
  load()
  {
    string obj_buf;
#if 0
    void *txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
	printf("load warehouse\n");
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    try {
      vector<warehouse::value> warehouses;
      for (uint i = 1; i <= NumWarehouses(); i++) {
        const warehouse::key k(i);
        const string w_name = RandomStr(r, RandomNumber(r, 6, 10));
        const string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_city = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_state = RandomStr(r, 3);
        const string w_zip = "123456789";

        warehouse::value *v = new warehouse::value();
        v->w_ytd = 300000;
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
#if 0
        tbl_warehouse(i)->insert(txn, Encode(k), Encode(obj_buf, v));
#endif

		store->tables[WARE]->Put(i, (uint64_t *)v);

		if (Encode(k).size() !=8) cerr << Encode(k).size() << endl;
        warehouses.push_back(*v);
      }
#if 0
      ALWAYS_ASSERT(db->commit_txn(txn));
      arena.reset();
      txn = db->new_txn(txn_flags, arena, txn_buf());
      for (uint i = 1; i <= NumWarehouses(); i++) {
        const warehouse::key k(i);
        string warehouse_v;
        ALWAYS_ASSERT(tbl_warehouse(i)->get(txn, Encode(k), warehouse_v));
        warehouse::value warehouse_temp;
        const warehouse::value *v = Decode(warehouse_v, warehouse_temp);
        ALWAYS_ASSERT(warehouses[i - 1] == *v);

        checker::SanityCheckWarehouse(&k, v);
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
#endif
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
                        DBTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store)
  {}

protected:
  virtual void
  load()
  {
    string obj_buf;
#if 0
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
    uint64_t total_sz = 0;
    try {
      for (uint i = 1; i <= NumItems(); i++) {
        // items don't "belong" to a certain warehouse, so no pinning
        const item::key k(i);

        item::value *v = new item::value();
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


		store->tables[ITEM]->Put(i, (uint64_t *)v);
#if 0
        tbl_item(1)->insert(txn, Encode(k), Encode(obj_buf, v)); // this table is shared, so any partition is OK

        if (bsize != -1 && !(i % bsize)) {
          ALWAYS_ASSERT(db->commit_txn(txn));
          txn = db->new_txn(txn_flags, arena, txn_buf());
          arena.reset();
        }
#endif
      }
#if 0
      ALWAYS_ASSERT(db->commit_txn(txn));
#endif
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
                        DBTables* store)
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
    const uint w_start = (warehouse_id == -1) ?
      1 : static_cast<uint>(warehouse_id);
    const uint w_end   = (warehouse_id == -1) ?
      NumWarehouses() : static_cast<uint>(warehouse_id);

    for (uint w = w_start; w <= w_end; w++) {
      const size_t batchsize =
         NumItems() ;
      const size_t nbatches = (batchsize > NumItems()) ? 1 : (NumItems() / batchsize);

      if (pin_cpus)
        PinToWarehouseId(w);

      for (uint b = 0; b < nbatches;) {
//        scoped_str_arena s_arena(arena);
#if 0
        void * const txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
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

            stock::value *v = new stock::value();
            v->s_quantity = RandomNumber(r, 10, 100);
            v->s_ytd = 0;
            v->s_order_cnt = 0;
            v->s_remote_cnt = 0;

//            stock_data::value v_data;
            const int len = RandomNumber(r, 26, 50);
            if (RandomNumber(r, 1, 100) > 10) {
              const string s_data = RandomStr(r, len);
//              v_data.s_data.assign(s_data);
//			  v->s_data.assign(s_data);
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


			store->tables[STOC]->Put(key, (uint64_t *)v);
#if 0
            tbl_stock(w)->insert(txn, Encode(k), Encode(obj_buf, v));
#endif
 //           tbl_stock_data(w)->insert(txn, Encode(k_data), Encode(obj_buf1, v_data));
          }
#if 0
          if (db->commit_txn(txn)) {
#endif
            b++;
#if 0
          } else {
            db->abort_txn(txn);
            if (verbose)
              cerr << "[WARNING] stock loader loading abort" << endl;
          }
#endif
        } catch (abstract_db::abstract_abort_exception &ex) {
#if 0
          db->abort_txn(txn);
#endif
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
                        DBTables* store)
    : bench_loader(seed, db),
      tpcc_worker_mixin( store)
  {}

protected:
  virtual void
  load()
  {
    string obj_buf;

    const ssize_t bsize = -1;
#if 0
    void *txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
    uint64_t district_total_sz = 0, n_districts = 0;
    try {
      uint cnt = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        if (pin_cpus)
          PinToWarehouseId(w);
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
		  uint64_t key = makeDistrictKey(w, d);
#if SHORTKEY

		  const district::key k(makeDistrictKey(w, d));
#else
          const district::key k(w, d);
#endif
          district::value *v = new district::value();
          v->d_ytd = 30000;
          v->d_tax = (float) (RandomNumber(r, 0, 2000) / 10000.0);
          v->d_next_o_id = 3001;
          v->d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
          v->d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          v->d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          v->d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          v->d_state.assign(RandomStr(r, 3));
          v->d_zip.assign("123456789");

          checker::SanityCheckDistrict(&k, v);
          const size_t sz = Size(*v);
          district_total_sz += sz;
          n_districts++;


		  store->tables[DIST]->Put(key, (uint64_t *)v);
#if 0
          tbl_district(w)->insert(txn, Encode(k), Encode(obj_buf, v));
          if (bsize != -1 && !((cnt + 1) % bsize)) {
            ALWAYS_ASSERT(db->commit_txn(txn));
            txn = db->new_txn(txn_flags, arena, txn_buf());
            arena.reset();
          }
#endif
        }
      }
#if 0
      ALWAYS_ASSERT(db->commit_txn(txn));
#endif
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
                        DBTables* store)
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

    const uint w_start = (warehouse_id == -1) ?
      1 : static_cast<uint>(warehouse_id);
    const uint w_end   = (warehouse_id == -1) ?
      NumWarehouses() : static_cast<uint>(warehouse_id);
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
#if 0
          void * const txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
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
              customer::value *v = new customer::value();
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

              checker::SanityCheckCustomer(&k, v);
              const size_t sz = Size(*v);
              total_sz += sz;

			  Memstore::MemNode *node = store->tables[CUST]->Put(key, (uint64_t *)v);
#if 0
              tbl_customer(w)->insert(txn, Encode(k), Encode(obj_buf, v));
#endif
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
			  #if USESECONDINDEX
			  	store->secondIndexes[CUST_INDEX]->Put(sec, key, node);
			  #else
				Memstore::MemNode* mn = store->tables[CUST_INDEX]->Get(sec);
				if (mn == NULL) {
					uint64_t *prikeys = new uint64_t[2];
					prikeys[0] = 1; prikeys[1] = key;
					//printf("key %ld\n",key);
					store->tables[CUST_INDEX]->Put(sec, prikeys);
				}
				else {
					printf("ccccc\n");
					uint64_t *value = mn->value;
					int num = value[0];
					uint64_t *prikeys = new uint64_t[num+2];
					prikeys[0] = num + 1;
					for (int i=1; i<=num; i++)
						prikeys[i] = value[i];
					prikeys[num+1] = key;
					store->tables[CUST_INDEX]->Put(sec, prikeys);
					//delete[] value;
				}
			  #endif


#if 0
              tbl_customer_name_idx(w)->insert(txn, Encode(k_idx), Encode(obj_buf, v_idx));
#endif
			 //cerr << Encode(k_idx).size() << endl;

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

              history::value *v_hist = new history::value();
              v_hist->h_amount = 10;
              v_hist->h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

			  store->tables[HIST]->Put(hkey, (uint64_t *)v_hist);
#if 0
              tbl_history(w)->insert(txn, Encode(k_hist), Encode(obj_buf, v_hist));
#endif
			  if (Encode(k_hist).size() !=8)cerr << Encode(k_hist).size() << endl;
            }
#if 0
            if (db->commit_txn(txn)) {
#endif
              batch++;
#if 0
            } else {
              db->abort_txn(txn);
              if (verbose)
                cerr << "[WARNING] customer loader loading abort" << endl;
            }
#endif
          } catch (abstract_db::abstract_abort_exception &ex) {
#if 0
            db->abort_txn(txn);
#endif
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
                        DBTables* store)
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

    const uint w_start = (warehouse_id == -1) ?
      1 : static_cast<uint>(warehouse_id);
    const uint w_end   = (warehouse_id == -1) ?
      NumWarehouses() : static_cast<uint>(warehouse_id);

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
#if 0
          void * const txn = db->new_txn(txn_flags, arena, txn_buf());
#endif
          try {
          	uint64_t okey = makeOrderKey(w, d, c);
#if SHORTKEY
            const oorder::key k_oo(makeOrderKey(w, d, c));
#else
			const oorder::key k_oo(w, d, c);
#endif
            oorder::value *v_oo = new oorder::value();
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

			Memstore::MemNode *node = store->tables[ORDE]->Put(okey, (uint64_t *)v_oo);


			uint64_t sec = makeOrderIndex(w, d, v_oo->o_c_id, c);
#if USESECONDINDEX
			store->secondIndexes[ORDER_INDEX]->Put(sec, okey, node);
#else
			Memstore::MemNode* mn = store->tables[ORDER_INDEX]->Get(sec);
			if (mn == NULL) {
				uint64_t *prikeys = new uint64_t[2];
				prikeys[0] = 1; prikeys[1] = okey;
				store->tables[ORDER_INDEX]->Put(sec, prikeys);
			}
			else {
				printf("oooo\n");
				uint64_t *value = mn->value;
				int num = value[0];
				uint64_t *prikeys = new uint64_t[num+2];
				prikeys[0] = num + 1;
				for (int i=1; i<=num; i++)
					prikeys[i] = value[i];
				prikeys[num+1] = okey;
				store->tables[ORDER_INDEX]->Put(sec, prikeys);
				delete[] value;
			}
#endif

#if 0
            tbl_oorder(w)->insert(txn, Encode(k_oo), Encode(obj_buf, v_oo));
#endif
#if SHORTKEY
            const oorder_c_id_idx::key k_oo_idx(makeOrderIndex(w, d, v_oo->o_c_id, c));
#else
            const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo->o_c_id, k_oo.o_id);
#endif
            const oorder_c_id_idx::value v_oo_idx(0);



#if 0
            tbl_oorder_c_id_idx(w)->insert(txn, Encode(k_oo_idx), Encode(obj_buf, v_oo_idx));
#endif
            if (c >= 2101) {
			  uint64_t nokey = makeNewOrderKey(w, d, c);
#if SHORTKEY
              const new_order::key k_no(makeNewOrderKey(w, d, c));
#else
			  const new_order::key k_no(w, d, c);
#endif
              const new_order::value *v_no = new new_order::value();

              checker::SanityCheckNewOrder(&k_no, v_no);
              const size_t sz = Size(*v_no);
              new_order_total_sz += sz;
              n_new_orders++;

			  store->tables[NEWO]->Put(nokey, (uint64_t *)v_no);
#if 0
              tbl_new_order(w)->insert(txn, Encode(k_no), Encode(obj_buf, v_no));
#endif
            }

            for (uint l = 1; l <= uint(v_oo->o_ol_cnt); l++) {
			  uint64_t olkey = makeOrderLineKey(w, d, c, l);
#if SHORTKEY
              const order_line::key k_ol(makeOrderLineKey(w, d, c, l));
#else
              const order_line::key k_ol(w, d, c, l);
#endif
              order_line::value *v_ol = new order_line::value();
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


			  store->tables[ORLI]->Put(olkey, (uint64_t *)v_ol);
#if 0
              tbl_order_line(w)->insert(txn, Encode(k_ol), Encode(obj_buf, v_ol));
#endif
            }
#if 0
            if (db->commit_txn(txn)) {
#endif
              c++;
#if 0
            } else {
              db->abort_txn(txn);
              ALWAYS_ASSERT(warehouse_id != -1);
              if (verbose)
                cerr << "[WARNING] order loader loading abort" << endl;
            }
#endif
          } catch (abstract_db::abstract_abort_exception &ex) {
#if 0
            db->abort_txn(txn);
#endif
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

	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint districtID = RandomNumber(r, 1, 10);
	int32_t o_id_first = 0;
	int32_t o_id_second = 0;
	int32_t dnext = 0;
	int32_t num = 0;
	int32_t o_id_min = 0;
	int32_t c = 0;
	int32_t c1 = 0;
	int32_t cid = 10000;

	DBTX rotx(store);
	bool f = false;
	while (!f) {
		rotx.Begin();

		//Consistency 2

		uint64_t *d_value;
		int64_t d_key = makeDistrictKey(warehouse_id, districtID);
		bool found = rotx.Get(DIST, d_key, &d_value);

		if(!found)
			return false;

		assert(found);
		district::value *d = (district::value *)d_value;

		int32_t o_id;
		DBTX::Iterator iter(&rotx, ORDE);
		uint64_t start = makeOrderKey(warehouse_id, districtID, 10000000 + 1);
		uint64_t end = makeOrderKey(warehouse_id, districtID, 1);
		iter.Seek(start);

		iter.Prev();
		if (iter.Valid() && iter.Key() >= end) {
			o_id = static_cast<int32_t>(iter.Key() << 32 >> 32);
			//assert(o_id == d->d_next_o_id - 1);
			o_id_first = o_id;
			dnext = d->d_next_o_id - 1;

		}

		start = makeNewOrderKey(warehouse_id, districtID, 10000000 + 1);
		end = makeNewOrderKey(warehouse_id, districtID, 1);
		DBTX::Iterator iter1(&rotx, NEWO);
		iter1.Seek(start);

		if(!iter1.Valid())
			return false;

		assert(iter1.Valid());

		iter1.Prev();

		if (iter1.Valid() && iter1.Key() >= end) {
			o_id = static_cast<int32_t>(iter1.Key() << 32 >> 32);
			//assert(o_id == d->d_next_o_id - 1);
			o_id_second = o_id;

			//Consistency 5
			uint64_t *o_value;
			rotx.Get(ORDE, iter.Key(), &o_value);
			cid = ((oorder::value *)(o_value))->o_carrier_id;


		}

		//Consistency 3

		iter1.Seek(end);
		int32_t min = static_cast<int32_t>(iter1.Key() << 32 >> 32);
		num = 0;
		while (iter1.Valid() && iter1.Key() < start) {
			num++;
			iter1.Next();
		}
		//if (o_id - min + 1 != num) printf("o_id %d %d %d",o_id, min, num);
		//assert(o_id - min + 1 == num);
		o_id_min = o_id - min +1;

		//Consistency 4

		end = makeOrderKey(warehouse_id, districtID, 10000000);
		start = makeOrderKey(warehouse_id, districtID, 1);
		iter.Seek(start);
		c = 0;
		while (iter.Valid() && iter.Key() <= end) {
			uint64_t *o_value = iter.Value();

			oorder::value *o = (oorder::value *)o_value;
			c += o->o_ol_cnt;
			iter.Next();
		}
		start = makeOrderLineKey(warehouse_id, districtID, 1, 1);
		end = makeOrderLineKey(warehouse_id, districtID, 10000000, 15);
		c1 = 0;
		DBTX::Iterator iter2(&rotx, ORLI);
		iter2.Seek(start);
		while (iter2.Valid() && iter2.Key() <= end) {
			c1++;
			iter2.Next();
		}
		//assert(c == c1);



		f = rotx.End();
	}

	if(c != c1)
	  return false;

	if(dnext != o_id_first)
	  return false;

	if(dnext != o_id_second)
	  return false;

	if(o_id_min != num)
	  return false;

	if (cid != 10000 && cid !=0)
	  return false;

	assert(c == c1);
	assert(dnext == o_id_first);
	assert(dnext == o_id_second);
	assert(o_id_min == num);

	return true;
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
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (likely(g_disable_xpartition_txn ||
               NumWarehouses() == 1 ||
               RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
       supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }
  INVARIANT(!g_disable_xpartition_txn || allLocal);

  ssize_t ret = 0;

  //TX BEGIN
  rawtx.Begin();

  bool found = false;

  //STEP 6.1 Check ITEM, get i_price
  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_i_id = itemIDs[ol_number - 1];

	uint64_t* i_value;
	found = rawtx.Get(ITEM, ol_i_id, &i_value);
	if(!found) {
		return txn_result(rawtx.Abort(), ret);
	}

    item::value *v_i = (item::value *)i_value;
	checker::SanityCheckItem(NULL, v_i);

	i_prices[ol_number - 1] = v_i->i_price;
  }

  //STEP 1: read w_tax from WAREHOUSE
  uint64_t *w_value;
  found = rawtx.Get(WARE, warehouse_id, &w_value);
  assert(found);

  warehouse::value *v_w = (warehouse::value *)w_value;
  checker::SanityCheckWarehouse(NULL, v_w);

  //STEP 3: read c_discount, c_last, c_credit from CUSTOMER
  uint64_t c_key = makeCustomerKey(warehouse_id, districtID, customerID);
  uint64_t *c_value;
  found = rawtx.Get(CUST, c_key, &c_value);
  assert(found);
  customer::value *v_c = (customer::value *)c_value;
  checker::SanityCheckCustomer(NULL, v_c);


  RTMTX::Begin(stock_lock);

  //STEP 2.1: read d_tax from DISTRICT
  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
  uint64_t *d_value;

  found = rawtx.Get(DIST, d_key, &d_value);
  assert(found);
  district::value *v_d = (district::value *)d_value;
  checker::SanityCheckDistrict(NULL, v_d);

  //STEP 2.2: update d_next_o_id of DISTRICT
  const uint64_t my_next_o_id = v_d->d_next_o_id;
  v_d->d_next_o_id++;

  //STEP 6.2 Update <quantity, order_cnt, remote_cnt> STOCK
  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {

	const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
	const uint ol_quantity = orderQuantities[ol_number - 1];
	float i_price = i_prices[ol_number - 1];

	uint64_t s_key = makeStockKey(ol_supply_w_id, ol_i_id);
	uint64_t* s_value;

	found = rawtx.Get(STOC, s_key, &s_value);
	assert(found);

    stock::value *v_s = (stock::value *)s_value;
	checker::SanityCheckStock(NULL, v_s);

    if (v_s->s_quantity - ol_quantity >= 10)
      v_s->s_quantity -= ol_quantity;
    else
      v_s->s_quantity += -int32_t(ol_quantity) + 91;

    v_s->s_ytd += ol_quantity;
    v_s->s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

	uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, ol_number);
    order_line::value v_ol;

	v_ol.ol_i_id = int32_t(ol_i_id);
    v_ol.ol_delivery_d = 0; // not delivered yet
    v_ol.ol_amount = float(ol_quantity) * i_price;
    v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
    v_ol.ol_quantity = int8_t(ol_quantity);

	rawtx.Add(ORLI, ol_key, (uint64_t *)(&v_ol), sizeof(v_ol));

  }

  RTMTX::End(stock_lock);

  //STEP 5.1: insert a new record to ORDER
  uint64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);
  oorder::value v_oo;
  v_oo.o_c_id = int32_t(customerID);
  v_oo.o_carrier_id = 0; // seems to be ignored
  v_oo.o_ol_cnt = int8_t(numItems);
  v_oo.o_all_local = allLocal;
  v_oo.o_entry_d = GetCurrentTimeMillis();

  rawtx.Add(ORDE, o_key, (uint64_t *)(&v_oo), sizeof(v_oo));


  //STEP 5.2: Update secondary index table of ORDER
  uint64_t o_sec = makeOrderIndex(warehouse_id, districtID, customerID, my_next_o_id);
  uint64_t *value;
  found = rawtx.Get(ORDER_INDEX, o_sec, &value);
  assert(!found);

  //XXX: Local version should have no exist seccondary index too, so directly insert
  uint64_t array_dummy[2];
  array_dummy[0] = 1;
  array_dummy[1] = o_key;
  rawtx.Add(ORDER_INDEX, o_sec, array_dummy, 16);

  //STEP 4: insert a new record to NEWORDER
  uint64_t no_key = makeNewOrderKey(warehouse_id, districtID, my_next_o_id);
  const new_order::value v_no;
  rawtx.Add(NEWO, no_key, (uint64_t *)(&v_no), sizeof(v_no));

  bool b = rawtx.End();


	{
#if CHECKTPCC

		leveldb::DBTX tx(store);
		//printf("Check\n");
		while(true) {

		  tx.Begin();

		  int64_t d_key = makeDistrictKey(warehouse_id, districtID);

		  uint64_t *d_value;
		  bool found = tx.Get(DIST, d_key, &d_value);

		  if(!found) {
			printf("Consistency Error record not found in DISTRICT\n");
			exit(1);
		  }

		  district::value *d = (district::value *)d_value;

		  if(my_next_o_id != d->d_next_o_id - 1) {
			printf("Consistency Error: my_next_o_id != d->d_next_o_id - 1\n");
			exit(1);
		  }

		  int64_t o_key = makeOrderKey(warehouse_id, districtID, my_next_o_id);
		  uint64_t *o_value;
		  found = tx.Get(ORDE, o_key, &o_value);

		  if(!found) {
			printf("Consistency Error record not found in ORDER\n");
			exit(1);
		  }

		  oorder::value *o = (oorder::value *)o_value;

		  if(o->o_c_id != customerID) {
			printf("Consistency Error record not found in o->o_c_id != customerID\n");
			exit(1);
		  }


		  uint64_t l_key = makeOrderLineKey(warehouse_id, districtID, my_next_o_id, 1);
		  uint64_t *l_value;
		  found = tx.Get(ORLI, l_key, &l_value);

		  if(!found) {
			printf("Consistency Error record not found in ORDERLINE\n");
			exit(1);
		  }

		  order_line::value *l = (order_line::value *)l_value;

		  if(l->ol_quantity != orderQuantities[0]) {
			printf("Consistency Error l->ol_quantity != orderQuantities[0]\n");
			exit(1);
		  }

		  if(l->ol_i_id > 100000) {
			printf("Consistency Error l->ol_i_id <= 100000\n");
			exit(1);
		  }


		  uint64_t s_key = makeStockKey(supplierWarehouseIDs[3], itemIDs[3]);
		  uint64_t *s_value;
		  found = tx.Get(STOC, s_key, &s_value);

		  if(!found) {
			printf("Consistency Error record not found in STOCK\n");
			exit(1);
		  }

		  stock::value *s = (stock::value *)s_value;

		  bool b = tx.End();
		  if (b) break;
		}
		printf("check...\n");
		if(!check_consistency()) {
			printf("Consistency Error in check_consistency\n");
		}
#endif
	}

	return txn_result(b, ret);

}



tpcc_worker::txn_result
tpcc_worker::txn_delivery()
{
  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  assert(NumDistrictsPerWarehouse() == 10);
  float sum_ol_amounts[10];

  ssize_t ret = 0;

  //TX Begin
  rawtx.Begin();


  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {

     int32_t no_o_id = 1;
	 uint64_t *no_value;

	 //STEP 1. delete record with minimal o_id from NEWORDER
	 int64_t start = makeNewOrderKey(warehouse_id, d, last_no_o_ids[d - 1]);
	 int64_t end = makeNewOrderKey(warehouse_id, d, numeric_limits<int32_t>::max());
	 int64_t no_key  = -1;

	 Memstore::Iterator* iter = rawtx.GetRawIterator(NEWO);
	 RTMTX::Begin(neworder_lock);
	 iter->Seek(start);

	 if (iter->Valid()) {

		no_key = iter->Key();

		if (no_key <= end) {
			no_o_id = static_cast<int32_t>(no_key << 32 >> 32);
		  	rawtx.Delete(NEWO, no_key);
		} else {
			no_key = -1;
		}
	 }
	 RTMTX::End(neworder_lock);

	if (no_key == -1) {
		printf("NoOrder for district %d!!\n",  d);
		iter->SeekToFirst();
		printf("Key %ld\n", iter->Key());
		continue;
	}

	last_no_o_ids[d - 1] = no_o_id + 1; // XXX: update last seen

  }


 RTMTX::Begin(customer_lock);


 for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {


	int32_t no_o_id = last_no_o_ids[d - 1] - 1;

 	//STEP 2. read o_c_id and update o_carriere_id  of ORDER
	uint64_t o_key = makeOrderKey(warehouse_id, d, no_o_id);
	uint64_t* o_value;
	bool found = rawtx.Get(ORDE, o_key, &o_value);
	oorder::value *v_oo = (oorder::value *)o_value;

	assert(v_oo->o_carrier_id == 0);
	v_oo->o_carrier_id = o_carrier_id;


	//STEP 3. update  ol_delivery_d and record ol_amount of ORDERLINE
	float sum_ol_amount = 0;

	Memstore::Iterator* iter1 = rawtx.GetRawIterator(ORLI);

	int64_t start = makeOrderLineKey(warehouse_id, d, no_o_id, 1);
	int64_t end = makeOrderLineKey(warehouse_id, d, no_o_id, 15);
	iter1->Seek(start);

	while (iter1->Valid()) {

	  int64_t ol_key = iter1->Key();

	  if (ol_key > end)
	  	break;

	  uint64_t *ol_value = iter1->CurNode()->value;

	  order_line::value *v_ol = (order_line::value *)ol_value;

	  sum_ol_amount += v_ol->ol_amount;

      v_ol->ol_delivery_d = ts;

	  iter1->Next();

	}

	sum_ol_amounts[d - 1] = sum_ol_amount;

    //STEP 4. update record of CUSTOMER

	uint64_t c_key = makeCustomerKey(warehouse_id, d, v_oo->o_c_id);
	uint64_t *c_value;
	rawtx.Get(CUST, c_key, &c_value);
	customer::value *v_c = (customer::value *)c_value;

    v_c->c_balance += sum_ol_amounts[d - 1];
	v_c->c_delivery_cnt++;

  }


  RTMTX::End(customer_lock);

  return txn_result(rawtx.End(), ret);

}


tpcc_worker::txn_result
tpcc_worker::txn_payment()
{
  const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn ||
             NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float) (RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  INVARIANT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  ssize_t ret = 0;
  rawtx.Begin();

  //STEP 1. update w_ytd of WAREHOUSE
  uint64_t *w_value;
  bool found = rawtx.Get(WARE, warehouse_id, &w_value);
  assert(found);

  warehouse::value *v_w = (warehouse::value *)w_value;
  checker::SanityCheckWarehouse(NULL, v_w);

  v_w->w_ytd += paymentAmount;


  //STEP 2. update d_ytd of DISTRICT
  uint64_t d_key = makeDistrictKey(warehouse_id, districtID);
  uint64_t *d_value;
  rawtx.Get(DIST, d_key, &d_value);

  district::value *v_d = (district::value *)d_value;
  checker::SanityCheckDistrict(NULL, v_d);
  v_d->d_ytd += paymentAmount;


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

	  RTMTX::Begin(customer_lock);

	  Memstore::Iterator* iter = rawtx.GetRawIterator(CUST_INDEX);

	  iter->Seek(c_start);

	  uint64_t c_keys[100];
	  int j = 0;

	  while (iter->Valid()) {

		if (compareCustomerIndex(iter->Key(), c_end)){


			uint64_t *prikeys = iter->CurNode()->value;
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

	 uint64_t *c_value;

	 found = rawtx.Get(CUST, c_key, &c_value);

	 assert(found);

	 v_c = (customer::value *)c_value;

    } else {

      // 3.2 search with C_ID
      const uint customerID = GetCustomerId(r);

	  c_key = makeCustomerKey(customerWarehouseID,customerDistrictID,customerID);
	  uint64_t *c_value;

	  RTMTX::Begin(customer_lock);
	  found = rawtx.Get(CUST, c_key, &c_value);
  	  assert(found);

	  v_c = (customer::value *)c_value;

    }

	checker::SanityCheckCustomer(NULL, v_c);

    v_c->c_balance -= paymentAmount;
    v_c->c_ytd_payment += paymentAmount;
    v_c->c_payment_cnt++;

    if (strncmp(v_c->c_credit.data(), "BC", 2) == 0) {
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


	RTMTX::End(customer_lock);

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
    v_h.h_data.resize_junk(v_h.h_data.max_size());
    int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
                     "%.10s    %.10s",
                     v_w->w_name.c_str(),
                     v_d->d_name.c_str());
    v_h.h_data.resize_junk(min(static_cast<size_t>(n), v_h.h_data.max_size()));

	rawtx.Add(HIST, h_key, (uint64_t *)(&v_h), sizeof(v_h));

	return txn_result(rawtx.End(), ret);

}




tpcc_worker::txn_result
tpcc_worker::txn_order_status()
{
	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

	//Transaction Begin
	rawtx.Begin();
	RTMTX::Begin(customer_lock);

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

	Memstore::Iterator* citer = rawtx.GetRawIterator(CUST_INDEX);

	citer->Seek(c_start);

	uint64_t *c_values[100];
	uint64_t c_keys[100];
	int j = 0;
	while (citer->Valid()) {

		if (compareCustomerIndex(citer->Key(), c_end)) {

			uint64_t *prikeys = citer->CurNode()->value;

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

	uint64_t *c_value;
	rawtx.Get(CUST, c_key, &c_value);
	v_c = *(customer::value *)c_value;

	} else {

		//1.2 cust by ID
		const uint customerID = GetCustomerId(r);
		c_key = makeCustomerKey(warehouse_id,districtID,customerID);
		uint64_t *c_value;

		rawtx.Get(CUST, c_key, &c_value);
		v_c = *(customer::value *)c_value;

	}

	checker::SanityCheckCustomer(NULL, &v_c);

	//STEP 2. read record from ORDER
	int32_t o_id = -1;
	int o_ol_cnt;

	Memstore::Iterator* iter = rawtx.GetRawIterator(ORDER_INDEX);

	uint64_t start = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 10000000+ 1);
	uint64_t end = makeOrderIndex(warehouse_id, districtID, static_cast<int32_t>(c_key << 32 >> 32), 1);

	iter->Seek(start);
	if(iter->Valid())
	  iter->Prev();
	else printf("ERROR: SeekOut!!!\n");

	if (iter->Valid() && iter->Key() >= end) {

		uint64_t *prikeys = iter->CurNode()->value;
		o_id = static_cast<int32_t>(prikeys[1] << 32 >> 32);
		uint64_t *o_value;

		rawtx.Get(ORDE, prikeys[1], &o_value);
		oorder::value *v_ol = (oorder::value *)o_value;
		o_ol_cnt = v_ol->o_ol_cnt;

	}

	//STEP 3. read record from ORDERLINE
	 if (o_id != -1) {

	    for (int32_t line_number = 1; line_number <= o_ol_cnt; ++line_number) {
		  uint64_t ol_key = makeOrderLineKey(warehouse_id, districtID, o_id, line_number);

		  uint64_t *ol_value;
		  bool found = rawtx.Get(ORLI, ol_key, &ol_value);
	    }
	 }
	 else {
	 	printf("ERROR: No order Found\n");
	 }

	RTMTX::End(customer_lock);
	return txn_result(rawtx.End(), 0);

	}


tpcc_worker::txn_result
tpcc_worker::txn_stock_level()
{

	const uint warehouse_id = PickWarehouseId(r, warehouse_id_start, warehouse_id_end);
	const uint threshold = RandomNumber(r, 10, 20);
	const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

	//STEP 1. read d_next_o_id from DISTRICT
	rawtx.Begin();

	RTMTX::Begin(stock_lock);

	uint64_t d_key = makeDistrictKey(warehouse_id, districtID);

	uint64_t *d_value;
	rawtx.Get(DIST, d_key, &d_value);

	district::value *v_d = (district::value *)d_value;
	checker::SanityCheckDistrict(NULL, v_d);

	const uint64_t cur_next_o_id = v_d->d_next_o_id;

    //STEP 2. read record from ORDERLINE
	const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
	uint64_t start = makeOrderLineKey(warehouse_id, districtID, lower, 0);
	uint64_t end = makeOrderLineKey(warehouse_id, districtID, cur_next_o_id, 0);
	std::vector<int32_t> ol_i_ids;
	ol_i_ids.reserve(300);


	Memstore::Iterator* iter = rawtx.GetRawIterator(ORLI);

	iter->Seek(start);

	while (iter->Valid()) {

		int64_t ol_key = iter->Key();
		if (ol_key >= end) break;

		uint64_t *ol_value = iter->CurNode()->value;
		order_line::value *v_ol = (order_line::value *)ol_value;

		ol_i_ids.push_back(v_ol->ol_i_id);
		iter->Next();
	}

	//STEP 3. read record from STOCK
	std::vector<int32_t> s_i_ids;
	s_i_ids.reserve(300);


	for (size_t i = 0; i < ol_i_ids.size(); ++i) {
    	int64_t s_key = makeStockKey(warehouse_id, ol_i_ids[i]);

		uint64_t *s_value;
		bool found = rawtx.Get(STOC, s_key, &s_value);

		stock::value *v_s = (stock::value *)s_value;
		if (v_s->s_quantity < int(threshold))
	    	s_i_ids.push_back(ol_i_ids[i]);
    }

	RTMTX::End(stock_lock);

	int num_distinct = 0;
	std::sort(s_i_ids.begin(), s_i_ids.end());

    int32_t last = -1;  // NOTE: This relies on -1 being an invalid s_i_id
    for (size_t i = 0; i < s_i_ids.size(); ++i) {
      if (s_i_ids[i] != last) {
        last = s_i_ids[i];
        num_distinct += 1;
      }
    }

	return txn_result(rawtx.End(), 0);

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

	DBTables* store;

	//Lock for different tables
	SpinLock *stock_lock;
	SpinLock *customer_lock;
	SpinLock *neworder_lock;
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
#if 0

  static vector<abstract_ordered_index *>
  OpenTablesForTablespace(abstract_db *db, const char *name, size_t expected_size)
  {

    const bool is_read_only = IsTableReadOnly(name);
    const bool is_append_only = IsTableAppendOnly(name);
    const string s_name(name);
    vector<abstract_ordered_index *> ret(NumWarehouses());
    if (g_enable_separate_tree_per_partition && !is_read_only) {
      if (NumWarehouses() <= nthreads) {
        for (size_t i = 0; i < NumWarehouses(); i++)
          ret[i] = db->open_index(s_name + "_" + to_string(i), expected_size, is_append_only);
      } else {
        const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
        for (size_t partid = 0; partid < nthreads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend   = (partid + 1 == nthreads) ?
            NumWarehouses() : (partid + 1) * nwhse_per_partition;
          abstract_ordered_index *idx =
            db->open_index(s_name + "_" + to_string(partid), expected_size, is_append_only);
          for (size_t i = wstart; i < wend; i++)
            ret[i] = idx;
        }
      }
    } else {
      abstract_ordered_index *idx = db->open_index(s_name, expected_size, is_append_only);
      for (size_t i = 0; i < NumWarehouses(); i++)
        ret[i] = idx;
    }
    return ret;
  }
#endif
public:
  tpcc_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
	stock_lock = new SpinLock();
	customer_lock = new SpinLock();
	neworder_lock = new SpinLock();
#if USESECONDINDEX
	store = new DBTables(9, NumWarehouses());
#else
	store = new DBTables(11, NumWarehouses());
	//store->RCUInit(NumWarehouses());

#endif
	//insert an end value
#if SEPERATE
	store->AddTable(WARE, HASH, NONE);
	store->AddTable(DIST, HASH, NONE);
	store->AddTable(CUST, HASH, SBTREE);
	store->AddTable(HIST, HASH, NONE);
	store->AddTable(NEWO, BTREE, NONE);
	store->AddTable(ORDE, HASH, IBTREE);
	store->AddTable(ORLI, BTREE, NONE);
	store->AddTable(ITEM, HASH, NONE);
	store->AddTable(STOC, HASH, NONE);
#else
#if 1
	printf("AddTable\n");
	for (int i=0; i<9; i++)
		if (i == CUST) store->AddTable(i, BTREE, SBTREE);
		else if (i == ORDE) store->AddTable(i, BTREE, IBTREE);
		else store->AddTable(i, BTREE, NONE);

#else
	for (int i=0; i<9; i++)
		if (i == CUST) {
			int a = store->AddTable(i, CUCKOO, SBTREE);
			if (a != CUST_INDEX) printf("Customer index Wrong!\n");
		}
		else if (i == ORDE) store->AddTable(i, CUCKOO, IBTREE);
		else store->AddTable(i, CUCKOO, NONE);
#endif
#endif

#if !USESECONDINDEX
	store->AddTable(CUST_INDEX, SBTREE, NONE);
	store->AddTable(ORDER_INDEX, BTREE, NONE);
#endif

	//Add the schema
	store->AddSchema(WARE, sizeof(uint64_t), sizeof(warehouse::value));
	store->AddSchema(DIST, sizeof(uint64_t), sizeof(district::value));
	store->AddSchema(CUST, sizeof(uint64_t), sizeof(customer::value));
	store->AddSchema(HIST, sizeof(uint64_t), sizeof(history::value));
	store->AddSchema(NEWO, sizeof(uint64_t), sizeof(new_order::value));
	store->AddSchema(ORDE, sizeof(uint64_t), sizeof(oorder::value));
	store->AddSchema(ORLI, sizeof(uint64_t), sizeof(order_line::value));
	store->AddSchema(ITEM, sizeof(uint64_t), sizeof(item::value));
	store->AddSchema(STOC, sizeof(uint64_t), sizeof(stock::value));

	//XXX FIXME: won't serialize sec index currently
	store->AddSchema(CUST_INDEX, sizeof(uint64_t), 0);
	store->AddSchema(ORDER_INDEX, sizeof(uint64_t), 0);




}

protected:

  virtual	void final_check(){
  	//Stock quantity keeps the same for multiple runs
#if 0
  	for (int i=1;i<=1000; i++){
		Memstore::MemNode *node = store->tables[STOC]->Get(makeStockKey(1,i));
		int q = ((stock::value *)(node->value))->s_quantity;
		printf("I %d Q %d\n",i, q);
  	}
#endif
	//Consistency5
	uint64_t start = makeOrderKey(1, 1, 1);
	Memstore::Iterator *iter = store->tables[ORDE]->GetIterator();
	iter->Seek(start);
	while (iter->Valid() && (iter->Key() < (uint64_t)1<<60)) {
		int32_t cid = ((oorder::value *)(iter->CurNode()->value))->o_carrier_id;
		bool found = store->tables[NEWO]->Get(iter->Key());
		if (cid !=0 && found) {
			printf("CARRIER ID != 0 & found in NEWORDER\n");
			return;
		}
		else if (cid == 0 && !found) {
			printf("Key %lx\n", iter->Key());
			printf("CARRIER ID == 0 & not found in NEWORDER\n");
			return;
		}

		iter->Next();
	}


  }
  virtual void sync_log() {

		RTMTX::localprofile.reportAbortStatus();
		store->Sync();
  }

  	virtual void initPut() {

		  Memstore::MemNode *mn;
		  for (int i=0; i<9; i++) {
			  //Fixme: invalid value pointer
			  Memstore::MemNode *node = store->tables[i]->Put((uint64_t)1<<60, (uint64_t *)new Memstore::MemNode());
			  if (i == ORDE) mn = node;
		  }
#if USESECONDINDEX
		  store->secondIndexes[ORDER_INDEX]->Put((uint64_t)1<<60, (uint64_t)1<<60, mn);
#else

		  //XXX: add empty record to identify the end of the table./
		  uint64_t *temp = new uint64_t[2];
		  temp[0] = 1; temp[1] = 0xFFFF;
		  store->tables[ORDER_INDEX]->Put((uint64_t)1<<60, temp);
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
    if (NumWarehouses() <= nthreads) {
      for (size_t i = 0; i < nthreads; i++)
        ret.push_back(
          new tpcc_worker(
            blockstart + i,
            r.next(), db,
            &barrier_a, &barrier_b,
            (i % NumWarehouses()) + 1, (i % NumWarehouses()) + 2, store, stock_lock, customer_lock, neworder_lock));
    } else {
      const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
      for (size_t i = 0; i < nthreads; i++) {
        const unsigned wstart = i * nwhse_per_partition;
        const unsigned wend   = (i + 1 == nthreads) ?
          NumWarehouses() : (i + 1) * nwhse_per_partition;
        ret.push_back(
          new tpcc_worker(
            blockstart + i,
            r.next(), db,
            &barrier_a, &barrier_b, wstart+1, wend+1, store, stock_lock, customer_lock, neworder_lock));
      }
    }
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
