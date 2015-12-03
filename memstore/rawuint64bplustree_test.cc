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


#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include <deque>
#include <set>
#include "port/port.h"
#include "port/atomic.h"
#include <iostream>
#include "util/mutexlock.h"
#include "leveldb/comparator.h"

#include <vector>
#include "raw_uint64bplustree.h"

static const char* FLAGS_benchmarks ="random";

static int FLAGS_num = 10000000;
static int FLAGS_threads = 1;

#define CHECK 0

namespace drtm {

  typedef uint64_t Key[5];

  __inline__ int64_t XADD64(int64_t* addr, int64_t val) {
    asm volatile(
		 "lock;xaddq %0, %1"
		 : "+a"(val), "+m"(*addr)
		 :
		 : "cc");

    return val;
  }


  class Benchmark {


  private:


    int64_t total_count;

    RAWStore *btree;

    RAWStore *rawbt;

    port::SpinLock slock;

    Random ramdon;

  private:

    struct SharedState {
      port::Mutex mu;
      port::CondVar cv;
      int total;

      volatile double start_time;
      volatile double end_time;

      int num_initialized;
      int num_done;
      bool start;

      std::vector<Arena*>* collector;

      SharedState() : cv(&mu) { }
    };

    // Per-thread state for concurrent executions of the same benchmark.
    struct ThreadState {
      int tid;			   // 0..n-1 when running in n threads
      SharedState* shared;
      int count;
      double time;
      Random rnd;         // Has different seeds for different threads

      ThreadState(int index)
	: tid(index),
	  rnd(1000 + index) {
      }

    };

    struct ThreadArg {
      Benchmark* bm;
      SharedState* shared;
      ThreadState* thread;
      void (Benchmark::*method)(ThreadState*);
    };

    static void ThreadBody(void* v) {

      printf("ThreadBody\n");
      ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
      SharedState* shared = arg->shared;
      ThreadState* thread = arg->thread;
      {
	MutexLock l(&shared->mu);


	shared->num_initialized++;
	if (shared->num_initialized >= shared->total) {
	  shared->cv.SignalAll();
	}
	while (!shared->start) {
	  shared->cv.Wait();
	}
      }

      double start = leveldb::Env::Default()->NowMicros();
      if(shared->start_time == 0)
	shared->start_time = start;

      (arg->bm->*(arg->method))(thread);

      double end = leveldb::Env::Default()->NowMicros();
      shared->end_time = end;
      thread->time = end - start;

      {
	MutexLock l(&shared->mu);

	shared->num_done++;
	if (shared->num_done >= shared->total) {
	  shared->cv.SignalAll();
	}
      }
    }




    void Insert(ThreadState* thread) {

      int tid = thread->tid;
      int seqNum = 0;
      uint64_t* arr = (uint64_t *)malloc(sizeof(uint64_t)*10000);

      uint64_t *key = (uint64_t *)malloc(sizeof(uint64_t)*5);
      for(int i = 0; i < 5; i++)
	key[i] = 0;

      key[1] = tid;

      for (int i=0; i<10000; i++) {
	key[2] = i + 1 + tid*10000;
	//	printf("Insert %d %ld\n", tid, k);
	arr[i] = i + 5;

	btree->Put((uint64_t)key, &arr[i]);
      }

      for(int i = 0; i < 5; i++)
	key[i] = 0;

      uint64_t *n = (uint64_t *)malloc(sizeof(uint64_t));
      *n = tid;
      btree->Put((uint64_t)key, n);
      //btree->PrintStore();
    }

    void ReadAndIter(ThreadState* thread) {
      Insert(thread);
      int tid = thread->tid;

      uint64_t *k = (uint64_t *)malloc(sizeof(uint64_t)*5);
      k[0] = k[1] = k[2] = k[3] = k[4] = 0;

      uint64_t *t = btree->Get((uint64_t)k);
      printf("0 Tid %d %ld\n", tid, *t);

      k[1] = tid;
      k[2] = 1000 + tid*10000;
      uint64_t* v = btree->Get((uint64_t)k);
      assert(v!=NULL && *v == 1004);

      RAWStore::Iterator* iter = btree->GetIterator();

      iter->Seek((uint64_t)k);

      assert(iter->Valid());
      assert(iter->Next());

      btree->Delete((uint64_t)k);

      v = btree->Get((uint64_t)k);
      assert(v==NULL);

      RAWStore::Iterator* iter1 = btree->GetIterator();
      iter1->Seek((uint64_t)k);
      assert(iter1->Valid());
      uint64_t* kf = (uint64_t*)iter1->Key();
      printf("Seek Key %d-%d-%d-%d-%d Get Key %d-%d-%d-%d-%d", k[0],k[1],k[2],
	     k[3], k[4], kf[0], kf[1], kf[2], kf[3], kf[4]);
      printf(" Value %d\n", *(iter1->Value()));

      printf("Prev %d\n", iter->Prev());
      kf = (uint64_t*)iter->Key();
      printf(" Old %d-%d-%d-%d-%d %d %d", kf[0],kf[1],kf[2],kf[3],kf[4], iter->Valid(), *(iter->Value()));

      iter->SeekToFirst();
      assert(iter->Valid());
      kf = (uint64_t*)iter->Key();
      printf(" Then %d-%d-%d-%d-%d %d %d\n", kf[0],kf[1],kf[2],kf[3],kf[4], iter->Valid(), *(iter->Value()));

      k[0] = k[1] = k[2] = k[3] = k[4] = 0;
      t = btree->Get((uint64_t)k);
      printf("0 Tid %d %ld\n", tid, *t);
    }




  public:

    Benchmark(): total_count(FLAGS_num), ramdon(1000){}
    ~Benchmark() {



    }

    void RunBenchmark(int n, int num,
		      void (Benchmark::*method)(ThreadState*)) {
      SharedState shared;
      shared.total = n;
      shared.num_initialized = 0;
      shared.start_time = 0;
      shared.end_time = 0;
      shared.num_done = 0;
      shared.start = false;


      //		double start = leveldb::Env::Default()->NowMicros();

      ThreadArg* arg = new ThreadArg[n];
      for (int i = 0; i < n; i++) {
	arg[i].bm = this;
	arg[i].method = method;
	arg[i].shared = &shared;
	arg[i].thread = new ThreadState(i);
	arg[i].thread->shared = &shared;
	arg[i].thread->count = num;
	arg[i].thread->time = 0;
	Env::Default()->StartThread(ThreadBody, &arg[i]);
      }

      shared.mu.Lock();
      while (shared.num_initialized < n) {
	shared.cv.Wait();
      }

      shared.start = true;
      printf("Send Start Signal\n");
      shared.cv.SignalAll();
      //		std::cout << "Startup Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;

      while (shared.num_done < n) {
	shared.cv.Wait();
      }
      shared.mu.Unlock();


      printf("Total Run Time : %lf ms\n", (shared.end_time - shared.start_time)/1000);

      for (int i = 0; i < n; i++) {
	printf("Thread[%d] Run Time %lf ms\n", i, arg[i].thread->time/1000);
      }

      for (int i = 0; i < n; i++) {
	delete arg[i].thread;
      }
      delete[] arg;
    }


    void Run(){

      btree = new RawUint64BPlusTree();
      //     RunBenchmark(1, 1, &Benchmark::DeleteSingleThread);

      //	return;

      int num_threads = 2; //FLAGS_threads;
      int num_ = FLAGS_num/num_threads;

      void (Benchmark::*wmethod)(ThreadState*) = NULL;
      void (Benchmark::*rmethod)(ThreadState*) = NULL;

      Slice name = FLAGS_benchmarks;


      //	  double start = leveldb::Env::Default()->NowMicros();
      total_count = FLAGS_num;
      /*	  uint64_t k = (uint64_t)1 << 35;
		  for (uint64_t i =1; i< 4; i++){

		  btree->insert( k - i);
		  btree->insert( k + i);
		  }*/
      RunBenchmark(num_threads, num_, &Benchmark::ReadAndIter);
      uint64_t *key = (uint64_t *)malloc(sizeof(uint64_t)*5);
      for(int i = 0; i < 5; i++)
	key[i] = 0;

      uint64_t *v = btree->Get((uint64_t)key);

      printf("0 V %d\n", *v);

      //	  total_count = FLAGS_num;
      //     std::cout << "Total Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;

      delete btree;
    }

  };




}  //



int main(int argc, char** argv) {

  for (int i = 1; i < argc; i++) {

    int n;
    char junk;

    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    }
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  //  while (1);
  return 1;
}
