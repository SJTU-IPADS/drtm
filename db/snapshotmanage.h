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

#ifndef SSMANAGE_H
#define SSMANAGE_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "util/spinlock.h"
#include <pthread.h>


class RAWTables;

// number of nanoseconds in 1 second (1e9)
#define ONE_SECOND_NS 1000000000

//20ms
#define UPDATEPOCH  ONE_SECOND_NS / 1000 * 20

#define PROFILESS

class SSManage {

static __thread int tid_;

uint64_t thr_num;

volatile uint64_t readSS;
volatile int workerNum;
SpinLock sslock;
volatile uint64_t *localSS;
//SpinLock *localLock;
pthread_t update_tid;

public:
 pthread_rwlock_t *rwLock;
volatile uint64_t curSS;
 RAWTables *rawtable;
#ifdef PROFILESS
 int totalss;
 long totalepoch;
 long maxepoch;
 long minepoch;
#endif



 SSManage(int thr);

 ~SSManage();

 static void* UpdateThread(void * arg);

 void RegisterThread(int tid);

 void UpdateSS();


 void UpdateReadSS();

 uint64_t GetLocalSS();
 uint64_t GetMySS();
 uint64_t GetReadSS();
 void WaitAll(uint64_t sn);

 void UpdateLocalSS(uint64_t ss);

 static int diff_timespec(struct timespec &end, struct timespec &start);

 void ReportProfile();
};


#endif
