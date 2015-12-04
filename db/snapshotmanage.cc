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
 *  Epoch Hao Qian
 *
 */

#include "snapshotmanage.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#include "memstore/rawtables.h"

__thread int SSManage::tid_;

SSManage::SSManage(int thr)
{
  thr_num = thr;
  localSS = new uint64_t[thr];
  curSS = 1;

  readSS = 0;

  workerNum = 0;

#ifdef PROFILESS
  totalss = 0;
  totalepoch = 0;
  maxepoch = 0;
  minepoch = 0;
#endif

  //Create Serialization Thread
  pthread_create(&update_tid, NULL, UpdateThread, (void *)this);
}

SSManage::~SSManage()
{
  //FIMME: should join the update thread here
}

int SSManage::diff_timespec(struct timespec &end, struct timespec &start)
{
  int diff = (end.tv_sec > start.tv_sec)?(end.tv_sec-start.tv_sec)*1000:0;
  assert(diff || end.tv_sec == start.tv_sec);
  if (end.tv_nsec > start.tv_nsec) {
    diff += (end.tv_nsec-start.tv_nsec)/1000000;
  } else {
    diff -= (start.tv_nsec-end.tv_nsec)/1000000;
  }
  return diff;
}

void* SSManage::UpdateThread(void * arg)
{
  SSManage* ssmm = (SSManage*)arg;

  while(true) {

#ifdef PROFILESS
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
#endif

    struct timespec t;
    t.tv_sec  = UPDATEPOCH / ONE_SECOND_NS ;
    t.tv_nsec = UPDATEPOCH % ONE_SECOND_NS ;
    nanosleep(&t, NULL);
    ssmm->UpdateSS();

#ifdef PROFILESS
    clock_gettime(CLOCK_REALTIME, &end);
    int epoch = diff_timespec(end, start);
    ssmm->totalepoch += epoch;
    ssmm->totalss++;
    if(ssmm->maxepoch < epoch)
      ssmm->maxepoch = epoch;
    if(ssmm->minepoch > epoch || ssmm->minepoch == 0)
      ssmm->minepoch = epoch;
#endif
  }
}

void SSManage::ReportProfile()
{
  printf("Avg Epoch %ld (ms) Max Epoch %ld (ms) Min Epoch %ld (ms) Snap Update Number %d\n",
	 totalepoch/totalss, maxepoch, minepoch, totalss);
}



void SSManage::RegisterThread(int tid)
{
  tid_ = tid;
  localSS[tid] = 1;
}

void SSManage::UpdateSS()
{
  struct timespec t;
  t.tv_sec  = 0;
  t.tv_nsec = ONE_SECOND_NS / 1000; //1ms

  curSS++;
}

void SSManage::UpdateReadSS()
{

}


uint64_t SSManage::GetLocalSS()
{
  assert(tid_ < thr_num);
  return curSS;
}

uint64_t SSManage::GetMySS(){
  return localSS[tid_] ;
}
uint64_t SSManage::GetReadSS()
{
  readSS = localSS[0];
  for (int i=1; i<thr_num; i++)
    if (readSS > localSS[i]) readSS = localSS[i];
  readSS--;

  return readSS;
}

void SSManage::UpdateLocalSS(uint64_t ss)
{
  localSS[tid_] = ss;
}

void SSManage::WaitAll(uint64_t ss)
{
  for (int i=0; i<thr_num; i++) {
    while(localSS[i] < ss);
  }
}
