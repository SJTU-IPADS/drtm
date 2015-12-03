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

#include <stdio.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <malloc.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>
#include <sched.h>

#define PROF
#include "rtmRegion.h"
#include "../util/random.h"
typedef unsigned long uint64_t;

#define PAGESIZE 4*1024 //4KB

//#define ARRAYSIZE 4*1024*1024/CASHELINESIZE //4M
#define ARRAYSIZE 8*1024*1024
#define CASHELINESIZE 64 //64 bytes

struct Cacheline {
  char data[CASHELINESIZE];
};

//critical data
char padding0[64];
//only used in mix model
int readset = 16*1024;
int writeset = 16*1024;
char padding[64];
int workingset = 16 * 1024; //Default ws: 16KB
__thread char *array;
char padding1[64];

volatile int ready = 0;
volatile int epoch = 0;
int thrnum = 1;
int bench = 1; // 1: read 2: write 3: mix
int length = ARRAYSIZE/4;

inline int Read(char * data) {
  register int res = 0;
  register int ws  = workingset / sizeof(int);
  for(register int i = 0; i < ws; i++) {
    res += ((int *)(data))[i];
  }
  return res;
}


inline int RandomRead(char * data) {
  register int res = 0;
  register int ws  = workingset / sizeof(int);
  for(register int i = 0;i < ws;++i){
    register int index = (i*i) % (ARRAYSIZE / sizeof(int));
    res += ((int *)data)[index];
  }
  return res;
}


inline int RandomWrite2(char * data) {
  register int res = 0;
  for(register int i = 0;i < workingset / sizeof(int);++i){
    register int index = (i*i) % (ARRAYSIZE / sizeof(int));
    ((int *)data)[index]++;
  }

}


inline int Read2(char * data) {
  int res = 0;
  for(int i = 0; i < workingset; i++) {
    res += (int)data[i];
  }
  return res;
}

inline void Write(int * data) {
  register int ws = workingset / sizeof(int);
  for(register int i = 0; i < (ws); i++) {
    data[i]++;
  }
}

inline void Write2(char * data) {
  register int ws = workingset;
  for(register int i = 0; i < ws; i++) {
    data[i]++;
  }
}

inline void RandomWrite(int *data) {

  register int next = 0;
  register int l = length;

  for(register int i = 0; i < workingset / sizeof(int); i++) {
    if(data[next] >= l - 2)
      data[next] = 0;
    next = ++data[next];
  }
  //end random write
}

inline int ReadWrite(char* data) {
  int res = 0;
  int i = 0;

  for(; i < writeset; i++) {
    data[i]++;
  }

  int j = i;
  for(; i < readset + j; i++) {
    res += (int)data[i];
  }


  return res;
}

int
diff_timespec(const struct timespec &end, const struct timespec &start)
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

void thread_init(){
  //Allocate the array at heap
  array = (char *)malloc(ARRAYSIZE);
  int *data = (int*)array;
  leveldb::Random r(0xdead);

  for (int i =0; i < length; i++) {
    data[i] = r.Next() % length ;
    //data[i] -= data[i] % 64;
    //printf("%d\n",data[i]);
  }
  //Touch every byte to avoid page fault
  //memset(array, 1, ARRAYSIZE * sizeof(Cacheline));

}

void* thread_body(void *x) {

  RTMRegionProfile prof;
  int count = 0;
  int lbench = 4;
  int lepoch = 0;

  struct timespec start, end;

  uint64_t tid = (uint64_t)x;
  int cpu = 1;
  if(tid == 1)
    cpu = 5;

  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(tid, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  thread_init();
  printf("init\n");
  __sync_fetch_and_add(&ready, 1);

  while(epoch == 0);

  clock_gettime(CLOCK_REALTIME, &start);


  lepoch = epoch;

  while(true) {

    {
      RTMRegion rtm(&prof);
      if(lbench == 1)
	count += RandomRead((char *)array);
      //count += Read((char *)array);
      else if(lbench == 2) {
	Write((int *)array);
      }
      else if(lbench == 3)
	count += ReadWrite((char *)array);
      else if(lbench == 4)
	RandomWrite2((char *)array);


    }

    if(lepoch < epoch) {



      clock_gettime(CLOCK_REALTIME, &end);
      printf("Thread [%d] Time: %.2f s \n",
	     tid, diff_timespec(end, start)/1000.0);

      prof.ReportProfile();
      prof.Reset();
      printf("count %d\n", count);

      clock_gettime(CLOCK_REALTIME, &start);


      lepoch = epoch;
    }

  }

  prof.ReportProfile();

}


int main(int argc, char** argv) {

  //Parse args
  if(argc >= 1)
    workingset = atoi(argv[1]) ;
  /*
    for(int i = 1; i < argc; i++) {

    int n = 0;
    char junk;
    if (strcmp(argv[i], "--help") == 0){
    printf("./a.out --ws=working set size (KB default:16KB)\n");
    return 1;
    }
    else if(sscanf(argv[i], "-ws=%d%c", &n, &junk) == 1) {
    workingset = n;// = n * 1024;
    }else if(sscanf(argv[i], "-wset=%d%c", &n, &junk) == 1) {
    writeset = n * 1024;
    }else if(sscanf(argv[i], "-thr=%d%c", &n, &junk) == 1) {
    thrnum = n;
    }else if(sscanf(argv[i], "-rset=%d%c", &n, &junk) == 1) {
    readset= n * 1024;
    }else if(strcmp(argv[i], "-ht") == 0) {
    thrnum = 2;
    }else if(strcmp(argv[i], "-r") == 0) {
    bench = 1;
    }else if(strcmp(argv[i], "-w") == 0) {
    bench = 2;
    }else if(strcmp(argv[i], "-m") == 0) {
    bench = 3;
    }
    }
  */

  if(bench != 3)
    printf("Touch Work Set %d\n", workingset);
  else
    printf("Read %d Write %d\n", readset, writeset);

  pthread_t *th = new pthread_t[thrnum];
  for(int i = 0; i < thrnum; i++)
    pthread_create(&th[i], NULL, thread_body, (void *)i);

  //Barriar to wait all threads become ready
  while (ready < 1);

  //Begin at the first epoch
  epoch = 1;

  sleep(1); //for warmup
  epoch++;

  while(true) {
    sleep(1);
    epoch++;
  }

  for(int i = 0; i < thrnum; i++)
    pthread_join(th[i], NULL);


  return 1;
}
