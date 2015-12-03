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


#ifndef STORAGE_LEVELDB_UTIL_RTMSCOPE_H_
#define STORAGE_LEVELDB_UTIL_RTMSCOPE_H_
#include <immintrin.h>
#include <sys/time.h>
#include "txprofile.h"
#include "spinlock.h"
#include "rtm_arena.h"

#define LOCKELISION 1
#define MAXRETRY 10
#define RTMArenaPROFILE 0


class RTMArenaScope {

  RTMProfile localprofile;
  RTMProfile* globalprof;
  SpinLock* slock;
  RTMArena* arena_;
  int retry;
  int capacity;
  int conflict;
  int explict;
  int lockacquire;
  int zero;

 public:
  inline RTMArenaScope(SpinLock* sl, RTMProfile* prof, RTMArena* arena) {

    arena_ = arena;

    globalprof = prof;
    retry = 0;
    capacity = 0;
    conflict = 0;
    lockacquire = 0;
    explict = 0;
    zero = 0;
    slock = sl;

    while(true) {
      unsigned stat;
      stat = _xbegin ();
      if(stat == _XBEGIN_STARTED) {
	if(!slock->IsLocked())
	  return;

	_xabort(0xff);

      }
      retry++;
      if((stat & _XABORT_CONFLICT) != 0)
	conflict++;
      else if((stat & _XABORT_CAPACITY) != 0)
	capacity++;
      else if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat)==0xf0) {
	// printf("allocation\n");
	explict++;
	arena_->AllocateFallback();

      }
      else if (stat == 0)
	zero++;


#if 0
      localprofile.localRecordAbortStatus(stat);
      if(retry > 100000000){
	localprofile.reportAbortStatus();
	printf("stat %d\n",stat);
	exit(1);
	retry = 0;
      }
#endif



#if LOCKELISION
      else if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat)==0xff)
	{
	  while(slock->IsLocked())
	    _mm_pause();
	}

      if(zero > 2)
	break;

      if(capacity > 4)
	break;
      if(conflict > 8)
	break;
      if(retry > 32)
	break;

#endif



    }
    //    printf("Hold Lock\n");
    lockacquire++;
    slock->Lock();

  }

  void Abort() {
    _xabort(0x1);
  }

  inline  ~RTMArenaScope() {

#if LOCKELISION

    if(slock->IsLocked())
      slock->Unlock();
    else
      {
	_xend();
	//		printf("!!!\n");
      }

#else
    _xend();

#endif

#if RTMArenaPROFILE
    //access the global profile info outside the transaction scope
    if(globalprof != NULL) {
      RTMProfile::atomic_inc32(&globalprof->succCounts);
      RTMProfile::atomic_add32(&globalprof->abortCounts, retry);
      RTMProfile::atomic_add32(&globalprof->status[XABORT_CONFLICT_INDEX], conflict);
      RTMProfile::atomic_add32(&globalprof->status[XABORT_CAPACITY_INDEX], capacity);
      RTMProfile::atomic_add32(&globalprof->status[XABORT_EXPLICIT_INDEX], explict);
      RTMProfile::atomic_add32(&globalprof->status[XABORT_DEBUG_INDEX], zero);
      RTMProfile::atomic_add32(&globalprof->status[XABORT_NESTED_INDEX], lockacquire);

      //globalprof->MergeLocalStatus(localprofile);

    }
#endif

  }

};


#endif  // STORAGE_LEVELDB_UTIL_RTM_H_
