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

#ifndef RTMRegion_H_
#define RTMRegion_H_

#include <immintrin.h>
#include <sys/time.h>

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)



struct RTMRegionProfile {
  int abort;
  int succ;
  int conflict;
  int capacity;
  int zero;
  int nest;

RTMRegionProfile():
  abort(0), succ(0), conflict(0), capacity(0), zero(0), nest(0){}

  void ReportProfile()
  {
    printf("Avg Abort %.5f [Conflict %.5f : Capacity %.5f Zero: %.5f Nest: %.5f] \n",
	   abort/(double)(abort+succ), conflict/(double)succ, capacity/(double)succ,
	   zero/(double)succ, nest/(double)succ);
  }

  void Reset(){
    abort = 0;
    succ = 0;
    conflict = 0;
    capacity = 0;
    zero = 0;
    nest = 0;
  }
};


#define MAXRETRY 10000000

class RTMRegion {

 public:

  RTMRegionProfile* prof;
  volatile int abort;
  int conflict;
  int capacity;
  int zero;
  int nest;

  inline RTMRegion(RTMRegionProfile *p) {

#ifdef PROF
    abort = 0;
    conflict = 0;
    capacity = 0;
    zero = 0;
    prof = p;
#endif

    while(true) {
      register unsigned stat;
      stat = _xbegin();

      //Likely is just make assembling code more readable
      if(likely(stat == _XBEGIN_STARTED)) {
	return;

      } else {

#ifdef PROF
	abort++;

	if(stat & _XABORT_NESTED)
	  nest++;

	if(stat & _XABORT_CONFLICT)
	  conflict++;

	if(stat & _XABORT_CAPACITY) {
	  capacity++;
	}

	if(stat == 0)
	  zero++;

	if(abort > MAXRETRY)
	  return;
#endif
      }
    }

  }

  void Abort() {

    _xabort(0x1);

  }
  inline  ~RTMRegion() {
    if(_xtest())
      _xend ();
    else
      return;
#ifdef PROF
    if(prof != NULL) {
      prof->abort += abort;
      prof->capacity += capacity;
      prof->conflict += conflict;
      prof->zero += zero;
      prof->nest += nest;
      prof->succ++;
    }
#endif
  }

};



#endif  // STORAGE_LEVELDB_UTIL_RTM_H_
