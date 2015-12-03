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

#ifndef DRTM_BENCH_BANK_H
#define DRTM_BENCH_BANK_H
//

#include "encoder.h"
#include "inline_str.h"
#include "macros.h"
#include "bench.h"

/*   table accounts   */

#define ACCOUNTS_KEY_FIELDS(x,y) \
  x(uint64_t,a_custid)

#define ACCOUNTS_VALUE_FIELDS(x,y) \
  x(inline_str_16<64>,a_name)
DO_STRUCT(account,ACCOUNTS_KEY_FIELDS,ACCOUNTS_VALUE_FIELDS)

/* ------- */

/*   table savings   */

#define SAVINGS_KEY_FIELDS(x,y) \
  x(uint64_t,s_cusitid)

#define SAVINGS_VALUE_FIELDS(x,y) \
  x(float,s_balance)
DO_STRUCT(savings,SAVINGS_KEY_FIELDS,SAVINGS_VALUE_FIELDS)

/* ------- */


/*   table checking   */

#define CHECKING_KEY_FIELDS(x,y) \
  x(uint64_t ,c_custid)

#define CHECKING_VALUE_FIELDS(x,y) \
  x(float,c_balance)
DO_STRUCT(checking,CHECKING_KEY_FIELDS,CHECKING_VALUE_FIELDS)

/* ------- */

#endif
