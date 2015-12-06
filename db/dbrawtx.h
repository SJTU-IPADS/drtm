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


#ifndef DBRAWTX_H
#define DBRAWTX_H

#include <string>
#include "memstore/rawtables.h"


class DBRAWTX {

 public:

  DBRAWTX(RAWTables* tables);


  ~DBRAWTX();

  void Begin();
  bool Abort();
  bool End();

  void Add(int tableid, uint64_t key, uint64_t* val);

  //Copy value
  void Add(int tableid, uint64_t key, uint64_t* val, int len);

  bool Get(int tableid, uint64_t key, uint64_t** val);

  __attribute__((always_inline)) uint64_t* GetRecord(int tableid, uint64_t key)
  {
    return txdb_->Get(tableid, key);
  }

  void Delete(int tableid, uint64_t key);

  RAWStore::Iterator *GetRawIterator(int tableid);

 public:
  RAWTables *txdb_;


};


inline void DBRAWTX::Add(int tableid, uint64_t key, uint64_t* val)

{
  txdb_->Put(tableid, key, val);
}


inline void DBRAWTX::Add(int tableid, uint64_t key, uint64_t* val, int len)
{

  char* value = new char[len];
  memcpy(value, val, len);
  txdb_->Put(tableid, key, (uint64_t *)value);

}


inline void DBRAWTX::Delete(int tableid, uint64_t key)
{

	txdb_->Delete(tableid, key);
}


inline RAWStore::Iterator *DBRAWTX::GetRawIterator(int tableid)
{
	return 	txdb_->GetIterator(tableid);
}

inline bool DBRAWTX::Get(int tableid, uint64_t key, uint64_t** val)
{

  *val = txdb_->Get(tableid, key);

  if(*val == NULL)
  	return false;

   return true;
}


#endif
