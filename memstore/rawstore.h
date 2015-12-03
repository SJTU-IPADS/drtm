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

#ifndef RAWSTORE_H
#define RAWSTORE_H

#include <stdlib.h>
#include <iostream>
#include <assert.h>


class RAWStore {

 public:

  class Iterator {

  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(){};
    Iterator(RAWStore* store){};

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid(){return false;}

    virtual uint64_t* Value(){return NULL;}

    virtual uint64_t Key() {return -1;}

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual bool Next() {return false;}

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual bool Prev() {return false;}

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key) {}

    virtual void SeekPrev(uint64_t key) {}

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() {}

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() {}

  };

 public:
  RAWStore(){}

  ~RAWStore() {
  }

  inline virtual uint64_t* Get(uint64_t key) {};

  virtual uint64_t* Delete(uint64_t key) = 0;

  virtual void Put(uint64_t key, uint64_t *value) = 0;

  virtual RAWStore::Iterator* GetIterator() {return NULL;}

  virtual void PrintStore() {};

  void PrintCSet() {};


};


#endif
