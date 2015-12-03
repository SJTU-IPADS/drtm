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

#ifndef RAWUINT64BPLUSTREE_H
#define RAWUINT64BPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include "util/rtmScope.h"
#include "util/rtm.h"

#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"
#include "rawstore.h"
#include "port/atomic.h"

#define RAWM  15
#define RAWN  15

#define BTREE_PROF 0
#define BTREE_LOCK 0


typedef uint64_t KEY[5];

class RawUint64BPlusTree: public RAWStore {

 private:
  struct LeafNode {
  LeafNode() : num_keys(0){}//, writes(0), reads(0) {}
    //		uint64_t padding[4];
    unsigned num_keys;
    KEY keys[RAWM];
    uint64_t *values[RAWM];
    LeafNode *left;
    LeafNode *right;
  };



  struct InnerNode {
  InnerNode() : num_keys(0) {}//, writes(0), reads(0) {}
    unsigned num_keys;
    KEY keys[RAWN];
    void*	 children[RAWN+1];
  };


  struct DeleteResult {
  DeleteResult(): value(0), freeNode(false){
    upKey[0] = -1;
  }
    uint64_t* value;  //The value of the record deleted
    bool freeNode;	//if the children node need to be free
    KEY upKey; //the key need to be updated -1: default value
  };


  class Iterator: public RAWStore::Iterator {
  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(){};
    Iterator(RawUint64BPlusTree* tree);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();

    // Returns the key at the current position.
    // REQUIRES: Valid()
    uint64_t* Value();

    uint64_t Key();

    // Advances to the next position.
    // REQUIRES: Valid()
    bool Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    bool Prev();

    // Advance to the first entry with a key >= target
    void Seek(uint64_t key);

    void SeekPrev(uint64_t key);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

  private:
    RawUint64BPlusTree* tree_;
    LeafNode* node_;
    int leaf_index;
    KEY key_;
    uint64_t* value_;
    // Intentionally copyable
  };

 public:
  RawUint64BPlusTree() {
    array_length = 5;
    root = new LeafNode();
    reinterpret_cast<LeafNode*>(root)->left = NULL;
    reinterpret_cast<LeafNode*>(root)->right = NULL;
    depth = 0;
  }

  ~RawUint64BPlusTree() {
    //delprof.reportAbortStatus();
  }

  inline LeafNode* new_leaf_node() {
    LeafNode* result = new LeafNode();
    return result;
  }

  inline InnerNode* new_inner_node() {
    InnerNode* result = new InnerNode();
    return result;
  }

  inline int Compare(uint64_t *a, uint64_t *b) {
    for (int i=0; i<array_length; i++) {
      if (a[i] > b[i]) return 1;
      if (a[i] < b[i]) return -1;
    }
    return 0;

  }

  inline void ArrayAssign(uint64_t *n, uint64_t *o) {
    for (int i=0; i<array_length; i++)
      n[i] = o[i];
    //		memcpy(n, o, array_length*8);
  }

  inline LeafNode* FindLeaf(KEY key)
  {
    InnerNode* inner;
    register void* node= root;
    register unsigned d= depth;
    unsigned index = 0;
    while( d-- != 0 ) {
      index = 0;
      inner= reinterpret_cast<InnerNode*>(node);
      while(index < inner->num_keys)  {
	int tmp = Compare(key, inner->keys[index]);
	if (tmp < 0) break;
	++index;
      }
      node= inner->children[index];
    }

    return reinterpret_cast<LeafNode*>(node);
  }


  inline uint64_t* Get(uint64_t key) {
#if 0
#if BTREE_LOCK
    MutexSpinLock lock(&slock);
#else
    RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif
#endif

#if SBTREE_PROF
    calls++;
#endif

    InnerNode* inner;
    register void* node= root;
    register unsigned d= depth;
    register unsigned index = 0;
    while( d-- != 0 ) {
      index = 0;
      inner= reinterpret_cast<InnerNode*>(node);

      while(index < inner->num_keys) {
	int tmp = Compare((uint64_t *)key, inner->keys[index]);
	if (tmp < 0) break;
	++index;
      }
      node= inner->children[index];
    }
    LeafNode* leaf= reinterpret_cast<LeafNode*>(node);

    if (leaf->num_keys == 0) return NULL;
    register unsigned k = 0;
    while(k < leaf->num_keys) {

      int tmp = Compare(leaf->keys[k], (uint64_t *)key);
      if (tmp == 0) return leaf->values[k];
      if (tmp > 0) return NULL;
      ++k;
    }

    return NULL;

  }

  void Put(uint64_t key, uint64_t* val){
#if 0
#if BTREE_LOCK
    MutexSpinLock lock(&slock);
#else
    RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif
#endif
    if (root == NULL) {
      root = new_leaf_node();
      reinterpret_cast<LeafNode*>(root)->left = NULL;
      reinterpret_cast<LeafNode*>(root)->right = NULL;
      depth = 0;
    }

    if (depth == 0) {
      LeafNode *new_leaf = LeafInsert((uint64_t *)key, reinterpret_cast<LeafNode*>(root), val);
      if (new_leaf != NULL) {
	InnerNode *inner = new_inner_node();
	inner->num_keys = 1;
	ArrayAssign(inner->keys[0], new_leaf->keys[0]);
	inner->children[0] = root;
	inner->children[1] = new_leaf;
	depth++;
	root = inner;
      }
    }
    else {
      InnerInsert((uint64_t *)key, reinterpret_cast<InnerNode*>(root), depth, val);
    }

  }

  inline int slotAtLeaf(KEY key, LeafNode* cur) {

    int slot = 0;

    while(slot < cur->num_keys) {
      int tmp = Compare(cur->keys[slot] ,key);
      if (tmp >= 0) break;
      slot++;
    }

    return slot;
  }

  inline uint64_t* removeLeafEntry(LeafNode* cur, int slot) {

    assert(slot < cur->num_keys);

    uint64_t* value = cur->values[slot];

    cur->num_keys--;

    //The key deleted is the last one
    if (slot == cur->num_keys)
      return value;

    //Re-arrange the entries in the leaf
    for(int i = slot + 1; i <= cur->num_keys; i++) {
      ArrayAssign(cur->keys[i - 1] , cur->keys[i]);
      cur->values[i - 1] = cur->values[i];
    }

    return value;

  }

  inline DeleteResult* LeafDelete(KEY key, LeafNode* cur) {

    //step 1. find the slot of the key
    int slot = slotAtLeaf(key, cur);

    //the record of the key doesn't exist, just return
    if(slot == cur->num_keys) {
      return NULL;
    }


    //	 assert(cur->values[slot]->value == (uint64_t *)2);

    //	printf("delete node\n");
    DeleteResult *res = new DeleteResult();

    //step 2. remove the entry of the key, and get the deleted value
    res->value = removeLeafEntry(cur, slot);

    //step 3. if node is empty, remove the node from the list
    if(cur->num_keys == 0) {
      if(cur->left != NULL)
	cur->left->right = cur->right;
      if(cur->right != NULL)
	cur->right->left = cur->left;

      //Parent is responsible for the node deletion
      res->freeNode = true;

      return res;
    }

    //The smallest key in the leaf node has been changed, update the parent key
    if(slot == 0) {
      ArrayAssign(res->upKey, cur->keys[0]);
    }

    return res;
  }

  inline int slotAtInner(KEY key, InnerNode* cur) {

    int slot = 0;

    while(slot < cur->num_keys) {
      int tmp = Compare(cur->keys[slot], key);
      if (tmp > 0) break;
      slot++;
    }

    return slot;
  }

  inline void removeInnerEntry(InnerNode* cur, int slot, DeleteResult* res) {

    assert(slot <= cur->num_keys);

    //If there is only one available entry
    if(cur->num_keys == 0) {
      assert(slot == 0);
      res->freeNode = true;
      return;
    }


    //The key deleted is the last one
    if (slot == cur->num_keys) {
      cur->num_keys--;
      return;
    }

    //replace the children slot
    for(int i = slot + 1; i <= cur->num_keys; i++)
      cur->children[i - 1] = cur->children[i];

    //delete the first entry, upkey is needed
    if (slot == 0) {

      //record the first key as the upkey
      ArrayAssign(res->upKey, cur->keys[slot]);

      //delete the first key
      for(int i = slot; i < cur->num_keys - 1; i++) {
	ArrayAssign(cur->keys[i], cur->keys[i + 1]);
      }

    } else {
      //delete the previous key
      for(int i = slot; i < cur->num_keys; i++) {
	ArrayAssign(cur->keys[i - 1], cur->keys[i]);
      }
    }

    cur->num_keys--;

  }

  inline DeleteResult* InnerDelete(KEY key, InnerNode* cur ,int depth)
  {

    DeleteResult* res = NULL;

    //step 1. find the slot of the key
    int slot = slotAtInner(key, cur);

    //step 2. remove the record recursively
    //This is the last level of the inner nodes
    if(depth == 1) {
      res = LeafDelete(key, (LeafNode *)cur->children[slot]);
    } else {
      //printf("Delete Inner Node  %d\n", depth);
      //printInner((InnerNode *)cur->children[slot], depth - 1);
      res = InnerDelete(key, (InnerNode *)cur->children[slot], (depth - 1));
    }

    //The record is not found
    if(res == NULL) {
      return res;
    }

    //step 3. Remove the entry if the total children node has been removed
    if(res->freeNode) {
      //FIXME: Should free the children node here

      //remove the node from the parent node
      res->freeNode = false;
      removeInnerEntry(cur, slot, res);
      return res;
    }

    //step 4. update the key if needed
    if(res->upKey[0] != -1) {
      if (slot != 0) {
	ArrayAssign(cur->keys[slot - 1] , res->upKey);
	res->upKey[0] = -1;
      }
    }

    return res;

  }

  inline uint64_t* Delete(uint64_t key) {
#if 0
#if BTREE_LOCK
    MutexSpinLock lock(&slock);
#else
    RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif
#endif

    DeleteResult* res = NULL;
    if (depth == 0) {
      //Just delete the record from the root
      res = LeafDelete((uint64_t *)key, (LeafNode*)root);
    }
    else {
      res = InnerDelete((uint64_t *)key, (InnerNode*)root, depth);
    }

    if (res == NULL)
      return NULL;

    if(res->freeNode)
      root = NULL;

    return res->value;

  }


  inline InnerNode* InnerInsert(KEY key, InnerNode *inner, int d, uint64_t* val) {

    unsigned k = 0;
    KEY upKey;
    InnerNode *new_sibling = NULL;

    while(k < inner->num_keys)  {
      int tmp = Compare(key, inner->keys[k]);
      if (tmp < 0) break;
      ++k;
    }
    void *child = inner->children[k];

    if (d == 1) {

      LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val);
      if (new_leaf != NULL) {
	InnerNode *toInsert = inner;


	if (inner->num_keys == RAWN) {

	  new_sibling = new_inner_node();

	  if (new_leaf->num_keys == 1) {
	    new_sibling->num_keys = 0;
	    ArrayAssign(upKey, new_leaf->keys[0]);
	    toInsert = new_sibling;
	    k = -1;
	  }
	  else {
	    unsigned treshold= (RAWN+1)/2;


	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      ArrayAssign(new_sibling->keys[i], inner->keys[treshold+i]);
	      new_sibling->children[i]= inner->children[treshold+i];
	    }
	    new_sibling->children[new_sibling->num_keys]=
	      inner->children[inner->num_keys];
	    inner->num_keys= treshold-1;
	    ArrayAssign(upKey, inner->keys[treshold-1]);

	    int tmp = Compare(new_leaf->keys[0],upKey);
	    if (tmp >= 0) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  ArrayAssign(new_sibling->keys[RAWN-1] , upKey);

	}

	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    ArrayAssign(toInsert->keys[i] , toInsert->keys[i-1]);
	    toInsert->children[i+1] = toInsert->children[i];
	  }
	  toInsert->num_keys++;
	  ArrayAssign(toInsert->keys[k] , new_leaf->keys[0]);
	}
	toInsert->children[k+1] = new_leaf;
      }

    }
    else {
      bool s = true;
      InnerNode *new_inner =
	InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val);


      if (new_inner != NULL) {
	InnerNode *toInsert = inner;
	InnerNode *child_sibling = new_inner;
	unsigned treshold = (RAWN+1)/2;
	if (inner->num_keys == RAWN) {

	  new_sibling = new_inner_node();

	  if (child_sibling->num_keys == 0) {
	    new_sibling->num_keys = 0;
	    ArrayAssign(upKey , child_sibling->keys[RAWN-1]);
	    toInsert = new_sibling;
	    k = -1;
	  }

	  else  {

	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      ArrayAssign(new_sibling->keys[i], inner->keys[treshold+i]);
	      new_sibling->children[i]= inner->children[treshold+i];
	    }
	    new_sibling->children[new_sibling->num_keys]=
	      inner->children[inner->num_keys];

	    //XXX: should threshold ???
	    inner->num_keys= treshold-1;

	    ArrayAssign(upKey , inner->keys[treshold-1]);
	    //printf("UP %lx\n",upKey);
	    int tmp = Compare(key, upKey);
	    if (tmp >= 0) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  //XXX: what is this used for???
	  ArrayAssign(new_sibling->keys[RAWN-1] , upKey);
	}

	if (k != -1 ) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    ArrayAssign(toInsert->keys[i] , toInsert->keys[i-1]);
	    toInsert->children[i+1] = toInsert->children[i];
	  }

	  toInsert->num_keys++;
	  ArrayAssign(toInsert->keys[k] , reinterpret_cast<InnerNode*>(child_sibling)->keys[RAWN-1]);
	}
	toInsert->children[k+1] = child_sibling;

      }

    }

    if (d==depth && new_sibling != NULL) {
      InnerNode *new_root = new_inner_node();
      new_root->num_keys = 1;
      ArrayAssign(new_root->keys[0], upKey);
      new_root->children[0] = root;
      new_root->children[1] = new_sibling;
      root = new_root;
      depth++;
    }

    return new_sibling;
  }

  inline LeafNode* LeafInsert(KEY key, LeafNode *leaf, uint64_t* val) {
    LeafNode *new_sibling = NULL;
    unsigned k = 0;
    while(k < leaf->num_keys)  {
      int tmp = Compare(leaf->keys[k], key);
      if (tmp >= 0) break;
      ++k;
    }

    if(k < leaf->num_keys)  {
      int tmp = Compare(leaf->keys[k], key);
      if (tmp == 0) {
	leaf->values[k] = val;
	return NULL;
      }
    }


    LeafNode *toInsert = leaf;
    if (leaf->num_keys == RAWM) {
      new_sibling = new_leaf_node();

      if (leaf->right == NULL && k == leaf->num_keys) {
	new_sibling->num_keys = 0;
	toInsert = new_sibling;
	k = 0;
      }
      else {

	unsigned threshold= (RAWM+1)/2;
	new_sibling->num_keys= leaf->num_keys -threshold;
	for(unsigned j=0; j < new_sibling->num_keys; ++j) {
	  ArrayAssign(new_sibling->keys[j], leaf->keys[threshold+j]);
	  new_sibling->values[j]= leaf->values[threshold+j];
	}
	leaf->num_keys= threshold;


	if (k>=threshold) {
	  k = k - threshold;
	  toInsert = new_sibling;
	}
      }

      if (leaf->right != NULL) leaf->right->left = new_sibling;
      new_sibling->right = leaf->right;
      new_sibling->left = leaf;
      leaf->right = new_sibling;
    }


    //printf("IN LEAF1 %d\n",toInsert->num_keys);
    //printTree();

    for (int j=toInsert->num_keys; j>k; j--) {
      ArrayAssign(toInsert->keys[j] , toInsert->keys[j-1]);
      toInsert->values[j] = toInsert->values[j-1];
    }

    toInsert->num_keys = toInsert->num_keys + 1;
    ArrayAssign(toInsert->keys[k] , key);
    toInsert->values[k] = val;

    return new_sibling;
  }




  RAWStore::Iterator* GetIterator() {
    return new RawUint64BPlusTree::Iterator(this);
  }

  void printLeaf(LeafNode *n);
  void printInner(InnerNode *n, unsigned depth);
  void PrintStore();
  void PrintList();

 private:
  char padding1[64];
  void *root;
  int depth;

  char padding2[64];
  RTMProfile delprof;
  char padding3[64];

  RTMProfile prof;
  char padding6[64];
  SpinLock slock;

  char padding4[64];
  SpinLock rtmlock;
  char padding5[64];

 public:
  int array_length;

};


#endif
