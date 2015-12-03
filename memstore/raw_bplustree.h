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

#ifndef RAWBPLUSTREE_H
#define RAWBPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include <assert.h>
#include "util/rtm.h"
#include "port/port_posix.h"
#include "rawstore.h"

#define RAWM  15
#define RAWN  15

#define BTREE_PROF 0
#define BTREE_LOCK 0


class BPlusTree {
  
 public:
  struct LeafNode {
  LeafNode() : num_keys(0){
#if BTREE_PROF
    accessed = false;
#endif
  }
    unsigned num_keys;
    uint64_t keys[RAWM];
    uint64_t *values[RAWM];
    LeafNode *left;
    LeafNode *right;
#if BTREE_PROF
    bool accessed;
#endif
  };
  
  struct InnerNode {
  InnerNode() : num_keys(0) {
#if BTREE_PROF
    accessed = false;
#endif
  }
    unsigned num_keys;
    uint64_t 	 keys[RAWN];
    void*	 children[RAWN+1];
#if BTREE_PROF
    bool accessed;
#endif
  };
  
  struct DeleteResult {
  DeleteResult(): value(0), freeNode(false), upKey(-1){}
    uint64_t* value;  //The value of the record deleted
    bool freeNode;	//if the children node need to be free
    uint64_t upKey; //the key need to be updated -1: default value
  };

  class Iterator: public RAWStore::Iterator {
    
  public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(){};
    Iterator(BPlusTree* tree);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();
    
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
    
    BPlusTree* tree_;
    LeafNode* node_;
    int leaf_index_;
    uint64_t key_;
    uint64_t* value_;
  };
  
 public:
  BPlusTree() {
    root = new LeafNode();
    reinterpret_cast<LeafNode*>(root)->left = NULL;
    reinterpret_cast<LeafNode*>(root)->right = NULL;
    depth = 0;
    getWithRTM = true;
    accessprof = false;
    for(int i = 0; i < 10; i++) {
      diff[i] = 0;
      access[i] = 0;
    }
    
  }
      
  ~BPlusTree() {
  }
  
  inline LeafNode* new_leaf_node() {
    LeafNode* result = new LeafNode();
    return result;
  }
  
  
  inline InnerNode* new_inner_node() {
    InnerNode* result = new InnerNode();
    return result;
  }

  __attribute__((always_inline)) LeafNode* FindLeaf(uint64_t key) {
    InnerNode* inner;
    register void* node= root;
    register unsigned d= depth;
    unsigned index = 0;
    while( d-- != 0 ) {
      index = 0;
      inner= reinterpret_cast<InnerNode*>(node);
      while((index < inner->num_keys)&& (key >= inner->keys[index])) {
	++index;
      }
      node= inner->children[index];
    }
    return reinterpret_cast<LeafNode*>(node);
  }
  
  
  __attribute__((always_inline)) uint64_t* Get(uint64_t key)
  {
    //if (!getWithRTM) return FindValue(key);
    
    
    register int nested = _xtest();

    if(0 == nested)
      RTMTX::Begin(&rtmlock);
    else if(rtmlock.IsLocked())
      _xabort(0xff);
    
    
    register uint64_t* res = FindValue(key);
    
    if(0 == nested)
      RTMTX::End(&rtmlock);
    
    return res;
    
  }
  
  __attribute__((always_inline)) uint64_t* FindValue(uint64_t key)
  {
    
    register InnerNode* inner;
    register void* node= root;
    register unsigned d= depth;
    register unsigned index = 0;
    
    
    while( d-- != 0 ) {
      
      index = 0;
      inner= reinterpret_cast<InnerNode*>(node);
      
#if BTREE_PROF
      if (accessprof) {
	if(inner->accessed == false) {
	  inner->accessed = true;
	  diff[d + 1]++;
	}
	access[d + 1]++;
      }
#endif
      
      while((index < inner->num_keys)&& (key >= inner->keys[index])) {
	++index;
      }
      node= inner->children[index];
    }
    
    register LeafNode* leaf= reinterpret_cast<LeafNode*>(node);
    
#if BTREE_PROF
    if (accessprof) {
      if(leaf->accessed == false) {
	leaf->accessed = true;
	diff[d + 1]++;
      }
			access[d + 1]++;
    }
#endif
    
    if (leaf->num_keys == 0)
      return NULL;
    
    register unsigned k = 0;
    
    while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
      ++k;
    }
    
    if (k == leaf->num_keys)
      return NULL;
    
    if( leaf->keys[k] == key ) {
      return leaf->values[k];
    } else {
      return NULL;
    }
  }
  
  inline int slotAtLeaf(uint64_t key, LeafNode* cur) {
    
    int slot = 0;
    
    while((slot < cur->num_keys) && (cur->keys[slot] < key)) {
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
      cur->keys[i - 1] = cur->keys[i];
      cur->values[i - 1] = cur->values[i];
    }
    
    return value;
    
  }
  
  
  inline DeleteResult* LeafDelete(uint64_t key, LeafNode* cur) {
    
    
    //step 1. find the slot of the key
    int slot = slotAtLeaf(key, cur);
    
    //the record of the key doesn't exist, just return
    if(slot == cur->num_keys) {
      return NULL;
    }
    
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
      res->upKey = cur->keys[0];
    }

    return res;
  }
  
  inline int slotAtInner(uint64_t key, InnerNode* cur) {
    
    int slot = 0;

    while((slot < cur->num_keys) && (cur->keys[slot] <= key)) {
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
      res->upKey = cur->keys[slot];

      //delete the first key
      for(int i = slot; i < cur->num_keys - 1; i++) {
	cur->keys[i] = cur->keys[i + 1];
      }
      
    } else {
      //delete the previous key
      for(int i = slot; i < cur->num_keys; i++) {
	cur->keys[i - 1] = cur->keys[i];
      }
    }
    
    cur->num_keys--;
    
  }
  
  inline DeleteResult* InnerDelete(uint64_t key, InnerNode* cur ,int depth)
  {
    
    DeleteResult* res = NULL;
    
		//step 1. find the slot of the key
    int slot = slotAtInner(key, cur);
    
    //step 2. remove the record recursively
    //This is the last level of the inner nodes
    if(depth == 1) {
      res = LeafDelete(key, (LeafNode *)cur->children[slot]);
    } else {
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
    if(res->upKey != -1) {
      if (slot != 0) {
	cur->keys[slot - 1] = res->upKey;
	res->upKey = -1;
      }
    }
    return res;
    
  }
  
  
  inline uint64_t* Delete(uint64_t key)
  {
    register char nested = _xtest();
    
    if(0 == nested)
      RTMTX::Begin(&rtmlock);
    else if(rtmlock.IsLocked())
      _xabort(0xff);

    register uint64_t* res = DeleteRecord(key);

    if(0 == nested)
      RTMTX::End(&rtmlock);
    
    return res;
  }

  inline uint64_t* DeleteRecord(uint64_t key) {
    
    DeleteResult* res = NULL;
    
    if (depth == 0) {
      //Just delete the record from the root
      res = LeafDelete(key, (LeafNode*)root);
    }
    else {
      res = InnerDelete(key, (InnerNode*)root, depth);
    }
    

    if (res == NULL)
      return NULL;
    
    if(res->freeNode) {
      root = NULL;
      depth = 0; //Only reset here
    }
    
    return res->value;
  }
  
  
  
  inline void Put(uint64_t key, uint64_t *value)
  {
    register char nested = _xtest();
    
    if(0 == nested)
      RTMTX::Begin(&rtmlock);
    else if(rtmlock.IsLocked())
      _xabort(0xff);
    
    PutRecord(key, value);
    
    if(0 == nested)
      RTMTX::End(&rtmlock);
    
  }
  
  
  inline void PutRecord(uint64_t key, uint64_t *value){

    if (root == NULL) {
      root = new_leaf_node();
      reinterpret_cast<LeafNode*>(root)->left = NULL;
      reinterpret_cast<LeafNode*>(root)->right = NULL;
      depth = 0;
    }
    
    if (depth == 0) {
      LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), value);
      if (new_leaf != NULL) {
	InnerNode *inner = new_inner_node();
	inner->num_keys = 1;
	inner->keys[0] = new_leaf->keys[0];
	inner->children[0] = root;
	inner->children[1] = new_leaf;
	depth++;
	root = inner;
      }
    }
    else {
      InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, value);
    }
    
  }
  
  
  
  inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, uint64_t *value) {
    
    LeafNode *new_sibling = NULL;
    unsigned k = 0;
    while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
      ++k;
    }
    
    if((k < leaf->num_keys) && (leaf->keys[k] == key)) {
      
      leaf->values[k] = value;
      return NULL;
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
	  new_sibling->keys[j]= leaf->keys[threshold+j];
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
    
    for (int j=toInsert->num_keys; j>k; j--) {
      toInsert->keys[j] = toInsert->keys[j-1];
      toInsert->values[j] = toInsert->values[j-1];
    }
    
    toInsert->num_keys = toInsert->num_keys + 1;
    toInsert->keys[k] = key;
    toInsert->values[k] = value;

    
    return new_sibling;
  }
  

  inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, uint64_t* value) {        
    
    unsigned k = 0;
    uint64_t upKey;
    InnerNode *new_sibling = NULL;
    
    while((k < inner->num_keys) && (key >= inner->keys[k])) {
      ++k;
    }

    void *child = inner->children[k];
    
    if (d == 1) {
      
      LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), value);
      
      if (new_leaf != NULL) {
	
	InnerNode *toInsert = inner;
	
	if (inner->num_keys == RAWN) {

	  new_sibling = new_inner_node();
	  
	  if (new_leaf->num_keys == 1) {
	    new_sibling->num_keys = 0;
	    upKey = new_leaf->keys[0];
	    toInsert = new_sibling;
	    k = -1;
	  }
	  else {
	    unsigned treshold= (RAWN+1)/2;
	    new_sibling->num_keys= inner->num_keys -treshold;

	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      new_sibling->keys[i]= inner->keys[treshold+i];
	      new_sibling->children[i]= inner->children[treshold+i];
	    }
	    
	    new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
	    inner->num_keys= treshold-1;
	    
	    upKey = inner->keys[treshold-1];
	    
	    if (new_leaf->keys[0] >= upKey) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  
	  new_sibling->keys[RAWN-1] = upKey;
	  
	  
	}
	
	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    toInsert->keys[i] = toInsert->keys[i-1];
	    toInsert->children[i+1] = toInsert->children[i];
	  }
	  toInsert->num_keys++;
	  toInsert->keys[k] = new_leaf->keys[0];
	}
	
	toInsert->children[k+1] = new_leaf;
	
      }
            
    }
    else {
      
      bool s = true;
      InnerNode *new_inner =
	InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, value);            
      if (new_inner != NULL) {
	InnerNode *toInsert = inner;
	InnerNode *child_sibling = new_inner;
	
	unsigned treshold= (RAWN+1)/2;
	if (inner->num_keys == RAWN) {
	  new_sibling = new_inner_node();
	  
	  if (child_sibling->num_keys == 0) {
	    new_sibling->num_keys = 0;
	    upKey = child_sibling->keys[RAWN-1];
	    toInsert = new_sibling;
	    k = -1;
	  }
	  
	  else  {
	    new_sibling->num_keys= inner->num_keys -treshold;
	    
	    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
	      new_sibling->keys[i]= inner->keys[treshold+i];
	      new_sibling->children[i]= inner->children[treshold+i];
	    }
	    new_sibling->children[new_sibling->num_keys]=
	      inner->children[inner->num_keys];
	    
	    //XXX: should threshold ???
	    inner->num_keys= treshold-1;
	    
	    upKey = inner->keys[treshold-1];
	    //printf("UP %lx\n",upKey);
	    if (key >= upKey) {
	      toInsert = new_sibling;
	      if (k >= treshold) k = k - treshold;
	      else k = 0;
	    }
	  }
	  
	  new_sibling->keys[RAWN-1] = upKey;
	  

	}
	if (k != -1) {
	  for (int i=toInsert->num_keys; i>k; i--) {
	    toInsert->keys[i] = toInsert->keys[i-1];
	    toInsert->children[i+1] = toInsert->children[i];
	  }

	  toInsert->num_keys++;
	  toInsert->keys[k] = reinterpret_cast<InnerNode*>(child_sibling)->keys[RAWN-1];
	}
	toInsert->children[k+1] = child_sibling;
	
	
      }                  
    }
    
    if (d==depth && new_sibling != NULL) {
      InnerNode *new_root = new_inner_node();
      new_root->num_keys = 1;
      new_root->keys[0]= upKey;
      new_root->children[0] = root;
      new_root->children[1] = new_sibling;
      root = new_root;
      depth++;
            
    }
    
    return new_sibling;
  }
  
  RAWStore::Iterator* GetIterator() {
    return new BPlusTree::Iterator(this);
  }
  
  void printLeaf(LeafNode *n);
  
  void printInner(InnerNode *n, unsigned depth);
  
  void PrintStore();
  
  int getCacheSet(uint64_t addr);
  
  void printLeafCSet(LeafNode *n);
  
  void printInnerCSet(InnerNode *n, unsigned depth);
  
  void PrintCSet();
  
  void PrintList();
  
  void Warmup(int layer, uint64_t start, uint64_t end);
  
  void Fetch(InnerNode* node, uint64_t start, uint64_t end, int layer);
  
 public:
  
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
  bool getWithRTM;
  uint64_t cacheset[64];
  int layer[10];
  int access[10];
  int diff[10];
  bool accessprof;
};

//__thread RTMArena* MemstoreBPlusTree::arena_ = NULL;
//__thread bool MemstoreBPlusTree::localinit_ = false;


#endif
