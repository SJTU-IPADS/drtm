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

#include "raw_bplustree.h"

void BPlusTree::printLeaf(LeafNode *n) {

	//layer[0]++;

//	for(int i = 0; i < depth; i++)
  //printf(" ");
	//printf("Leaf Addr %lx Key num %d  :", n, n->num_keys);
  for (int i=0; i<n->num_keys;i++)
    printf("key  %ld value %ld \t ",n->keys[i], n->values[i]);
  printf("\n");
  //	total_key += n->num_keys;
}


void BPlusTree::printInner(InnerNode *n, unsigned depth) {

  layer[depth]++;
  for (int i=0; i<=n->num_keys; i++)
    if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
    else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
}

void BPlusTree::PrintStore() {

  for(int i = 0; i <= depth; i++)
    layer[i] = 0;

  printf("===============B+ Tree=========================\n");
  if(root == NULL) {
    printf("Empty Tree\n");
    return;
  }

  if (depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
  else {
    printInner(reinterpret_cast<InnerNode*>(root), depth);
  }
  printf("========================================\n");

#if 0
  printf("[layer: #node]: ");
  for(int i = 0; i <= depth; i++) {
    printf("[%d:%d] ", i, layer[i]);
  }
  printf("\n");

  printf("[access: #node]: ");
  for(int i = 0; i <= depth; i++) {
    printf("[%d:%d] ", i, access[i]);
	}
  printf("\n");

  printf("[diff: #node]: ");
  for(int i = 0; i <= depth; i++) {
    printf("[%d:%d] ", i, diff[i]);
  }
  printf("\n");
#endif

}

int BPlusTree::getCacheSet(uint64_t addr)
{
  int res = (int)((addr << 52) >> 58);

  assert(res < 64);
  cacheset[res]++;

  return res;
}

void BPlusTree::printLeafCSet(LeafNode *n)
{
  for(int i = 0; i < depth; i++)
    printf(" ");
  printf("Leaf Key num %d  Cache Set:", n->num_keys);
  for (int i=0; i<n->num_keys;i++)
    printf("\t%d",getCacheSet((uint64_t)n->values[i]));
  printf("\n");

}

void BPlusTree::printInnerCSet(InnerNode *n, unsigned depth)
{
  for(int i = 0; i < this->depth - depth; i++)
    printf(" ");
  printf("Inner %d Key num %d  Cache Set:", depth, n->num_keys);
  for (int i=0; i<n->num_keys;i++)
    printf("\t%d",getCacheSet((uint64_t)n->children[i]));
  printf("\n");
  for (int i=0; i<=n->num_keys; i++)
    if (depth>2) printInnerCSet(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
  //			else printLeafCSet(reinterpret_cast<LeafNode*>(n->children[i]));
}

void BPlusTree::PrintCSet()
{
  printf("===============B+ Tree Cache Set=====================\n");
  if(root == NULL) {
    printf("Empty Tree\n");
    return;
  }

  printf("Root %d\n", getCacheSet((uint64_t)root));

  if (depth == 0) {
    printLeafCSet(reinterpret_cast<LeafNode*>(root));
  }
  else {
    printInnerCSet(reinterpret_cast<InnerNode*>(root), depth);
  }
  printf("======================================================\n");

  int total = 0;
  for(int i = 0; i < 64; i ++) {

    //    if(cacheset[i] != 0)
    //      printf("Cache Set %d #Elems %d\n", i, cacheset[i]);
    total+= cacheset[i];
  }


  printf("Total elems %d\n", total);

}

void BPlusTree::PrintList() {
  void* min = root;
  int d = depth;
  while (d > 0) {
    min = reinterpret_cast<InnerNode*>(min)->children[0];
    d--;
  }
  LeafNode *leaf = reinterpret_cast<LeafNode*>(min);
  while (leaf != NULL) {
    printLeaf(leaf);
    if (leaf->right != NULL)
      assert(leaf->right->left == leaf);
    leaf = leaf->right;
  }

}

void BPlusTree::Fetch(InnerNode* node, uint64_t start, uint64_t end, int layer)
{

  if(node->num_keys == -1)
    printf("ERROR\n");

  for (int i = 0; i <= node->num_keys; i++) {
    if (layer > 1 && node->keys[i] <= end && node->keys[i] >= start)
      Fetch(reinterpret_cast<InnerNode*>(node->children[i]), start, end, layer - 1);
  }

}



void BPlusTree::Warmup(int layer, uint64_t start, uint64_t end)
{
  Fetch((InnerNode *)root, start, end, layer);
}



BPlusTree::Iterator::Iterator(BPlusTree* tree)
{
  tree_ = tree;
  node_ = NULL;
}

bool BPlusTree::Iterator::Valid()
{
  return (node_ != NULL) && (node_->num_keys > 0);
}

// Advances to the next position.
// REQUIRES: Valid()
bool BPlusTree::Iterator::Next()
{
  bool b = true;

#if 0
#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
#endif

  register int nested = 1;

  nested = _xtest();
  if(0 == nested)
    RTMTX::Begin(&(tree_->rtmlock));
  else if(tree_->rtmlock.IsLocked())
    _xabort(0xff);

  //else printf("no\n");
  //printf("A  key_ %ld node %ld\n" ,key_, node_->keys[leaf_index_]);
  register bool found = false;
  if (key_ != node_->keys[leaf_index_] || leaf_index_ >= node_->num_keys){
    while (node_ != NULL) {
      register int k = 0;
      register int num = node_->num_keys;
      while ((k < num) && (key_ >= node_->keys[k])) {
	++k;
      }
      if (k == num) {
	node_ = node_->right;
	if (node_ == NULL) return b;
      }
      else {
	leaf_index_ = k;
	break;
      }
    }

  }
  else leaf_index_++;

  if (leaf_index_ >= node_->num_keys) {
    node_ = node_->right;
    leaf_index_ = 0;
  }
  if (node_ != NULL) {
    key_ = node_->keys[leaf_index_];
    value_ = node_->values[leaf_index_];
  }


  if(0 == nested)
    RTMTX::End(&(tree_->rtmlock));
  return b;
}

// Advances to the previous position.
// REQUIRES: Valid()
bool BPlusTree::Iterator::Prev()
{
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());

  //FIXME: This function doesn't support link information
  //  printf("PREV\n");
  bool b = true;


#if 0
#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
#endif
  register int nested = 1;
  nested = _xtest();
  if(0 == nested)
    RTMTX::Begin(&(tree_->rtmlock));
  else if(tree_->rtmlock.IsLocked())
    _xabort(0xff);



  leaf_index_--;
  if (leaf_index_ < 0) {
    node_ = node_->left;
    //if (node_ != NULL) printf("NOTNULL\n");
    if (node_ != NULL) {
      leaf_index_ = node_->num_keys - 1;
    }
  }

  if (node_ != NULL) {
    key_ = node_->keys[leaf_index_];
    value_ = node_->values[leaf_index_];
  }


  if(0 == nested)
    RTMTX::End(&(tree_->rtmlock));
  return b;
}

uint64_t BPlusTree::Iterator::Key()
{
  return key_;
}

uint64_t* BPlusTree::Iterator::Value()
{
  if (!Valid()) return NULL;
  return value_;
}

// Advance to the first entry with a key >= target
void BPlusTree::Iterator::Seek(uint64_t key)
{

#if 0
#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
#endif
  register int nested = 1;
  nested = _xtest();
  if(0 == nested)
    RTMTX::Begin(&(tree_->rtmlock));
  else if(tree_->rtmlock.IsLocked())
    _xabort(0xff);

  LeafNode *leaf = tree_->FindLeaf(key);
  int num = leaf->num_keys;
  assert(num > 0);
  int k = 0;

  while ((k < num) && (key > leaf->keys[k])) {
    ++k;
  }

  if (k == num) {
    node_ = leaf->right;
    leaf_index_ = 0;
  }
  else {
    leaf_index_ = k;
    node_ = leaf;
  }

  key_ = node_->keys[leaf_index_];
  value_ = node_->values[leaf_index_];
  if(0 == nested)
    RTMTX::End(&(tree_->rtmlock));
}

void BPlusTree::Iterator::SeekPrev(uint64_t key)
{

#if 0
#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
#endif
  register int nested = 1;
  nested = _xtest();
  if(0 == nested)
    RTMTX::Begin(&(tree_->rtmlock));
  else if(tree_->rtmlock.IsLocked())
    _xabort(0xff);


  LeafNode *leaf = tree_->FindLeaf(key);

  int k = 0;
  int num = leaf->num_keys;
  while ((k < num) && (key > leaf->keys[k])) {
    ++k;
  }

  leaf_index_ = k;

  if (k == 0) {
    node_ = leaf->left;
    leaf_index_ = leaf->num_keys - 1;
  }
  else {
    leaf_index_ = leaf_index_ - 1;
    node_ = leaf;
  }

  key_ = node_->keys[leaf_index_];
  value_ = node_->values[leaf_index_];


  if(0 == nested)
    RTMTX::End(&(tree_->rtmlock));
}


void BPlusTree::Iterator::SeekToFirst()
{

  register int nested = 1;
  nested = _xtest();
  if(0 == nested)
    RTMTX::Begin(&(tree_->rtmlock));
  else if(tree_->rtmlock.IsLocked())
    _xabort(0xff);

  void* min = tree_->root;
  int d = tree_->depth;
  while (d > 0) {
    min = reinterpret_cast<InnerNode*>(min)->children[0];
    d--;
  }
  node_ = reinterpret_cast<LeafNode*>(min);
  leaf_index_ = 0;

  if(node_ != NULL) {
    key_ = node_->keys[0];
    value_ = node_->values[0];
  }else {
    assert(tree_->root == NULL);
  }

  if(0 == nested)
    RTMTX::End(&(tree_->rtmlock));

}


// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void BPlusTree::Iterator::SeekToLast()
{
  //TODO
  assert(0);
}
