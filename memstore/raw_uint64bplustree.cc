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

#include "memstore/raw_uint64bplustree.h"

void RawUint64BPlusTree::PrintStore() {
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
}

void RawUint64BPlusTree::printLeaf(LeafNode *n) {
  for(int i = 0; i < depth; i++)
    printf(" ");
  printf("Leaf Addr %lx Key num %d  :", n, n->num_keys);
  printf("\n");

}


void RawUint64BPlusTree::printInner(InnerNode *n, unsigned depth) {
  for(int i = 0; i < this->depth - depth; i++)
    printf(" ");
  printf("Inner %d Key num %d  :", depth, n->num_keys);
  printf("\n");
  for (int i=0; i<=n->num_keys; i++)
    if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
    else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
}


void RawUint64BPlusTree::PrintList() {
#if 0
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
#endif
}

RawUint64BPlusTree::Iterator::Iterator(RawUint64BPlusTree* tree)
{
  tree_ = tree;
  node_ = NULL;
}


// Returns true iff the iterator is positioned at a valid node.
bool RawUint64BPlusTree::Iterator::Valid()
{
  bool b = (node_ != NULL) && (node_->num_keys > 0);
  return b;
}

// Advances to the next position.
// REQUIRES: Valid()
bool RawUint64BPlusTree::Iterator::Next()
{

  bool b = true;

#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif

  leaf_index++;
  if (leaf_index >= node_->num_keys) {
    node_ = node_->right;
    leaf_index = 0;
  }
  if (node_ != NULL) {
    tree_->ArrayAssign(key_ , node_->keys[leaf_index]);
    value_ = node_->values[leaf_index];
  }

  return b;
}

// Advances to the previous position.
	// REQUIRES: Valid()
bool RawUint64BPlusTree::Iterator::Prev()
{
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());

	  //FIXME: This function doesn't support link information
  //  printf("PREV\n");
  bool b = true;

#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif

  leaf_index--;
  if (leaf_index < 0) {
    node_ = node_->left;
    if (node_ != NULL) {
      leaf_index = node_->num_keys - 1;
    }
  }

  if (node_ != NULL) {
    tree_->ArrayAssign(key_ , node_->keys[leaf_index]);
    value_ = node_->values[leaf_index];
  }
  return b;
}

uint64_t RawUint64BPlusTree::Iterator::Key()
{
  return (uint64_t)key_;
}

uint64_t* RawUint64BPlusTree::Iterator::Value()
{
  if (!Valid()) return NULL;
  return value_;
}

// Advance to the first entry with a key >= target
void RawUint64BPlusTree::Iterator::Seek(uint64_t key)
{
#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, tree_->depth, 1, &tree_->rtmlock);
#endif

  LeafNode *leaf = tree_->FindLeaf((uint64_t *)key);
  int num = leaf->num_keys;
  int k = 0;
  while (k < num)  {
    int tmp = tree_->Compare((uint64_t *)key, leaf->keys[k]);
    if (tmp <= 0) break;
    ++k;
  }
  if (k == num) {
    node_ = leaf->right;
    leaf_index = 0;
  }
  else {
    leaf_index = k;
    node_ = leaf;
  }
  tree_->ArrayAssign(key_ , node_->keys[leaf_index]);
  value_ = node_->values[leaf_index];
}

void RawUint64BPlusTree::Iterator::SeekPrev(uint64_t key)
{
  LeafNode *leaf = tree_->FindLeaf((uint64_t *)key);

  int k = 0;
  int num = leaf->num_keys;
  while (k < num)  {
    int tmp = tree_->Compare((uint64_t *)key, leaf->keys[k]);
    if (tmp <= 0) break;
    ++k;
  }
  if (k == 0) {
    node_ = leaf->left;
    leaf_index = node_->num_keys - 1;
  }
  else {
    k = k - 1;
    node_ = leaf;
  }
}


// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void RawUint64BPlusTree::Iterator::SeekToFirst()
{
  void* min = tree_->root;
  int d = tree_->depth;
  while (d > 0) {
    min = reinterpret_cast<InnerNode*>(min)->children[0];
    d--;
  }
  node_ = reinterpret_cast<LeafNode*>(min);
  leaf_index = 0;

#if BTREE_LOCK
  MutexSpinLock lock(&tree_->slock);
#else
  RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
  tree_->ArrayAssign(key_ , node_->keys[0]);
  value_ = node_->values[0];
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void RawUint64BPlusTree::Iterator::SeekToLast()
{
  //TODO
  assert(0);
}
