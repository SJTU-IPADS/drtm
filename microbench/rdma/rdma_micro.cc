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

//a file for testing rdma read performance

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <set>
#include <vector>
#include <unordered_map>

#include "network_node.h"
#include <sys/mman.h>
#include "rdma_resource.h"
#include "rdma_cuckoohash.h"
#include "rdma_hophash.h"
#include "rdma_clusterhash.h"

int THREAD_NUM  = 6;
uint64_t cycles_per_second;
RdmaResource *rdma = NULL;
//for binding
int socket_0[] = {
  0,2,4,6,8,10,12,14,16,18
};
int socket_1[] =  {
  1,3,5,7,9,11,13,15,17,19

};
void pin_to_core(size_t core) {
  cpu_set_t  mask;
  CPU_ZERO(&mask);
  CPU_SET(core , &mask);
  int result=sched_setaffinity(0, sizeof(mask), &mask);
}


inline  uint64_t
rdtsc(void)
{
  uint32_t hi, lo;
  __asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)lo)|(((uint64_t)hi)<<32);
}

struct Thread_config  {
  int id;
  double throughput;
  int latency;
  void * ptr;
  int * buffer;
};
uint64_t rdma_size;
uint64_t val_num=1000*1000;
double rate=0.9;

int current_partition ;
int total_partition ;
int target_partition ;
void Run(void *info) ;
void Run1(void *info);
void Run2(void *info);

struct cache_entry{
  //pthread_spinlock_t lock;
  uint64_t seq_num;
  uint64_t key;
  uint64_t loc;
};
struct cache_entry *limited_cache;
uint64_t cache_num;
void limited_cache_init(){
  cache_num = val_num ;
  limited_cache = new cache_entry[cache_num];
  for(int i=0;i<cache_num;i++){
    limited_cache[i].seq_num=0;
  }
}
void limited_cache_clear(){
  for(int i=0;i<cache_num;i++){
    limited_cache[i].seq_num=0;
    limited_cache[i].key=-1;
    limited_cache[i].loc=-1;
  }
}
bool limited_cache_lookup(uint64_t key){
  bool ret=false;
  uint64_t old_seq=limited_cache[key%cache_num].seq_num;
  if(old_seq%2==1)
    return false;

  if(limited_cache[key%cache_num].key==key)
    ret=true;

  uint64_t new_seq=limited_cache[key%cache_num].seq_num;
  if(new_seq==old_seq)
    return ret;
  return false;
}
void limited_cache_insert(uint64_t key,uint64_t loc){
  uint64_t old_seq=limited_cache[key%cache_num].seq_num;
  if(old_seq%2==1)
    return;
  if(__sync_bool_compare_and_swap(&limited_cache[key%cache_num].seq_num,old_seq,old_seq+1) == false){
    return ;
  }
  limited_cache[key%cache_num].key=key;
  limited_cache[key%cache_num].loc=loc;
  limited_cache[key%cache_num].seq_num=old_seq+2;
  return ;
}


int val_length;
int main(int argc,char** argv){
  void (* ptr)(void *) = Run;

  int id = atoi(argv[1]);
  current_partition = id;
  total_partition = atoi(argv[2]);
  target_partition = 0;
  ptr = Run2;
  if(current_partition== target_partition){
    ptr = Run1;
  }

  ibv_fork_init();

  fprintf(stdout,"Start with %d threads.\n",THREAD_NUM);
  // main tests
  void *status;
  uint64_t start_t = rdtsc();
  sleep(1);
  cycles_per_second = rdtsc() - start_t;


  Thread_config *configs = new Thread_config[THREAD_NUM];
  pthread_t     *thread  = new pthread_t[THREAD_NUM];


  rdma_size = 1024*1024*1024;  //1G
  rdma_size = rdma_size*20; //20G
  uint64_t total_size=rdma_size+1024*1024*1024;

  // rdma_size = 1024*4;  //4K
  // //rdma_size = rdma_size*20; //20G
  // uint64_t total_size=rdma_size+1024*4;

  Network_Node *node = new Network_Node(total_partition,current_partition,THREAD_NUM);


  fprintf(stdout,"size %ld ... ",total_size);
  char *buffer= (char*) malloc(total_size);
  char data[2050];
  val_num=1000*1000;
  val_num=val_num*20;
  limited_cache_init();

  //Rdma_3_1_CuckooHash *table = new Rdma_3_1_CuckooHash(val_length,val_num,buffer);
  //RdmaHopHash *table = new RdmaHopHash(val_length,val_num,buffer);
  RdmaClusterHash *table = new RdmaClusterHash(val_length,val_num,buffer);
  for(uint64_t i=0;i<val_num*rate;i++){
    table->Insert(i,data);
  }
  limited_cache_clear();
  rdma=new RdmaResource(total_partition,THREAD_NUM,current_partition,(char *)buffer,total_size,1024*1024*128,rdma_size);
  //rdma=new RdmaResource(total_partition,THREAD_NUM,current_partition,(char *)buffer,total_size,512,rdma_size);
  rdma->node = node;
  rdma->Servicing();
  rdma->Connect();
  for(size_t id = 0;id < THREAD_NUM;++id) {
    configs[id].buffer = NULL;
  }

  fprintf(stdout,"connection done\n");

  val_length=64;
  while(true){
    //val_length=val_length*2;
    if(val_length>2048)
      break;
    limited_cache_clear();
    for(size_t id = 0;id < THREAD_NUM;++id) {
      configs[id].id = id;
      configs[id].throughput=0;
      configs[id].ptr=table;
      pthread_create (&(thread[id]), NULL, ptr, (void *) &(configs[id]));
    }
    for(size_t t = 0 ; t < THREAD_NUM; t++) {
      int rc = pthread_join(thread[t], &status);
      if (rc) {
        printf("ERROR; return code from pthread_join() is %d\n", rc);
        exit(-1);
      }
    }
    int total_throughput=0;
    for(size_t t = 0 ; t < THREAD_NUM; t++){
      total_throughput+=configs[t].throughput;
    }
    int average_latency=0;
    for(size_t t = 0 ; t < THREAD_NUM; t++){
      average_latency+=configs[t].latency;
    }
    average_latency=average_latency / THREAD_NUM;

    //last partition will print the total throughput of all clients
    if(current_partition == total_partition-1){
      for(int i=1;i<current_partition;i++){
        std::string str=node->Recv();
        //first 2 char is for internal usage
        int tmp=std::stoi(str.substr(2));
        total_throughput+=tmp;
      }
      for(int i=1;i<current_partition;i++){
        node->Send(i,THREAD_NUM,std::to_string(total_throughput));
      }
      //barrier();
      for(int i=1;i<current_partition;i++){
        std::string str=node->Recv();
        //first 2 char is for internal usage
        int tmp=std::stoi(str.substr(2));
        average_latency+=tmp;
      }
      average_latency=average_latency/(total_partition-1);
      for(int i=1;i<current_partition;i++){
        node->Send(i,THREAD_NUM,std::to_string(total_throughput));
      }
      printf("val_length\t%d\tthroughput\t%d\tlatency\t%d\n",val_length,total_throughput,average_latency);
    } else {
      node->Send(total_partition-1,THREAD_NUM,std::to_string(total_throughput));
      std::string str=node->Recv();
      node->Send(total_partition-1,THREAD_NUM,std::to_string(average_latency));
      str=node->Recv();
      printf("local throughput:%d\n",total_throughput);
    }
  }

}

void random_read(void *ptr,uint64_t key,uint64_t val_length){
  struct Thread_config *config = (struct Thread_config*) ptr;
  int id = config->id;
  uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(id);
  uint64_t start_addr=(key%(rdma_size/val_length))*val_length;
  rdma->RdmaRead(id,target_partition,(char *)local_buffer,val_length,start_addr);
}
int cuckoo_read(void *ptr,uint64_t key,uint64_t val_length){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  Rdma_3_1_CuckooHash *table = (Rdma_3_1_CuckooHash *) (config->ptr);
  uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(id);
  uint64_t loc=0;
  uint64_t p[3];
  p[0]=table->GetHash(key);
  int count=0;
  for(uint64_t slot=0;slot<3;slot++){
    count++;
    if(slot==1)
      p[1]=table->GetHash2(key);
    if(slot==2)
      p[2]=table->GetHash3(key);
    loc = p[slot]* table->bucketsize;
    rdma->RdmaRead(id,target_partition,(char *)local_buffer,sizeof(Rdma_3_1_CuckooHash::RdmaArrayNode),loc);
    Rdma_3_1_CuckooHash::RdmaArrayNode * node =(Rdma_3_1_CuckooHash::RdmaArrayNode *)local_buffer;
    if(node->valid==true && node->key==key){
      loc = table->get_dataloc(node->index);
      break;
    }
  }
  //assert(rdma->RdmaRead(id,target_partition,(char *)local_buffer ,val_length , 0) == 0);
  uint64_t index = ((key *key) %(val_num));
  uint64_t start_addr=index*val_length;
  //while(start_addr>off)
  //  start_addr=start_addr/2;
  rdma->RdmaRead(id,target_partition,(char *)local_buffer,val_length,start_addr);
  count++;
  return count;
}


void Run(void *ptr) {
  struct Thread_config *config = (struct Thread_config*) ptr;
  int id = config->id;
  pin_to_core(socket_1[id]);
  int num_operations=1000000;//*16;
  int seed=id+current_partition*THREAD_NUM;
  sleep(2);
  uint64_t start_t = rdtsc();
  int count=0;
  for(int i=0;i<num_operations;i++){
    uint64_t key =rand_r(&seed)% ((int)(val_num*rate));
    //int val_length=64;
    uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(id);
    uint64_t start_addr=(key%(rdma_size/val_length))*val_length;
    count+=cuckoo_read(ptr,key,val_length);

    // rdma->post(id,target_partition,(char *)local_buffer+(i%32)*val_length,val_length,start_addr,IBV_WR_RDMA_READ);
    // if(i%32==31){
    //   for(int tmp=0;tmp<32;tmp++)
    //     rdma->poll(id,target_partition);
    // }
  }
  printf("count %f\n",count*1.0/num_operations);
  uint64_t cycle_num = rdtsc() - start_t;
  config->throughput=cycles_per_second *1.0*num_operations/ cycle_num;
}

const int batch_factor=4;//32;
struct post_window{
  uint64_t key;
  uint64_t loc;
  uint64_t start;
  int stage;
  int winid;
};
void rdmaread_post(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  int id = config->id;
  uint64_t *local_buffer = (uint64_t *)rdma->GetMsgAddr(id)+win.winid*val_length;//2048;//val_length ;
  uint64_t start_addr=(win.key%(rdma_size/val_length))*val_length;
  rdma->post(id,target_partition,(char *)local_buffer,val_length,start_addr,IBV_WR_RDMA_READ);
}

post_window rdmaread_poll(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  int id = config->id;
  rdma->poll(id,target_partition);
  win.stage=-1;
  return win;
}

void cuckoo_post(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  Rdma_3_1_CuckooHash *table = (Rdma_3_1_CuckooHash *) (config->ptr);
  //char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  int node_size=sizeof(Rdma_3_1_CuckooHash::RdmaArrayNode);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*std::max(32,val_length);
  uint64_t loc=0;
  uint64_t hash;
  if(win.stage==0){
    hash=table->GetHash(win.key);
    loc = hash * table->bucketsize;
    rdma->post(id,target_partition,(char *)local_buffer,sizeof(Rdma_3_1_CuckooHash::RdmaArrayNode),loc,IBV_WR_RDMA_READ);
  } else if(win.stage==1){
    hash=table->GetHash2(win.key);
    loc = hash * table->bucketsize;
    rdma->post(id,target_partition,(char *)local_buffer,sizeof(Rdma_3_1_CuckooHash::RdmaArrayNode),loc,IBV_WR_RDMA_READ);
  } else if(win.stage==2){
    hash=table->GetHash3(win.key);
    loc = hash * table->bucketsize;
    rdma->post(id,target_partition,(char *)local_buffer,sizeof(Rdma_3_1_CuckooHash::RdmaArrayNode),loc,IBV_WR_RDMA_READ);
  } else {
    uint64_t start_addr=(win.key%(rdma_size/val_length))*val_length;
    rdma->post(id,target_partition,(char *)local_buffer,val_length,start_addr,IBV_WR_RDMA_READ);
  }
}

post_window cuckoo_poll(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  Rdma_3_1_CuckooHash *table = (Rdma_3_1_CuckooHash *) (config->ptr);
  //char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  int node_size=sizeof(Rdma_3_1_CuckooHash::RdmaArrayNode);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*std::max(32,val_length);
  uint64_t loc=0;
  rdma->poll(id,target_partition);
  if(win.stage==0 || win.stage==1 || win.stage==2){
    Rdma_3_1_CuckooHash::RdmaArrayNode * node =(Rdma_3_1_CuckooHash::RdmaArrayNode *)local_buffer;
    if(node->valid==true && node->key==win.key){

      loc = table->get_dataloc(node->index);
      win.stage=3;
      return win;
    } else {
      win.stage=win.stage+1;
      return win;
    }
  } else {
    win.stage=-1;
    return win;
  }
}

void hop_indirect_post(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaHopHash *table = (RdmaHopHash *) (config->ptr);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  uint64_t loc=0;
  uint64_t hash;
  if(win.stage==0){
    hash = table->GetHash(win.key);
    uint64_t read_header = 8*(sizeof(uint64_t)+16);
    uint64_t loc = read_header / 2 * hash;
    rdma->post(id,target_partition,(char *)local_buffer,read_header,loc,IBV_WR_RDMA_READ);
  } else if(win.stage==1){
    uint64_t read_chain = (sizeof(RdmaHopHash::RdmaChainNode));
    rdma->post(id,target_partition,(char *)local_buffer,read_chain,loc,IBV_WR_RDMA_READ);
  } else {
    uint64_t start_addr=(win.key%(rdma_size/val_length))*val_length;
    rdma->post(id,target_partition,(char *)local_buffer,val_length,start_addr,IBV_WR_RDMA_READ);
  }
}
post_window hop_indirect_poll(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaHopHash *table = (RdmaHopHash *) (config->ptr);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  uint64_t loc=0;
  rdma->poll(id,target_partition);
  if(win.stage==0){
    uint64_t hash = table->GetHash(win.key);
    if(hash%100<5){
      win.stage=1;
    } else {
      win.stage=2;
    }
    return win;
  } else if(win.stage==1){
    win.stage=2;
    return win;
  } else {
    win.stage=-1;
    return win;
  }
}

void hop_post(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaHopHash *table = (RdmaHopHash *) (config->ptr);
  //int msg_size=2*(sizeof(RdmaHopHash::RdmaHopNode)+4*val_length);
  //char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*msg_size;//*10;///batch_factor*128 ;
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;//*10;///batch_factor*128 ;
  uint64_t loc=0;
  uint64_t hash;
  if(win.stage==0){
    hash = table->GetHash(win.key);
    uint64_t read_header = 2*(sizeof(RdmaHopHash::RdmaHopNode)+4*val_length);
    uint64_t loc = read_header / 2 * hash;
    rdma->post(id,target_partition,(char *)local_buffer,read_header,loc,IBV_WR_RDMA_READ);
  } else {
    uint64_t read_chain = (sizeof(RdmaHopHash::RdmaChainNode)+2* val_length);
    rdma->post(id,target_partition,(char *)local_buffer,read_chain,loc,IBV_WR_RDMA_READ);
  }
}
post_window hop_poll(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaHopHash *table = (RdmaHopHash *) (config->ptr);
  //int msg_size=2*(sizeof(RdmaHopHash::RdmaHopNode)+4*val_length);
  //char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*msg_size;//*10;///batch_factor*128 ;
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;//*10;///batch_factor*128 ;
  uint64_t loc=0;
  rdma->poll(id,target_partition);
  if(win.stage==0){
    uint64_t hash = table->GetHash(win.key);
    if(hash%100<5){
      win.stage=1;
    } else {
      win.stage=-1;
    }
    return win;
  } else {
    win.stage=-1;
    return win;
  }
}

void clustering_post(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaClusterHash *table = (RdmaClusterHash *) (config->ptr);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  uint64_t loc=0;
  uint64_t hash;
  hash = table->GetHash(win.key);
  uint64_t read_header = CLUSTER_H*(sizeof(uint64_t)+8);
  if(win.stage==0){
    uint64_t loc = read_header * hash;
    rdma->post(id,target_partition,(char *)local_buffer,read_header,loc,IBV_WR_RDMA_READ);
  } else if(win.stage==1){
    rdma->post(id,target_partition,(char *)local_buffer,read_header,0,IBV_WR_RDMA_READ);
  } else {
    uint64_t start_addr=(win.key%(rdma_size/val_length))*val_length;
    rdma->post(id,target_partition,(char *)local_buffer,val_length,start_addr,IBV_WR_RDMA_READ);
  }
}
post_window clustering_poll(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaClusterHash *table = (RdmaClusterHash *) (config->ptr);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  uint64_t loc=0;
  rdma->poll(id,target_partition);
  if(win.stage==0){
    uint64_t hash = table->GetHash(win.key);
    if(hash%100<10){
      win.stage=1;
    } else {
      win.stage=2;
    }
    return win;
  } else if(win.stage==1){
    win.stage=2;
    return win;
  } else {
    win.stage=-1;
    return win;
  }
}


post_window clustering_limited_cache_post(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaClusterHash *table = (RdmaClusterHash *) (config->ptr);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  uint64_t loc=0;
  if(win.stage==0){
    if(limited_cache_lookup(win.key)){
      win.stage=2;
      uint64_t start_addr=(win.key%(rdma_size/val_length))*val_length;
      rdma->post(id,target_partition,(char *)local_buffer,val_length,start_addr,IBV_WR_RDMA_READ);
    } else {
      uint64_t index = table->GetHash(win.key);
      uint64_t loc = table->getHeaderNode_loc(index);
      rdma->post(id,target_partition,(char *)local_buffer,sizeof(RdmaClusterHash::HeaderNode),loc,IBV_WR_RDMA_READ);
    }
  } else if(win.stage==1){
    uint64_t loc = win.loc;
    rdma->post(id,target_partition,(char *)local_buffer,sizeof(RdmaClusterHash::HeaderNode),loc,IBV_WR_RDMA_READ);
  } else {
    uint64_t start_addr=(win.key%(rdma_size/val_length))*val_length;
    rdma->post(id,target_partition,(char *)local_buffer,val_length,start_addr,IBV_WR_RDMA_READ);
  }
  return win;
}

post_window clustering_limited_cache_poll(void *ptr,post_window win){
  struct Thread_config *config = (struct Thread_config*) ptr;
  uint64_t id = config->id;
  RdmaClusterHash *table = (RdmaClusterHash *) (config->ptr);
  char *local_buffer = (char *)rdma->GetMsgAddr(id)+win.winid*2048;///batch_factor*128 ;
  uint64_t loc=0;
  rdma->poll(id,target_partition);
  if(win.stage==0 || win.stage==1){
    //win.stage==0 means cache miss , and first time to read data
    //win.stage==1 means read the next ptr
    RdmaClusterHash::HeaderNode* node= (RdmaClusterHash::HeaderNode*)local_buffer;
    bool found=false;
    for(int i=0;i<CLUSTER_H;i++){
      if(node->indexes[i]!=0){
	loc = table->getDataNode_loc(node->indexes[i]);
	loc+= sizeof(RdmaClusterHash::DataNode);
	limited_cache_insert(node->keys[i],loc);
      }
    }
    for(int i=0;i<CLUSTER_H;i++){
      if(node->keys[i]==win.key && node->indexes[i]!=0){
	loc = table->getDataNode_loc(node->indexes[i]);
	loc+= sizeof(RdmaClusterHash::DataNode);
	found=true;
	break;
      }
    }
    if(found){
      win.stage=2;
    } else {
      //stage 1 will read the next pointer
      win.stage=1;
      if(node->next !=NULL){
        win.loc = table->getHeaderNode_loc(node->next) ;
      }
      else{
        assert(false);
      }
    }
    return win;
  } else {
    win.stage=-1;
    return win;
  }
}



void Run2(void *ptr) {
  struct Thread_config *config = (struct Thread_config*) ptr;
  int id = config->id;
  pin_to_core(socket_1[id]);
  int num_operations=1000000*20;
  ;//00*16;
  int seed=id+current_partition*THREAD_NUM;
  post_window window[batch_factor];
  for(int i=0;i<batch_factor;i++){
    window[i].stage=-1;
    window[i].winid=i;
  }
  /*
    if(config->buffer==NULL){
    char filename[]="/home/datanfs/nfs0/zipf_data/0output";
    filename[29]+=id;
    std::ifstream infile(filename);
    config->buffer=new int[num_operations];
    for(int i=0;i<num_operations;i++){
    if(infile>> config->buffer[i])//=dist.get_zipf_random(&seed)% ((int)(val_num*rate));
    continue;
    else {
    printf("error when loading zipf data\n");
    assert(false);
    }
    }
    printf("%d loading zipf data OK\n",id);
    }
  */
  sleep(2);
  uint64_t start_t = rdtsc();

  int finish_ops=0;
  int num_post=0;
  int count=0;
  int curr_op=0;
  uint64_t latency_accum=0;
  while(true){
    if(finish_ops+batch_factor>=num_operations)
      break;
    for(int k=0;k<batch_factor;k++){
      if(window[k].stage == -1){
	//window[k].key = config->buffer[curr_op];
	//curr_op++;
	window[k].key = rand_r(&seed)% ((int)(val_num*rate));
	window[k].stage=0;
	window[k].start=rdtsc();
      }
      count++;
      //this line is special, because when cache hit , it will modify window
      window[k]=clustering_limited_cache_post(ptr,window[k]);

      //rdmaread_post(ptr,window[k]);
      //hop_post(ptr,window[k]);
      //clustering_post(ptr,window[k]);
      //hop_indirect_post(ptr,window[k]);
      //cuckoo_post(ptr,window[k]);
    }
    for(int k=0;k<batch_factor;k++){
      window[k]=clustering_limited_cache_poll(ptr,window[k]);
      //window[k]=rdmaread_poll(ptr,window[k]);
      //window[k]=hop_poll(ptr,window[k]);
      //window[k]=clustering_poll(ptr,window[k]);
      //window[k]=hop_indirect_poll(ptr,window[k]);
      //window[k]=cuckoo_poll(ptr,window[k]);
      if(window[k].stage == -1){
	latency_accum+=(rdtsc()-window[k].start);
	finish_ops++;
      }
    }
  }
  uint64_t cycle_num = rdtsc() - start_t;
  //printf("count %d\n",count);
  //printf("average latency %ld ns\n",latency_accum/finish_ops*1000000000 /cycles_per_second  );
  //printf("average cycle_num %ld ns\n",cycle_num*1000000000/finish_ops /cycles_per_second  );
  config->latency=latency_accum/finish_ops*1000000000 /cycles_per_second;
  config->throughput=0;
  config->throughput=cycles_per_second *1.0*num_operations/ cycle_num;
}

void Run1(void *ptr) {
  fprintf(stdout,"polling...\n");
  while(1) {
    sleep(1);
  }
}
