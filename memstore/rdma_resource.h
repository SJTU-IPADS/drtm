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

#ifndef DRTM_MEM_RDMARESOURCE_H
#define DRTM_MEM_RDMARESOURCE_H

#include "db/network_node.h"

#pragma GCC diagnostic warning "-fpermissive"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <vector>

#define MAX_BUFFER_COUNT 8
#define MAX_THREADS 16

struct config_t
{
  const char *dev_name;         /* IB device name */
  char *server_name;            /* server host name */
  u_int32_t tcp_port;           /* server TCP port */
  int ib_port;                  /* local IB port to work with */
  int gid_idx;                  /* gid index to use */
};

/* structure to exchange data which is needed to connect the QPs */
struct cm_con_data_t
{
  uint64_t addr;                /* Buffer address */
  uint32_t rkey;                /* Remote key */
  uint32_t qp_num;              /* QP number */
  uint16_t lid;                 /* LID of the IB port */
  uint8_t gid[16];              /* gid */
} __attribute__ ((packed));
/* structure of system resources */

struct dev_resource {
  struct ibv_device_attr device_attr;   /* Device attributes */
  struct ibv_port_attr port_attr;       /* IB port attributes */
  struct ibv_context *ib_ctx;   /* device handle */

  struct ibv_pd *pd;            /* PD handle */
  struct ibv_mr *mr;            /* MR handle for buf */
  char *buf;                    /* memory buffer pointer, used for RDMA and send*/

};

struct QP {
  struct cm_con_data_t remote_props;  /* values to connect to remote side */
  struct ibv_pd *pd;            /* PD handle */
  struct ibv_cq *cq;            /* CQ handle */
  struct ibv_qp *qp;            /* QP handle */
  struct ibv_mr *mr;            /* MR handle for buf */

  struct dev_resource *dev;

};

struct resources
{
  struct ibv_device_attr device_attr;   /* Device attributes */
  struct ibv_port_attr port_attr;       /* IB port attributes */
  struct cm_con_data_t remote_props;    /* values to connect to remote side */
  struct ibv_context *ib_ctx;   /* device handle */
  struct ibv_pd *pd;            /* PD handle */
  struct ibv_cq *cq;            /* CQ handle */
  struct ibv_qp *qp;            /* QP handle */
  struct ibv_mr *mr;            /* MR handle for buf */
  char *buf;                    /* memory buffer pointer, used for RDMA and send
                                   ops */
  int sock;                     /* TCP socket file descriptor */
};

struct normal_op_req
{
  ibv_wr_opcode opcode;
  char *local_buf;
  int size; //default set to sizeof(uint64_t)
  uint64_t remote_offset;

  //for atomicity operations
  uint64_t compare_and_add;
  uint64_t swap;

  //for internal usage!!
  struct ibv_send_wr sr;
  struct ibv_sge sge;

};

extern size_t nthreads;
extern size_t total_partition;
extern size_t current_partition;

namespace  drtm {

  class RdmaResource {

    //site configuration settings
    int _total_partition = -1;
    int _total_threads = -1;
    int _current_partition = -1;


    struct dev_resource *dev0;//for remote usage
    struct dev_resource *dev1;//for local usage

    struct QP **res;
    struct QP  *own_res;

    uint64_t size;             // The size of the rdma region,should be the same across machines!
    uint64_t off ;             // The offset to send message
    char *buffer;         // The ptr of registed buffer

    int rdmaOp(int t_id,int m_id,char*buf,int size,uint64_t off,int op) ;
    void init();

  public:

    uint64_t bufferSize;
    int slotsize;
    int bufferEntrySize;

    Network_Node* node;
    RdmaResource(char *buffer,uint64_t size,uint64_t buffer_size,char *end);
    RdmaResource();

    //for testing , not use this for initialization
    RdmaResource(int t_partition,int t_threads,int current,char *_buffer,int _size,int _slotsize,int _off = 0);

    void Connect();
    void Servicing();

    //0 on success,-1 otherwise
    int RdmaOps (int t_id,int m_id,normal_op_req *reqs,int size);
    int RdmaFetchAdd(int t_id,int m_id,char *local,uint64_t add,uint64_t remote_offset);
    int RdmaCmpSwap(int t_id,int m_id,char *local,uint64_t compare,uint64_t swap,int size,uint64_t remote_offset);
    int RdmaRead(int t_id,int m_id,char *local,int size,uint64_t remote_offset);
    int RdmaWrite(int t_id,int m_id,char *local,int size,uint64_t remote_offset);


    void rdmaTest();

    //RDMA address for local msgs
    inline char *RdmaResource::GetMsgAddr(int t_id) {
      return (char *)( buffer + off + t_id * bufferEntrySize);
    }

    inline char *RdmaResource::GetMsgAddr(int t_id,int b_id ){
      assert(b_id < MAX_BUFFER_COUNT && b_id >= 0) ;
      return (char *)(buffer + off + t_id * bufferEntrySize + b_id * bufferSize);
    }


    static void* RecvThread(void * arg);//The listening thread to exchange queue pairs

  };
}

#endif
