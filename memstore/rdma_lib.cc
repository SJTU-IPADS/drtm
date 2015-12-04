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

/*
 *  RDMA libs - XingDa
 */

#include "rdma_resource.h"
#include <errno.h>

struct config_t rdma_config = {
  NULL,                         /* dev_name */
  NULL,                         /* server_name */
  19875,                        /* tcp_port */
  1,                            /* ib_port */
  -1                            /* gid_idx */
};



#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t
htonll (uint64_t x)
{
  return bswap_64 (x);
}

static inline uint64_t
ntohll (uint64_t x)
{
  return bswap_64 (x);
}
#elif __BYTE_ORDER == __BIG_ENDIAN

static inline uint64_t
htonll (uint64_t x)
{
  return x;
}

static inline uint64_t
ntohll (uint64_t x)
{
  return x;
}
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif


static void
dev_resources_init (struct dev_resource *res)
{
  memset (res, 0, sizeof *res);
}

static int
dev_resources_create (struct dev_resource *res,char* buf,uint64_t size)
{
  struct ibv_device **dev_list = NULL;
  struct ibv_qp_init_attr qp_init_attr;
  struct ibv_device *ib_dev = NULL;
  int i;
  int num_devices;
  int mr_flags = 0;
  int rc = 0;

  dev_list = ibv_get_device_list (&num_devices);
  if (!dev_list)
    {
      fprintf (stderr, "failed to get IB devices list\n");
      rc = 1;
      goto dev_resources_create_exit;
    }
  /* if there isn't any IB device in host */
  if (!num_devices)
    {
      fprintf (stderr, "found %d device(s)\n", num_devices);
      rc = 1;
      goto dev_resources_create_exit;
    }
  //  fprintf (stdout, "found %d device(s)\n", num_devices);
  /* search for the specific device we want to work with */
  for (i = 0; i < num_devices; i++)
    {
      if (!rdma_config.dev_name)
        {
          rdma_config.dev_name = strdup (ibv_get_device_name (dev_list[i]));
          fprintf (stdout,
                   "device not specified, using first one found: %s\n",
                   rdma_config.dev_name);
        }
      if (!strcmp (ibv_get_device_name (dev_list[i]), rdma_config.dev_name))
        {
          ib_dev = dev_list[i];
          break;
        }
    }
  /* if the device wasn't found in host */
  if (!ib_dev)
    {
      fprintf (stderr, "IB device %s wasn't found\n", rdma_config.dev_name);
      rc = 1;
      goto dev_resources_create_exit;
    }
  /* get device handle */
  res->ib_ctx = ibv_open_device (ib_dev);

  if (!res->ib_ctx)
    {
      fprintf (stderr, "failed to open device %s\n", rdma_config.dev_name);
      rc = 1;
      goto dev_resources_create_exit;
    }

  //check the atomicity level for rdma operation
  int ret;
  ret = ibv_query_device(res->ib_ctx,&(res->device_attr));
  if (ret) {
    fprintf(stderr,"ibv quert device %d\n",ret);
    assert(false);
  }

  fprintf(stdout,"The max size can reg: %ld\n",res->device_attr.max_mr_size);

  switch(res->device_attr.atomic_cap) {

  case IBV_ATOMIC_NONE:
    fprintf(stdout,"atomic none\n");
    break;
  case IBV_ATOMIC_HCA:
    fprintf(stdout,"atmoic within device\n");
    break;
  case IBV_ATOMIC_GLOB:
    fprintf(stdout,"atomic globally\n");
    break;
  default:
    fprintf(stdout,"atomic unknown !!\n");
    assert(false);
  }

  /* We are now done with device list, free it */
  ibv_free_device_list (dev_list);
  dev_list = NULL;
  ib_dev = NULL;
  /* query port properties */
  if (ibv_query_port (res->ib_ctx, rdma_config.ib_port, &res->port_attr))
    {
      fprintf (stderr, "ibv_query_port on port %u failed\n", rdma_config.ib_port);
      rc = 1;
      goto dev_resources_create_exit;
    }

  /* allocate Protection Domain */
  res->pd = ibv_alloc_pd (res->ib_ctx);
  if (!res->pd)
    {
      fprintf (stderr, "ibv_alloc_pd failed\n");
      rc = 1;
      goto dev_resources_create_exit;
    }

  res->buf=buf;
  assert(buf != NULL);
  //  memset (res->buf, 0, size);//TODO!!!

  /* register the memory buffer */
  mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;//add cmp op

  fprintf(stdout,"registering memory\n");
  res->mr = ibv_reg_mr (res->pd, res->buf, size, mr_flags);
  if (!res->mr)
    {
      fprintf (stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
      rc = 1;
      goto dev_resources_create_exit;
    }
  fprintf (stdout,
	   "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
	   res->buf, res->mr->lkey, res->mr->rkey, mr_flags);

 dev_resources_create_exit:
  if (rc)
    {
      /* Error encountered, cleanup */
      if (res->mr)
        {
          ibv_dereg_mr (res->mr);
          res->mr = NULL;
        }
      if (res->pd)
        {
          ibv_dealloc_pd (res->pd);
          res->pd = NULL;
        }
      if (res->ib_ctx)
        {
          ibv_close_device (res->ib_ctx);
          res->ib_ctx = NULL;
        }
      if (dev_list)
        {
          ibv_free_device_list (dev_list);
          dev_list = NULL;
        }

    }
  return rc;
}

static void
QP_init (struct QP *res) {
  memset (res, 0, sizeof *res);
}

static int
QP_create(struct QP *res,struct dev_resource *dev)
{
  res->dev = dev;

  struct ibv_qp_init_attr qp_init_attr;

  res->pd = dev->pd;
  res->mr = dev->mr;

  int rc = 0;
  int cq_size = 1;
  res->cq = ibv_create_cq (dev->ib_ctx,cq_size,NULL,NULL,0);
  if(!res->cq) {
    fprintf (stderr, "failed to create CQ with %u entries\n", cq_size);
    rc = 1;
    goto resources_create_exit;
  }

  memset (&qp_init_attr, 0, sizeof (qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 1;
  qp_init_attr.send_cq = res->cq;
  qp_init_attr.recv_cq = res->cq;
  qp_init_attr.cap.max_send_wr = 1;
  qp_init_attr.cap.max_recv_wr = 1;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;
  res->qp = ibv_create_qp (res->pd, &qp_init_attr);
  if (!res->qp)
    {
      fprintf (stderr, "failed to create QP\n");
      rc = 1;
      goto resources_create_exit;
    }

 resources_create_exit:
  if(rc) {

    /* Error encountered, cleanup */
    if (res->qp)
      {
	ibv_destroy_qp (res->qp);
	res->qp = NULL;
      }
    if(res->cq) {
      ibv_destroy_cq(res->cq);
      res->cq  = NULL;
    }
  }

  return rc;
}



static struct cm_con_data_t
get_local_con_data(struct QP *res)
{
  struct cm_con_data_t local_con_data;
  union ibv_gid my_gid;
  int rc;
  if (rdma_config.gid_idx >= 0)
    {
      rc =
        ibv_query_gid (res->dev->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx, &my_gid);
      if (rc)
        {
          fprintf (stderr, "could not get gid for port %d, index %d\n",
                   rdma_config.ib_port, rdma_config.gid_idx);
          assert(false);
        }
    }
  else
    memset (&my_gid, 0, sizeof my_gid);

  local_con_data.addr = htonll ((uintptr_t) res->dev->buf);
  local_con_data.rkey = htonl (res->mr->rkey);
  local_con_data.qp_num = htonl (res->qp->qp_num);
  local_con_data.lid = htons (res->dev->port_attr.lid);
  memcpy (local_con_data.gid, &my_gid, 16);
  //  fprintf (stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  return local_con_data;
}



static int
modify_qp_to_init (struct ibv_qp *qp)
{
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset (&attr, 0, sizeof (attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = rdma_config.ib_port;
  attr.pkey_index = 0;
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

  flags =
    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp (qp, &attr, flags);
  if (rc)
    fprintf (stderr, "failed to modify QP state to INIT\n");
  return rc;
}


static int
modify_qp_to_rtr (struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid,
                  uint8_t * dgid)
{
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset (&attr, 0, sizeof (attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_256;
  attr.dest_qp_num = remote_qpn;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 0x12;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = dlid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = rdma_config.ib_port;
  if (rdma_config.gid_idx >= 0)
    {
      attr.ah_attr.is_global = 1;
      attr.ah_attr.port_num = 1;
      memcpy (&attr.ah_attr.grh.dgid, dgid, 16);
      attr.ah_attr.grh.flow_label = 0;
      attr.ah_attr.grh.hop_limit = 1;
      attr.ah_attr.grh.sgid_index = rdma_config.gid_idx;
      attr.ah_attr.grh.traffic_class = 0;
    }
  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
    IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp (qp, &attr, flags);
  if (rc)
    fprintf (stderr, "failed to modify QP state to RTR\n");
  return rc;
}


static int
modify_qp_to_rts (struct ibv_qp *qp)
{
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset (&attr, 0, sizeof (attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0x12;
  attr.retry_cnt = 6;
  attr.rnr_retry = 0;
  attr.sq_psn = 0;
  attr.max_rd_atomic = 1;
  attr.max_dest_rd_atomic = 1;
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
    IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp (qp, &attr, flags);
  if (rc)
    fprintf (stderr, "failed to modify QP state to RTS\n");
  return rc;
}


static int
connect_qp (struct QP *res,struct cm_con_data_t tmp_con_data)
{
  struct cm_con_data_t remote_con_data;
  int rc = 0;
  char temp_char;

  /* exchange using TCP sockets info required to connect QPs */

  remote_con_data.addr = ntohll (tmp_con_data.addr);
  remote_con_data.rkey = ntohl (tmp_con_data.rkey);
  remote_con_data.qp_num = ntohl (tmp_con_data.qp_num);
  remote_con_data.lid = ntohs (tmp_con_data.lid);
  memcpy (remote_con_data.gid, tmp_con_data.gid, 16);
  /* save the remote side attributes, we will need it for the post SR */
  res->remote_props = remote_con_data;

  if (rdma_config.gid_idx >= 0)
    {
      uint8_t *p = remote_con_data.gid;
      fprintf (stdout,
               "Remote GID = %02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n",
               p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9],
               p[10], p[11], p[12], p[13], p[14], p[15]);
    }
  /* modify the QP to init */
  rc = modify_qp_to_init (res->qp);
  if (rc)
    {
      fprintf (stderr, "change QP state to INIT failed\n");
      goto connect_qp_exit;
    }
  /* let the client post RR to be prepared for incoming messages */

  /* modify the QP to RTR */
  rc =
    modify_qp_to_rtr (res->qp, remote_con_data.qp_num, remote_con_data.lid,
                      remote_con_data.gid);
  if (rc)
    {
      fprintf (stderr, "failed to modify QP state to RTR\n");
      goto connect_qp_exit;
    }
  //  fprintf (stderr, "Modified QP state to RTR\n");
  rc = modify_qp_to_rts (res->qp);
  if (rc)
    {
      fprintf (stderr, "failed to modify QP state to RTR\n");
      goto connect_qp_exit;
    }
  //  fprintf (stdout, "QP state was change to RTS\n");
  /* sync to make sure that both sides are in states that they can connect to prevent packet loose */

 connect_qp_exit:
  return rc;
}



static int
post_send(struct QP *res,normal_op_req *reqs,int size) {
  assert(size > 0 && size < MAX_BUFFER_COUNT);

  struct ibv_send_wr *bad_wr = NULL;

  for(int i = 0;i < size ;++i) {
    reqs[i].sge.addr = (uintptr_t) reqs[i].local_buf;
    reqs[i].sge.length = reqs[i].size?reqs[i].size:sizeof(uint64_t);
    reqs[i].sge.lkey   = res->mr->lkey;

    if(i != size - 1) {
      reqs[i].sr.next = &(reqs[i+1].sr);
    }else
      reqs[i].sr.next = NULL;

    reqs[i].sr.wr_id = i;//TODO!!!
    reqs[i].sr.num_sge = 1;
    reqs[i].sr.sg_list = &(reqs[i].sge);

    switch (reqs[i].opcode) {
    case  IBV_WR_ATOMIC_FETCH_AND_ADD:
    case  IBV_WR_ATOMIC_CMP_AND_SWP:
      reqs[i].sr.opcode = reqs[i].opcode;
      reqs[i].sr.wr.atomic.remote_addr = res->remote_props.addr + reqs[i].remote_offset;//this field is uint64_t
      reqs[i].sr.wr.atomic.rkey = res->remote_props.rkey;
      reqs[i].sr.wr.atomic.compare_add = reqs[i].compare_and_add;
      reqs[i].sr.wr.atomic.swap = reqs[i].swap;
      break;
    case IBV_WR_RDMA_READ:
    case IBV_WR_RDMA_WRITE:
      reqs[i].sr.opcode = reqs[i].opcode;
      reqs[i].sr.wr.rdma.remote_addr = res->remote_props.addr + reqs[i].remote_offset;
      reqs[i].sr.wr.rdma.rkey = res->remote_props.rkey;
      break;
    default:
      fprintf(stdout,"unknown rdma operation\n");
      assert(false);

    }
    reqs[i].sr.send_flags = IBV_SEND_SIGNALED  | IBV_SEND_FENCE;
  }

  int rc;
  rc = ibv_post_send (res->qp, &(reqs[0].sr), &bad_wr);

  if(rc) {
    fprintf(stderr,"failed to post SR CAS\n");
  }else {

  }
  return rc;
}

static int
post_send (struct QP *res, int opcode,char* local_buf,size_t size,size_t remote_offset)
{
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr *bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset (&sge, 0, sizeof (sge));
  sge.addr = (uintptr_t) local_buf;
  sge.length = size;
  sge.lkey = res->mr->lkey;
  /* prepare the send work request */
  memset (&sr, 0, sizeof (sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = opcode;
  sr.send_flags = IBV_SEND_SIGNALED;
  if (opcode != IBV_WR_SEND)
    {
      sr.wr.rdma.remote_addr = res->remote_props.addr+remote_offset;
      sr.wr.rdma.rkey = res->remote_props.rkey;
    }
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  rc = ibv_post_send (res->qp, &sr, &bad_wr);
  if (rc)
    fprintf (stderr, "failed to post SR\n");
  else
    {
      /*
	switch (opcode)
	{
	case IBV_WR_SEND:
	fprintf (stdout, "Send Request was posted\n");
	break;
	case IBV_WR_RDMA_READ:
	fprintf (stdout, "RDMA Read Request was posted\n");
	break;
	case IBV_WR_RDMA_WRITE:
	fprintf (stdout, "RDMA Write Request was posted\n");
	break;
	default:
	fprintf (stdout, "Unknown Request was posted\n");
	break;
	}*/
    }
  return rc;
}


static int
poll_completion (struct QP *res) {
  struct ibv_wc wc;
  unsigned long start_time_msec;
  unsigned long cur_time_msec;
  struct timeval cur_time;
  int poll_result;
  int rc = 0;
  /* poll the completion for a while before giving up of doing it .. */
  //  gettimeofday (&cur_time, NULL);
  //  start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

  do
    {
      poll_result = ibv_poll_cq (res->cq, 1, &wc);
      //      gettimeofday (&cur_time, NULL);
      //      cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    }
  while ((poll_result == 0));
  //         && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
  if (poll_result < 0)
    {
      /* poll CQ failed */
      fprintf (stderr, "poll CQ failed\n");
      rc = 1;
    }
  else if (poll_result == 0)
    {
      /* the CQ is empty */
      fprintf (stderr, "completion wasn't found in the CQ after timeout\n");
      rc = 1;
    }
  else
    {
      /* CQE found */
      //      fprintf (stdout, "completion was found in CQ with status 0x%x\n",
      //               wc.status);
      /* check the completion status (here we don't care about the completion opcode */
      if (wc.status != IBV_WC_SUCCESS)
	{
	  fprintf (stderr,
		   "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
		   wc.status, wc.vendor_err);
	  rc = 1;
	}
    }
  return rc;
}

static int
poll_completions (struct QP *res,int size) {

  struct ibv_wc wc[MAX_BUFFER_COUNT];
  unsigned long start_time_msec;
  unsigned long cur_time_msec;
  struct timeval cur_time;
  int poll_result;
  int rc = 0;
  /* poll the completion for a while before giving up of doing it .. */
  //  gettimeofday (&cur_time, NULL);
  //  start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  int finish_count = 0;

  while(finish_count != size) {
    do
      {
	poll_result = ibv_poll_cq (res->cq, size, wc);
	//      gettimeofday (&cur_time, NULL);
	//      cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
      }
    while ((poll_result == 0));
    //         && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
    if (poll_result < 0)
      {
	/* poll CQ failed */
	fprintf (stderr, "poll CQ failed\n");
	rc = 1;
	break;
      }
    else if (poll_result == 0)
      {
	/* the CQ is empty */
	fprintf (stderr, "completion wasn't found in the CQ after timeout\n");
	rc = 1;
	break;
      }
    else
      {
	finish_count += poll_result;

	/* CQE found */
	//      fprintf (stdout, "completion was found in CQ with status 0x%x\n",
	//               wc.status);
	/* check the completion status (here we don't care about the completion opcode */
	/*
	  if (wc.status != IBV_WC_SUCCESS)
	  {
	  fprintf (stderr,
	  "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
	  wc.status, wc.vendor_err);
	  rc = 1;
	  }*/
	for(int i = 0;i < poll_result;++i) {

	  if (wc[i].status != IBV_WC_SUCCESS)
	    {
	      fprintf (stderr,
		       "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
		       wc[i].status, wc[i].vendor_err);
	      rc = 1;
	    }
	}

      }
    //end while
  }
  return rc;
}



namespace drtm {


  RdmaResource::RdmaResource (char *mem,uint64_t length,uint64_t bufSize ,char *end){
    _total_partition = total_partition;
    _total_threads = nthreads;
    _current_partition = current_partition;

    buffer = mem;
    size = length;
    bufferSize = bufSize;
    bufferEntrySize = bufferSize * MAX_BUFFER_COUNT;
    off = size - MAX_THREADS * bufferEntrySize;
    assert( off > (end - mem));

    //assume that there are 11 tables at most
    //    for(int i = 0;i < 11;++i) {
    //right now we assume that the cache has infinte length
    //      loc_hashes[i] = new MemstoreHashTableR();
    //    }

    init();
  }

  RdmaResource::RdmaResource(int t_partition,int t_threads,int current,char *_buffer,int _size,int _slotsize,int _off) {

    _total_threads = t_threads;
    _total_partition = t_partition;
    _current_partition = current;

    buffer = _buffer;
    size   = _size;

    off = _off;
    slotsize = _slotsize;
    bufferSize = slotsize;
    bufferEntrySize = bufferSize;
    init();
  }

  void RdmaResource::init() {
    assert(_total_partition >= 0 && _total_threads >= 0 && _current_partition >= 0);

    dev0 = new dev_resource;
    dev1 = new dev_resource;

    dev_resources_init(dev0);
    dev_resources_init(dev1);

    if(dev_resources_create(dev0,buffer,size) || dev_resources_create(dev1,buffer,size)) {
      fprintf(stderr,"failed to create dev resources");
    }

    res = new struct QP *[_total_threads];

    for(int i = 0; i< _total_threads ; i++){
      res[i] = new struct QP[_total_partition];
      for(int j = 0; j < _total_partition;j++){
        QP_init (res[i]+j);
        if (QP_create (res[i]+j,dev0))
	  {
	    fprintf (stderr, "failed to create qp\n");
	    assert(false);
	  }
      }
      //end for
    }

    own_res = new struct QP[_total_threads];
    for(int i = 0;i < _total_threads;++i) {
      QP_init(own_res + i);
      if(QP_create(own_res + i,dev1)) {
	fprintf(stderr,"failed to create own resources\n");
	assert(false);
      }
    }
    //done
  }
  void RdmaResource::Servicing() {
    pthread_t update_tid;
    pthread_create(&update_tid, NULL, RecvThread, (void *)this);

  }

  void RdmaResource::Connect() {

    std::vector<int> partitions;
    for(int i = 0;i < total_partition;++i) {
      partitions.push_back(i);
    }
    std::random_shuffle(partitions.begin(),partitions.end());

    for(int j = 0;j < total_partition;++j) {
      char address[30];
      zmq::context_t context(1);
      zmq::socket_t socket(context,ZMQ_REQ);

      int port = j * 200 + nthreads + 73 + 5500;
      snprintf(address,30,"tcp://%s:%d",node->net_def[j].c_str(),port);
      socket.connect(address);

      for(int i = 0;i < nthreads;++i) {

	zmq::message_t request(2);
	std::string msg = "00";
	msg[0] = (char)current_partition;
	msg[1] = (char)i;

	memcpy(request.data(),msg.c_str(),2);
	socket.send(request);

	//get reply
	zmq::message_t reply;
	socket.recv(&reply);
	struct cm_con_data_t remote_con_data;

	memcpy(&remote_con_data,(char *)reply.data(),sizeof(remote_con_data));
	if(connect_qp(res[i] + j,remote_con_data) ){
	  fprintf (stderr, "failed to connect QPs\n");
	  assert(false);
	  exit(-1);
	}

      }
    }
    fprintf(stdout,"connection done------------\n");
  }

  int RdmaResource::RdmaRead(int t_id,int m_id,char *local,int size,uint64_t off) {

    return rdmaOp(t_id,m_id,local,size,off,IBV_WR_RDMA_READ);
  }

  int RdmaResource::RdmaWrite(int t_id,int m_id,char* local,int size,uint64_t off) {
    return rdmaOp(t_id,m_id,local,size,off,IBV_WR_RDMA_WRITE);
  }

  int RdmaResource::RdmaOps(int t_id,int m_id,normal_op_req *reqs,int _s) {
    if(post_send(res[t_id] + m_id,reqs,_s) ){
      fprintf(stderr,"failed to post request.\n");
      assert(false);
    }
    if(poll_completions(res[t_id] + m_id,_s)) {
      fprintf(stderr,"poll completion failed\n");
      assert(false);
    }
    //TODO! we need to
    return 0;
  }

  int RdmaResource::rdmaOp(int t_id,int machine_id,char* local,int size,uint64_t remote_offset,int op) {
    //simple wrapper function for handling rdma compare and swap

    assert(remote_offset < this->size);
    assert(machine_id < _total_partition);
    assert(t_id < _total_threads);

    if(post_send(res[t_id] + machine_id,op,local,size,remote_offset) ) {
      fprintf(stderr,"failed to post request.");
      assert(false);
    }
    if(poll_completion(res[t_id] + machine_id)) {
      fprintf(stderr,"poll completion failed\n");
      assert(false);
    }
    //TODO! we need to
    return 0;
  }

  int RdmaResource::RdmaFetchAdd(int t_id,int m_id,char *local,uint64_t add,uint64_t remote_offset) {
    struct QP *r = res[t_id] + m_id;
    assert(r != NULL);

    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;

    memset(&sge,0,sizeof(sge));
    sge.addr = (uintptr_t)local;
    sge.length = sizeof(uint64_t);
    sge.lkey = r->mr->lkey;

    memset(&sr,0,sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.atomic.remote_addr = r->remote_props.addr + remote_offset;//this field is uint64_t
    sr.wr.atomic.rkey = r->remote_props.rkey;
    sr.wr.atomic.compare_add = add;

    rc = ibv_post_send(r->qp,&sr,&bad_wr);
    if(rc) {
      fprintf(stderr,"failed to post SR fetch & add\n");
    }else {

    }
    if(poll_completion(r) ){
      fprintf(stderr,"poll completion failed\n");
      assert(false);
    }
    //TODO! we need to
    return 0;
  }

  int RdmaResource::RdmaCmpSwap(int t_id,int m_id,char*local,uint64_t compare,uint64_t swap,int size,uint64_t off) {

    struct QP *r = res[t_id] + m_id;
    assert(r != NULL);

    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;

    memset(&sge,0,sizeof(sge));
    sge.addr = (uintptr_t)local;
    sge.length = sizeof(uint64_t);
    sge.lkey = r->mr->lkey;

    memset(&sr,0,sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    sr.send_flags = IBV_SEND_SIGNALED;
    sr.wr.atomic.remote_addr = r->remote_props.addr + off;//this field is uint64_t
    sr.wr.atomic.rkey = r->remote_props.rkey;
    sr.wr.atomic.compare_add = compare;
    sr.wr.atomic.swap = swap;

    rc = ibv_post_send(r->qp,&sr,&bad_wr);
    if(rc) {
      fprintf(stderr,"failed to post SR CAS\n");
    }else {

    }
    if(poll_completion(r) ){
      fprintf(stderr,"poll completion failed\n");
      assert(false);
    }
    //TODO! we need to
    return 0;
  }


  void RdmaResource::rdmaTest() {
    fprintf(stdout,"start testing...@ %d\n",_current_partition);
    uint64_t *ptr = (uint64_t *)buffer;
    for(int i = 0;i < 96;++i) {
      buffer[i] = i + _current_partition;
    }

    if(_current_partition != 0){
      while(1)
	sleep(5);
    }



    uint64_t *local = (uint64_t *)GetMsgAddr(0);
    for(int i = 0; i < 64;++i){
      assert(RdmaRead(0,1,(char *)buffer,96 * sizeof(uint64_t),sizeof(uint64_t) * i) == 0);
      fprintf(stdout,"test %lld\n",*buffer);
    }

    exit(0);
  }


  void* RdmaResource::RecvThread(void * arg) {

    RdmaResource *rdma = (RdmaResource *)arg;

    zmq::context_t context(1);
    zmq::socket_t socket(context,ZMQ_REP);

    char address[30]="";
    int port = current_partition * 200 + nthreads + 73 + 5500;
    sprintf(address,"tcp://%s:%d",rdma->node->net_def[current_partition].c_str(),port);
    fprintf(stdout,"binding: %s\n",address);
    socket.bind(address);

    while(1) {

      zmq::message_t request;
      /*
	if(socket.recv(&request)) {
      	fprintf(stderr,"recving thread meets an error %s\n",strerror(errno));
      	exit(-1);
	}
	//
	*/
      socket.recv(&request);
      std::string s((char *)request.data(),request.size());

      int remote_pid = s[0];
      int remote_tid = s[1];
      //      fprintf(stdout,"recv %d %d\n",remote_pid,remote_tid);
      struct cm_con_data_t local_con_data = get_local_con_data((rdma->res)[remote_tid]+remote_pid);
      zmq::message_t reply(sizeof(local_con_data));

      memcpy((char *)(reply.data()),(char *)(&local_con_data),sizeof(local_con_data));
      socket.send(reply);

    }

  }

}
