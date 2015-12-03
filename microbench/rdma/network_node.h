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

#ifndef NETWORK_NODE_H
#define NETWORK_NODE_H

#include <zmq.hpp>
#include <string>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <fstream>
#include <errno.h>


class Network_Node {

 public:
  size_t total_partition;
  int pid;
  int nid;
  zmq::context_t context;
  zmq::socket_t* receiver;

  std::vector<std::string> net_def;
  std::unordered_map<int,zmq::socket_t*> socket_map;
  inline int hash(int _pid,int _nid){
    return _pid*200+_nid;
  }

 Network_Node(size_t _total_partition,int _pid,int _nid):nid(_nid),pid(_pid),context(1){
    total_partition =_total_partition;
    fprintf(stdout,"start %d %d listening...\n",_pid,_nid);

    fprintf(stdout,"using default network fun\n");
    net_def.push_back("10.0.0.100");
    net_def.push_back("10.0.0.101");
    net_def.push_back("10.0.0.102");
    net_def.push_back("10.0.0.103");
    net_def.push_back("10.0.0.104");
    net_def.push_back("10.0.0.105");



    receiver=new zmq::socket_t(context, ZMQ_PULL);
    char address[30]="";
    sprintf(address,"tcp://*:%d",5500+hash(pid,nid));
    fprintf(stdout,"tcp binding address %s\n",address);
    receiver->bind (address);
    fprintf(stdout,"init netpoint done\n");
  }

  ~Network_Node(){
    for(auto iter:socket_map){
      if(iter.second!=NULL){
	delete iter.second;
	iter.second=NULL;
      }
    }
    delete receiver;
  }

  void Send(int _pid,int _nid,std::string msg){
    std::string header="00";
    header[0]=pid;
    header[1]=nid;
    msg=header+msg;
    int id=hash(_pid,_nid);
    if(socket_map.find(id)== socket_map.end()){
      socket_map[id]=new zmq::socket_t(context, ZMQ_PUSH);
      char address[30]="";

      assert(_pid < total_partition);
      snprintf(address,30,"tcp://%s:%d",net_def[_pid].c_str(),5500 + id);
      fprintf(stdout,"mul estalabish %s\n",address);

      socket_map[id]->connect (address);
    }
    zmq::message_t request(msg.length());
    memcpy ((void *) request.data(), msg.c_str(), msg.length());
    socket_map[id]->send(request);
  }

  std::string Recv(){
    zmq::message_t reply;
    if(receiver->recv(&reply) < 0) {
      fprintf(stderr,"recv with error %s\n",strerror(errno));
      exit(-1);
    }
    return std::string((char *)reply.data(),reply.size());
  }

  std::string tryRecv(){
    zmq::message_t reply;
    if (receiver->recv(&reply, ZMQ_NOBLOCK))
      return std::string((char *)reply.data(),reply.size());
    else
      return "";
  }

};

#endif
