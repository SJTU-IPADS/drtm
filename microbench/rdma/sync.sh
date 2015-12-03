#!/bin/bash

rm micro;
make micro;
rsync -rtuv /home/sjx/rdma_microbench/ cube1:/home/sjx/rdma_microbench/
rsync -rtuv /home/sjx/rdma_microbench/ cube2:/home/sjx/rdma_microbench/
rsync -rtuv /home/sjx/rdma_microbench/ cube3:/home/sjx/rdma_microbench/
rsync -rtuv /home/sjx/rdma_microbench/ cube4:/home/sjx/rdma_microbench/
rsync -rtuv /home/sjx/rdma_microbench/ cube5:/home/sjx/rdma_microbench/
