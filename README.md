About DrTM
======
This is the repository of our prototype database system of the paper *Fast In-memory Transaction Processing Using RDMA and RTM*.
DrTM exploits advanced hardeare features such as HTM (Hardware Transactional Memory) and RDMA (Remote Directly Memory Access) to execute distributed transactions.DrTM achieves high performance by offloading the transaction's execution to a local processor using HTM, and uses RDMA to support distributed transactions.

If you use DrTM in your work or research, please kindly let us know about it. We also encourage you to reference our paper:)

Here is the bibtex:

	@inproceedings{Wei:2015:FIT:2815400.2815419,
	 author = {Wei, Xingda and Shi, Jiaxin and Chen, Yanzhe and Chen, Rong and Chen, Haibo},
	 title = {Fast In-memory Transaction Processing Using RDMA and HTM},
	 booktitle = {Proceedings of the 25th Symposium on Operating Systems Principles},
	 series = {SOSP '15},
	 year = {2015},
	 location = {Monterey, California},
	 pages = {87--104},
	 publisher = {ACM},
	} 

For more information,please visit: http://ipads.se.sjtu.edu.cn/drtm.html



Dependencies
------------

* `zeromq` 4.0.5 or higher
* Intel processors with RTM enabled
* Mellanox OFED v3.0-2.0.1 stack or higher
* ptpd
* SSMalloc



Build
-----

To install ptpd, just use

	 apt-get install ptpd;
	 service start ptp

SSMalloc can be found at https://github.com/SJTU-IPADS/SSMalloc.



Compiling
---------

At main directory, use
   
   	make clean;make C=3 dbtest -j

We recommend using make’s parallel build feature to accelerate the compilation
process.

After the compilation,you will get an execuatable file named `dbtest`.



How to use
---------

You can write a configure file for the ease of evaluation.
The format of the file is very simple.
Each line in the file will be parsed to an IP address or host name.
The line numbers will corresponding to machine ids.
For example, the following file will corresponding to a cluster which the machine 0's ip is
10.0.0.1,machine 1's ip is 10.0.0.2 .
		 
	####
	10.0.0.1
	10.0.0.2
	####

By default, the machine with id 0 (the fist ip in the configuration file) will serve as the master.
It will collect the evaluation results from other machines.


Use the following comands to start DrTM instances at server(s) other than master.
./run.py i (config_filename) (thread_num)
The default execuation directory is the home directory , but you  can change it at run.py script.
Then you can start master for the benchmark's execution.

Here is an example of starting an TPCC test on master.

	./dbtest --bench tpcc --db-type ndb-proto2 --num-threads 8 --scale-factor 8 --txn-flags 1 --ops-per-worker 500000 --bench-opts "--w 45,43,4,4,4 -r 1" --verbose --retry-aborted-transactions --total-partition 4 --current-partition 0 --config   config_file

In this command,it assumes there are 4 machines configured in `config_file`.

Note that the thread number and the configure will be exact to the on in the `run.py`.
The number of total partition must match the number of lines in the configure file.
Now it's all set.



Acknowledgments
---------------

Some part of DrTM's codebase follows from [Silo](https://github.com/stephentu/silo) and [leveldb](https://github.com/google/leveldb).






