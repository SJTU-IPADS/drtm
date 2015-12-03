#!/usr/bin/env python

import sys
import commands
import subprocess
import time

machine_mapping = {
    0:"10.0.0.100",
    1:"10.0.0.101",
    2:"10.0.0.102",
    3:"10.0.0.103",
    4:"10.0.0.104",
    5:"10.0.0.105"    
}

sent_map = {}

start_id = 0
target_dir = ""
timeout    = 20 ##20 seconds
#num_threads = 8
num_threads = 10
scale_factor = num_threads
opt_per_worker = 1000000
distributed_rate = 20
ratio = 1


f = "dafault.txt"
f = "scalability_config"
f = "config_file"

prev_par    = -1
cur_machine = ""

def start_server(id):
    ##now for testing banks
    i, = id
    #print "spreading %s" % machine_mapping[i]
    start_par = 0
    end_par = 0
    
    #tpcc with logging
    #    base_cmd = """ cd ~/%s; ./dbtest --bench tpcc --db-type ndb-proto2 --num-threads %d --scale-factor %d --txn-flags 1 --ops-per-worker %d --bench-opts "--w 45,43,4,4,4 -r 1" --verbose --retry-aborted-transactions --total-partition %d --current-partition %d --config  %s  1>%s  2>%s &""" % (target_dir,num_threads,num_threads,opt_per_worker,len(machine_mapping),i,f,str(i)+"log",str(i) + "err")

    #tpcc no logging

    base_cmd = """ cd ~/%s; ./dbtest --bench tpcc --db-type ndb-proto2 --num-threads %d --scale-factor %d --txn-flags 1 --ops-per-worker %d --bench-opts "--w 45,43,4,4,4 -r %d" --verbose --retry-aborted-transactions --total-partition %d --current-partition %d --config   %s 1>/dev/null 2>&1 &""" % (target_dir,num_threads,num_threads,opt_per_worker,ratio,len(machine_mapping),i,f)

##small bank cmd without logging
#    base_cmd = """ cd ~/%s;nohup ./dbtest --bench bank --db-type ndb-proto2 --num-threads %d --scale-factor %d --txn-flags 1 --ops-per-worker %d --bench-opts "--w 25,15,15,15,15,15 -r 1" --verbose  --total-partition %d --current-partition %d --config  %s 1>/dev/null 2>&1 &""" % (target_dir,num_threads,num_threads,opt_per_worker,len(machine_mapping),i,f)


#small bank cmd with logging
#    base_cmd = """ cd ~/%s;nohup ./dbtest --bench bank --db-type ndb-proto2 --num-threads %d --scale-factor %d --txn-flags 1 --ops-per-worker %d --bench-opts "--w 25,15,15,15,15,15 -r 1" --verbose  --total-partition %d --current-partition %d --config  %s  1>%s  2>%s &""" % (target_dir,num_threads,num_threads,opt_per_worker,len(machine_mapping),i,f,str(i)+"log",str(i) + "err")
    print base_cmd

    if (i == start_id):
#        [status,m] = commands.getstatusoutput(base_cmd)
#        print m
        pass
    else:        
        print "called %d" % i
        subprocess.call(["ssh","-n","-f",machine_mapping[i],base_cmd])
        time.sleep(0.05)
        pass

def kill_all():    
    cmd =  "pkill dbtest"
    for i in machine_mapping.keys():
        subprocess.call(["ssh","-n","-f",machine_mapping[i],cmd])


#if len(sys.argv) == 4:
#    kill_all()
#    exit()

copy_mode = False

if len(sys.argv) >= 2:
    ##switch cmds
    if sys.argv[1] == "k":
        #kill commands
        kill_all()
        exit()
    elif (sys.argv[1] == "i" or sys.argv[1] == "r"):
        #init run
        if len(sys.argv) == 2:
            print "Need a config file!"
            exit()
        f = sys.argv[2]
        file = open(f)
    
        machine_mapping = {}
        
        i = 0
        for line in file:
            machine_mapping[i] = line.strip()
            i  = i + 1
        print machine_mapping
        
        if (len(sys.argv) >= 4) :
            num_threads = int(sys.argv[3])
            scale_factor = num_threads;
        if (len(sys.argv) == 5):
            ratio = int(sys.argv[4]);
            
    if sys.argv[1] == "i":
        copy_mode = True


##warm up
if copy_mode:
    print 'copying...'
    for  i in machine_mapping.keys():
        if machine_mapping[i] == machine_mapping[start_id]:
            continue
        ##copy the executes    
        if sent_map.has_key(machine_mapping[i]):
            continue
        else:
            print "copy to %s" % machine_mapping[i]
            subprocess.call(["scp","./dbtest","%s:~/%s" % (machine_mapping[i],target_dir)])   
            subprocess.call(["scp","./%s" % f,"%s:~/%s" % (machine_mapping[i],target_dir)])
            subprocess.call(["scp","./start_server.py","%s:~/%s" % (machine_mapping[i],target_dir)])
            sent_map[machine_mapping[i]] = True

#exit()
##start

for i in machine_mapping.keys() :
    if i == start_id:
        pass
    else: 
        start_server((i,))
        pass
exit()
cur = 1
while cur < len(machine_mapping):
    end_par = cur
    while machine_mapping[end_par] == machine_mapping[cur]:
        end_par += 1
        if end_par  >= len(machine_mapping):
            break

    base_cmd = """ cd ~/%s;./start_server.py %d %d %d %d %s""" % (target_dir,cur,end_par-1,num_threads,len(machine_mapping),f)
    print base_cmd
    subprocess.call(["ssh","-n","-f",machine_mapping[cur],base_cmd])
    
    cur = end_par

#start_server((0,))
print "start_all_server except master done"
#kill_all()    
    
        
