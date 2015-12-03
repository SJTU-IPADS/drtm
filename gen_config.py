#!/usr/bin/python

import sys


total_partition = 48

#machine_mapping = {0:"cube0",1:"cube1",2:"cube2",3:"cube3",4:"cube4",5:"cube5"}
machine_mapping = {
    0:"10.0.0.100",
    1:"10.0.0.101",
    2:"10.0.0.102",
    3:"10.0.0.103",
    4:"10.0.0.104",
    5:"10.0.0.105"    
}

machine_capacity = [0,0,0,0,0,0]
machine_count    = [0,0,0,0,0,0]


f = open("config_file","w")

def gen_config_avg(thr_num,partition_num):
    p_per_mac = 16 / thr_num
    
    for i in xrange(len(machine_capacity)):
        machine_capacity[i] = p_per_mac        
    
    current_machine = 0
    for i in xrange(partition_num):
        counter = 0        
        while machine_capacity[current_machine] == 0:            
            current_machine = (current_machine + 1) % 6
            counter = counter + 1
            if counter >= 6:
                print "There is no more machine to alloc!"
                exit()
        machine_capacity[current_machine] -= 1
        machine_count[current_machine] += 1
#        f.write(machine_mapping[current_machine])
#        f.write("\n")

        current_machine = (current_machine + 1) % 6
    for i in xrange(len(machine_count)):
        while machine_count[i] > 0:
            machine_count[i] -= 1
            f.write(machine_mapping[i])
            f.write("\n")
    f.close()
    

def gen_config_greedy(thr_num,partition_num):
    p_per_mac = 16 / thr_num
    for i in xrange(partition_num):
        f.write(machine_mapping[i / p_per_mac])
        f.write("\n")
    f.close()

    
if len(sys.argv) == 3:
    thr_num = int(sys.argv[1])
    partition_num = int(sys.argv[2])
    gen_config_avg(thr_num,partition_num)
#    gen_config_greedy(thr_num,partition_num)
    




