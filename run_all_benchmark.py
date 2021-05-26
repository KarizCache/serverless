#!/usr/bin/python3

import simpy
from workload import Workload
from scheduler import Scheduler
from cluster import Cluster
import argparse
import yaml
import json
from os import path
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='path to the configuration file for running the simulation.',
        type=str, default='./configs/e2ebenchmark.yaml')
args = parser.parse_args()

print(args.config)


params = yaml.load(open(args.config, 'r'), Loader=yaml.FullLoader)

print(json.dumps(params, indent=4))

cluster_conf_path = params['cluster']['configs']
cluster_configs = yaml.load(open(cluster_conf_path, 'r'), Loader=yaml.FullLoader)


print(params['benchmark'])

statistics_fpath = params['benchmark']['statistics']
#with open(statistics_fpath, 'w') as fd:
#    # write the header
#    fd.write('appname,scheduler,end2end,remote_read,local_read,transimit(s),compute(s),deseriation(s),serialization(s),task_time\n')



for sched_policy in params['cluster']['policy']['scheduling']:
    cluster_configs['cluster']['scheduling'] = sched_policy
    for ser_policy in params['cluster']['policy']['serialization']:
        cluster_configs['cluster']['serialization'] = ser_policy
        for workload in params['benchmark']['workloads']:
            cluster_configs['benchmark']['workloads'] = [workload]
            yaml.dump(cluster_configs, open(cluster_conf_path, 'w'))
            result = subprocess.run(['/local0/serverless/serverless-sim/run.py', '--config', cluster_conf_path])
            #print(json.dumps(cluster_configs, indent=4)km )
            #break
        #break
    #break
