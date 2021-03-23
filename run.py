#!/usr/bin/python3

import simpy
from workload import Workload
from scheduler import Scheduler
from cluster import Cluster
import argparse
import yaml

config = '/local0/serverless-sim/configs/config.4n1c.yaml' 

parser = argparse.ArgumentParser()
parser.add_argument('--config', help='path to the configuration file for running this experiment.',
        type=str, default=config)
args = parser.parse_args()
configs = yaml.load(open(args.config, 'r'), Loader=yaml.FullLoader)

env = simpy.Environment()
cluster = Cluster(env, topology=configs)
scheduler = Scheduler(env, cluster=cluster, configs = configs)
workload = Workload(env, scheduler, config)
env.run(until=150000)

