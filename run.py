#!/usr/bin/python3

import simpy
from workload import Workload
from scheduler import Scheduler
from cluster import Cluster

config = '/local0/serverless-sim/config.4node.yaml' 
env = simpy.Environment()
cluster = Cluster(env, config)
scheduler = Scheduler(env, cluster=cluster)
workload = Workload(env, scheduler, config)
env.run(until=15000)

