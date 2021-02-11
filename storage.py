#!/usr/bin/python3
import random
import queue
import simpy
import pandas as pd
from netsim import Request, NetworkInterface

class Object:
    def __init__(self, name, size):
        self.color = 0 # The color range is 0x000000-0xFFFFFF
        self.size = size
        self.name = name
        self.who_has = ''


class Cache:
    def __init__(self, env, size, policy, port, hostname=None):
        self.env = env
        self.eviction_policy = policy # This could be LRU, LRU, Fifo. Lets go with Fifo
        self.hostname = hostname
        self.size = size
        self.queue = queue.Queue(size)
        self.req_queue = simpy.Store(env)
        self.cache = {}
        self.port = port
        self.out_port = None
        executor = self.env.process(self.run())


    def put(self, req):
        return self.req_queue.put(req)

    def run(self):
        while True:
            req = yield self.req_queue.get()
            #print(f'Cache {self.hostname} recieved fetch request {req.rpc} at {self.env.now}')
            key = req.data['obj']
            size = self.peek(key)
            resp = Request(time=self.env.now,
                        req_id= req.reqid, src=self.out_port.ip, sport=self.port,
                        dst = req.src, dport= req.sport,
                        rpc = 'cache_response_data', data = {'obj': key, 'size' : size, 'status': 'hit'})\
                                if size else\
                                Request(time=self.env.now,
                                        req_id= req.reqid, src=self.out_port.ip, sport=self.port,
                                        dst = req.src, dport= req.sport,
                                        rpc = 'cache_response_data', data = {'obj': key, 'size': 0, 'status': 'miss'})
            self.out_port.put(resp)



    def insert(self, obj):
        # insert object into the cache 
        self.cache[obj.name] = obj
        pass

    def peek(self, key):
        if key in self.cache:
            obj = self.cache[key]
            print(f'{self.hostname}: request for Cache object {obj.name} with size {obj.size} hit in cache at {self.env.now}')
            return obj.size
        return 0




class CacheController:
    def __init__(self, env):
        self.env = env
        self.stages = {'0': Stage(env)}
        self.dependencies = {}

    def get_ready_stages(self):
        return self.stages

    def get_ready_tasks(self):
        return self.stages['0'].tasks



class Storage:
    def __init__(self, env, name, ip, port, nic_rate, gateway, storage_rate):
        self.metadata = None
        self.env = env
        self.name = name
        self.ip = ip
        self.port = port
        self.storage_rate = storage_rate
        self.task_queue = simpy.Store(env)
        self.nic = NetworkInterface(env, name=name, ip=ip, rate=nic_rate, gateway=gateway)
        self.nic.add_flow(port, self)
        executor = env.process(self.run())


    def load_metadata(self, fpath):
        self.metadata = pd.read_csv(fpath, index_col='fname')
        self.metadata['size'] = self.metadata['size']*1024*1024 # assign sizes


    def run(self):
        while True:
            req = yield self.task_queue.get()
            print(f'Storage {self.name} recieved message {req.rpc} at {self.env.now}')
            if req.rpc == 'fetch_data':
                obj = req.data['obj']
                obj_size = int(self.metadata.loc[obj]['size'])
                fetch_time = round(obj_size/self.storage_rate, 2)
                yield self.env.timeout(fetch_time) # need 5 minutes to fuel the tank
                print(f'Storage {self.name} fetched object {obj} with size {obj_size} and fetch time {fetch_time} at {self.env.now}')
                resp = Request(time=self.env.now,
                        req_id= req.reqid, src=self.ip, sport=self.port,
                        dst = req.src, dport= req.sport,
                        rpc = 'response_data', data = {'obj': obj, 'size' : obj_size})
                self.nic.put(resp)


    def put(self, req):
        return self.task_queue.put(req)

