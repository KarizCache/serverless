#!/usr/bin/python3
import random
import queue
import simpy
import pandas as pd
import utils
from netsim import Request, NetworkInterface
from colorama import Fore, Style

class Object:
    def __init__(self, name, size):
        self.color = 0 # The color range is 0x000000-0xFFFFFF
        self.size = size
        self.name = name
        self.who_has = ''

    def __repr__(self):
        return f'{self.name}:{self.size}'

    def __str__(self):
        return f'{self.name}:{self.size}'


class Cache:
    def __init__(self, env, size, policy, port, hostname=None):
        self.env = env
        self.eviction_policy = policy # This could be LRU, LRU, Fifo. Lets go with Fifo
        self.hostname = hostname
        self.size = size
        self.queue = queue.Queue(size)
        self.req_queue = simpy.Store(env)
        self.cache = {}
        self.outstanding = {} # list of requests that are waiting for this object
        self.serialization_latency = utils.fit_serialization()
        self.deserialization_latency = utils.fit_deserialization()
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
            size = yield self.env.process(self.peek(key))
            rpc = 'localcache_response_data' if req.src == self.out_port.ip else 'cache_response_data'
            resp = Request(time=self.env.now,
                        req_id= req.reqid, src=self.out_port.ip, sport=self.port,
                        dst = req.src, dport= req.sport,
                        rpc = rpc, data = {'obj': key, 'size' : size, 'status': 'hit'})\
                                if size else\
                                Request(time=self.env.now,
                                        req_id= req.reqid, src=self.out_port.ip, sport=self.port,
                                        dst = req.src, dport= req.sport,
                                        rpc = rpc, data = {'obj': key, 'size': 0, 'status': 'miss'})
            self.out_port.put(resp)


    def insert(self, obj):
        ser_latency = self.serialization_latency(obj.size)
        #print(f'Cache insertion for obj {obj.name} at {obj.size} serialization cost: {ser_latency}')
        self.outstanding[obj.name] = self.env.event() 
        yield self.env.timeout(ser_latency)
        self.outstanding[obj.name].succeed(value={'size': obj.size})
        
        # insert object into the cache 
        self.cache[obj.name] = obj
        del self.outstanding[obj.name]


    def peek(self, key):
        size = 0
        if key in self.outstanding:
            #print(f'Wait for {key} to be serialized in the cache')
            yield self.outstanding[key]
        if key in self.cache:
            obj = self.cache[key]
            #print(f'{Fore.RED} Obj {obj.name}:{obj.size} exist in the cache {Style.RESET_ALL}')
            size = obj.size
        return size



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
            #print(f'Storage {self.name} recieved message {req.rpc} at {self.env.now}')
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

