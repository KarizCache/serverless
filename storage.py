#!/usr/bin/python3
import random
import queue


class Object:
    def __init__(self, name, size):
        self.color = 0 # The color range is 0x000000-0xFFFFFF
        self.size = size
        self.name = name


class Cache:
    def __init__(self, env, size, policy):
        self.eviction_policy = policy # This could be LRU, LRU, Fifo. Lets go with Fifo
        self.size = size
        self.queue = queue.Queue(size)
        self.metadata = {}


    def put(self, obj):
        self.fifo_admit(obj)


    def fifo_admit(self, obj):
        if self.queue.full():
           yield self.env.process(self.fifo_evict()) 
        self.queu.put(obj)


    def fifo_evict(self):
        if self.queue.empty(): return
        return self.queue.get()



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
            print(f'Storage {self.name} recieved message {req} at {self.env.now}')
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

