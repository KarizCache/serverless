#!/usr/bin/python3


from job import Job
from netsim import Router, Request, NetworkInterface
import yaml
import json
import simpy
import itertools
import pandas as pd
from colorama import Fore, Style
from storage import Cache, Object

class Executor(object):
    def __init__(self, env, ip, port, storage_host, hostname=None, debug=False):
        self.env = env
        self.debug = debug
        self.ip = ip
        self.port = port
        self.hostname = hostname or ip
        self.storage_ip, self.storage_port = storage_host.split(':')
        self.storage_port = int(self.storage_port)

        self.mem_port = None
        
        self.out_port = None
        self.request_id = 0
        
        self.task_queue = simpy.Store(env)
        env.process(self.control_plane())
        
        self.msg_queue = simpy.Store(env)
        env.process(self.data_plane())

        self.outstanding = {}

        
    def put(self, msg):
        print(f'{Fore.YELLOW} Executor {self.hostname}:{self.port} recieved message {msg} at {self.env.now} {Style.RESET_ALL}')
        return self.msg_queue.put(msg)

    def data_plane(self):
        while True:
            msg = yield self.msg_queue.get()
            print(f'{Fore.MAGENTA} Executor {self.hostname}:{self.port} recieved message {msg} at {self.env.now} {Style.RESET_ALL}')
            self.outstanding[msg.reqid].succeed(msg.data['size'])


    def submit(self, task):
        if self.debug:
            print(f'Submit task {task} to executor {self.hostname}:{self.port} at {self.env.now}')
        yield self.task_queue.put(task)
        return


    def control_plane(self):
        while True:
            task = yield self.task_queue.get()
            print(f'Executor {self.hostname}:{self.port} lunches task {task} at {self.env.now}')
            execute_proc = self.env.process(self.execute_function(task))


    def execute_function(self, task):
        print(f'Executor {self.hostname}:{self.port} reads data for {task.id} at {self.env.now} {Style.RESET_ALL}')
        self.request_id = 1
        for obj in task.inputs:
            req = Request(time=self.env.now,
                    req_id= self.request_id, src=self.ip, sport=self.port, 
                    dst = self.storage_ip, dport= self.storage_port,
                    rpc = 'fetch_data', data = {'obj': obj['name']})
            self.out_port.put(req)
            self.outstanding[self.request_id] = self.env.event() 
            self.request_id += 1
        yield simpy.events.AllOf(self.env, self.outstanding.values())

        data_size = 0
        for eve in self.outstanding:
            data_size += self.outstanding[eve].value

        #process data
        print(f'{Fore.MAGENTA} Executor {self.hostname}:{self.port} executes the task {task.id} with data size {data_size} at {self.env.now} {Style.RESET_ALL}')
        yield self.env.timeout(task.exec_time)

        # write data
        print(f'{Fore.MAGENTA} Executor {self.hostname}:{self.port} writes data for {task.id} at {self.env.now} {Style.RESET_ALL}')
        self.mem_port.put(task.obj_out)
        yield self.env.timeout(5)

        # notify the completion of this task
        task.completion_event.succeed()


class Worker:
    def __init__(self, env, name, ip, rate, executors, gateway, storage_host, memsize, cache_policy):
        self.hostname = name
        self.env = env;
        self.n_exec = executors
        self.nic = NetworkInterface(env, name=name, ip=ip, rate=rate, gateway=gateway)
        self.cache = Cache(env=env, size=memsize, policy=cache_policy)
        self.executors = {}
        for i in range(executors):
            self.executors[i] = Executor(env, hostname=name, ip=ip, port=5000+i, storage_host=storage_host, debug=False)
            self.executors[i].out_port = self.nic
            self.nic.add_flow(self.executors[i].port, self.executors[i])
            self.executors[i].mem_port = self.cache 
        self.exec_it = itertools.cycle(self.executors.keys()) 
        pass


    def submit_task(self, task):
        self.env.process(self.executors[next(self.exec_it)].submit(task))


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



class Cluster:
    def __init__(self, env, topology):
        self.env = env
        self.workers = {}
        self.routers = {}
        self.storages = {}
        self.deploy_cluster(env, topology)

    def deploy_cluster(self, env, fpath):
        try:
            data = yaml.load(open(fpath, 'r'), Loader=yaml.FullLoader)
            topology = data['topology']
            ''' Add nodes '''
            for name in topology:
                node = topology[name]
                name = node['name']
                if node['type'] == 'worker':
                    print(node)
                    worker = Worker(env = env, name=name, ip=node['ip'], rate=node['rate'], executors=node['executors'], 
                            memsize=node['memory'], gateway=node['gateway'], storage_host=node['storage'], cache_policy=node['cache.policy'])
                    self.workers[name] = worker
                elif node['type'] == 'router':
                    router = Router(env=env, name=name, ip=node['ip'], ports=node['ports'], rate=node['rate'], gateway=node['gateway'], debug=False)
                    self.routers[name] = router
                elif node['type'] == 'storage':
                    storage = Storage(env, name=name, ip=node['ip'], port=node['port'], nic_rate=node['rate'], gateway=node['gateway'], storage_rate=node['storage_rate'])
                    storage.load_metadata(node['metadata'])
                    self.storages[name] = storage

            ''' connect nodes '''
            for name in self.workers:
                gateway = self.workers[name].nic.gateway
                self.workers[name].nic.out = self.routers[gateway]
                self.routers[gateway].connect(self.workers[name].nic)
            for name in self.routers:
                router = self.routers[name]
                if router.gateway == 'None':
                    continue
                gateway = self.routers[self.routers[name].gateway]
                gateway.connect(router)
                router.connect(gateway, gateway=True)
                gateway.add_route(router.ip)
            for name in self.storages:
                storage = self.storages[name]
                gateway = self.routers[storage.nic.gateway]
                storage.nic.out = gateway
                gateway.connect(storage.nic)
        except:
            raise  

    def worker_count(self):
        return len(self.workers)

    def get_workers(self):
        return self.workers.keys()

    def submit_task(self, wid, task):
        self.workers[wid].submit_task(task)





