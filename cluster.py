#!/usr/bin/python3


from job import Job
from netsim import Router, Request, NetworkInterface
import yaml
import json
import simpy
import itertools
from storage import Storage, Cache, Object
from colorama import Fore, Style

class Executor(object):
    def __init__(self, env, ip, port, localcache, storage_host, hostname=None, debug=False):
        self.request_id = 1
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
        self.localcache = localcache

        
    def put(self, msg):
        # print(f'{Fore.YELLOW}Executor {self.hostname}:{self.port} recieved message {msg} at {self.env.now} {Style.RESET_ALL}')
        return self.msg_queue.put(msg)

    def data_plane(self):
        while True:
            msg = yield self.msg_queue.get()
            if msg.rpc == 'cache_response_data' and msg.data['status'] == 'hit':
                #print(f'{Fore.LIGHTMAGENTA_EX}Executor {self.hostname}:{self.port} read {msg.data["obj"]} with size {msg.data["size"]} from {msg.src}:{msg.sport},{msg.reqid} at {self.env.now} {Style.RESET_ALL}')
                self.outstanding[msg.reqid].succeed(msg.data['size'])
                del self.outstanding[msg.reqid]


    def submit(self, task):
        if self.debug:
            print(f'Submit task {task} to executor {self.hostname}:{self.port} at {self.env.now}')
        yield self.task_queue.put(task)
        return


    def control_plane(self):
        while True:
            task = yield self.task_queue.get()
            execute_proc = self.env.process(self.execute_function(task))
            yield execute_proc


    def execute_function(self, task):
        start = self.env.now
        data_size = 0
        for obj in task.inputs:
            if self.ip == obj.who_has.split(':')[0]:
                # The data should be found in the local cache 
                # just increase the hit ratio
                #print(f'{Fore.LIGHTBLUE_EX}Local cache request for {obj.name} {Style.RESET_ALL}')
                data_size += self.localcache.peek(obj.name)

            else:
                # send it over network
                req = Request(time=self.env.now,
                        req_id= self.request_id, src=self.ip, sport=self.port,
                        dst = obj.who_has.split(':')[0], dport= int(obj.who_has.split(':')[1]),
                        rpc = 'fetch_data', data = {'obj': obj.name})
                #print(f'{Fore.LIGHTRED_EX}Executor {self.hostname}:{self.port} request for {req.data["obj"]},{req.reqid} from {req.dst}:{req.dport} at {self.env.now} {Style.RESET_ALL}')
                self.outstanding[self.request_id] = self.env.event() 
                self.out_port.put(req)
            self.request_id += 1
        yield simpy.events.AllOf(self.env, self.outstanding.values())

        for eve in self.outstanding:
            data_size += self.outstanding[eve].value

        #process data
        yield self.env.timeout(task.exec_time)

        # write data
        if self.debug:
            print(f'{Fore.GREEN}Executor {self.hostname}:{self.port} writes data for {task.id} at {self.env.now} {Style.RESET_ALL}')
        self.mem_port.insert(task.obj)

        # notify the completion of this task
        print(f'{Fore.LIGHTYELLOW_EX}Executor {self.hostname}:{self.port} lunches task {task.id} at {start} and ends at {self.env.now}, execution time: {self.env.now - start} {Style.RESET_ALL}')
        task.completion_event.succeed()


class Worker:
    def __init__(self, env, name, ip, rate, executors, gateway, storage_host, memsize, cache_policy, cache_port):
        self.hostname = name
        self.env = env;
        self.n_exec = executors
        self.nic = NetworkInterface(env, name=name, ip=ip, rate=rate, gateway=gateway)

        self.cache = Cache(env=env, size=memsize, policy=cache_policy, port=cache_port, hostname=self.hostname)
        self.nic.add_flow(self.cache.port, self.cache)
        self.cache.out_port = self.nic

        self.executors = {}
        for i in range(executors):
            self.executors[i] = Executor(env, hostname=name, ip=ip, port=5000+i, localcache=self.cache, storage_host=storage_host, debug=False)
            self.executors[i].out_port = self.nic
            self.nic.add_flow(self.executors[i].port, self.executors[i])
            self.executors[i].mem_port = self.cache 
        self.exec_it = itertools.cycle(self.executors.keys()) 
        pass


    def submit_task(self, task):
        task.obj.who_has = f'{self.nic.ip}:{self.cache.port}'
        self.env.process(self.executors[next(self.exec_it)].submit(task))


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
                    worker = Worker(env = env, name=name, ip=node['ip'], rate=node['rate'], executors=node['executors'], 
                            memsize=node['memory'], gateway=node['gateway'], 
                            storage_host=node['storage'], cache_policy=node['cache.policy'], cache_port=node['cache.port'])
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





