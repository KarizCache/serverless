#!/usr/bin/python3


from job import Job
from netsim import Router, Request, NetworkInterface
import utils 
import yaml
import json
import simpy
import itertools
import queue
from storage import Storage, Cache, Object
from colorama import Fore, Style


class CPU(object):
    def __init__(self, env, hostname):
        self.env = env
        self.slot = 1
        self.hostname = hostname
        self.proc_queue = simpy.Store(env)
        #self.proc_queue = simpy.PriorityStore(env)
        self.running_tasks = {}
        env.process(self.execute())
        self.current_event = None
        pass


    def update_running_tasks_stats(self):
        # update the progress of the running tasks
        n_tasks = len(self.running_tasks)
        #print(f'{self.env.now} {Fore.GREEN} {self.hostname}: {Style.RESET_ALL} {n_tasks}')
        for key in self.running_tasks:
            ts = self.running_tasks[key]
            ts.stats.exectuion_history.append({'start': ts.stats.cur_exec_rate_start, 'end': self.env.now, 'n_tasks': n_tasks, 'progress': ts.stats.progress})
            coretime_progress = round((self.env.now - ts.stats.cur_exec_rate_start)/n_tasks, 6)
            ts.stats.progress += coretime_progress;


    def update_task_time(self):
        n_tasks = len(self.running_tasks)
        for key in self.running_tasks:
            ts = self.running_tasks[key]
            ts.stats.cur_exec_rate_start = self.env.now
            ts.stats.estimated_finish_time = self.env.now + (ts.exec_time - ts.stats.progress)*n_tasks;

        # get the task with the minimum estimated finish time to set the timer for. 
        if not len(self.running_tasks): return None
        return min(self.running_tasks.values())

    def put(self, task):
        #print(f'task {task} is recieved by {self.hostname} at {self.env.now} ')
        self.update_running_tasks_stats();

        # update the estimated finish time
        self.running_tasks[task] = task

        ts_next = self.update_task_time()
        if not ts_next: return
        if self.current_event and self.current_event.triggered:
            # Interrupt charging if not already done.
            self.current_event.interrupt('Need to go!')
        # cancel the previous event.
        return self.proc_queue.put(ts_next)

    def execute(self):
        while True:
            ts = yield self.proc_queue.get()
            if len(self.running_tasks):
                ts = min(self.running_tasks.values())
            try:
                #print(f'{Fore.BLUE} {self.env.now}: {Fore.GREEN} {self.hostname} {Style.RESET_ALL} has scheduled task {Fore.YELLOW} {ts} {Style.RESET_ALL} to be finished at {ts.stats.estimated_finish_time}')
                self.current_event = yield self.env.timeout(abs(ts.stats.estimated_finish_time - self.env.now))

                ts.computation_completion_event.succeed()
                self.update_running_tasks_stats()
                del self.running_tasks[ts]
                #ts_next = self.update_task_time()
                self.update_task_time()
                #if ts_next:
                #    self.proc_queue.put(ts_next)
            except simpy.Interrupt as i:
                print('Bat. ctrl. interrupted at', env.now, 'msg:', i.cause)


class Executor(object):
    def __init__(self, env, ip, port, localcache, storage_host, serialization, hostname=None, debug=False):
        self.request_id = 1
        self.env = env
        self.debug = debug
        self.ip = ip
        self.port = port
        self.hostname = hostname or ip
        self.storage_ip, self.storage_port = storage_host.split(':')
        self.storage_port = int(self.storage_port)

        self.cpu = CPU(env, hostname)
        self.mem_port = None
        
        self.out_port = None
        self.request_id = 0
        
        self.task_queue = simpy.Store(env)
        env.process(self.control_plane())
        
        self.msg_queue = simpy.Store(env)
        env.process(self.data_plane())

        self.outstanding = {}
        self.timeoutstanding = {}
        self.localcache = localcache
        self.transmit_time = 0
        self.compute_time = 0

        if serialization not in ['lazy', 'syncwdeser', 'syncnodeser']:
            raise NameError(f'Serialization type: {serialization} is not supported.')
        
        self.serialization_policy = serialization # lazy syncwdeser syncnodeser
        self.deserialization_latency = utils.fit_deserialization();

        self.active_task = []
        pass

        
    def put(self, msg):
        #print(f'{Fore.YELLOW}Executor {self.hostname}:{self.port} recieved message {msg} at {self.env.now} {Style.RESET_ALL}')
        return self.msg_queue.put(msg)

    def data_plane(self):
        while True:
            msg = yield self.msg_queue.get()
            transfer_start = self.timeoutstanding[msg.reqid]
            transfer_stop = self.env.now
            #print(f'{Fore.WHITE}Executor {self.hostname}:{self.port} read {msg} at {self.env.now} {Style.RESET_ALL}')
            if msg.rpc == 'cache_response_data' and msg.data['status'] == 'hit':
                _type = 'remote'
                deser_latency = self.deserialization_latency(msg.data['size']) 
                yield self.env.timeout(deser_latency)
                #print(f'{Fore.LIGHTMAGENTA_EX}Executor {self.hostname}:{self.port} read {msg.data["obj"]} with size {msg.data["size"]} from {msg.src}:{msg.sport},{msg.reqid} at {self.env.now} {Style.RESET_ALL}')
            elif msg.rpc == 'localcache_response_data':
                _type = 'local' 
                deser_latency = 0
                if self.serialization_policy == 'syncwdeser':
                    deser_latency = self.deserialization_latency(msg.data['size']) 
                    yield self.env.timeout(deser_latency)
                #print(f'{Fore.LIGHTGREEN_EX}Executor {self.hostname}:{self.port} read {msg.data["obj"]} with size {msg.data["size"]} from local cache at {self.env.now} {Style.RESET_ALL}')
            self.outstanding[msg.reqid].succeed(value={'size': msg.data['size'], 'transfer_time': transfer_stop - transfer_start, 'type': _type, 'deserialization_time': deser_latency, 'ser_wait_time': msg.data['ser_wait']})


    def submit(self, task):
        debug=False
        if debug:
            print(f'Submit task {task} to executor {self.hostname}:{self.port} at {self.env.now}')
        yield self.task_queue.put(task)


    def control_plane(self):
        while True:
            task = yield self.task_queue.get()
            #print(f'task {Fore.YELLOW} {task.name} {Fore.WHITE} is executed on {Fore.GREEN}{self.hostname} {Fore.WHITE} for {Fore.RED}{task.exec_time} at {Fore.BLUE} {self.env.now} {Style.RESET_ALL}')
            execute_proc = self.env.process(self.execute_function(task))
            #yield execute_proc


    def execute_function(self, task):
        data_size = 0
        local_read = 0
        remote_read = 0
        outstanding = {}

        # incorporate the scheduling delay here:
        yield self.env.timeout(task.schedule_delay)

        start = self.env.now 
        #print(f'at {self.hostname} inputs are', task.inputs)
        for obj in task.inputs:
            self.outstanding[self.request_id] = self.env.event()
            outstanding[self.request_id] = self.outstanding[self.request_id]  
            self.timeoutstanding[self.request_id] = self.env.now
            if self.ip == obj.who_has.split(':')[0]:
                # The data should be found in the local cache 
                # just increase the hit ratio and pay for the deserialization on the read
                #print(f'{Fore.LIGHTGREEN_EX}Local cache request for {obj.name} {Style.RESET_ALL}')
                req = Request(time=self.env.now,
                        req_id= self.request_id, src=self.ip, sport=self.port,
                        dst = self.ip, dport= int(self.port),
                        rpc = 'fetch_from_local_cache', data = {'obj': obj.name})
                self.localcache.put(req)
            else:
                # send it over network
                req = Request(time=self.env.now,
                        req_id= self.request_id, src=self.ip, sport=self.port,
                        dst = obj.who_has.split(':')[0], dport= int(obj.who_has.split(':')[1]),
                        rpc = 'fetch_data', data = {'obj': obj.name})
                #print(f'{Fore.LIGHTRED_EX}Executor {self.hostname}:{self.port} request for {req.data["obj"]},{req.reqid} from {req.dst}:{req.dport} at {self.env.now} {Style.RESET_ALL}')
                #self.outstanding[self.request_id] = self.env.event()
                #self.timeoutstanding[self.request_id] = self.env.now
                self.out_port.put(req)
            self.request_id += 1
        yield simpy.events.AllOf(self.env, outstanding.values())

        fetch_time = self.env.now - start
        transmit_time = 0
        deser_time = 0
        ser_time = 0
        #print('outstanding events are', outstanding)
        for req_id in outstanding:
            #print('event ', outstanding[req_id], 'at', self.hostname)
            val = outstanding[req_id].value
            transmit_time += self.outstanding[req_id].value['transfer_time']
            ser_time += self.outstanding[req_id].value['ser_wait_time']
            data_size += self.outstanding[req_id].value['size']
            if self.outstanding[req_id].value['type'] == 'remote':
                remote_read += self.outstanding[req_id].value['size']
            else:
                local_read += self.outstanding[req_id].value['size']
            deser_time += self.outstanding[req_id].value['deserialization_time']
            del self.outstanding[req_id]

        #process data
        self.cpu.put(task)
        yield simpy.events.AllOf(self.env, [task.computation_completion_event])

        # write data
        debug=False
        if debug:
            print(f'{Fore.GREEN}Executor {self.hostname}:{self.port} writes object {task.obj} for {task.id} at {self.env.now} at {self.mem_port} {Style.RESET_ALL}')
        
        #print(ser_time)
        serialization_delay = 0
        if task.name != 'NOP':
            serialization_start = self.env.now 
            if self.serialization_policy == 'syncwdeser' or self.serialization_policy == 'syncnodeser':
                yield self.env.process(self.mem_port.insert(task.obj))
            elif self.serialization_policy == 'lazy':
                self.env.process(self.mem_port.insert(task.obj))
            serialization_delay = self.env.now - serialization_start

        task_endtoend_time = self.env.now - start


        # notify the completion of this task
        debug=True
        if debug:
            print(f'Executor {Fore.LIGHTGREEN_EX}{self.hostname}:{self.port}{Style.RESET_ALL} lunches {Fore.LIGHTBLUE_EX}task {task} at {Fore.LIGHTYELLOW_EX}{start}{Style.RESET_ALL} and ends at {Fore.LIGHTRED_EX}{self.env.now}{Style.RESET_ALL}, execution time: {Fore.LIGHTRED_EX}{self.env.now - start}{Style.RESET_ALL}')
        task.end_ts = self.env.now
        task.completion_event.succeed(value={'name': task.name, 'transfer': transmit_time, 'cpu_time': task.exec_time, 
            'remote_read': remote_read, 'local_read': local_read, 'fetch_time': fetch_time, 'start_ts': task.stats.start_time, 'worker': task.worker, 
            'deserialization_time': deser_time, 'serialization_time': serialization_delay, 'end_ts': task.stats.end_time,
            'task_endtoend_delay': task_endtoend_time, 'write': task.obj.size if task.obj else 0, 'wait_for_serialization': ser_time})


class Worker:
    def __init__(self, env, name, ip, rate, executors, gateway, storage_host, memsize, cache_policy, cache_port, serialization):
        self.hostname = name
        self.env = env;
        self.n_exec = executors
        self.nic = NetworkInterface(env, name=name, ip=ip, rate=rate, gateway=gateway)

        self.cache = Cache(env=self.env, size=memsize, policy=cache_policy, port=cache_port, hostname=self.hostname, serialization_policy=serialization)
        self.nic.add_flow(self.cache.port, self.cache)
        self.cache.out_port = self.nic

        self.executors = {}
        for i in range(executors):
            self.executors[i] = Executor(env, hostname=name, ip=ip, port=5000+i, localcache=self.cache, storage_host=storage_host, serialization=serialization, debug=False)
            self.executors[i].out_port = self.nic
            self.nic.add_flow(self.executors[i].port, self.executors[i])
            self.executors[i].mem_port = self.cache 
        self.exec_it = itertools.cycle(self.executors.keys()) 
        pass


    def submit_task(self, task):
        if task.obj: 
            task.obj.who_has = f'{self.nic.ip}:{self.cache.port}'
        task.worker = self.nic.ip
        self.env.process(self.executors[next(self.exec_it)].submit(task))
        #self.executors[next(self.exec_it)].submit(task)


class Cluster:
    def __init__(self, env, topology):
        self.env = env
        self.workers = {}
        self.routers = {}
        self.storages = {}
        self.deploy_cluster(env, topology)


    def deploy_cluster(self, env, configs):
        try:
            serialization = configs['cluster']['serialization']
            topology = configs['topology']
            ''' Add nodes '''
            for name in topology:
                node = topology[name]
                name = node['name']
                if node['type'] == 'worker':
                    name = node['ip']
                    worker = Worker(env = env, name=name, ip=node['ip'], rate=node['rate'], executors=node['executors'], 
                            memsize=node['memory'], gateway=node['gateway'], serialization=serialization, 
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
        #print(self.workers)
        self.workers[wid].submit_task(task)

