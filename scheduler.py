#!/usr/bin/python3

import simpy
import itertools
from colorama import Fore, Style
from uhashring import HashRing
import random

class Scheduler(object):
    def __init__(self, env, cluster):
        self.env = env
        self.job_queue = simpy.Store(env)
        self.task_queue = simpy.Store(env)
        self.cluster = cluster
        env.process(self.schedule_job())
        env.process(self.schedule_task())
        self.workers_it = itertools.cycle(cluster.get_workers())
        self.hash_ring = HashRing(nodes=list(cluster.get_workers()))
        self.event_to_task = {}
        self.cache_controller = {}
        self.policy = 'consistent_hash'
        self.transmit_time = 0
        self.cpu_time = 0
        self.local_read = 0
        self.remote_read = 0


    def put(self, job):
        return self.job_queue.put(job)


    def schedule_job(self):
        while True:
            job = yield self.job_queue.get()
            print(f'{Fore.GREEN} Scheduler got {job} at {self.env.now} {Style.RESET_ALL}')
            execute_proc = self.env.process(self.execute_job(job))


    def schedule_task(self):
        while True:
            task = yield self.task_queue.get()
            #print(f'{Fore.LIGHTYELLOW_EX}Task scheduler picks {task.id} to execute at {self.env.now} {Style.RESET_ALL}')
            self.submit_task(task)


    def task_finished_cb(self, event):
        # mark task as completed
        task = self.event_to_task[event]
        task.set_status('finished')
        self.transmit_time += event.value['transfer']
        self.cpu_time += event.value['cpu_time']
        self.remote_read += event.value['remote_read']
        self.local_read += event.value['local_read']
        #print(f'{Fore.LIGHTBLUE_EX}Task {task.id} is finished at {self.env.now}.{Style.RESET_ALL}')
        next_tasks = task.job.get_next_tasks(task)
        # send next joba to the sescheduler 
        for t in next_tasks:
            self.task_queue.put(t)



    def execute_job(self, job):
        start_time = self.env.now
        completions = job.get_task_completions()
        tasks = job.get_next_tasks()

        for t in tasks:
            self.task_queue.put(t)
        
        # wait for all tasks of this job to be done
        yield simpy.events.AllOf(self.env, completions)
        print(f'{Fore.LIGHTRED_EX}Job {job} is finished at {self.env.now}, executin time: {self.env.now - start_time}, remote read: {self.remote_read}, local_read: {self.local_read}, transfer time: {self.transmit_time}, cput_time: {self.cpu_time} {Style.RESET_ALL}')


    def decide_worker(self, task=None):
        # lets do the round robin for now
        print(task)
        if self.policy == 'round_robin':
            return next(self.workers_it)
        if self.policy == 'random':
            return random.choice(self.workers)
        if self.policy == 'consistent_hash':
            assert(task)
            return self.hash_ring.get_node(task.obj.name)
        if self.policy == 'rodrigos':
            pass
        if self.policy == 'manias':
            pass



    def submit_task(self, task):
        w = self.decide_worker() if not self.policy == 'consistent_hash' else self.decide_worker(task)
        task.obj.who_has = w
        self.event_to_task[task.completion_event] = task
        task.completion_event.callbacks.append(self.task_finished_cb)
        self.cluster.submit_task(w, task)
        self.cache_controller[task.obj] = w


