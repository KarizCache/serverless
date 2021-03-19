#!/usr/bin/python3

import simpy
import itertools
from colorama import Fore, Style
from uhashring import HashRing
import random

import time
import datetime
import timeit


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
        self.policy = 'chain_color'
        self.transmit_time = 0
        self.cpu_time = 0
        self.local_read = 0
        self.remote_read = 0
        self.deser_time = 0



    def put(self, job):
        return self.job_queue.put(job)



    def schedule_job(self):
        while True:
            job = yield self.job_queue.get()
            #print(f'{Fore.GREEN} Scheduler got {job} at {self.env.now} {Style.RESET_ALL}')
            execute_proc = self.env.process(self.execute_job(job))



    def schedule_task(self):
        while True:
            ts = yield self.task_queue.get()
            yield self.env.timeout(ts.schedule_delay)
            #print(f'{Fore.LIGHTYELLOW_EX}Task scheduler picks {ts.id} w/ scheduler delay {ts.schedule_delay} to execute at {self.env.now} {Style.RESET_ALL}')
            self.submit_task(ts)



    def task_finished_cb(self, event):
        # mark task as completed
        task = self.event_to_task[event]
        task.set_status('finished')
        self.transmit_time += event.value['transfer']
        self.cpu_time += event.value['cpu_time']
        self.remote_read += event.value['remote_read']
        self.local_read += event.value['local_read']
        self.deser_time += event.value['deserialization_time']
        #print(f'{Fore.LIGHTBLUE_EX}Task {task.id} is finished at {self.env.now}.{Style.RESET_ALL}')
        sched_start_time = timeit.default_timer()
        next_tasks = task.job.get_next_tasks(task)
        sched_finish_time = timeit.default_timer()

        event = self.env.event()
        schedule_delay = sched_finish_time - sched_start_time
        # send next joba to the sescheduler 
        for t in next_tasks:
            t.set_schedule_delay(schedule_delay)
            self.task_queue.put(t)


    def execute_job(self, job):
        start_time = self.env.now
        completions = job.get_task_completions()
        tasks = job.get_next_tasks()

        for t in tasks:
            self.task_queue.put(t)
        
        # wait for all tasks of this job to be done
        yield simpy.events.AllOf(self.env, completions)
        print(f'{Fore.LIGHTRED_EX}Job {job} is finished at {self.env.now}, executin time: {self.env.now - start_time}, remote read: {self.remote_read}, local_read: {self.local_read}, transfer time: {self.transmit_time}, cput_time: {self.cpu_time} deserializaion time: {self.deser_time} {Style.RESET_ALL}')



    def decide_worker(self, task=None):
        # lets do the round robin for now
        if self.policy == 'round_robin':
            return next(self.workers_it)
        if self.policy == 'random':
            return random.choice(list(self.cluster.get_workers()))
        if self.policy == 'consistent_hash':
            assert(task)
            return self.hash_ring.get_node(task.obj.name)
        if self.policy == 'chain_color':
            assert(task)
            return self.hash_ring.get_node(task.color)
        if self.policy == 'manias':
            pass



    def submit_task(self, task):
        w = self.decide_worker() if not (self.policy == 'consistent_hash' or self.policy=='chain_color') else self.decide_worker(task)
        task.obj.who_has = w
        self.event_to_task[task.completion_event] = task
        task.completion_event.callbacks.append(self.task_finished_cb)
        self.cluster.submit_task(w, task)
        self.cache_controller[task.obj] = w


