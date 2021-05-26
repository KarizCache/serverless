#!/usr/bin/python3

import simpy
import itertools
from colorama import Fore, Style
from uhashring import HashRing
import random

import time
import datetime
import timeit
import json
import os

from graphviz import Digraph
import matplotlib.colors as mcolors


class Scheduler(object):
    def __init__(self, env, cluster, configs):
        self.env = env
        self.job_queue = simpy.Store(env)
        self.task_queue = simpy.Store(env)
        self.cluster = cluster
        env.process(self.schedule_job())
        env.process(self.schedule_task())
        self.workers_it = itertools.cycle(cluster.get_workers())
        self.hash_ring = HashRing(nodes=list(cluster.get_workers()))
        self.color2worker_map = {}
        self.event_to_task = {}
        self.cache_controller = {}
        self.policy = configs['cluster']['scheduling']
        self.logdir = configs['benchmark']['logdir']
        self.stats_fpath = configs['benchmark']['statistics']
        self.transmit_time = 0
        self.cpu_time = 0
        self.local_read = 0
        self.remote_read = 0
        self.deser_time = 0
        self.ser_time = 0 
        self.task_time = 0
        self.stats = {'tasks': [], 
                'task_time': 0, 'ser_time': 0, 'deser_time': 0, 
                'remote_read': 0, 'local_read': 0, 
                'cpu_time': 0, 'transmit_time': 0}


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
            #yield self.env.timeout(ts.schedule_delay)
            #print(f'{Fore.LIGHTYELLOW_EX}Task scheduler picks {ts.id} w/ scheduler delay {ts.schedule_delay} to execute at {self.env.now} {Style.RESET_ALL}')
            self.submit_task(ts)


    def task_finished_cb(self, event):
        # mark task as completed
        task = self.event_to_task[event]
        task.set_status('finished')
        self.stats['transmit_time'] += event.value['transfer']
        self.stats['cpu_time'] += event.value['cpu_time']
        self.stats['remote_read'] += event.value['remote_read']
        self.stats['local_read'] += event.value['local_read']
        self.stats['deser_time'] += event.value['deserialization_time']
        self.stats['ser_time'] += event.value['serialization_time']
        self.stats['task_time'] += event.value['task_endtoend_delay'] 
        #print(f'{Fore.LIGHTBLUE_EX}Task {task.id} is finished at {self.env.now}.{Style.RESET_ALL}')
        sched_start_time = timeit.default_timer()
        next_tasks = task.job.get_next_tasks(task)
        sched_finish_time = timeit.default_timer()
        self.stats['tasks'].append(event.value)

        schedule_delay = sched_finish_time - sched_start_time
        # send next joba to the sescheduler 
        for t in next_tasks:
            t.set_schedule_delay(schedule_delay)
            self.task_queue.put(t)


    def execute_job(self, job):
        start_time = self.env.now
        job.optimal_placement()

        completions = job.get_task_completions()
        tasks = job.get_next_tasks()

        for t in tasks:
            self.task_queue.put(t)
        
        # wait for all tasks of this job to be done
        yield simpy.events.AllOf(self.env, completions)
        #print(f'{Fore.LIGHTRED_EX}Job {job} is finished at {self.env.now}, executin time: {self.env.now - start_time}, remote read: {self.stats["remote_read"]}, local_read: {self.stats["local_read"]}, transfer time: {self.stats["transmit_time"]}, cput_time: {self.stats["cpu_time"]} deserializaion time: {self.stats["deser_time"]}, serialization time: {self.stats["ser_time"]} {Style.RESET_ALL}')
        
        self.stats['execution_time'] = self.env.now - start_time

        print(f'{Fore.LIGHTRED_EX}Job {job} is finished,executin time,remote read,local_read,transfer time,cput_time,deserializaion time,serialization time,task time {Style.RESET_ALL}')
        print(f'{Fore.LIGHTRED_EX}Job {job} is finished at {self.env.now},{self.env.now - start_time},{self.stats["remote_read"]},{self.stats["local_read"]},{self.stats["transmit_time"]},{self.stats["cpu_time"]},{self.stats["deser_time"]},{self.stats["ser_time"]},{self.stats["task_time"]} {Style.RESET_ALL}')
        
        with open(f'{os.path.join(self.logdir, job.name)}.log', 'w') as fd:
            fd.write(json.dumps(self.stats))

        with open(self.stats_fpath, 'a') as fd:
            fd.write(f'{job.name},{self.policy},{self.env.now - start_time},{self.stats["remote_read"]},{self.stats["local_read"]},{self.stats["transmit_time"]},{self.stats["cpu_time"]},{self.stats["deser_time"]},{self.stats["ser_time"]},{self.stats["task_time"]}\n')

        # plot the graph
        self.plot_graph(job);



    def plot_graph(self, job):
        worker_color = {'10.255.23.108': '#e41a1c',
                '10.255.23.109': '#984ea3',
                '10.255.23.110': '#ff7f00',
                '10.255.23.115': '#4daf4a'}
        css_colors = list(mcolors.CSS4_COLORS.keys())

        dg = Digraph('G', filename=f'{job.name}.{self.policy}.gv', format='png')
        for v in job.g.vertices():
            #print(job.g.vp.tasks[v].color)
            #dg.attr('node', shape='ellipse', style='filled', color=job.g.vp.tasks[v].color)
            dg.attr('node', shape='ellipse', style="filled,solid",
                    penwidth="3",
                    fillcolor=mcolors.CSS4_COLORS[css_colors[int(job.g.vp.tasks[v].color)]] if 'chain_color' in self.policy else '#f0f0f0' ,
                    color=worker_color[job.g.vp.tasks[v].worker])

            color = '-'
            if 'chain_color' in self.policy:
                color = job.g.vp.tasks[v].color
            dg.node(f'{v}, color({color})')
        for e in job.g.edges():
            dg.edge(f'{e.source()}, color({job.g.vp.tasks[e.source()].color if "chain_color" in self.policy else "-"})',
            
                    f'{e.target()}, color({job.g.vp.tasks[e.target()].color if "chain_color" in self.policy else "-"})')
        dg.view(f'{os.path.join(self.logdir,job.name)}.{self.policy}', quiet=False)




    def decide_worker(self, task=None):
        # lets do the round robin for now
        if self.policy == 'round_robin':
            return next(self.workers_it)
        if self.policy == 'random':
            return random.choice(list(self.cluster.get_workers()))
        if self.policy == 'consistent_hash':
            assert(task)
            return self.hash_ring.get_node(task.obj.name)
        if self.policy == 'chain_color_ch':
            assert(task)
            #print(f'task {Fore.YELLOW}{task.name}{Style.RESET_ALL}, the color is {Fore.LIGHTYELLOW_EX}{task.color}{Style.RESET_ALL}')
            return self.hash_ring.get_node(task.color)
        if self.policy == 'chain_color_rr':
            assert(task)
            #print(f'task {Fore.YELLOW}{task.name}{Style.RESET_ALL}, the color is {Fore.LIGHTYELLOW_EX}{task.color}{Style.RESET_ALL}')
            if task.color not in self.color2worker_map:
                self.color2worker_map[task.color] = next(self.workers_it)
            return  self.color2worker_map[task.color]
        if self.policy == 'optimal':
            return task.optimal_placement;
        if self.policy == 'vanilla':
            return task.vanilla_placement;
        if self.policy == 'manias':
            pass
        raise NameError('The scheduler is not supported')



    def submit_task(self, task):
        w = self.decide_worker() if not (self.policy in ['consistent_hash' , 'chain_color_ch', 'chain_color_rr', 'optimal', 'vanilla']) else self.decide_worker(task)
        if task.name != 'NOP': 
            task.obj.who_has = w # set the current worker as the owner of this task. 
            self.event_to_task[task.completion_event] = task
            task.completion_event.callbacks.append(self.task_finished_cb)
            self.cache_controller[task.obj] = w
        self.cluster.submit_task(w, task)
