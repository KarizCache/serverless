#!/usr/bin/python3

import simpy
import itertools
from colorama import Fore, Style



class Scheduler(object):
    def __init__(self, env, cluster):
        self.env = env
        self.job_queue = simpy.Store(env)
        self.task_queue = simpy.Store(env)
        self.cluster = cluster
        env.process(self.schedule_job())
        env.process(self.schedule_task())
        self.workers_it = itertools.cycle(cluster.get_workers())
        self.event_to_task = {}


    def put(self, job):
        return self.job_queue.put(job)


    def schedule_job(self):
        while True:
            yield self.env.timeout(1)
            print(f'Scheduler waiting for job at {self.env.now}')
            job = yield self.job_queue.get()
            print(f'Scheduler got {job} at {self.env.now}')
            execute_proc = self.env.process(self.admit(job))
   
    def task_finished_cb(self, event):
        print(f'{Fore.BLUE} Executor {event}')
        # mark task as completed
        task = self.event_to_task[event]
        task.set_status('finished')
        nt = task.job.get_next_task(task)

        # send next joba to the sescheduler 


    def admit(self, job):
        completions = job.get_task_completions()
        tasks = job.get_next_tasks()

        for t in tasks:
            self.submit_task(t)

            w = self.decide_worker()
            self.event_to_task[t.completion_event] = t
            t.completion_event.callbacks.append(self.task_finished_cb)
            self.cluster.submit_task(w, t)
        
        # wait for all tasks of this job to be done
        yield simpy.events.AllOf(self.env, completions)
        print(f'Job {job} is done at {self.env.now}')


    def decide_worker(self):
        # lets do the round robin for now
        w = next(self.workers_it)
        return w


    def submit_task(self, task)
        w = self.decide_worker()
        self.event_to_task[t.completion_event] = t
        t.completion_event.callbacks.append(self.task_finished_cb)
        self.cluster.submit_task(w, t)


