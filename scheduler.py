#!/usr/bin/python3

import simpy
import itertools

class Scheduler(object):
    def __init__(self, env, cluster):
        self.env = env
        self.job_queue = simpy.Store(env)
        self.cluster = cluster
        env.process(self.schedule())
        self.workers_it = itertools.cycle(cluster.get_workers())


    def put(self, job):
        return self.job_queue.put(job)


    def schedule(self):
        while True:
            yield self.env.timeout(1)
            print(f'Scheduler waiting for job at {self.env.now}')
            job = yield self.job_queue.get()
            print(f'Scheduler got {job} at {self.env.now}')
            execute_proc = self.env.process(self.admit(job))
   

    def admit(self, job):
        tasks = job.get_ready_tasks()
        completions = []
        for t in tasks:
            w = self.decide_worker()
            self.cluster.submit_task(w, t)
            completions.append(t.completion_event)     
        # barrier waits 
        yield simpy.events.AllOf(self.env, completions)
        print(f'Job {job} is done at {self.env.now}')


    def decide_worker(self):
        # lets do the round robin for now
        w = next(self.workers_it)
        return w


