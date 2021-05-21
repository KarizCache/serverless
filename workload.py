#!/usr/bin/python3


from job import Job
import itertools
import yaml
import json

class Workload:
    def __init__(self, env, scheduler, fpath):
        self.env = env
        self.jobs = itertools.cycle(self.build_workload(fpath))
        self.scheduler = scheduler
        generator = env.process(self.job_generator())

    def job_generator(self):
        while True:
            job = next(self.jobs)
            print(f'Workload generator submitted job {job} at {self.env.now}')
            yield self.scheduler.put(job)
            yield self.env.timeout(200000)

    def build_workload(self, fpath):
        jobs = []
        try:
            data = yaml.load(open(fpath, 'r'), Loader=yaml.FullLoader)
            workload = data['benchmark']['workloads']
            prefetch = data['cluster']['prefetch']
            jobs = [Job(self.env, prefetch=prefetch).build_job_from_file(w['graph'], w['execution']) for w in workload]
        except:
            raise
        return jobs
