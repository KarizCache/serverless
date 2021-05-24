#!/usr/bin/python3


from job import Job
import itertools
import yaml
import json
import os

class Workload:
    def __init__(self, env, scheduler, config_fpath):
        self.env = env
        self.jobs = itertools.cycle(self.build_workload(config_fpath))
        self.scheduler = scheduler
        generator = env.process(self.job_generator())

    def job_generator(self):
        while True:
            job = next(self.jobs)
            print(f'Workload generator submitted job {job} at {self.env.now}')
            yield self.scheduler.put(job)
            yield self.env.timeout(200000)

    def build_workload(self, config_fpath):
        jobs = []
        try:
            data = yaml.load(open(config_fpath, 'r'), Loader=yaml.FullLoader)
            workloaddir = data['benchmark']['workloaddir']
            if 'workloads' in data['benchmark']:
                workloads = [{'name': w, 'path': os.path.join(workloaddir, w)} for w in data['benchmark']['workloads']]
            else:
                # remember to add the case to read of the 
                pass 
            print(workloads)
            prefetch = data['cluster']['prefetch']
            for w in workloads:
                print(w["name"])
            jobs = [Job(self.env, prefetch=prefetch).build_job_from_file(f'{workload["path"]}.g', f'{workload["path"]}.json', name=workload['name']) for workload in workloads]
        except:
            raise
        return jobs
