#!/usr/bin/python3


from job import Job
import itertools
import yaml

class Workload:
    def __init__(self, env, scheduler, fpath):
        self.env = env
        self.jobs = itertools.cycle(self.build_workload(fpath))
        self.scheduler = scheduler
        generator = env.process(self.job_generator())

    def job_generator(self):
        while True:
            job = Job(self.env)
            yield self.scheduler.put(next(self.jobs))
            print(f'Workload generator produced job {job} at {self.env.now}')
            yield self.env.timeout(20000)

    def build_workload(self, fpath):
        jobs = []
        try:
            data = yaml.load(open(fpath, 'r'), Loader=yaml.FullLoader)
            workload = data['workloads']
            jobs = [Job(self.env).build_job_from_file(w['graph'], w['execution']) for w in workload]
            print(jobs)
        except:
            raise
        return jobs
