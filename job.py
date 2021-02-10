#!/usr/bin/python3
import random
from storage import Object

class Task:
    def __init__(self, env, id):
        self.type = 'Map'
        self.inputs = [{'name': 'a0'}]
        self.id = id
        self.env = env
        self.exec_time = random.randint(5, 15)
        self.inout_ratio = 0.5
        self.obj_out = Object(f't{id}_out', 2048)
        self.completion_event = env.event()


class Stage:
    def __init__(self, env):
        self.env = env
        self.n_tasks = 5# random.randint(1, 10) # FIXME
        self.tasks = [Task(env, t) for t in range(self.n_tasks)]



class Job:
    def __init__(self, env):
        self.env = env
        self.tasks = {}
        self.dependencies = {}

    def build_job_from_file(self, graph, history):
        print(graph)
        print(history)
        return self


    def get_ready_stages(self):
        return self.stages

    def get_ready_tasks(self):
        return self.stages['0'].tasks
