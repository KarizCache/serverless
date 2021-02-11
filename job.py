#!/usr/bin/python3
import random
from storage import Object
import graph_tool.all as gt
import ast
import json

class Task:
    def __init__(self, env, job):
        self.env = env
        self.status = 'waiting' 
        self.inputs = []
        self.id = 0
        self.name = ''
        self.exec_time = 0
        self.nbytes = 0
        self.obj = None
        self.completion_event = env.event()
        self.job = job

    def set_output_size(self, size):
        self.nbytes = size
        self.obj = Object(self.name, size)


    def set_exec_time(self, time, start=0, stop=0):
        self.exec_time = time
        self.start = start
        self.stop = stop

    def add_input(self, obj):
        self.inputs.append(obj)


    def get_output(self):
        return self.obj

    def set_name(self, name):
        self.name = name

    def set_id(self, id):
        self.id = id 

    def set_status(self, status):
        self.status = status



class Job:
    def __init__(self, env):
        self.env = env
        self.g = None
        self.candidates = set()
        self.pendings = set()
        self.vid_to_vtx = {}

    def build_job_from_file(self, gfile, histfile):
        name_to_task = {}
        with open(histfile, 'r') as fd:
            taskstat = ast.literal_eval(fd.read())
            for name in taskstat:
                ts = Task(self.env, job = self)
                name_to_task[name] = ts
                ts.set_output_size(taskstat[name]['msg']['nbytes'])
                for ss in taskstat[name]['msg']['startstops']:
                    if ss['action'] == 'compute': 
                        ts.set_exec_time(ss['stop'] - ss['start'], ss['start'], ss['stop']) 
        
        with open(gfile, 'r') as fd:
            g = gt.Graph(directed=True)
            gstr = fd.read().split('\n')
            vid_to_vtx = {}
            vname_to_vtx = {}
            g.vertex_properties['tasks'] = g.new_vertex_property('python::object')
            for s in gstr:
                if s.startswith('v'):
                    vid, name = int(s.split(',', 2)[1]), s.split(',', 2)[2]
                    v = g.add_vertex()
                    vid_to_vtx[vid] = v
                    vname_to_vtx[name] = v
                    ts = name_to_task[name]
                    ts.set_name(name)
                    ts.set_id(vid)
                    g.vp.tasks[v] = ts

            for s in gstr:
                if s.startswith('e'):
                    src, dst = vid_to_vtx[int(s.split(',')[1])], vid_to_vtx[int(s.split(',')[2])]
                    g.add_edge(src, dst)
                    g.vp.tasks[dst].add_input(g.vp.tasks[src].get_output())
        self.g = g
        self.vid_to_vtx = vid_to_vtx
        gt.graph_draw(g, vertex_text=g.vertex_index, output="g.png")
        return self


    def get_next(self, task=None):
        if not task:
            return self.get_ready_tasks()

        assert(task.status == 'finished')
        v = self.vid_to_vtx[task.vid]
        ready = set()
        for vo in v.out_neighbors():
            is_ready = True
            for vi in vo.out_neighbors:
                if vi.task.status != 'finished':
                    is_ready = False
                    break
            if is_ready:
                ready.add(vo)
        return [self.g.vp.tasks[r] for r in ready]


    def get_ready_tasks(self):
        ready = set()
        for v in g.vertices():
            is_ready = True
            for vi in v.in_neighbors():
                if g.vp.tasks[vi].status != 'finished':
                    is_ready = False
                    break
            if is_ready:
                ready.add(v)
                self.g.vp.tasks[v].status = 'submitted'
        return [self.g.vp.tasks[r] for r in ready]
