#!/usr/bin/python3
import random
from storage import Object
from collections import defaultdict, deque
import graph_tool.all as gt
import ast
import json
import re
import sys


class Task:
    class Stats:
        def __init__(self):
            self.start_time = -1
            self.end_time = -1
            self.cur_exec_rate_start = -1;
            self.progress = 0
            self.estimated_finish_time = 0 
            self.exectuion_history =[]; # this should be a list of {'start of plan', 'end of plan', 'progress at the begining of plan', # of concurrent running task during plan, estimated end time}

    def __init__(self, env, job):
        self.env = env
        self.inputs = []
        self.id = 0
        self.name = ''
        self.exec_time = 0
        self.schedule_delay = 0
        self.nbytes = 0
        self.obj = None
        self.computation_completion_event = env.event()
        self.completion_event = env.event()
        self.job = job
        self.color = None
        self.child_color = None
        self.worker = None
        self.optimal_placement=None
        self.status = 'waiting' 
        self.stats = self.Stats()



    def __str__(self):
        return f'{self.name}'

    def __repr__(self):
        return f'{self.name}'

    def __lt__(self, other):
         return self.stats.estimated_finish_time < other.stats.estimated_finish_time

    def __gt__(self, other):
         return self.stats.estimated_finish_time > other.stats.estimated_finish_time

    def set_output_size(self, size):
        self.nbytes = size
        self.obj = Object(self.name, size)

    def set_exec_time(self, time, start=0, stop=0):
        self.exec_time = int(time)

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

    def set_schedule_delay(self, delay):
        self.schedule_delay = delay



class Job:
    def __init__(self, env, prefetch):
        self.env = env
        self.g = None
        self.completion_events = []
        self.vid_to_vtx = {}
        self.name = None
        self.name_to_task = {}
        self.prefetch_enabled = prefetch

    def __str__(self):
        return f'{self.name}'


    def build_job_from_file(self, gfile, histfile, name):
        self.name_to_task = {}
        self.name = name 
        print(name, gfile, histfile)
        with open(histfile, 'r') as fd:
            taskstat = json.load(fd)
            for _ts in taskstat:
                _tname = _ts['key']
                ts = Task(self.env, job = self)
                ts.set_name(_tname)
                for dd in _ts['drilldown']:
                    if dd['name'] != '<Functions Host Timing>': continue
                    output_size = dd['custom_info']['nbytes']
                    ts.set_output_size(output_size)

                for edd in dd['drilldown']:
                    if edd['name'] == '<Azure Functions Host Invocation>':
                        execution_time = edd['duration']
                        ts.set_exec_time(execution_time)
                self.completion_events.append(ts.completion_event)
                self.name_to_task[_tname] = ts
       
        
        with open(gfile, 'r') as fd:
        
        return self



    def build_job_from_file2(self, gfile, histfile, name):
        self.name_to_task = {}
        self.name = name #re.search(r'stats/(.*?)\.json', histfile).group(1)
        with open(histfile, 'r') as fd:
            #print(json.dumps(taskstat, indent=4))
            taskstat = ast.literal_eval(fd.read())
            for name in taskstat:
                #print(name)
                ts = Task(self.env, job = self)
                self.completion_events.append(ts.completion_event)
                self.name_to_task[name] = ts
                ts.set_output_size(taskstat[name]['msg']['nbytes'])
                ts.vanilla_placement=taskstat[name]['worker'].split(':')[1].replace('//', '')
                for ss in taskstat[name]['msg']['startstops']:
                    if ss['action'] == 'compute': 
                        ts.set_exec_time(ss['stop'] - ss['start'], ss['start'], ss['stop']) 
        
        with open(gfile, 'r') as fd:
            g = gt.Graph(directed=True)
            gstr = fd.read().split('\n')
            vid_to_vtx = {}
            self.vname_to_vtx = {}
            g.vertex_properties['tasks'] = g.new_vertex_property('python::object')
            for s in gstr:
                if s.startswith('v'):
                    vid, name = int(s.split(',', 2)[1]), s.split(',', 2)[2]
                    v = g.add_vertex()
                    vid_to_vtx[vid] = v
                    self.vname_to_vtx[name] = v
                    ts = self.name_to_task[name]
                    ts.set_name(name)
                    ts.set_id(vid)
                    g.vp.tasks[v] = ts

            for s in gstr:
                if s.startswith('e'):
                    src, dst = vid_to_vtx[int(s.split(',')[1])], vid_to_vtx[int(s.split(',')[2])]
                    g.add_edge(src, dst)
                    g.vp.tasks[dst].add_input(g.vp.tasks[src].get_output())
        self.g = g
        self.generate_graph_chains(g)
        self.vid_to_vtx = vid_to_vtx
        #gt.graph_draw(g, vertex_text=g.vertex_index, output="g.png")
        return self

    def optimal_placement(self):
        with open(f'/opt/dask-distributed/benchmark/stats/{self.name}.optimal', 'r') as fd:
            lines = fd.read().split('\n')[:-1]
            optimal_placement = {}
            for l in lines:
                self.name_to_task[l.split(',')[0]].optimal_placement = l.split(',')[-1]


    def get_task_completions(self):
        return self.completion_events


    def get_next_tasks(self, task=None):
        if not task:
            return self.get_ready_tasks()

        assert(task.status == 'finished')
        v = self.vid_to_vtx[task.id]
        ready = set()
        prefetch = []
        for vo in v.out_neighbors():
            is_ready = True
            for vi in vo.in_neighbors():
                if self.g.vp.tasks[vi].status != 'finished':
                    is_ready = False
                    break
            if is_ready:
                ready.add(vo)
            elif self.prefetch_enabled and self.g.vp.tasks[vo].color != self.g.vp.tasks[v].color:
                pt = Task(self.env, self) # prefetch task
                pt.add_input(self.g.vp.tasks[v].get_output())
                pt.color = self.g.vp.tasks[vo].color
                pt.name = 'NOP'
                prefetch.append(pt)
                print(f'task NOP with input {pt.inputs} is created with color {pt.color} for task {self.g.vp.tasks[vo].name} at {self.env.now}')

        for r in ready:
            self.g.vp.tasks[r].start_ts = self.env.now
        to_be_sent = [self.g.vp.tasks[r] for r in ready]
        for pt in prefetch:
            to_be_sent.append(pt)
        return to_be_sent


    def get_ready_tasks(self):
        ready = set()
        for v in self.g.vertices():
            is_ready = True
            for vi in v.in_neighbors():
                if self.g.vp.tasks[vi].status != 'finished':
                    is_ready = False
                    break
            if is_ready:
                ready.add(v)
                self.g.vp.tasks[v].status = 'submitted'
            #elif self.g.vp.tasks[vi].color != self.g.vp.tasks[v].color:
            #    pt = Task(self.env, self.job) # prefetch task
            #    pt.add_input(self.g.vp.tasks[v].get_output())
            #    pt.color = self.g.vp.tasks[vo].color
            #    pt.name = 'NOP'
            #    prefetch.add(pt)
        to_be_sent = [self.g.vp.tasks[r] for r in ready]
        #for pt in prefetch:
        #    to_be_sent.append(pt)
        return to_be_sent

    # RF-MSR B
    def generate_graph_chains(self, g):
        """
        Generate a chain decomposition of the graph. This algorithm is not optimal,
        but is quicker than the optimal O(v+e). It is by Simon, "An Improved Algorithm for Transitive 
        Closure on Acyclic Graphs", TCS v58, i 1-3, 1988.
        This is called by update_graph.
        The result is stored in the TaskState of each task in the graph, in the 'color' and 'child_color'
        attributes, and is sent to the worker for possible use in scheduling / data placement.
        """
        def DFS( current_ts, open_time, close_time, current_time, sorted_nodes=None):
            """
            Do a topological sort of the graph, following the 'dependents' links
            Note that if the graph has multiple "starting points", nodes with no dependencies,
            this will not traverse the entire graph. This is why we call DFS multiple times
            below, from different starting_tasks.

            Arguments:
            current_ts -- a task structure
            open_time  -- dictionary keyed by ts.key with the time the node was visited
            close_time -- dictionary keyed by ts.key with the time when the node was finished.
                          This, sorted in decreasing order, gives the topological sort.
            current_time -- current step of the algorithm
            sorted_nodes -- nodes in topological order
            """
            assert(not g.vp.tasks[current_ts].name in open_time)
            open_time[g.vp.tasks[current_ts].name] = current_time
            current_time += 1
            for t in current_ts.out_neighbors():
                #print(f'{g.vp.tasks[current_ts].name} -> {g.vp.tasks[t].name}')
                if (not g.vp.tasks[t].name in close_time):
                    assert(not g.vp.tasks[t].name in open_time) #cycle!
                    current_time = DFS(t, open_time, close_time, current_time, sorted_nodes)
                else: #don't visit, node is closed (visited)
                    assert(g.vp.tasks[t].name in open_time) #error (closed but never open!)
            close_time[g.vp.tasks[current_ts].name] = current_time
            current_time += 1
            if (not sorted_nodes == None):
                sorted_nodes.appendleft(current_ts)
            return current_time

        # First, do a topological sort of the graph, starting at all of the nodes with no
        # dependencies.
        open_time = {}
        close_time = {}
        sorted_nodes = deque()
        current_time = 0

        starting_tasks = []
        for v in g.vertices():
            if v.in_degree() == 0:
                starting_tasks.append(v)


        for starting_ts in starting_tasks:
            current_time = DFS(starting_ts, open_time, close_time, current_time, sorted_nodes)

        #Now we do the chain partitioning. Will store the results in the task itself,
        #in the color field.
        # TODO: For now, assuming the nodes haven't been colored. It's possible that they
        # would have been, in which case it would be good to extend the chains    

        cfd = open(f'/local0/serverless/serverless-sim/results/{self.name}.simcolors', 'w')
        print("Chain Coloring...")
        c = 1
        # The topo order makes us start as far back as possible, and color all nodes
        for v in sorted_nodes:
            ts = g.vp.tasks[v]
            if ts.color is not None:
                continue
            cur_v = v
            current_ts = ts
            current_ts.color = str(c)
            current_ts.child_color = str(c)
            # Follow the chain, coloring each node. Stop when we have no dependents,
            #  or when all dependents have been colored (go_on = False at the end of the loop)
            while True:
                # Find the first non-colored dependent (*in topological order, this is important*)
                #  color it, and follow it
                #        +-------------------------+
                #       /                           \
                #    -- a ------- b ------ c ------ d --
                #    The topo sort always prevents picking d first in this case, which would create 2 
                #      chains instead of one: ad and bc. 
                #    In the loop below, the topological sort is given by the reverse of the closing time
                #      of the DFS done above.
                # We also want to record the color of one of our children:
                #   1. if there are multiple children, give preference to the one with the same
                #    color as ours
                #   2. if no children have our color (because they were all colored before), we
                #    choose arbitrarily (the last one, just because we have to exhaust the list anyway)
                go_on = False
                if cur_v.out_degree() == 0:
                    current_ts.child_color = current_ts.color
                else:
                    for dep_v in sorted(cur_v.out_neighbors(), key = lambda t:close_time[g.vp.tasks[t].name], reverse=True):
                        dep_ts = g.vp.tasks[dep_v]
                        if dep_ts.color is None:
                            dep_ts.color = str(c)
                            go_on = True
                            break
                        current_ts.child_color = dep_ts.color
                #print(" \"%s\" [style=filled fillcolor=\"/paired12/%s\" color=\"/paired12/%s\"]"%(current_ts.name, current_ts.color, current_ts.child_color))
                cfd.write(f'{current_ts.name},{current_ts.color}\n')
                if go_on:
                    current_ts = dep_ts
                    cur_v = dep_v
                else:
                    break
            # Reached the end of the chain, next color
            c += 1
        #done coloring
        cfd.close()
        #RF this is just for basic statistics about the graph
        count_edges = 0
        count_nodes_with_deps = 0
        for v in g.vertices():
            ts = g.vp.tasks[v]
            if ts.color is None or ts.child_color is None:
                print(" uncolored task: {}".format(ts.name))
            e = v.in_degree()
            if (e > 0):
                count_nodes_with_deps += 1
                count_edges += e

        # This assumes that for every node with at least one dependency, then one of the inputs
        # should be local if we are scheduling according to chains. 
        print("Graph Stats: tasks: %d edges: %d min_local_hit: %d max_local_miss: %d"%(
                        g.num_vertices(),
                        count_edges,
                        count_nodes_with_deps,
                        count_edges - count_nodes_with_deps))

        # RF-MSR E

