#!/usr/bin/python3
import random
from storage import Object
from collections import defaultdict, deque
import graph_tool.all as gt
import ast
import json
import re
import sys
import numpy as np
import copy
from colorama import Fore, Style


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
        self.hcolor_bits = 0;
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
        self.exec_time = time
        print(self.name, '------->', self.exec_time)

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


    def build_dag_from_file(self, dagf):
        with open(dagf, 'r') as fd:
            g = gt.Graph(directed=True)
            g.vp.tasks = g.new_vertex_property('python::object')
            g.vp.vid = g.new_vertex_property('string')
            g.vp.color = g.new_vertex_property('int', -1)
            g.vp.tmp_color = g.new_vertex_property('int', -1)
            g.vp.cset = g.new_vertex_property('object')
            vid2v = {}
            self.vname_to_vtx = {}
            
            lines = fd.read().split('\n')[:-1]
            for ln in lines:
                if ln.startswith('v'):
                    _, vid, vname = ln.split(',', 2)

                    ts = self.name_to_task[vname]
                    ts.set_id(vid)
                    
                    v = g.add_vertex()
                    g.vp.tasks[v] = ts

                    g.vp.vid[v] = vid
                    g.vp.color[v] = -1
                    g.vp.tmp_color[v] = -1
                    g.vp.cset[v] = []

                    #print(vname, ts.exec_time)

                    self.vname_to_vtx[vname] = v
                    vid2v[vid] = v
            for ln in lines:
                if ln.startswith('e'):
                    _, src, dst, *args = ln.split(',')
                    g.add_edge(vid2v[src], vid2v[dst])
                    g.vp.tasks[vid2v[dst]].add_input(g.vp.tasks[vid2v[src]].get_output())
            self.vid_to_vtx = vid2v
            return g


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
        g = self.build_dag_from_file(gfile)
        self.generate_chain_color(g)
        #self.generate_graph_chains(g)
        self.g = g
        return self

    def assign_heirarchy(self, n_chains, g):
        def DFS(v, visited, H):
            assert (not v in visited)
            visited[v] = 1
            for vo in v.out_neighbors():
                if g.vp.color[v] != g.vp.color[vo]:
                    H[g.vp.color[v], g.vp.color[vo]] += 1
                    H[g.vp.color[vo], g.vp.color[v]] += 1
                if not vo in visited:
                    H = DFS(vo, visited, H)
            return H
    
        visited = {}
        H = np.zeros((n_chains, n_chains))
        start_tasks = []
        for v in g.vertices():
            if not v.in_degree():
                start_tasks.append(v)
        for v in start_tasks:
            H = DFS(v, visited, H)
        return H
    
    
    def checkpoint_colors(self, g):
        for v in g.vertices():
            g.vp.cset[v].append(g.vp.color[v])
    
    
    def recolor_graph(self, g, merged):
        self.checkpoint_colors(g)
        for v in g.vertices():
            g.vp.color[v] = merged[g.vp.color[v]]


    def merge_chains(self, n_chains, H):
        merged = {}
        for ch in range(0, n_chains):
            if ch in merged: continue
            merged[ch] = ch
            options = np.where(H[ch, :] > 0)[0]
            if len(options):
                min_chain = sys.maxsize
                for opt in options:
                    #print(H[opt, :], H[opt, :].sum())
                    if H[opt, :].sum() < min_chain:
                        min_chain = H[opt, :].sum()
                        m_color = opt
    
                #m_color = np.random.choice(options)
                #print(f'chose {m_color} to merge with {ch}')
                merged[m_color] = ch
                H[:, m_color] = 0; H[m_color, :] = 0;
                H[:, ch] = 0; H[ch, :] = 0;
        return merged
    
    def finalize(self, g):
        def reverseBits(n) :
            rev = 0
            while (n > 0) :
                rev = rev << 1
                if (n & 1 == 1) :
                    rev = rev ^ 1
                n = n >> 1
            return rev
        
        n_steps = len(g.vp.cset[0])
        fc = {}
        for s in range(n_steps - 1, -1, -1):
            fc_old = copy.deepcopy(fc)
            for v in g.vertices():
                c = g.vp.cset[v][s]
                if s == n_steps - 1:
                    fc[c] = 0
                else:
                    c_old = g.vp.cset[v][s+1]
                    fc[c] = fc_old[c_old] << 1;
                    if c != c_old:
                        fc[c] += 1
        for v in g.vertices():
            c = g.vp.tmp_color[v]
            g.vp.color[v] = fc[c]
            g.vp.tasks[v].color = fc[c]
            #print(f'{Fore.CYAN} {g.vp.tasks[v].name} --> {g.vp.tasks[v].color} --> {format(g.vp.tasks[v].color, "#011b")} <------> {format(fc[c], "#011b")} {Style.RESET_ALL}')
            g.vp.tasks[v].hcolor_bits = n_steps
        pass



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


    def generate_graph_chains(self, g):
        """
        Generate a chain decomposition of the graph. This algorithm is not optimal,
        but is quicker than the optimal O(v+e). It is by Simon, "An Improved Algorithm for Transitive
        Closure on Acyclic Graphs", TCS v58, i 1-3, 1988.
        This is called by update_graph.
        The result is stored in the TaskState of each task in the graph, in the 'color' and 'child_color'
        attributes, and is sent to the worker for possible use in scheduling / data placement.
        """
        def DFS( v, open_time, close_time, current_time, sorted_nodes=None):
            """
            Do a topological sort of the graph, following the 'dependents' links
            Note that if the graph has multiple "starting points", nodes with no dependencies,
            this will not traverse the entire graph. This is why we call DFS multiple times
            below, from different starting_tasks.
            Note: we return current_time because otherwise the updates inside the recursive
                  call are kept only in the local copy (as Python passes ints by value)

            Arguments:
            current_ts -- a task structure
            open_time  -- dictionary keyed by ts.key with the time the node was visited
            close_time -- dictionary keyed by ts.key with the time when the node was finished.
                          This, sorted in decreasing order, gives the topological sort.
            current_time -- current step of the algorithm
            sorted_nodes -- nodes in topological order
            """
            assert(not v in open_time)
            open_time[v] = current_time
            current_time += 1
            for vo in v.out_neighbors():
                #print("\"%s\" -> \"%s\"", v, vo)
                if (not vo in close_time):
                    assert(not vo in open_time) #cycle!
                    current_time = DFS(vo, open_time, close_time, current_time, sorted_nodes)
                else: #don't visit, node is closed (visited)
                    assert(vo in open_time) #error (closed but never open!)
            close_time[v] = current_time
            current_time += 1
            if (not sorted_nodes == None):
                sorted_nodes.appendleft(v)
            return current_time

        # First, do a topological sort of the graph, starting at all of the nodes with no
        # dependencies.
        open_time = {}
        close_time = {}
        sorted_nodes = deque()
        current_time = 0

        starting_tasks = []

        # if the current task has no color
        # this task has 0 parents with no color
        # Mania: # I think this algorithm was originally works with O(V + E)
        # and after this modification in worst case it runs with O(V^2) I should double check this claim.
        for v in g.vertices():
            if not v.in_degree():
                starting_tasks.append(v)

        #print(f'{Fore.GREEN} starting tasks are: {starting_tasks}{Style.RESET_ALL}')
        for vs in starting_tasks:
            current_time = DFS(vs, open_time, close_time, current_time, sorted_nodes)


        # Check: assert that the close_times of sorted_nodes is sorted and has no duplicates
        #cts = [n.close_time for n in sorted_nodes]
        #assert(cts == sorted(cts))
        #assert(len(cts) == len(set(cts)))

        #Now we do the chain partitioning. Will store the results in the task itself,
        #in the color field.
        # TODO: For now, assuming the nodes haven't been colored. It's possible that they
        # would have been, in which case it would be good to extend the chains

        c = 0
        # The topo order makes us start as far back as possible, and color all nodes
        for v in sorted_nodes:
            if g.vp.color[v] != -1:
                continue

            current_ts = v
            g.vp.color[v] = c
            #current_ts.child_color = str(c)
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
                if v.out_degree() == 0:
                    pass
                #    current_ts.child_color = current_ts.color
                else:
                    for vo in sorted(v.out_neighbors(), key = lambda t:close_time[t], reverse=True):
                        if g.vp.color[vo] == -1:
                            g.vp.color[vo] = c
                            go_on = True
                            break
                if go_on:
                    v = vo
                else:
                    break
            # Reached the end of the chain, next color
            c += 1

        for v in g.vertices():
            g.vp.tmp_color[v] = g.vp.color[v]

        #RF this is just for basic statistics about the graph
        count_edges = 0
        count_nodes_with_deps = 0
        for v in g.vertices():
            if g.vp.color[v] == -1:
                print(" uncolored task: {}".format(v))
            e = v.out_degree()
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

        return c


    

    def generate_chain_color(self, g):
        n_chains = self.generate_graph_chains(g)
        while True:
            H = self.assign_heirarchy(n_chains, g)

            if not H.sum():
                break
            merged = self.merge_chains(n_chains, H)
            self.recolor_graph(g, merged)

        self.checkpoint_colors(g)
        self.finalize(g)
