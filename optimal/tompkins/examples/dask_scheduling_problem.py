#!/usr/bin/python

from tompkins.ilp import schedule, jobs_when_where
from collections import defaultdict
from pulp import value
"""
A sample problem
>>> x = matrix('x')
>>> y = x*x
>>> z = y.sum()
>>> f = function([x], z)

Architecture
CPU --- GPU

Data starts and must end on the CPU
"""


import re
import ast
import json
from graphviz import Digraph
import pandas as pd

# color the graph
import graph_tool.all as gt
import copy
import matplotlib.colors as mcolors
import sys

import seaborn as sns

def color_assignment(benchmark, task_style):
    cfile = f'/opt/dask-distributed/benchmark/stats/{benchmark}.colors'
    with open(cfile, 'r') as cfd:
        raw = cfd.read().split('\n')
        for ln in raw:
            if not ln: 
                continue
            task_name, actual = ln.split('---')
            if task_name not in task_style:
                task_style[task_name] = {}
            task_style[task_name]['actual'] = actual    

    #cfile = f'/local0/serverless-sim/results/{benchmark}.simcolors'
    #with open(cfile, 'r') as cfd:
    #    raw = cfd.read().split('\n')
    #    for ln in raw:
    #        if not ln: 
    #            continue
    #        task_name, simulator = ln.split(',')
    #        if task_name not in task_style:
    #            task_style[task_name] = {}
    #        task_style[task_name]['simulator'] = simulator 
        
    
    
def build_graph(benchmark, task_style):
    css_colors = list(mcolors.CSS4_COLORS.keys())
    gfile = f'/opt/dask-distributed/benchmark/stats/{benchmark}.g'

    with open(gfile, 'r') as fd:
        raw = fd.read().split('\n')
        g = gt.Graph(directed=True)
        vid_to_vx = {}
        name_to_vid = {}

        g.vertex_properties['name'] = g.new_vertex_property("string")
        g.vertex_properties['color'] = g.new_vertex_property("string")
        g.vertex_properties['worker'] = g.new_vertex_property("string")
        g.vertex_properties['icolor'] = g.new_vertex_property("int")
        g.vertex_properties['simcolor'] = g.new_vertex_property("string")
        g.vertex_properties['isimcolor'] = g.new_vertex_property("string")
        for ln in raw:
            if ln.startswith('v'):
                _, vid, name = ln.split(',', 2)
                v = g.add_vertex()
                vid_to_vx[vid] = v
                name_to_vid[name] = vid

                g.vp.name[v] = name
                try:
                    g.vp.icolor[v] = int(task_style[name]['actual'])
                    if g.vp.icolor[v] >= len(css_colors):
                        g.vp.color[v] = mcolors.CSS4_COLORS[css_colors[0]]
                    else:
                        g.vp.color[v] = mcolors.CSS4_COLORS[css_colors[int(task_style[name]['actual'])]]

                    #g.vp.simcolor[v] = mcolors.CSS4_COLORS[css_colors[int(task_style[name]['simulator'])]]
                    #g.vp.isimcolor[v] = int(task_style[name]['simulator'])
                #print(name, g.vp.icolor[v])
                except KeyError:
                    print(f'Keyerror for {name}')
                    g.vp.color[v] = 'yellow'
                    g.vp.icolor[v] = 2

        for ln in raw:        
            if ln.startswith('e'):
                _, vsrc, vdst, _ = ln.split(',', 3)
                g.add_edge(vid_to_vx[vsrc], vid_to_vx[vdst])
    return g
            
colors = {'w0': mcolors.CSS4_COLORS[list(mcolors.CSS4_COLORS.keys())[0]],
    '10.255.23.108': mcolors.CSS4_COLORS[list(mcolors.CSS4_COLORS.keys())[0]],
    '10.255.23.109': mcolors.CSS4_COLORS[list(mcolors.CSS4_COLORS.keys())[1]],
    '10.255.23.110': mcolors.CSS4_COLORS[list(mcolors.CSS4_COLORS.keys())[2]],
    '10.255.23.115': mcolors.CSS4_COLORS[list(mcolors.CSS4_COLORS.keys())[3]]}
def update_runtime_state(benchmark, g, task_style):
    tasks = []
    jfile = f'/opt/dask-distributed/benchmark/stats/{benchmark}.json'
    with open(jfile, 'r') as fd:
        stats = ast.literal_eval(fd.read())

        min_ts = sys.maxsize
        for s in stats:
            task_style[s]['output_size'] = stats[s]['msg']['nbytes']
            task_style[s]['input_size'] = 0
            task_style[s]['remote_read'] = 0
            task_style[s]['local_read'] = 0
            task_style[s]['worker'] = stats[s]['worker'].split(':')[1].replace('/', '')
            startsstops = stats[s]['msg']['startstops']
            #print(s, stats[s]['worker'])
            for ss in startsstops:
                if ss['action'] == 'inputsize': continue
                if ss['action'] == 'compute':
                    task_style[s]['compute_end'] = ss['stop']
                    task_style[s]['compute_start'] = ss['start']
                    task_style[s]['runtime'] = ss['stop'] - ss['start']
                if ss['start'] < min_ts:  min_ts = ss['start']
                if ss['stop'] < min_ts:  min_ts = ss['stop']
            #break


        for s in stats:
            startsstops = stats[s]['msg']['startstops']
            min_start = sys.maxsize
            max_end = 0

            for ss in startsstops:
                if ss['action'] == 'inputsize': continue
                if ss['start'] < min_start:  min_start = ss['start']
                if ss['stop'] > max_end:  max_end = ss['stop']

            tasks.append({'name': s, 'start_ts': min_start - min_ts, 'end_ts': max_end - min_ts, 
                          'worker': stats[s]['worker'].split(':')[1].replace('/', '')})
    
    
        #total amount of data accessed, data accessed remotely, data accessed locally
        for v in g.vertices():
            for vi in v.in_neighbors():
                task_style[g.vp.name[v]]['input_size'] += task_style[g.vp.name[vi]]['output_size']
                if task_style[g.vp.name[v]]['worker'] == task_style[g.vp.name[vi]]['worker']:
                    task_style[g.vp.name[v]]['local_read'] += task_style[g.vp.name[vi]]['output_size']
                else:
                    task_style[g.vp.name[v]]['remote_read'] += task_style[g.vp.name[vi]]['output_size']
                  
        for v in g.vertices():
            g.vp.worker[v] = task_style[g.vp.name[v]]['worker']
            g.vp.color[v] = colors[task_style[g.vp.name[v]]['worker']]
            
        #Check the slack for the prefetching
        bw = 10*(1<<27) # 10 Gbps (1<<30)/(1<<3)
        not_from_remote = 0
        for v in g.vertices():
            parents_end = []
            for vi in v.in_neighbors():
                parents_end.append(task_style[g.vp.name[vi]]['compute_end'])
                
            if len(parents_end):
                max_end = max(parents_end)
                
                for vi in v.in_neighbors():
                    if max_end == task_style[g.vp.name[vi]]['compute_end'] and task_style[g.vp.name[vi]]['worker'] != task_style[g.vp.name[v]]['worker']:
                        #print(f'Slack come from local chain')
                        not_from_remote += 1
            

                #print(f'slack for {g.vp.name[v]}: {round(1000*(max(parents_end) - min(parents_end)), 2)}msec',
                #     '\t runtime:', round(1000*task_style[g.vp.name[vi]]['runtime'], 4), 'msec',
                #     '\t remote read', task_style[g.vp.name[v]]['remote_read']/bw)
        #print(not_from_remote)



def plot_graph(g):        
    dg = Digraph('G', filename=f'{benchmark}.gv', format='png')
    for v in g.vertices():
        dg.attr('node', shape='ellipse', style='filled', color=g.vp.color[v])
        dg.node(f'{v}, color({g.vp.icolor[v]})')

    for e in g.edges():
        dg.edge(f'{e.source()}, color({g.vp.icolor[e.source()]})', 
                f'{e.target()}, color({g.vp.icolor[e.target()]})')



    dg.view(f'./plots/{benchmark}', quiet=False)

    '''
    dg = Digraph('G', filename=f'{benchmark}.simulator.gv', format='png')
    for v in g.vertices():
        dg.attr('node', shape='ellipse', style='filled', color=g.vp.simcolor[v])
        dg.node(f'{v}, color({g.vp.isimcolor[v]})')

    for e in g.edges():
        dg.edge(f'{e.source()}, color({g.vp.isimcolor[e.source()]})', 
                f'{e.target()}, color({g.vp.isimcolor[e.target()]})')


    dg.view(f'{benchmark}.simulator', quiet=False)
    '''


benchmark = 'svd_tall_skinny_matrix_800k_x_20k_vanilla_7e91c4b7'
task_style = {}
color_assignment(benchmark, task_style)
g = build_graph(benchmark, task_style)
update_runtime_state(benchmark, g, task_style)

#plot_graph(g)
#task_style



# 4.2.1 problem Variables
# Input Parameters - Sets
Jobs = []
for v in g.vertices():
    Jobs.append(f'j{v}')

print(Jobs)

Agents = ['10.255.23.108', '10.255.23.109','10.255.23.110', '10.255.23.115']

#Agents = ['w1', 'w2', 'w3', 'w4'] # workers
n = len(Jobs)
m = len(Agents)

# Indicator Variables
# Immediate job precedence graph - specifies DAG
# P[job1, job2] == 1 if job1 directly precedes job2
P = defaultdict(lambda:0)
for e in g.edges():
    P[f'j{e.source()}',f'j{e.target()}'] = 1

print(P)

# Agent ability matrix
# B[job, agent] == 1 if job can be performed by agent
B = defaultdict(lambda:1)
#for agent in Agents:
#    if agent != 'cpu':
#        B['start', agent] = 0 # must start on cpu
#        B['end', agent] = 0 # must start on cpu

# DATA Values
# Execution Delay Matrix - Time cost of performing Jobs[i] on Agent[j]
D = defaultdict(lambda:0)
for v in g.vertices():
    for a in Agents:
        D[f'j{v}', a] = task_style[g.vp.name[v]]['runtime']


# Communication Delay matrix - Cost of sending results of job from
# agent to agent
bw = 10*(1<<30)/(1<<3)

C = defaultdict(lambda:0)
for v in g.vertices():
    for a in Agents:
        for b in Agents:
            C[f'j{v}', a, b] = 0 if a == b else round(task_style[g.vp.name[v]]['output_size']/bw, 3) # 0 --> cost_serialization, :q

print(C)
#i += 1
# Job Release Times - Additional constraints on availablility of Jobs
# R = np.zeros(n)
R = defaultdict(lambda:0)

# Maximum makespan
M = 100

if __name__ == '__main__':
    # Set up the Mixed Integer Linear Program
    prob, X, S, Cmax = schedule(Jobs, Agents, D, C, R, B, P, M)

    prob.solve()

    print("Makespan: ", value(Cmax))
    sched = jobs_when_where(prob, X, S, Cmax)
    print("Schedule: ", sched)

    sched2 = []
    for j in sched:
        new = j + (j[1] + D[j[0], j[2]], g.vp.name[int(j[0].replace('j', ''))])
        sched2.append(new)
    
    print("Schedule: ", sched2)
