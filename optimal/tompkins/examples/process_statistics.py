#!/usr/bin/python3

import os
import json
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
import utils

from tompkins.ilpwnc import schedule, jobs_when_where
from collections import defaultdict
from pulp import value

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

def get_benchmarks():
    benchmarks = {}
    for _file in os.listdir(stats_dir):
        try:
            bnch = _file.rsplit('.', 1)[0]
            assert os.path.isfile(os.path.join(stats_dir, f'{bnch}.g')) \
                    and os.path.isfile(os.path.join(stats_dir, f'{bnch}.json')) \
                    and os.path.isfile(os.path.join(stats_dir, f'{bnch}.colors'))
                    
            app = bnch #, scheduler =  bnch.rsplit(':', 1)
            scheduler = 'vanilla'
            benchmarks[bnch] = {'app': app, 'scheduler': scheduler, 'benchmark': bnch}
        except AssertionError:
            pass
    return benchmarks


def build_graph(benchmark):
    css_colors = list(mcolors.CSS4_COLORS.keys())
    gfile = os.path.join(stats_dir, f'{benchmark}.g')

    with open(gfile, 'r') as fd:
        raw = fd.read().split('\n')
        g = gt.Graph(directed=True)
        vid_to_vx = {}
        name_to_vid = {}

        g.vertex_properties['name'] = g.new_vertex_property("string")
        g.vertex_properties['worker'] = g.new_vertex_property("string")
        g.vertex_properties['color'] = g.new_vertex_property("string", '#e0e0e0')
        g.vertex_properties['icolor'] = g.new_vertex_property("int")
        g.vertex_properties['output_size'] = g.new_vertex_property("int")
        g.vertex_properties['runtime'] = g.new_vertex_property("int")

        for ln in raw:
            if ln.startswith('v'):
                _, vid, name, runtime = ln.split(',', 3)
                v = g.add_vertex()
                vid_to_vx[vid] = v
                name_to_vid[name] = vid
                g.vp.name[v] = name
                g.vp.runtime[v] = int(runtime) # 1 second
                g.vp.output_size[v] = 1<<30 # 1GB
                g.vp.color[v] = '#e0e0e0'


        for ln in raw:
            if ln.startswith('e'):
                _, vsrc, vdst, _ = ln.split(',', 3)
                g.add_edge(vid_to_vx[vsrc], vid_to_vx[vdst])
    return g


def get_runtime_statistics(benchmark):
    tasks = []
    statistics = {}
    jfile = os.path.join(stats_dir, f'{benchmark}.json')
    with open(jfile, 'r') as fd:
        stats = ast.literal_eval(fd.read())
        for ts in stats:
            ops = 'ts'; #ts.replace("(", '').replace(')', '').split("'")[1].split('-')[0]
            statistics[ts] = {'key': ts, 'op': ops,
                    'output_size': stats[ts]['msg']['nbytes'],  'worker': stats[ts]['worker'].split(':')[1].replace('/', '')}

            startsstops = stats[ts]['msg']['startstops']
            for ss in startsstops:
                if ss['action'] == 'compute':
                    statistics[ts]['compute_end'] = ss['stop']
                    statistics[ts]['compute_start'] = ss['start']
                    statistics[ts]['runtime'] = ss['stop'] - ss['start']

    cfile = os.path.join(stats_dir, f'{benchmark}.colors')
    with open(cfile, 'r') as cfd:
        raw = cfd.read().split('\n')
        for ln in raw:
            if not ln: 
                continue
            ts, color = ln.split(',')
            #ts += ')'
            statistics[ts]['color'] = int(color)
    return statistics



def plot_graph(g, benchmark, optimal=False):        
    print(benchmark["benchmark"])
    post = ".optimal" if optimal else ""
    dg = Digraph('G', filename=f'{benchmark["benchmark"]}{post}.gv', format='png')
    for v in g.vertices():
        dg.attr('node', shape='ellipse', style="filled,solid",
                penwidth="3",
                fillcolor=g.vp.color[v],
                color=worker_color[g.vp.statistics[v]['worker']])
        #if benchmark['scheduler'] == "vanilla":
        #    dg.node(f'{v}')
        #else:
        dg.node(f'{v}, color({g.vp.icolor[v]})')

    for e in g.edges():
        #if benchmark['scheduler'] == "vanilla":
        #    dg.edge(f'{e.source()}', f'{e.target()}')
        #else:
        dg.edge(f'{e.source()}, color({g.vp.icolor[e.source()]})', 
                f'{e.target()}, color({g.vp.icolor[e.target()]})')
    dg.view(os.path.join(f'{results_dir}',f'{benchmark["benchmark"]}{post}'), quiet=False)


import pulp as pl
from colorama import Fore, Style


def find_optimal(g):
    n_workers = 6
    workers = [f'w{i}' for i in range(n_workers)]
    
    links = defaultdict(lambda:0)
    for src in workers:
        for dst in workers:
            links[f'{src}',f'{dst}'] = f'{src}->{dst}'

    # Maximum makespan
    M = 100
    tasks = defaultdict(lambda:0)
    for v in g.vertices():
        tasks[f't{v}'] = 0

    comms = defaultdict(lambda:0)
    for e in g.edges():
        comms[f't{e.source()}',f't{e.target()}'] = 0

    P = defaultdict(lambda:0)
    for e in g.edges():
         P[f't{e.source()}', f't{e.target()}'] = 1
    
    # computation
    D = defaultdict(lambda:0)
    for v in g.vertices():
        for w in workers:
            D[f't{v}', w] = g.vp.runtime[v]

    # Communication Delay matrix - Cost of sending results of job from
    # agent to agent
    bw = 8*(1<<30)/(1<<3)
    C = defaultdict(lambda:0)
    for v in g.vertices():
        for src in workers:
            for dst in workers:
                C[f't{v}', src, dst] = 0 if src == dst else g.vp.output_size[v]/bw # 0 --> cost_serialization, :q
    

    # Set up the Mixed Integer Linear Program
    prob, X, S, N, SN, Cmax = schedule(tasks, comms, workers, links, D, C, P, M)
    solver = pl.GUROBI_CMD()
    prob.solve(solver)
    #prob.solve()
    print(N)

    print("Makespan: ", value(Cmax))
    sched = jobs_when_where(prob, X, S, N, SN, Cmax)
    print("Schedule: ", sched)

    #sched2 = []
    #for j in sched:
    #    new = j + (j[1] + D[j[0], j[2]], g.vp.name[int(j[0].replace('t', ''))])
    #    sched2.append(new)
    #print("Schedule: ", sched2)
    return sched




worker_color = {'10.255.23.108': '#e41a1c',
    '10.255.23.109': '#984ea3',
    '10.255.23.110': '#ff7f00',
    '10.255.23.115': '#4daf4a'}

results_dir = '/local0/serverless/evaluation/plots' 
stats_dir='/opt/dask-distributed/benchmark/stats'
#benchmarks = get_benchmarks()
benchmarks = ['ingressw6']
for bnch in benchmarks:
    #if bnch != 'ingress': 
        #print(f'skip {bnch} for now')
    #    continue

    print(f'process {bnch}')
    g = build_graph(bnch)
    sched2 = find_optimal(g)

    with open(f'/opt/dask-distributed/benchmark/stats/{bnch.split("_")[0]}.optimal', 'w') as fd:
        for s in sched2:
            if isinstance(s[0], tuple): continue
            print(s)
            fd.write(f'{s[0]},{s[1]},{s[2]}\n')
            #v = int(s[0].replace('t', ''))
            #g.vp.worker[v] = s[2] 
    break
