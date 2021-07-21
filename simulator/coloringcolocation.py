#!/usr/bin/python3

import os
import graph_tool.all as gt
from graphviz import Digraph


wd = '/home/mania/Northeastern/MSR/serverless/simulator/test_data'

dag_files = [os.path.join(wd, f) for f in os.listdir(wd) if os.path.isfile(os.path.join(wd, f)) and f.endswith('.g')]

def generate_chain_color(g):
    pass



def build_dag_from_file(dagf):
    with open(dagf, 'r') as fd:
        g = gt.Graph(directed=True)
        g.vp.name = g.new_vertex_property('string')
        g.vp.vid = g.new_vertex_property('string')
        g.vp.color = g.new_vertex_property('int', 0)
        vid2v = {}
        lines = fd.read().split('\n')[:-1]
        for ln in lines:
            if ln.startswith('v'):
                _, vid, vname = ln.split(',', 2)
                v = g.add_vertex()
                g.vp.vid[v] = vid
                g.vp.name[v] = vname
                vid2v[vid] = v
                print(vname)

        for ln in lines:
            if ln.startswith('e'):
                _, src, dst, *args = ln.split(',')
                g.add_edge(vid2v[src], vid2v[dst])
        return g

def plot_graph(g, benchmark):
    print(benchmark)
    dg = Digraph('G', filename=f'{benchmark}.gv', format='png')
    dg.graph_attr['rankdir'] = 'LR'
    dg.attr('node', shape='box', style="filled,solid",
            penwidth="1",
            fillcolor="#f0f0f0",
            color='#f0f0f0')
    dg.node(f'{benchmark}')
    for v in g.vertices():
        vid = g.vp.vid[v]
        vname = g.vp.name[v]
        vcolor = g.vp.color[v]
        dg.attr('node', shape='ellipse', style="filled,solid",
                penwidth="3",
                fillcolor="#f0f0f0",
                color="#252525")
        dg.node(f'{vid}')
    for e in g.edges():
        sid = g.vp.vid[e.source()]
        scolor = g.vp.color[e.source()]
        tid = g.vp.vid[e.target()]
        tcolor = g.vp.vid[e.target()]
        dg.edge(f'{sid}',
                f'{tid}')
    dg.view(f'{benchmark}')


for gfile in dag_files:
    benchmark = gfile.rsplit('/', 1)[1].rsplit('.')[0]
    g = build_dag_from_file(gfile)
    plot_graph(g, benchmark)


