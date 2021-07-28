#!/usr/bin/python3
import copy
import os
import sys

import graph_tool.all as gt

from colorama import Fore, Style
from collections import deque, defaultdict
from graphviz import Digraph
import numpy as np

wd = '/home/mania/Northeastern/MSR/serverless/simulator/test_data'

dag_files = [os.path.join(wd, f) for f in os.listdir(wd) if os.path.isfile(os.path.join(wd, f)) and f.endswith('.g')]

def generate_chain_color(g):
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


def build_dag_from_file(dagf):
    with open(dagf, 'r') as fd:
        g = gt.Graph(directed=True)
        g.vp.name = g.new_vertex_property('string')
        g.vp.vid = g.new_vertex_property('string')
        g.vp.color = g.new_vertex_property('int', -1)
        g.vp.tmp_color = g.new_vertex_property('int', -1)
        g.vp.cset = g.new_vertex_property('object')
        vid2v = {}
        lines = fd.read().split('\n')[:-1]
        for ln in lines:
            if ln.startswith('v'):
                _, vid, vname = ln.split(',', 2)
                v = g.add_vertex()
                g.vp.vid[v] = vid
                g.vp.name[v] = vname
                g.vp.color[v] = -1
                g.vp.tmp_color[v] = -1
                g.vp.cset[v] = []

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
        dg.node(f'{vid}-{vcolor}')
    for e in g.edges():
        sid = g.vp.vid[e.source()]
        scolor = g.vp.color[e.source()]
        tid = g.vp.vid[e.target()]
        tcolor = g.vp.color[e.target()]
        dg.edge(f'{sid}-{scolor}',
                f'{tid}-{tcolor}')
    dg.view(f'{benchmark}')


def assign_heirarchy(n_chains, g):
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


def checkpoint_colors(g):
    for v in g.vertices():
        g.vp.cset[v].append(g.vp.color[v])


def recolor_graph(g, merged):
    checkpoint_colors(g)
    for v in g.vertices():
        g.vp.color[v] = merged[g.vp.color[v]]


def merge_chains(n_chains, H):
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
            print(f'chose {m_color} to merge with {ch}')
            merged[m_color] = ch
            H[:, m_color] = 0; H[m_color, :] = 0;
            H[:, ch] = 0; H[ch, :] = 0;
    return merged

def finalize(g):
    n_steps = len(g.vp.cset[0])
    fc = {}

    print(n_steps)
    for s in range(n_steps - 1, -1, -1):
        fc_old = copy.deepcopy(fc)
        print(fc_old)
        for v in g.vertices():
            c = g.vp.cset[v][s]
            #if c not in fc_old:
            if s == n_steps - 1:
                fc[c] = 0
            else:
                c_old = g.vp.cset[v][s+1]
                fc[c] = fc_old[c_old] << 1;
                if c != c_old:
                    fc[c] += 1
                #print(c, c_old, fc[c], fc_old[c_old])
    print(fc)
    for c in fc:
        print(format(fc[c], '#011b'), '\t' , c, fc[c])
    for v in g.vertices():
        c = g.vp.tmp_color[v]
        g.vp.color[v] = fc[c]




for gfile in dag_files:
    benchmark = gfile.rsplit('/', 1)[1].rsplit('.')[0]
    g = build_dag_from_file(gfile)
    n_chains = generate_chain_color(g)

    while True:
        print(f'\n\n\n# of chains are {n_chains}')
        #plot_graph(g, benchmark)
        H = assign_heirarchy(n_chains, g)
        #print(H)

        if not H.sum():
            break
        #print(H)
        merged = merge_chains(n_chains, H)
        recolor_graph(g, merged)

    checkpoint_colors(g)
    for v in g.vertices():
        print(v, g.vp.cset[v])

    finalize(g)
    plot_graph(g, benchmark)
    #input()