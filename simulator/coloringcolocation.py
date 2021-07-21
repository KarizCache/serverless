#!/usr/bin/python3

import os
import graph_tool.all as gt
from graphviz import Digraph


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
        def DFS( current_ts, open_time, close_time, current_time, sorted_nodes=None):
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
            assert(not current_ts.key in open_time)
            open_time[current_ts.key] = current_time
            current_time += 1
            for t in (current_ts.dependents):
                logger.info("\"%s\" -> \"%s\"", current_ts.key, t.key)
                if (not t.key in close_time):
                    assert(not t.key in open_time) #cycle!
                    current_time = DFS(t, open_time, close_time, current_time, sorted_nodes)
                else: #don't visit, node is closed (visited)
                    assert(t.key in open_time) #error (closed but never open!)
            close_time[current_ts.key] = current_time
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

        # if the current task has no color
        # this task has 0 parents with no color
        # Mania: # I think this algorithm was originally works with O(V + E)
        # and after this modification in worst case it runs with O(V^2) I should double check this claim.
        for task_key, ts in self.tasks.items():
            isroot = True
            for dep in ts.dependencies:
                if dep.color is None:
                    isroot = False
                    break

            #if (len(ts.dependencies) == 0 and (ts.state == "released" or ts.state == "no-worker")):
            if (isroot  and (ts.state == "released" or ts.state == "no-worker")):
                # also if the color of all of the parents is not none and this task has no color.
                starting_tasks.append(ts)

        print(f'{Fore.GREEN} starting tasks are: {starting_tasks}{Style.RESET_ALL}')
        for starting_ts in starting_tasks:
            current_time = DFS(starting_ts, open_time, close_time, current_time, sorted_nodes)


        # Check: assert that the close_times of sorted_nodes is sorted and has no duplicates
        #cts = [n.close_time for n in sorted_nodes]
        #assert(cts == sorted(cts))
        #assert(len(cts) == len(set(cts)))

        #Now we do the chain partitioning. Will store the results in the task itself,
        #in the color field.
        # TODO: For now, assuming the nodes haven't been colored. It's possible that they
        # would have been, in which case it would be good to extend the chains

        logger.info("Chain Coloring...")
        logdir = dask.config.get("distributed.scheduler.logdir")
        cfd = open(os.path.join(logdir, f'{client.split("-")[1]}.colors'), 'a')

        c = random.randrange(1<<32) #15781
        # The topo order makes us start as far back as possible, and color all nodes
        for ts in sorted_nodes:
            if ts.color is not None:
                continue

            print(f'{Fore.CYAN} task dependencies are {ts.dependencies} {Style.RESET_ALL}')
            inhereted_color = None
            for parent_ts in ts.dependencies:
                if parent_ts.transferred_color: continue
                c = int(parent_ts.color)
                parent_ts.transferred_color = 1
                break

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
                if len(current_ts.dependents) == 0:
                    current_ts.child_color = current_ts.color
                else:
                    for dep_ts in sorted(current_ts.dependents, key = lambda t:close_time[t.key], reverse=True):
                        if dep_ts.color is None:
                            dep_ts.color = str(c)
                            current_ts.transferred_color = 1;
                            go_on = True
                            break
                    current_ts.child_color = dep_ts.color # coloring the child
                logger.info(" \"%s\" [style=filled fillcolor=\"/paired12/%s\" color=\"/paired12/%s\"]", current_ts.key, current_ts.color, current_ts.child_color)
                cfd.write(f'{current_ts.key}___{current_ts.color}\n')
                if go_on:
                    current_ts = dep_ts
                else:
                    break
            # Reached the end of the chain, next color
            #c += 1
            c =  random.randrange(1<<32) #15781
        #done coloring

        #RF this is just for basic statistics about the graph
        count_edges = 0
        count_nodes_with_deps = 0
        for task_key, ts in self.tasks.items():
            if ts.color is None or ts.child_color is None:
                logger.warn(" uncolored task: {}".format(task_key))
            e = len(ts.dependencies)
            if (e > 0):
                count_nodes_with_deps += 1
                count_edges += e

        # This assumes that for every node with at least one dependency, then one of the inputs
        # should be local if we are scheduling according to chains.
        logger.info("Graph Stats: tasks: %d edges: %d min_local_hit: %d max_local_miss: %d",
                        len(self.tasks),
                        count_edges,
                        count_nodes_with_deps,
                        count_edges - count_nodes_with_deps)


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


