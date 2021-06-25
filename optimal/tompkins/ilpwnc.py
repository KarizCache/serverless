"""
This module describes a highly flexible scheduling problem by encoding it as a
Mixed Integer Linear Program (MILP) using the pulp framework[1].
This framework can then call any of several external codes[2] to solve

This formulation of the MILP was specified in chapters 4.1, 4.2 in the
following masters thesis. Section and equation numbers are cited.

"Optimization Techniques for Task Allocation and Scheduling in Distributed Multi-Agent Operations"
by
Mark F. Tompkins, June 2003, Masters thesis for MIT Dept EECS[3]

[1] http://code.google.com/p/pulp-or/ -- "sudo easy_install pulp"
[2] On ubuntu we used "sudo apt-get install glpk"
[3] http://dspace.mit.edu/bitstream/handle/1721.1/16974/53816027.pdf?sequence=1
"""

from pulp import (LpVariable, LpProblem, LpMinimize, LpInteger, LpContinuous,
        lpSum, LpStatus)
from collections import defaultdict
from tompkins.util import reverse_dict, dictify, merge, intersection

def schedule(Tasks, Comms, Workers, Links, D, C, P, M):
    """
    Finds the optimal scheduling of Jobs to Agents (workers) given
    Tasks - a set of Jobs (an iterable of hashable objects)
    Comms - a set of communications tasks
    Workers - a set of workers (an iterable of hashable objects)
    Links - a set of links between workers, the indexes should be the same as workers indexes.  
    D -  Dictionary detailing execution cost of running jobs on agents :
         D[job, agent] = time to run job on agent
    C -  Dictionary detailing Communication cost sending results  of jobs
         between  agents :
         C[job, a1, a2] = time to comm results of job on from a1 to a2
    R -  Additional constraint on start time of jobs, usually defaultdict(0)
    B -  Dict saying which jobs can run on which agents:
         B[job, agent] == 1 if job can run on agent
    P -  Dictionary containing precedence constraints - specifies DAG:
         P[job1, job2] == 1 if job1 immediately precedes job2
    M -  Upper bound on makespan

    Returns
    prob - the pulp problem instance
    X  - Dictionary detailing which jobs were run on which agents:
         X[job][agent] == 1 iff job was run on agent
    S  - Starting time of each job
    N  - Dictionary detailing which communication task were run on which link:
         N[i][j][src][dst] edge i->j in the DAG
         X[i][j][src][dst] == 1 iff edge i->j executed on src and dst.
    SN - Start time of communication task  
    Cmax - Optimal makespan

    """

    # Set up global precedence matrix
    Q = PtoQ(P)
    #LQ = PtoLQ(P)

    # PULP SETUP
    # The prob variable is created to contain the problem data
    prob = LpProblem("Scheduling Problem - Tompkins Formulation", LpMinimize)

    # The problem variables are created
    # X - 1 if job is scheduled to be completed by agent
    X = LpVariable.dicts("X", (Tasks, Workers), 0, 1, LpInteger)

    # S - Scheduled start time of job
    S = LpVariable.dicts("S", Tasks, 0, M, LpContinuous)

    # The problem variables are created
    # X - 1 if job is scheduled to be completed by agent
    N = LpVariable.dicts("N", (Comms, Links), 0, 1, LpInteger)


    # SN - Scheduled start time of job
    SN = LpVariable.dicts("SN", Comms, 0, M, LpContinuous)
    
    
    # Theta - Whether two jobs overlap
    Theta = LpVariable.dicts("Theta", (Tasks, Tasks), 0, 1, LpInteger)
    # Delta - Whether two links overlap
    Delta = LpVariable.dicts("Delta", (Comms, Comms), 0, 1, LpInteger)
    
    # Miu - Whether two links overlap
    #Miu = LpVariable.dicts("Miu", (Comms, Comms), 0, 1, LpInteger)
  
    # Makespan
    Cmax = LpVariable("C_max", 0, M, LpContinuous)

    #####################
    # 4.2.2 CONSTRAINTS #
    #####################

    # Objective function
    prob += Cmax

    # Subject to:
    # 4-1 all communication tasks should be finished before the Makespan. 
    # 4-1 Cmax is greater than the ending schedule time of all jobs
    # Constraint 1-1
    for ts in Tasks:
        prob += Cmax >= S[ts] + lpSum([D[ts, w] * X[ts][w] for w in Workers])

    # Mania 4-1: part 2
    # Constraint 1-2
    for ts in Comms:
        prob += Cmax >= SN[ts] + lpSum([C[ts[0], l[0], l[1]] * N[ts][l] for l in Links])


    # 4-3 specifies that each job must be assigned once to exactly one agent
    # constraint 2 
    for ts in Tasks:
        prob += lpSum([X[ts][w] for w in Workers]) == 1

    # Mania 4-3: part 2
    # Constraint 6
    # each communication task must be assigned once exactly to one link
    for ts in Comms:
        prob += lpSum([N[ts][l] for l in Links]) == 1


    # build Inv 

    # Mania
    # Constraint 7
    # If task i executes on worker w, all of its incoming edges should be sent to w as well.
    for ts in Tasks:
        for tc in P:
            if ts != tc[1]: continue
            for w  in Workers:
                prob +=  X[ts][w] - lpSum([N[tc][l] for l in Links if l[1] == w]) == 0

  
    # Mania
    # Constraint 8
    # If task i executes on worker w, all of its incoming edges should be sent to w as well.
    for ts in Tasks:
        for tc in P:
            if ts != tc[0]: continue
            for w  in Workers:
                prob +=  X[ts][w] - lpSum([N[tc][l] for l in Links if l[0] == w]) == 0

    # Mania
    # Constraint 9
    # Task ts must be started after all its inputs are transferred to it. 
    for ts in Tasks:
        for tc in P:
            if ts != tc[1]: continue
            for w in Workers:
                for l in Links:
                    prob += S[ts] >= SN[tc] + C[tc[0], l[0], l[1]]*N[tc][l]


    
    # Mania
    # Constraint 10
    # Communication task tc must be started after the execution of the parent task is done. 
    for ts in Tasks:
        for tc in P:
            if ts != tc[0]: continue
            for w in Workers:
                prob += SN[tc] >= S[ts] + D[ts, w]*X[ts][w]
    
    
    
    # FIXME: I don't need this
    # Lets replace this one with the new constraints 
    # 4-4 a job cannot start until its predecessors are completed and data has
    # been communicated to it if the preceding jobs were executed on a
    # different agent
    #for (j,k), prec in P.items():
    #    if prec>0: # if j precedes k in the DAG
    #        prob += S[k]>=S[j]
    #        for a in Agents:
    #            for b in Agents:
    #                if B[j,a] and B[k,b]: # a is capable of j and b capable of k
    #                    prob += S[k] >= (S[j] +
    #                            (D[j,a] + C[j,a,b]) * (X[j][a] + X[k][b] -1))


    # FIXME: Mania I don't need this
    # 4-5 a job cannot start until after its release time
    #for job in Jobs:
    #    if R[job]>0:
    #        prob += S[job] >= R[job]

    # Collectively, (4-6) and (4-7) specify that an agent may process at most
    # one job at a time

    # Mania
    # Constraint 11
    # Two incomming stream to the same node W cannot overlap
    for tc1 in Comms:
        for tc2 in Comms:
            if tc1 == tc2 or Q[tc1[0], tc2[0]] != 0: continue
            prob += SN[tc2] - lpSum([C[tc1[0],l[0], l[1]]*N[tc1][l] for l in Links]) - SN[tc1] >= (-M*Delta[tc1][tc2]) ;
            prob += SN[tc2] - lpSum([C[tc1[0],l[0], l[1]]*N[tc1][l] for l in Links]) - SN[tc1] <= (M*(1-Delta[tc1][tc2]));

    
    # Mania
    # Constrain 12
    # Two incomming stream cannot overlap.
    for tc1 in Comms:
        for tc2 in Comms:
            if tc1 == tc2: continue
            for w in Workers:
                prob += lpSum(N[tc1][l] for l in Links if l[1] == w) + lpSum(N[tc2][l] for l in Links if l[1] == w) + Delta[tc1][tc2] + Delta[tc2][tc1] <= 3


    # Mania
    # Constraint 13
    # Two incomming stream to the same node W cannot overlap
    for tc1 in Comms:
        for tc2 in Comms:
            if tc1 == tc2 or Q[tc1[0], tc2[0]] != 0: continue
            prob += SN[tc2] - lpSum([C[tc1[0],l[0], l[1]]*N[tc1][l] for l in Links]) - SN[tc1] >= (-M*Delta[tc1][tc2]);
            prob += SN[tc2] - lpSum([C[tc1[0],l[0], l[1]]*N[tc1][l] for l in Links]) - SN[tc1] <= (M*(1-Delta[tc1][tc2]));
    
    # Mania
    # Constraint 14
    # Two outgoing stream cannot overlap.
    for tc1 in Comms:
        for tc2 in Comms:
            if tc1 == tc2: continue
            for w in Workers:
                prob += lpSum(N[tc1][l] for l in Links if l[0] == w) + lpSum(N[tc2][l] for l in Links if l[0] == w) + Delta[tc1][tc2] + Delta[tc2][tc1] <= 3 
    
    # 4-6
    for j in Tasks:
        for k in Tasks:
            if j==k or Q[j,k]!=0:
                continue
            prob += S[k] - lpSum([D[j,a]*X[j][a] for a in Workers]) - S[j] >= (
                    -M*Theta[j][k])
            # The following line had a < in the paper. We've switched to <=
            # Uncertain if this is a good idea
            prob += S[k] - lpSum([D[j,a]*X[j][a] for a in Workers]) - S[j] <= (
                    M*(1-Theta[j][k]))
    # 4-7 if two jobs j and k are assigned to the same agent, their execution
    # times may not overlap
    for j in Tasks:
        for k in Tasks:
            for a in Workers:
                prob += X[j][a] + X[k][a] + Theta[j][k] + Theta[k][j] <= 3

    return prob, X, S, N, SN, Cmax

def PtoQ(P):
    """
    Construct full job precedence graph from immediate precedence graph

    Inputs:
    P - Dictionary encoding the immediate precedence graph:
        P[job1, job2] == 1 iff job1 immediately precedes job2

    Outputs:
    Q - Dictionary encoding full precedence graph:
        Q[job1, job2] == 1 if job1 comes anytime before job2
    """
    Q = defaultdict(lambda:0)
    # Add one-time-step knowledge
    for (i,j), prec in P.items():
        Q[i,j] = prec

    changed = True
    while(changed):
        changed = False
        for (i,j), prec in P.items(): # i comes immediately before j
            if not prec:
                continue
            #for (jj, k), prec in Q.items(): # j comes sometime before k
            for (jj, k) in list(Q): # j comes sometime before k
                prec = Q[(jj, k)]
                if jj != j or not prec:
                    continue
                if not Q[i,k]: # Didn't know i comes sometime before k?
                    changed = True # We changed Q
                    Q[i,k] = 1 # Now we do.
    return Q

def jobs_when_where(prob, X, S, N, SN, Cmax):
    """
    Take the outputs of schedule and produce a list of the form
    [(job, start_time, agent),
     (job, start_time, agent),
     ... ]

    sorted by start_time.

    >>> sched = jobs_when_where(*make_ilp(env, ... ))
    """
    status = LpStatus[prob.solve()]
    if status != 'Optimal':
        print ("ILP solver status: ", status)

    def runs_on(job, X):
        return [k for k,v in X[job].items() if v.value()][0]

    sched = [(job, time.value(), runs_on(job,X)) for job, time in S.items()]
    sched.extend([(job, time.value(), runs_on(job,N)) for job, time in SN.items()])
    return sched #list(sorted(sched, key=lambda x:x[1:]))
