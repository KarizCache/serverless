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

# 4.2.1 problem Variables
# Input Parameters - Sets
Jobs = ['j0', 'j1', 'j2', 'j3', 'j4', 'j5', 'j6', 'j7', 'j8', 'j9', 'j10', 'j11']
Agents = ['w1', 'w2', 'w3', 'w4'] # workers
n = len(Jobs)
m = len(Agents)

# Indicator Variables
# Immediate job precedence graph - specifies DAG
# P[job1, job2] == 1 if job1 directly precedes job2
P = defaultdict(lambda:0)
P['j10','j3'] = 1
P['j7','j5'] = 1
P['j11','j9'] = 1
P['j6','j0'] = 1
P['j3','j4'] = 1
P['j5','j4'] = 1
P['j9','j4'] = 1
P['j0','j4'] = 1
P['j4','j1'] = 1
P['j1','j8'] = 1
P['j8','j2'] = 1

# Agent ability matrix
# B[job, agent] == 1 if job can be performed by agent
B = defaultdict(lambda:1)
for agent in Agents:
    if agent != 'cpu':
        B['start', agent] = 0 # must start on cpu
        B['end', agent] = 0 # must start on cpu

# DATA Values
# Execution Delay Matrix - Time cost of performing Jobs[i] on Agent[j]
D = defaultdict(lambda:0)
D['gemm','cpu'] = 10 # gemm on cpu
D['gemm','gpu'] = 3 # gemm on gpu
D['sum','cpu'] = 5 # sum on cpu
D['sum','gpu'] = 9 # sum on gpu

# Communication Delay matrix - Cost of sending results of job from
# agent to agent
C = defaultdict(lambda:0)
for a,b in [('cpu', 'gpu'), ('gpu', 'cpu')]:
    # cost to communicate matrices is 3 (symmetric)
    C['start', a,b] = 3
    C['gemm', a,b] = 3
    # cost to communicate scalar is .01 (symmetric)
    C['sum', a,b] = .01

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
