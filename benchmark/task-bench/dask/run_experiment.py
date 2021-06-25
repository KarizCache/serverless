#!/usr/bin/python3
import subprocess
#from subprocess import PIPE, Popen
import os
from pathlib import Path

scheduler='chaincolorch'
exp = 'dom'
width = 2
height = 4
rep = 5

path = '/opt/dask-distributed/benchmark/stats'
for r in range(0, rep):
    proc = subprocess.Popen(['/local0/serverless/task-bench/dask/task_bench.py', '-scheduler', '10.255.23.115:8786',
        '-name', f'{exp.replace("_", "")}{width}x{height}1GB1B', '-type', f'{exp}', '-output', '1073741824', '-kernel', 
        'compute_bound', '-iter', '1073741824', '-width', f'{width}', '-steps', f'{height}', '-vv' ],
        stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    stdout_ln = proc.communicate()[0].decode().split('\n')
    #print ('\tstdout:', stdout_ln)

    paths = sorted(Path(path).iterdir(), key = os.path.getmtime)
    benchmark = str(paths[-1]).rsplit('/', 1)[1].split('.')[0]
    print(benchmark)


    # Elapsed Time 7.346152e+01 seconds
    runtime = 0
    for ln in stdout_ln:
        if ln.startswith('Elapsed'):
            runtime = ln.split(' ')[2]
            print(f'Welcome {runtime}')
    with open('stats.csv', 'a') as fd:
        fd.write(f'{benchmark},{exp.replace("_", "")}{width}x{height}1GB1B,{scheduler},{exp},{width},{height},{runtime}\n')


#for p in paths:
#    print(p)
