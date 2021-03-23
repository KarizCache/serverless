#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import operator 

import time
import timeit

import datetime
import sys


K = 1 << 10
mx = 64*K
cx = 4*K

name = 'matrixmul_64k_x_4k'

client = Client('10.255.23.115:8786', name = name)

x = da.random.random((mx, mx), chunks = (cx, cx))
y = da.random.random((mx, mx), chunks = (cx, cx))
z = da.matmul(x, y)

# Start the computation.

start = datetime.datetime.now()
results = z.compute(scheduler='distributed')
end = datetime.datetime.now()



print(f'Matrix multiplication is done in {end - start}')

z.visualize(filename=f'{name}.png')

