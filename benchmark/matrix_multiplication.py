#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import operator 

import time
import timeit

client = Client('10.255.23.115:8786')

print('I am before compute')

x = da.random.random((10000, 10000), chunks = (1000, 1000))
y = da.random.random((10000, 10000), chunks = (1000, 1000))
z = da.matmul(x, y)

# Start the computation.

results = z.compute(scheduler='distributed')


print("Done with the compute")

z.visualize(filename='matrix_multipication.png')
