#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import operator 

import time
import timeit
import datetime

MB=1<<20
KB = 1<<10
size = 16*KB
size_s = '16KB'
name = f'tree_reduction_{size_s}'


client = Client('10.255.23.115:8786', name = name)

L = range(size)
while len(L) > 1:
  L = list(map(delayed(operator.add), L[0::2], L[1::2]))


# Start the computation.
start = datetime.datetime.now()
results = L[0].compute(scheduler='distributed')
end = datetime.datetime.now()


print(f'Tree reduction with size {size_s} is done in {end - start}')

L[0].visualize(filename=f'{name}.png')
