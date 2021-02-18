#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import time
import timeit
import datetime
import sys


name = 'svd_tall_skinny_matrix_2M_x_4k'

client = Client('10.255.23.115:8786', name = name)


# Compute the SVD of 'Tall-and-Skinny' Matrix

matrix_dim = (2000000, 4000)
chunk_dim = (20000, 4000)

X = da.random.random(matrix_dim, chunks=chunk_dim)
u, s, v = da.linalg.svd(X)

# Start the computation.
start = datetime.datetime.now()
results = v.compute(scheduler='distributed')

end = datetime.datetime.now()

print(f'Parallelizing svm is done in {end - start}')

v.visualize(filename=f'{name}.png')



