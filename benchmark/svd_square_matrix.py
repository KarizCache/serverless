#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import time
import timeit
import datetime

name = 'svd_square_matrix_10k_x_2k'
client = Client('10.255.23.115:8786', name=name)


matrix_dim = (1000, 1000)
chunk_dim = (200, 200)


# Compute the SVD of 'Tall-and-Skinny' Matrix 
X = da.random.random(matrix_dim, chunks=chunk_dim)
u, s, v = da.linalg.svd_compressed(X, k=5)


start = datetime.datetime.now()
# Start the computation.
v.compute(scheduler='distributed')

end = datetime.datetime.now()

print(f'SVD Square matrix is done in {end - start}')


v.visualize(filename=f'{name}.png')

