#!/usr/bin/python3

from dask.distributed import Client
from dask import delayed

import time
import timeit


def inc(x):
    time.sleep(1)
    return x + 1

def add(x, y):
    time.sleep(1)
    return x + y


client = Client('127.0.0.1:8786')

#tic = time.perf_counter()
#x = inc(1)
#y = inc(2)
#z = add(x, y)
#toc = time.perf_counter()
#print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")

tic = time.perf_counter()
x = delayed(inc)(1)
y = delayed(inc)(2)
z = delayed(add)(x, y)

z.compute()

print(z)

toc = time.perf_counter()
print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")

z.visualize()
