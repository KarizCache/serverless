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

data = [1, 2, 3, 4, 5, 6, 7, 8]
results = []

tic = time.perf_counter()
for x in data:
    y = inc(x)
    results.append(y)

total = sum(results)
toc = time.perf_counter()

print(total)
print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")

