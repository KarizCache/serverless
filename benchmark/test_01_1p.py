#!/usr/bin/python3

from dask.distributed import Client
from dask import delayed

import time
import timeit


def inc(x):
    time.sleep(2)
    return x + 1

def add(x, y):
    time.sleep(2)
    return x + y


client = Client('10.255.23.115:8786')

data = [1, 2, 3, 4, 5, 6, 7, 8]
results = []

tic = time.perf_counter()
results = []

for x in data:
    y = delayed(inc)(x)
    results.append(y)
    
total = delayed(sum)(results)
print("Before computing:", total)  # Let's see what type of thing total is
result = total.compute(scheduler='distributed')
print("After computing :", result)  # After it's computed

toc = time.perf_counter()

print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")

total.visualize()
