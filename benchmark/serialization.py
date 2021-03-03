#!/usr/bin/python3

import dill
import dask.array as da
from sys import getsizeof
import timeit

K = 1 << 10
M = 1 << 20
G = 1 << 30

sizes = [1*K, 128*K, 1*M, 2*M, 4*M, 16*M, 32*M, 64*M, 128*M, 256*M, 512*M, 768*M, 1*G, 2*G, 4*G, 6*G, 8*G, 10*G, 12*G, 14*G]
repeat = 5
dim2 = 1024
chunks = (dim2, dim2)
logf = open('serialization.log', 'a')



for s in sizes:
    for r in range(0, repeat):
        matrixdim = (s, dim2)
        ds = da.random.random(matrixdim, chunks=chunks)
        ser_start_time = timeit.default_timer()
        ser_ds = dill.dumps(ds)
        ser_finish_time = timeit.default_timer()

        deser_start_time = timeit.default_timer()
        deser_ds = dill.loads(ser_ds)
        deser_finish_time = timeit.default_timer()

        print(f'serialization: size: {getsizeof(ser_ds)} runtim: {ser_finish_time - ser_start_time}')
        logf.write(f'{s*dim2},{getsizeof(ser_ds)},{ser_finish_time - ser_start_time},{deser_finish_time - deser_start_time},{r}\n')
    #break


logf.close()
