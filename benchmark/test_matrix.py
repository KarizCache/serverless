import sys
import time

import dask
import dask.array as da
from dask.distributed import Client, get_task_stream

from collections import defaultdict
from benchmarks import utils

import argparse



def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", required=True, help="Address of the dask scheduler")
    parser.add_argument("--test", required=True, choices=['aggregate','shuffle'], help="Which test to run (aggregate or shuffle)")
    parser.add_argument("--par", required=True, type=int, help="Max number of parallel tasks")
    parser.add_argument("--mem", required=True, type=int, help="Max memory per task, in MB")
    parser.add_argument("--timing", action='store_true', help="Whether to display timing information")  
    parser.add_argument("--viz", help="If present, name of the file to store the task graph as svg")
    parser.add_argument("--saveprof", help="If present, name of the html file to save profile of execution")
    global args
    args = parser.parse_args()

    print(args)

    client = Client(args.scheduler)
    enable_timing = args.timing

   
    if (args.test == 'aggregate'): 
        matrix_aggregate(
            client=client, 
            max_parallelism=args.par,
            max_pertask_memory_mb=args.mem,
            enable_timing=enable_timing,
        )
    elif (args.test == 'shuffle'):
        matrix_shuffle(
            client=client, 
            max_parallelism=args.par,
            max_pertask_memory_mb=args.mem,
            enable_timing=enable_timing,
        )


"""
matrix_aggegrate is implemented as a distributed array mean.

It is a simple multi-stage aggregation task.
Data shipping is insignificant and there are no shuffles,
but stage depth is logarithmic wrt parallelism due to aggregation stages.

Returns end-to-end time in seconds.
"""
def matrix_aggregate(*,
    client, 
        # Dask client to use.
    max_parallelism,
        # The amount of parallelism desired in the generated task graph.
    max_pertask_memory_mb,
        # The maximum amount of memory to use per task (in megabytes).
    enable_timing,
):
    ints_per_mb = (2**20 / 8)
    pertask_ints = int(max_pertask_memory_mb * ints_per_mb)
    total_ints = pertask_ints * max_parallelism

    array_mean_task = da.random.randint(
        1, 100, size=total_ints, chunks=pertask_ints).mean()
    start = time.time()
    if (args.viz):
        dask.visualize(array_mean_task, filename=args.viz, optimize_graph=True)
    future = client.compute(array_mean_task)
    result = client.gather(future)
    duration = time.time() - start


    print("")
    print("== TASK COMPLETE: matrix_aggregate(parallelism: {}, pertask memory: {} MB) ==".format(max_parallelism, max_pertask_memory_mb))
    print("Client E2E time: {:.3f} seconds.".format(duration))
    if enable_timing:
        try:
            utils.prettyprint_timing_info(client.timing_info(future))
        except Exception as e:
            print(str(e))
    return future

"""
matrix_shuffle is implemented as a matrix multiply.

There is one shuffle stage (with factor sqrt(parallelism))
with all data being shipped (total network io: parallelism * pertask_memory).
Total stage depth is logarithmic due to aggregation stages.
"""
def matrix_shuffle(*,
    client, 
    max_parallelism,
    max_pertask_memory_mb,
    enable_timing,
):
    # Floor parallelism to nearest smaller square.
    parallelism = int(max_parallelism ** 0.5) ** 2

    ints_per_mb = (2**20 / 8)
    pertask_ints = int(max_pertask_memory_mb * ints_per_mb)
    persource_ints = pertask_ints / (2 * int(parallelism ** 0.5))
    persource_dim = int(persource_ints ** 0.5)
    total_dim = persource_dim * int(parallelism ** 0.5)

#    print("max P: {}, P: {}, m: {}, ints/MB: {}, pertask_ints: {}, persource_ints: {}, persource_dim: {}, total_dim: {} ".format(max_parallelism, parallelism, max_pertask_memory_mb, 
#                                             ints_per_mb, pertask_ints, persource_ints, 
#                                             persource_dim, total_dim), file=sys.stderr)

    array_x_task = da.random.normal(1, 1, 
        size=(total_dim, total_dim), 
        chunks=(persource_dim, persource_dim))
    mult_task = da.matmul(array_x_task, array_x_task)
    # Add an aggregate so we can wait on a single result.
    # Adds some more stages, but not more shuffling/parallelism/etc.
    agg_task = mult_task.sum()

    if (args.viz):
        dask.visualize(agg_task, filename=args.viz)
        #return

    start = time.time()
    if (args.saveprof):
      with get_task_stream(plot='save', filename=args.saveprof) as ts:  
        future = client.compute(agg_task)
        result = client.gather(future)
        duration = time.time() - start
    else:
        future = client.compute(agg_task)
        result = client.gather(future)
        duration = time.time() - start

    data = defaultdict(float)
    utils.print_trace_data(client.timing_info(future, full=True))

    print("")
    print("== TASK COMPLETE: matrix_shuffle(parallelism: {}, pertask memory: {} MB) ==".format(
        parallelism, ((persource_dim ** 2) * (2 * int(parallelism ** 0.5))) // ints_per_mb))
    print("Client E2E time: {:.3f} seconds.".format(duration))
    if enable_timing:
        try:
            utils.prettyprint_timing_info(client.timing_info(future))
        except Exception as e:
            print(str(e))
    return future

if __name__ == "__main__":
    main()
