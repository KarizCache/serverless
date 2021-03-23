#!/usr/bin/python3
import pandas as pd
import seaborn as sns
import sklearn.datasets
from sklearn.svm import SVC

import dask_ml.datasets
from dask_ml.wrappers import ParallelPostFit

import dask.array as da

from dask.distributed import Client
from dask import delayed

import operator 

import time
import datetime



# Start the computation.
n_samples_classification = 1000
n_samples = 100000000
chunks = n_samples//20

name = f'parallelizing_svm_800k_40k'

client = Client('10.255.23.115:8786')

X, y = sklearn.datasets.make_classification(n_samples=n_samples_classification)
clf = ParallelPostFit(SVC(gamma='scale'))
clf.fit(X, y)

X, y = dask_ml.datasets.make_classification(n_samples = n_samples,
                                            random_state = n_samples,
                                            chunks = chunks)

# Start the computation.
start = datetime.datetime.now()
results = clf.predict(X).compute(scheduler='distributed')
end = datetime.datetime.now()

print(f'Parallelizing svm is done in {end - start}')

clf.predict(X).visualize(filename=f'{name}.png')
