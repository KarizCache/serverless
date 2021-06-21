#!/usr/bin/env python

import dask.dataframe as dd

from dask.distributed import (
    wait,
    futures_of,
    Client,
)

def load_customer(path):
    customer_df = dd.read_table(
        path,
        sep='|',
        names=[
            'c_custkey',
            'c_name',
            'c_address',
            'c_nationkey',
            'c_phone',
            'c_acctbal',
            'c_mktsegment',
            'c_comment',
            '(empty)',
        ],
    )
    return customer_df

client = Client('10.255.23.115:8786')

table = load_customer("https://gochaudhstorage001.blob.core.windows.net/tpch/customer.tbl")
table = client.persist(table)
wait(table)

print(len(table))




#https://gochaudhstorage001.blob.core.windows.net/tpch/customer.tbl


