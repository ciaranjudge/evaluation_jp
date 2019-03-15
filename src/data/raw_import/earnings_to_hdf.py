# %%
import numpy as np
import pandas as pd
import tables

from pandas.api.types import union_categoricals

import datetime as dt
import time
import calendar

from IPython.display import display

import sys
import os
import psutil
import gc
from pathlib import Path

pd.options.display.max_columns = None
pd.set_option("io.hdf.default_format", "table")

# %%
# def timeit(method):
#     def timed(*args, **kw):
#         ts = dt.datetime.now()
#         result = method(*args, **kw)
#         te = dt.datetime.now()
#         elapsed = te - ts

#         if "log_time" in kw:
#             name = kw.get("log_name", method.__name__.upper())
#             kw["log_time"][name] = int((te - ts) * 1000)
#         else:
#             print(f"""{method.__name__}: {elapsed}""")
#         return result

#     return timed

# Categorical helper function for joining dataframes with different categories
def concat_categorical(df_a, df_b, ignore_index=True):
    for cat_col in df_a.select_dtypes(["category"]):
        a = df_a[cat_col]
        b = df_b[cat_col]
        a_b = union_categoricals([a, b], ignore_order=True)
        a.cat.set_categories(a_b.categories, inplace=True)
        b.cat.set_categories(a_b.categories, inplace=True)
    return pd.concat([df_a, df_b], axis="index", ignore_index=ignore_index)

def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return mem

# %%
store_filepath = Path("data//interim/earnings.h5")

# %%
df = pd.read_csv("D:/repos/evaluation_jp/data/raw/CON_YEAR_PAYMENT_LINE.csv")

# %%
with pd.HDFStore(store_filepath, mode="r") as store:
    %time master_df = store.get("/payments")

# %%
table_merge_list = ['/pmts2016', '/pmts2017', '/pmts2018']
with pd.HDFStore(store_filepath, mode="r") as source_store:
    for key in table_merge_list:
        start = dt.datetime.now()
        print(
            f"""\n
        ----------------------------------------
        Key: {key}
        Start time: {start.strftime("%H:%M:%S")}
        """
        )
        print("Load key")
        %time df_to_merge = source_store.get(key)

        print("Append to master df and concat categoricals")
        %time master_df = concat_categorical(master_df, df_to_merge)

        print(f"psutil memory usage before cleanup: {memory_usage_psutil()}")
        df_to_merge = None
        gc.collect()    
        print(f"psutil memory usage after cleanup: {memory_usage_psutil()}")

        print("Save updated master df to store")
        with pd.HDFStore("data/test_data_store.h5", mode="w") as target_store:
            %time master_df.to_hdf(target_store, "payments", complib='blosc')

        print(f"Memory usage (master df): {sys.getsizeof(master_df)//1024**2}")
        gc.collect()    
        print(f"psutil memory usage after cleanup: {memory_usage_psutil()}")
        end = dt.datetime.now()
        print(end)

#%%
"""
    Get union_categorical of ppsn, scheme
    Use them + weeks of covered periods to generate a single table
    ppsn x scheme x week
"""

"""
    For records where from_date and to_date exist:
        Loop over weeks
        Join on ppsn, scheme 
        where week overlaps from_date, to_date
"""

# %%
with pd.HDFStore("data/master_data_store.h5", mode="w") as store:
    master_df.to_hdf(store, "payments", data_columns=True, complib='blosc', dropna=True)


#%%
date = pd.Timestamp("2013-07-01")
with pd.HDFStore("data/master_data_store.h5", mode="r") as store:
    %time df = store.select("/payments", "'ppsn' == cluster_df['ppsn']")


#%%
df.info(verbose=True)

#%%
gc.collect()


#%%
cluster_df = pd.read_csv("data\interim\lr_q_clusters.csv")

#%%
cluster_df.head()

#%%
