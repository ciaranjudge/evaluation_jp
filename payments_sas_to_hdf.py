#%%
import numpy as np
import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader
from pandas.api.types import union_categoricals

import datetime as dt
import time
import calendar

from IPython.display import display

import sys
from pathlib import Path

pd.options.display.max_columns = None
pd.set_option("io.hdf.default_format", "table")

# %%
def timeit(method):
    def timed(*args, **kw):
        ts = dt.datetime.now()
        result = method(*args, **kw)
        te = dt.datetime.now()
        elapsed = te - ts

        if "log_time" in kw:
            name = kw.get("log_name", method.__name__.upper())
            kw["log_time"][name] = int((te - ts) * 1000)
        else:
            print(f"""{method.__name__}: {elapsed}""")
        return result

    return timed


@timeit
def load_sas(filepath, rows=False, cols=False):
    """
    Given a filepath corresponding to a .SAS7BDAT file, 
    return a dataframe with rows: rows (all rows if blank)
    and cols: cols (all cols if blank)
    """
    # Irritatingly, have to use SAS7BDATReader instead of more natural pd.read_sas()
    # ...because of an issue with converting SAS dates that causes the read to fail.
    # pd.read_sas() is just a thin wrapper that checks SAS datafile type from file extension.
    # For '.sas7bdat' files, it then creates a SAS7BDATReader anyway...
    # ...but pd.read_csv() doesn't expose SAS7BDATReader's "convert_dates=False"
    # ...so only way out is to just directly create the SAS7BDATReader!
    in_reader = SAS7BDATReader(filepath, convert_dates=False, convert_text=False)

    # This creates a pd.DataFrame from the SAS7BDATReader object.
    # Can specify a number of rows to read()...
    if rows:
        data = in_reader.read(rows)
    # ...otherwise leaving read() empty means read all the rows!
    else:
        data = in_reader.read()

    # Restrict output to cols if cols have been specified
    if cols:
        data = data[cols]

    return data


@timeit
def bytestrings_to_categories(data, running_categories=pd.Categorical([])):
    for col in data.select_dtypes(include="object"):
        data.loc[:, col] = data[col].str.decode("utf-8").astype("category")
        union_categories = union_categoricals([data[col], running_categories])
        data[col] = data[col].cat.set_categories(union_categories.categories)
    return data, union_categories


# Categorical helper function for joining dataframes with different categories
@timeit
def concat_categorical(df_a, df_b, ignore_index=True):
    for cat_col in df_a.select_dtypes(["category"]):
        a = df_a[cat_col]
        b = df_b[cat_col]
        a_b = union_categoricals([a, b], ignore_order=True)
        a.cat.set_categories(a_b.categories, inplace=True)
        b.cat.set_categories(a_b.categories, inplace=True)
    return pd.concat([df_a, df_b], axis="index", ignore_index=ignore_index)


@timeit
def sas_date_to_datetime(in_col, max_date=dt.datetime.now()):
    """
    Converter function for dates in sas7bdat format.
    Automatic conversion via pd.
    """
    max_sas_date_days = (max_date - pd.Timestamp("1960-01-01")).days
    out_of_range = in_col > max_sas_date_days
    in_col.loc[out_of_range] = np.nan
    out_col = pd.to_timedelta(in_col, unit="D") + pd.Timestamp("1960-01-01")
    return out_col


@timeit
def sas_dates_to_datetimes(data):
    date_cols = [col for col in data.columns.to_list() if "date" in col.lower()]
    for col in date_cols:
        fixed_col = sas_date_to_datetime(data[col])
        data.loc[:, col] = fixed_col
    return data


@timeit
def running_index(data, offset_so_far=0):
    if offset_so_far != 0:
        data.index.name = "running_index"
        data = data.reset_index()
        data["running_index"] = data["running_index"] + offset_so_far
        data = data.set_index("running_index")
    new_offset = offset_so_far + data.shape[0]
    return data, new_offset


@timeit
def data_to_hdf(store_filepath, key, data, append=False):
    # Create or overwrite the store if this is the first file to be processed
    with pd.HDFStore(store_filepath) as store:
        store.append(key, data, append=append, data_columns=True)
    # if overwrite:
    #     data.to_hdf(str(store_filepath), key, append=False, data_columns=True, complib="blosc")
    # else:
    #     data.to_hdf(str(store_filepath), key, append=True, data_columns=True, complib="blosc")


# %%
source_folder = Path("data/payments")
source_filepaths = list(source_folder.glob("*.sas7bdat"))
key_names = [f.name.split(".")[0] for f in source_filepaths]
key__source_filepaths = dict(zip(key_names, source_filepaths))

# %%
store_filepath = Path("data/datastore.h5")

# %%
append=False
running_categories = pd.Categorical([])
index_offset = 0
for key, filepath in key__source_filepaths.items():
    start = dt.datetime.now()
    print(
        f"""\n
    ----------------------------------------
    File: {filepath}
    Start time: {start.strftime("%H:%M:%S")}
    """
    )
    print("Load the SAS dataset")
    data = load_sas(filepath)

    print("Convert bytestring columns to categoricals")
    data, running_categories = bytestrings_to_categories(
        data, running_categories=running_categories
    )

    print("Convert SAS dates to datetimes")
    data = sas_dates_to_datetimes(data)

    print("Fix running index")
    data, index_offset = running_index(data, index_offset)

    
    print("Save the cleaned dataset to HDF5")
    data_to_hdf(store_filepath, key, data, append)

    print(f"Memory usage (this dataframe): {sys.getsizeof(data)//1024**2}")
    end = dt.datetime.now()
    print(end)

#%%
%time df = pd.read_hdf(store_filepath, key_names[1])

#%%
df

#%%
df.info()


# %%
old = set(df["scheme"].cat.categories)
new = set(data["scheme"].cat.categories)

# %%
len(old)

# %%
len(new)

# %%
data.loc[data["scheme"] == "PRSI"]

# %%
import gc
gc.collect()

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