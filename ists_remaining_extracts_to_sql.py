#%%
import numpy as np
import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader
from pandas.api.types import union_categoricals

import datetime as dt
import calendar

from IPython.display import display

import os
from pathlib import Path

import hashlib

pd.options.display.max_columns = None

# %%
def last_thurs_date(year, month):
    """
    Given a year (YYYY) and month (number from 1 to 12),
    returns the date of the last Thursday of that month

    Adapted from here: https://stackoverflow.com/a/52721988
    # Calendar weeks are indexed from 0 == first week of month
    # Calendar days are indexed from 0 == Monday, so Thursday is at index 3
    # ...so if day [4][3] exists that means it's the last (5th) Thursday in the month
    # ...otherwise day [3][3] must be the last Thursday.
    """
    cal = calendar.monthcalendar(year, month)
    if cal[4][3]:
        last_thurs_date = cal[4][3]
    else:
        last_thurs_date = cal[3][3]
    return dt.datetime(year, month, last_thurs_date)


# Live Register reporting date is the day after the last Thursday...
def lr_reporting_date(year, month):
    return last_thurs_date(year, month) + dt.timedelta(days=1)


#%%
ists_extract_folder = Path("//cskma0294/F/ISTS")
ists_extract_filepaths = list(ists_extract_folder.glob("ists_ext*.sas7bdat"))
# ists_extract_filenames = [p.name for p in ists_extract_filepaths]

ists_dated_files = {}
unmatched_files = []
for f in ists_extract_filepaths:
    try:
        filename = f.name
        ists_date = dt.datetime.strptime(filename, "ists_ext_%d%b%Y.sas7bdat").date()
        ists_dated_files[ists_date] = f
    except:
        unmatched_files.append(f)

ists_files_df = pd.DataFrame.from_dict(
    ists_dated_files, orient="index", columns=["filepath"]
)
sundays = pd.date_range(start="2014-01-05", end="2019-12-29", freq="W")
ists_files_df = ists_files_df.reindex(sundays)
ists_files_df.index.name = "ists_file_date"
ists_files_df = ists_files_df.reset_index()
ists_files_df["lr_friday"] = ists_files_df["ists_file_date"] - dt.timedelta(days=2)
ists_files_df = ists_files_df.set_index("lr_friday")
ists_files_df = ists_files_df.sort_index()


# %%
lr_reporting_dates = pd.Series(
    lr_reporting_date(year, month)
    for year in range(2014, 2020)
    for month in range(1, 13)
).rename("lr_reporting_date")
ists_files_df["is_lr_reporting_date"] = False
ists_files_df.loc[lr_reporting_dates, "is_lr_reporting_date"] = True
ists_files_df = ists_files_df[["is_lr_reporting_date", "ists_file_date", "filepath"]]
ists_files_df

# %%
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
        missing_columns = list(set(cols) - set(data.columns))
        for c in missing_columns[:]:
            data[c] = 0
        data = data[cols]

    return data


def bytestrings_to_categories(data, union_categories={}):
    for col in data.select_dtypes(include="object"):
        data.loc[:, col] = data[col].str.decode("utf-8")#.astype("category")
        col_cats = set(data[col].astype("category").cat.categories)
        if col not in union_categories:
            union_categories[col] = col_cats
        else:
            union_categories[col] = union_categories[col] | col_cats
        # data[col] = data[col].cat.set_categories(union_categories[col])
    return data, union_categories


# Categorical helper function for joining dataframes with different categories
def concat_categorical(df_a, df_b, ignore_index=True):
    for cat_col in df_a.select_dtypes(["category"]):
        a = df_a[cat_col]
        b = df_b[cat_col]
        a_b = union_categoricals([a, b], ignore_order=True)
        a.cat.set_categories(a_b.categories, inplace=True)
        b.cat.set_categories(a_b.categories, inplace=True)
    return pd.concat([df_a, df_b], axis="index", ignore_index=ignore_index)

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

def sas_dates_to_datetimes(data):
    date_cols = [col for col in data.columns.to_list() if "date" in col.lower()]
    for col in date_cols:
        fixed_col = sas_date_to_datetime(data[col])
        data.loc[:, col] = fixed_col
    return data

def running_index(data, offset_so_far=0):
    if offset_so_far != 0:
        data.index.name = "running_index"
        data = data.reset_index()
        data["running_index"] = data["running_index"] + offset_so_far
        data = data.set_index("running_index")
    new_offset = offset_so_far + data.shape[0]
    return data, new_offset

def data_to_hdf(store_filepath, key, data, append=True):
    # Create or overwrite the store if this is the first file to be processed
    with pd.HDFStore(store_filepath, complib="blosc") as store:
        store.append(
            key, 
            data, 
            append=append, 
            min_itemsize={"CLM_STATUS": 3, 'lr_code': 7, 'lls_code': 10, "location": 9}, 
            data_columns=True
        )


# %%
in_df = ists_files_df.dropna(axis="rows").loc[pd.Timestamp("2015-11-13"):]
lr_dates = list(in_df.index)
lr_filenames = list(in_df["filepath"])
lr_dates_filenames = dict(zip(lr_dates, lr_filenames))

# %%
in_df

# %%
cols = [
    "ppsn",
    "lr_code",
    "lr_flag",
    "lls_code",
    "clm_reg_date",
    "clm_comm_date",
    "location",
    "CLM_STATUS",
    "CLM_SUSP_DTL_REAS_CODE",
    "CDAS",
    "ada_code",
    "JobPath_Flag",
    "JobPathHold",
    "PERS_RATE",
    "ADA_AMT",
    "CDA_AMT",
    "MEANS",
    "EMEANS",
    "NEMEANS",
    "NET_FLAT",
    "FUEL",
    "RRA",
    "WEEKLY_RATE",
]

# byte_cols = ["lr_code", "lls_code", "ppsn"]
# categorical_cols = ["sex", "lr_code", "lls_code"]
to_int_cols = ["lr_flag", "JobPath_Flag", "JobPathHold"]
union_categories = {}
append=False
offset_so_far = 0
store_filepath = Path("data/ists_store.h5")
key = "/ists_extracts"

for lr_date, filepath in lr_dates_filenames.items():
    start = dt.datetime.now()
    print(
        f"""\n
    ----------------------------------------
    File date: {lr_date}
    Start time: {start.strftime("%H:%M:%S")}
    """
    )

    print("Load the SAS dataset")
    %time data = load_sas(filepath, cols=cols)

    print("Convert bytestring columns to categoricals (ish)")
    %time data,union_categories = bytestrings_to_categories(data, union_categories)

    print("Convert SAS dates to datetimes")
    %time data = sas_dates_to_datetimes(data)

    print("Fix running index")
    %time data, index_offset = running_index(data, offset_so_far)

    print("Ints are ints!")
    %time data[to_int_cols] = data[to_int_cols].fillna(0).astype("int8")

    print("Add LR date")
    %time data["lr_date"] = lr_date

    print("Save the cleaned dataset to HDF5")
    if append == False:
        %time data_to_hdf(store_filepath, key, data, append=False)
        append=True
    else:
        %time data_to_hdf(store_filepath, key, data, append=True)
    print(f"Memory usage (this dataframe): {sys.getsizeof(data)//1024**2}")
    end = dt.datetime.now().strftime("%H:%M:%S")
    print(end)


#%%
store_filepath = Path("data/ists_store.h5")
with pd.HDFStore(store_filepath, mode="r") as source_store:
    df = source_store.get("/ists_extracts")

#%%
df.describe()

#%%
