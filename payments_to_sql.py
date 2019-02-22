#%%
import numpy as np
import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader
from pandas.api.types import union_categoricals

import sqlalchemy as sa

import datetime as dt
import time
import calendar

from IPython.display import display

import sys
from pathlib import Path

pd.options.display.max_columns = None

#%%
engine = sa.create_engine("sqlite:///data/jobpath.db")
metadata = sa.MetaData()

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
        data = data[cols]

    return data


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


def bytestrings_to_categories(data):
    for col in data.select_dtypes(include="object"):
        data[col] = data[col].str.decode("utf-8").astype("category")
    return data


def sas_dates_to_datetimes(data):
    date_cols = [col for col in data.columns.to_list() if "date" in col.lower()]
    for col in date_cols:
        fixed_col = sas_date_to_datetime(data[col])
        data.loc[col] = fixed_col
    return data


def data_to_sql(data, first=False):
    # Create or overwrite the database table if this is the first file to be processed
    if first:
        data.to_sql("payments", con=engine, if_exists="replace")
        first = False
    # ...and otherwise add this extract to the end of out_df and the database table
    else:
        data.to_sql("payments", con=engine, if_exists="append")


# Categorical helper function for joining dataframes with different categories
def concat_categorical(df_a, df_b, ignore_index=True):
    for cat_col in df_a.select_dtypes(["category"]):
        a = df_a[cat_col]
        b = df_b[cat_col]
        a_b = union_categoricals([a, b], ignore_order=True)
        a.cat.set_categories(a_b.categories, inplace=True)
        b.cat.set_categories(a_b.categories, inplace=True)
    return pd.concat([df_a, df_b], axis="index", ignore_index=ignore_index)

# %%
folder = Path("data/payments")
filepaths = list(folder.glob("*.sas7bdat"))
if type(filepaths) is not list:
    filepaths = [filepaths]

# %%
first=True
for filepath in filepaths:
    start = dt.datetime.now()
    print(
        f"""
    \n
    ----------------------------------------
    File: {filepath}
    Start time: {start.strftime("%H:%M:%S")}
    """
    )
    print("Load the SAS dataset")
    %time data = load_sas(filepath)
    
    print("Convert bytestring columns to categoricals")
    %time data = bytestrings_to_categories(data)

    print("Convert SAS dates to datetimes")
    %time data = sas_dates_to_datetimes(data)
    
    if first:
        print("Save the cleaned dataset to SQL")
        %time data_to_sql(data, first=True)

        print("Create master dataframe for all periods")
        %time all_data = data
        first=False
    else:
        print("Save the cleaned dataset to SQL")
        %time data_to_sql(data)
        print("Append this dataframe to the full dataframe for all periods")
        %time all_data = concat_categorical(all_data, data)
        data.info(memory_usage="deep")

    print(f"""
    Memory usage (this dataframe): {sys.getsizeof(data)}
    Memory usage (full dataframe): {sys.getsizeof(all_data)})
    end = dt.datetime.now()
    """)
    print(end)


# %%
all_data.info()

# # %%
# query = """
#     SELECT lr_date, ppsn, lr_code FROM ists_extracts 
#     WHERE lr_flag = 1
# """
# df = pd.read_sql_query(query, engine)

# df.groupby(["lr_date"])["ppsn"].count()
