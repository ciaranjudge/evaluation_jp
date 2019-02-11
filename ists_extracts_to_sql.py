#%%
import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader
from pandas.api.types import union_categoricals

from sqlalchemy import create_engine
import sqlalchemy as sa

import datetime as dt
import calendar

from IPython.display import display

import os
from pathlib import Path

import hashlib

pd.options.display.max_columns = None

# %%
# Converter function for dates in sas7bdat format.
# Automatic conversion via pd.
def sas_dates_to_datetime(in_col):
    out_col = pd.to_timedelta(in_col, unit="D") + pd.Timestamp("1960-01-01")
    return out_col


# %%
# Calculate the last Thursday of a month
# Adapted from here: https://stackoverflow.com/a/52721988
def last_thurs_date(year, month):
    # Years are just YYYY and months are numbers from 1 to 12.
    # Calendar weeks are indexed from 0 == first week of month
    # Calendar days are indexed from 0 == Monday, so Thursday is at index 3
    # ...so if day [4][3] exists that means it's the last (5th) Thursday in the month
    # ...otherwise day [3][3] must be the last Thursday.
    cal = calendar.monthcalendar(year, month)
    if cal[4][3]:
        last_thurs_date = cal[4][3]
    else:
        last_thurs_date = cal[3][3]
    return dt.datetime(year, month, last_thurs_date)


# Live Register reporting date is the day after the last Thursday...
def lr_reporting_date(year, month):
    return last_thurs_date(year, month) + dt.timedelta(days=1)


# %%
# Categorical helper function for joining dataframes with different categories
def concat_categorical(df_a, df_b, ignore_index=True):
    for cat_col in df_a.select_dtypes(["category"]):
        a = df_a[cat_col]
        b = df_b[cat_col]
        a_b = union_categoricals([a, b], ignore_order=True)
        a.cat.set_categories(a_b.categories, inplace=True)
        b.cat.set_categories(a_b.categories, inplace=True)
    return pd.concat([df_a, df_b], axis="index", ignore_index=ignore_index)


#%%
engine = sa.create_engine("sqlite:///data/jobpath.db")
metadata = sa.MetaData()

#%%
f_drive = Path(r"\\cskma0294\F")
ists_extract_folder = f_drive / "ISTS"
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


# %%
lr_reporting_dates = pd.Series(
    lr_reporting_date(year, month)
    for year in range(2014, 2020)
    for month in range(1, 13)
).rename("lr_reporting_date")
ists_files_df["is_lr_reporting_date"] = False
ists_files_df.loc[lr_reporting_dates, "is_lr_reporting_date"] = True
ists_files_df = ists_files_df[["is_lr_reporting_date", "ists_file_date", "filepath"]]
# %%
ists_files_df.loc[ists_files_df["is_lr_reporting_date"] == True]

# %%
cso_lr = pd.read_json(
    "https://www.cso.ie/StatbankServices/StatbankServices.svc/jsonservice/responseinstance/LRM01"
)

# %%
cso_lr.info(verbose=True)

# %%
first = False
keep_cols = [
    "ppsn",
    "sex",
    "age",
    "Recip_flag",
    "lr_code",
    "lr_flag",
    "lls_code",
    "clm_reg_date",
    "clm_comm_date",
    "JobPath_Flag",
    "JobPathHold",
]

byte_cols = ["sex", "lr_code", "lls_code"]

categorical_cols = ["sex", "lr_code", "lls_code"]

to_int_cols = ["age", "Recip_flag", "lr_flag", "JobPath_Flag", "JobPathHold",]

# Iterate through the dataframe that holds the list of LR dates and ISTS extract files
# Only want to include weeks with a corresponding datafile so dropna() !
ists_months_df = ists_files_df.loc[ists_files_df["is_lr_reporting_date"]==False]
# print(ists_months_df.head())

for row in ists_months_df.dropna().itertuples():
    #####
    print("-----------------------------")
    start_time = dt.datetime.now()

    # Irritatingly, have to use SAS7BDATReader instead of more natural pd.read_sas()
    # ...because of an issue with converting SAS dates that causes the read to fail.
    # pd.read_sas() is just a thin wrapper that checks SAS datafile type from file extension.
    # For '.sas7bdat' files, it then creates a SAS7BDATReader anyway...
    # ...but pd.read_csv() doesn't expose SAS7BDATReader's "convert_dates=False"
    # ...so only way out is to just directly create the SAS7BDATReader!
    in_reader = SAS7BDATReader(row.filepath, convert_dates=False, convert_text=False)

    # This creates a pd.DataFrame from the SAS7BDATReader object.
    # Can specify a number of rows inside read() - empty () means read all the rows!
    in_df = in_reader.read()
   
    # Now we can fix those dates!
    date_cols = [col for col in in_df.columns.to_list() if "date" in col.lower()]
    for col in date_cols:
        in_df[col] = sas_dates_to_datetime(in_df[col])

    # Some ISTS extracts don't have these JobPath columns so need to check...
    if "JobPath_Flag" not in in_df.columns.to_list():
        in_df["JobPath_Flag"] = 0

    if "JobPathHold" not in in_df.columns.to_list():
        in_df["JobPathHold"] = 0

    # Just keep the columns that we actually need
    in_df = in_df[keep_cols]

    # Decode SAS bytestrings (brrrrrr) as normal strings
    for col in byte_cols:
        in_df[col] = in_df[col].str.decode("utf-8")

    # Categoricals should be categorical!
    # in_df[categorical_cols] = in_df[categorical_cols].astype("category")

    # And small integers should be small integers...
    in_df[to_int_cols] = in_df[to_int_cols].fillna(0).astype("int8")

    # Add the date of the current ISTS extract to the dataframe
    in_df["lr_date"] = row.Index
    in_df["is_lr_reporting_date"] = row.is_lr_reporting_date

    # # Mask ppsns as 'unique_id'
    # in_df["unique_id"] = [hashlib.sha3_256(ppsn).hexdigest() for ppsn in in_df["ppsn"]]
    # in_df = in_df.drop("ppsn", axis="columns")

    # How's it going?
    print(row.Index)
    # print(in_df.groupby(["lr_date", "lr_flag"])["ppsn"].count())

    # Wow, got this far! Create new out_df and database table if it's the first extract
    if first:
        # out_df = in_df
        in_df.to_sql("ists_extracts", con=engine, if_exists="replace")
        first = False
    # ...and otherwise add this extract to the end of out_df and the database table
    else:
        # out_df = concat_categorical(out_df, in_df)
        in_df.to_sql("ists_extracts", con=engine, if_exists="append")

    end_time = dt.datetime.now()
    import_time = end_time - start_time
    print(f"Time taken: {import_time}")
    print("-----------------------------")



# %%
query = """
    SELECT lr_date, ppsn, lr_code FROM ists_extracts 
    WHERE lr_flag = 1
"""
%time df = pd.read_sql_query(query, engine)

df.groupby(["lr_date"])["ppsn"].count()




#%%
date_cols = [col for col in df.columns.to_list() if "date" in col.lower()]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
df.info(verbose=True)


#%%
# in_df = in_df.sort_values("ppsn").drop_duplicates(subset="ppsn", keep="first")
df = ists_files_df



#%%
out_df.info(verbose=True)


#%%
