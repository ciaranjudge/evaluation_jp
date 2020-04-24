#%%
import pandas as pd
from pandas.api.types import union_categoricals

from sqlalchemy import create_engine
import sqlalchemy as sa

import datetime as dt
import calendar

import os
from pathlib import Path

import dask as dd
from glob import glob

pd.options.display.max_columns = None

#%%
engine = sa.create_engine("sqlite:///data/jobpath.db")
metadata = sa.MetaData()

# %%
f_drive = Path(r"\\cskma0294\F")
folderpath = f_drive / "Evaluations" / "JobPath" / "Python" / "Data" / "Outcomes"
filepaths = list(folderpath.glob("*.csv"))

df_list = []

for f in filepaths:
    filename = f.name
    date = dt.datetime.strptime(filename, "JobPath_%Y%m%d.csv").date()
    df = dd.delayed(pd.read_csv)(f, usecols=[0, 1], names=[date, "ppsn"])
    df_list.append(df)

dd_df = dd.compute(*df_list)

# %%
out_df = dd_df[0]
for df in dd_df[1:]:
    out_df = pd.merge(out_df, df, how="outer", on="ppsn")

# %%
df.info(verbose=True)

#%%
df = out_df.set_index("ppsn")

#%%
df.to_sql("jld_q_clusters", con=engine, if_exists="replace")

# %%
query = """
        SELECT *
        FROM jld_q_clusters
        """

df = pd.read_sql_query(query, engine)

# %%
dt_index = pd.DatetimeIndex(df.columns)
dt_index
#%%
df.columns = dt_index

#%%
df.columns

#%%
df.to_csv("data/lr_q_clusters.csv")

#%%
df_2016 = df.loc[:, "2016-01-01":"2016-10-01"]

#%%
df_2016.head()

#%%
df_2016_true = df_2016.dropna(axis="rows", how="all")

#%%
df_2016_true.describe()

#%%
df_2016_true.shape

#%%
df_2016_true.info()

#%%
df_2016.describe()

#%%
df_long = df.reset_index()


#%%
rogue_rows = df_long.head()

#%%
