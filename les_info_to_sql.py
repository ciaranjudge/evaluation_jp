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

#%%
engine = sa.create_engine("sqlite:///data/jobpath.db")
metadata = sa.MetaData()

# %%
df = pd.read_excel("data/LES_New_Registrations2016-2017.xlsx")
df.columns = ["client_group", "ppsn", "start_date"]

# Get Friday date for every record to make matching to ISTS etc easier
days_to_friday = pd.to_timedelta((4 - df["start_date"].dt.dayofweek), unit="d")
df["start_week"] = df["start_date"] + days_to_friday

# %%
df.to_sql("les_starts", con=engine, if_exists="replace")



#%%
