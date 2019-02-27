#%%
import pandas as pd

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
query = """
        SELECT i.ppsn, start_week, lr_flag, lr_code, client_group 
        FROM ists_extracts as i
        INNER JOIN les_starts as l
        ON i.ppsn = l.ppsn 
        AND date(lr_date) = date(start_week)
        """
%time df = pd.read_sql_query(query, engine)


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


# %%
lr_reporting_dates = pd.Series(
    lr_reporting_date(year, month)
    for year in range(2014, 2020)
    for month in range(1, 13)
).rename("lr_reporting_date")

# %%
lr_reporting_dates

# %%
query = """
        SELECT ppsn, lr_code, lr_flag, clm_comm_date, lr_date
        FROM ists_extracts
            WHERE date(lr_date) 
                BETWEEN date("2015-12-31") AND date("2017-01-01")
            AND is_lr_reporting_date = 1
            AND lr_flag = 1
        """
%time ists_df = pd.read_sql_query(query, engine)

# %%
ists_df.groupby(["lr_date", "lr_flag", "lr_code"]).count()

# %%
ists_df.groupby(["lr_date", "lr_flag"]).count()
#%%
