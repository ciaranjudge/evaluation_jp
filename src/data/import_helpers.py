# %%
from typing import List, Set, Dict, Tuple, Optional
import datetime as dt
import calendar
import dateutil.relativedelta as rd

import numpy as np
import pandas as pd

pd.set_option("io.hdf.default_format", "table")

from sqlalchemy import create_engine
import sqlalchemy as sa

from src.features.metadata_helpers import nearest_lr_date




def decode_bytestrings(series: pd.Series) -> pd.Series:
    """
    Given a pd.Series of dtype 'object', decode it as utf-8 if it's in bytecode format

    Parameters
    ----------
    series: pd.Series
        Expect this to be an object-type pd.Series (may or may not need decoding)

    Returns
    -------
    series: pd.Series
        If series needed decoding, return decoded series. Otherwise, return original.
    """
    # Check type of first element of series. If type is bytes, return decoded series.
    if type(series.iloc[0]) == bytes:
        return series.str.decode("utf8")
    # Otherwise, return original series.
    else:
        return series


def get_clusters(date: pd.Timestamp) -> pd.DataFrame:
    """
    Given a date, returns a dataframe with all available IDs and clusters for that date

    Parameters
    ----------
    date: pd.Timestamp
        Exact date of cluster slice

    Returns
    -------
    df: pd.DataFrame
        Columns returned: "ppsn", "date", "cluster"
    """
    engine = sa.create_engine("sqlite:///./data/interim/jobpath.db")
    metadata = sa.MetaData()
    column_list = ["ppsn", "date", "cluster"]
    columns = ",".join(str(x) for x in column_list)
    query = (
        f"""SELECT {columns}
                FROM jld_q_clusters 
                WHERE date(date) = date('{date}')"""
    ).replace("\n", "\r\n")
    print(query)
    df = pd.read_sql_query(query, engine, parse_dates=True)

    # Parse dates explicitly if they weren't already parsed by pd.read_sql_query
    for col in [col for col in df.columns.to_list() if "date" in col.lower()]:
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True)

    # Any bytestring-coded strings that snuck in from SAS? Decode them!
    for col in [col for col in df.columns if df[col].dtype == "object"]:
        df[col] = decode_bytestrings(df[col]).astype("category")

    return df


def get_vital_statistics(
    date: pd.Timestamp, id_series: pd.Series = None, column_list: List[str] = None
) -> pd.DataFrame:
    """
    Given a date and (optional) series of IDs, return a df with vital statistics

    Vital statistics are taken from last LR date before given date

    Parameters
    ----------
    id_series: pd.Series
        Pandas Series with IDs for lookup

    date: pd.Timestamp
        Exact date of cluster slice

    Returns
    -------
    df: pd.DataFrame
        Columns returned are given in column_list
    """

    engine = sa.create_engine("sqlite:///./data/interim/jobpath.db")
    metadata = sa.MetaData()

    lookup_date = nearest_lr_date(date, how="previous")
    print(lookup_date)

    # Generate default column list if needed
    if column_list is None:
        column_list = [
            "lr_date",
            "date_of_birth",
            "sex",
            "nat_code",
            "occupation",
            "ppsn",
        ]

    # Create comma-separated list of columns to pass into SELECT part of query
    columns = ",".join(str(x) for x in column_list)

    # Create SQL query to get columns based on date
    query = (
        f"""SELECT {columns} 
        FROM ists_personal WHERE date(lr_date) = date('{lookup_date}')"""
    ).replace("\n", "\r\n")

    # Add this to query if an id_series has been provided
    if id_series is not None:
        ids = ",".join([f"'{str(x)}'" for x in id_series.to_list()])
        query += f" AND ppsn IN ({ids})"
    print(query)

    # Use pd.read_sql_query to execute the finalised query
    df = pd.read_sql_query(query, engine, parse_dates=True)

    # Parse dates explicitly if they weren't already parsed by pd.read_sql_query
    for col in [col for col in df.columns.to_list() if "date" in col.lower()]:
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True)

    # Any bytestring-coded strings that snuck in from SAS? Decode them!
    for col in [col for col in df.columns if df[col].dtype == "object"]:
        df[col] = decode_bytestrings(df[col]).astype("category")

    return df


#%%
def get_ists_claims(date: pd.Timestamp, id_series: pd.Series = None, lr_flag=True) -> pd.DataFrame:
    """
    Given a series of IDs and a date, return a df with ISTS claim info for each ID

    Claim info is taken from last LR date before cluster date.

    Parameters
    ----------
    date: pd.Timestamp
        Exact date of cluster slice
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    Returns
    -------
    df: pd.DataFrame
        Columns returned: 
    """
    lookup_date = nearest_lr_date(date, how="previous")
    print(lookup_date)

    # Query to get columns based on date
    query = f"""('lr_date' == '{lookup_date}')"""

    # Add this to query if an id_series has been provided
    if id_series is not None:
        query += f" & ('ppsn' in ({id_series.to_list()}))"

    # Add this to query if an id_series has been provided
    if lr_flag == True:
        query += f" & ('lr_flag' == 1)"

    print(query)

    with pd.HDFStore("data/interim/ists_store.h5", mode="r") as store:
        df = store.select("/ists_extracts", query)

    return df


# %%
def restrict_by_age(
    dates_of_birth: pd.Series(pd.Timestamp),
    date: pd.Timestamp,
    min_age: int = None,
    max_age: int = None,
) -> pd.Series(bool):
    """
    Given a series of dates of birth, a reference date, and a min and/or age (in years), 
    return a boolean series that is True for each date of birth within the min/max range

    Parameters
    ----------
    dates_of_birth: pd.Series(pd.Timestamp)
        Should be datetime series; expected to be dates of birth

    date: pd.Timestamp
        Reference date for calculating ages

    min_age: int = None
        Min (=youngest) possible age, in years. Can be None, which means no min age.

    max_age: int = None
        Max (=oldest) possible age, in years. Can be None, which means no max age.

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original dates_of_birth.
        True for each record between given min/max ages, False otherwise.
    """

    if min_age is not None:
        min_age_date_of_birth = date - rd.relativedelta(years=min_age)
        at_least_min_age = dates_of_birth <= min_age_date_of_birth
    else:
        at_least_min_age = pd.Series(data=True, index=dates_of_birth.index)

    if max_age is not None:
        max_age_date_of_birth = date - rd.relativedelta(years=max_age)
        under_max_age = max_age_date_of_birth < dates_of_birth
    else:
        under_max_age = pd.Series(data=True, index=dates_of_birth.index)

    return at_least_min_age & under_max_age


def restrict_by_code(
    codes: pd.Series(str), eligible_code_list: List[str] = None
) -> pd.Series(bool):
    """
    Given a code_series and a list of eligible_codes, return a boolean series 
    that is True for each record in code_series with code in eligible_codes, and False otherwise

    Parameters
    ----------
    codes: pd.Series(str)
        Series of strings (=eligible codes) 

    eligible_code_list: List[str]
        List of eligible. If no list given, assume no restriction.

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original code_series.
        True for each code in codes where code is in eligible_code_list, False otherwise.
        If eligble_code_list is Null, then assume no restriction and return True for all.
    """

    if eligible_code_list is not None:
        eligible_codes = codes.isin(eligible_code_list)
        return eligible_codes
    else:
        return codes


# %%
# criteria {age, codes, duration}
date = pd.Timestamp("2016-01-01")



# %%
def get_eligible_slice(date: pd.Timestamp) -> pd.DataFrame:
    # Parameters
    max_age = 60
    on_lr = True
    eligible_code_list = ["UA", "UB"]
    #min_duration_days = 365
    # LES
    # Previous JP
    # JP hold

    vital_statistics_column_list = [
            "date_of_birth",
            "sex",
            "nat_code",
            "occupation",
            "ppsn",
        ]
    # Initial df is everone who is clustered on date.
    # Inner join with 
    df = pd.merge(
        left=get_clusters(date), 
        right=get_vital_statistics(date, vital_statistics_column_list), 
        on="ppsn"
        )
    # Generate boolean column for age restriction - keep original dataframe for reporting
    df["eligible_age"] = restrict_by_age(df["date_of_birth"], date, max_age=max_age)

    df = pd.merge(left=df, right=get_ists_claims(date, lr_flag=True), on="ppsn")

    # %%
    df["eligible_code"] = restrict_by_code(df["lr_code"], eligible_code_list)

    return df


#%%
