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
    # Reset time to 00:00:00
    date = date.normalize()

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
    date: pd.Timestamp,
    id_series: Optional[pd.Series] = None,
    columns: Optional[List] = None,
) -> pd.DataFrame:
    """
    Given a date and (optional) series of IDs, return a df with vital statistics

    Vital statistics are taken from last LR date before given date

    Parameters
    ----------
    date: pd.Timestamp
        Date of interest

    id_series: Optional[pd.Series]
        Pandas Series with IDs for lookup. If None, return all records for date.

    columns: Optional[List]
        Columns from the ISTS claim database to return. Default is all columns.


    Returns
    -------
    df: pd.DataFrame
        Columns returned are given in column_list
    """
    # Reset time to 00:00:00
    date = date.normalize()

    engine = sa.create_engine("sqlite:///./data/interim/jobpath.db")
    metadata = sa.MetaData()

    lookup_date = nearest_lr_date(date, how="previous")
    print(lookup_date)

    # Generate default column list if needed
    if columns is None:
        columns = ["date_of_birth", "sex", "nat_code", "occupation", "ppsn"]

    # Create comma-separated list of columns to pass into SELECT part of query
    column_query = ",".join(str(x) for x in columns)

    # Create SQL query to get columns based on date
    query = (
        f"""SELECT {column_query} 
        FROM ists_personal WHERE date(lr_date) = date('{lookup_date}')"""
    ).replace("\n", "\r\n")

    # Add this to query if an id_series has been provided
    if id_series is not None:
        ids = ",".join([f"'{str(x)}'" for x in id_series.to_list()])
        query += f" AND ppsn IN ({ids})"
    print(query)

    # Use pd.read_sql_query to execute the finalised query
    df = pd.read_sql_query(query, engine, parse_dates=True)
    duplicated = df.duplicated(subset="ppsn", keep="first")
    df = df.mask(duplicated)

    # Parse dates explicitly if they weren't already parsed by pd.read_sql_query
    for col in [col for col in df.columns.to_list() if "date" in col.lower()]:
        df[col] = pd.to_datetime(df[col], infer_datetime_format=True)

    # Any bytestring-coded strings that snuck in from SAS? Decode them!
    for col in [col for col in df.columns if df[col].dtype == "object"]:
        df[col] = decode_bytestrings(df[col]).astype("category")

    return df


def get_ists_claims(
    date: pd.Timestamp,
    lr_flag: bool = True,
    id_series: Optional[pd.Series] = None,
    columns: Optional[List] = None,
) -> pd.DataFrame:
    """
    Given a series of IDs and a date, return a df with ISTS claim info for each ID

    Claim info is taken from last LR date before given date.

    Parameters
    ----------
    date: pd.Timestamp
        Date to look up

    lr_flag: bool = True
        If True, just return records flagged as on the Live Register 
        
    id_series: pd.Series = None
        Pandas Series with IDs for lookup. 
        Causing weird problem with Jupyter kernel so not using!

    columns: Optional[List] = None
        Columns from the ISTS claim database to return. Default is all columns.

    Returns
    -------
    df: pd.DataFrame
    """
    # Reset time to 00:00:00
    date = date.normalize()

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

    duplicated = df.duplicated(subset="ppsn", keep="first")
    df = df.mask(duplicated)
    if columns is not None:
        df = df[columns]
    return df


def get_les_data(columns: Optional[List] = None) -> pd.DataFrame:
    df = pd.read_excel("data/raw/LES_New_Registrations2016-2017.xlsx")
    df = df.rename(
        columns={
            "PPSN": "ppsn",
            "LES Client group": "group",
            "Start Date": "start_date",
        }
    )
    df["end_date"] = df["start_date"] + pd.DateOffset(years=1)
    df["start_month"] = df["start_date"].dt.to_period("M")
    df["ppsn"] = df["ppsn"].str.strip()
    if columns is not None:
        df = df[columns]
    return df


def get_jobpath_data(columns: Optional[List] = None) -> pd.DataFrame:
    df = pd.read_excel("data/raw/Data_for_Evaluation_230718.xlsx")
    df = df.rename(
        columns={
            "Referral Status Description": "referral_status",
            "Start Date": "referral_date",
            "Date of Interview": "start_date",
            "PPP Agreed Date": "ppp_agreed_date",
            "Date of Cancellation": "cancellation_date",
            "Reason for Cancellation": "cancellation_reason",
            "Cancellationsubcategory": "cancellation_subcategory",
            "End Date": "admin_end_date",
            "Pps No": "ppsn",
            "Referral Id": "referral_id",
        }
    )
    df["end_date"] = df["start_date"] + pd.DateOffset(years=1)
    df["start_month"] = df["start_date"].dt.to_period("M")
    df["ppsn"] = df["ppsn"].str.strip()
    if columns is not None:
        df = df[columns]
    return df


#%%
