# %%
from typing import List, Set, Dict, Tuple, Optional

import numpy as np
import pandas as pd

import sqlalchemy as sa

from evaluation_jp.features.metadata_helpers import nearest_lr_date


engine = sa.create_engine(
    "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
)
insp = sa.engine.reflection.Inspector.from_engine(engine)


def get_datetime_cols(table_name):
    column_metadata = insp.get_columns(table_name)
    datetime_cols = [
        col["name"]
        for col in column_metadata
        if type(col["type"]) == sa.sql.sqltypes.DATETIME
    ]
    return datetime_cols


def get_col_list(table_name, columns=None, required_columns=None):
    column_metadata = insp.get_columns(table_name)
    table_columns = [col["name"] for col in column_metadata]
    if columns is not None:
        ok_columns = (set(columns) | set(required_columns)) & set(table_columns)
    else:
        ok_columns = table_columns
    return [col for col in ok_columns if col not in ["index", "id"]]


def unpack(listlike):
    return ", ".join([str(i) for i in listlike])


def compose_query(table, ids, columns, required_columns):
    col_list = get_col_list(table, columns, required_columns)
    if ids is not None:
        query_text = f"""select {unpack(col_list)} from {table} where ppsn in :ids"""
        query = sa.sql.text(query_text).bindparams(
            sa.sql.expression.bindparam("ids", expanding=True)
        )
        params = {"ids": ids.to_list()}
    else:
        query = f"""select {unpack(col_list)} from {table}"""
        params = None
    datetime_cols = get_datetime_cols(table)
    return query, params, datetime_cols


#%%
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


#%%
def get_ists_claims(
    date: pd.Timestamp,
    ids: Optional[pd.Index] = None,
    lr_flag: bool = True,
    columns: Optional[List] = None,
) -> pd.DataFrame:
    """
    Given a series of IDs and a date, return a df with ISTS claim info for each ID

    Claim info is taken from last LR date before given date.

    Parameters
    ----------
    date: pd.Timestamp
        Date to look up

    ids: pd.Index
        optional list of ids to select

    lr_flag: bool = True
        If True, just return records flagged as on the Live Register 
        
    columns: Optional[List] = None
        Columns from the ISTS claim database to return. Default is all columns.

    Returns
    -------
    df: pd.DataFrame
    """
    # Reset time to 00:00:00 and get relevant LR reporting date
    lookup_date = nearest_lr_date(date.normalize(), how="previous")

    # Query to get columns based on date
    query = f"""select *
        from ists_claims c
        join ists_personal p
        on c.personal_id=p.id
        where lr_date = '{lookup_date}'
        """
    datetime_cols = get_datetime_cols("ists_personal") + get_datetime_cols(
        "ists_claims"
    )
    sql_ists = pd.read_sql(query, con=engine, parse_dates=datetime_cols)

    duplicated = sql_ists["ppsn"].duplicated(keep="first")
    sql_ists = sql_ists[~duplicated]

    # for col in [col for col in sql_ists.columns if sql_ists[col].dtype == "object"]:
    #     sql_ists[col] = sql_ists[col].astype("category")
    if ids is not None:
        selected_ids = sql_ists["ppsn"].isin(ids)
        sql_ists = sql_ists.loc[selected_ids]

    if lr_flag == True:
        on_lr = sql_ists["lr_flag"] == 1
        sql_ists = sql_ists.loc[on_lr]

    if columns is not None:
        columns.append("ppsn")
        sql_ists = sql_ists[columns]

    return (
        sql_ists.dropna(axis=0, how="all")
        .reset_index(drop=True)
        .set_index("ppsn", verify_integrity=True)
    )


#%%
def get_les_data(
    ids: Optional[pd.Index] = None, columns: Optional[List] = None
) -> pd.DataFrame:
    required_columns = ["ppsn", "start_date"]
    query, params, datetime_cols = compose_query(
        table="les", ids=ids, columns=columns, required_columns=required_columns
    )
    sql_les = pd.read_sql(query, con=engine, params=params, parse_dates=datetime_cols)
    if columns is None or (columns is not None and "end_date" in columns):
        sql_les["end_date"] = sql_les["start_date"] + pd.DateOffset(years=1)
    if columns is None or (columns is not None and "start_month" in columns):
        sql_les["start_month"] = sql_les["start_date"].dt.to_period("M")
    if columns is not None:
        sql_les = sql_les[columns]
    return sql_les


#%%
def get_jobpath_data(
    ids: Optional[pd.Index] = None, columns: Optional[List] = None
) -> pd.DataFrame:
    required_columns = ["ppsn", "jobpath_start_date"]
    query, params, datetime_cols = compose_query(
        table="jobpath_referrals",
        ids=ids,
        columns=columns,
        required_columns=required_columns,
    )
    sql_jobpath = pd.read_sql(
        query, con=engine, params=params, parse_dates=datetime_cols,
    )
    if columns is None or (columns is not None and "jobpath_end_date" in columns):
        sql_jobpath["jobpath_end_date"] = sql_jobpath["jobpath_end_date"].fillna(
            sql_jobpath["jobpath_start_date"] + pd.DateOffset(years=1)
        )
    if columns is None or (columns is not None and "jobpath_start_month" in columns):
        sql_jobpath["jobpath_start_month"] = sql_jobpath[
            "jobpath_start_date"
        ].dt.to_period("M")
    if columns is not None:
        sql_jobpath = sql_jobpath[columns]
    return sql_jobpath


# def get_earnings(
#     # date: pd.Timestamp,
#     ids: pd.Index,
#     columns: Optional[List] = None,
# ) -> pd.DataFrame:
#     """
#     Given a series of IDs, return all available employment information for those IDs

#     Parameters
#     ----------
#     ids: pd.Index = None
#         IDs for lookup.

#     columns: Optional[List] = None
#         Columns from the database to return. Default is all columns.

#     Returns
#     -------
#     df: pd.DataFrame
#     """
#     query = f"('RSI_NO' in ({ids.to_list()}))"

#     # print(query)

#     with pd.HDFStore("data/interim/earnings.h5", mode="r") as store:
#         df = store.select("/earnings", where=query, columns=columns)

#     # Remove records with any error flags
#     error_flags = [
#         "PAY_ERR_IND",
#         "PRSI_ERR_IND",
#         "WIES_ERR_IND",
#         "CLASS_ERR_IND",
#         "PRSI_REFUND_IND",
#         "CANCELLED_IND",
#     ]
#     no_error_flag = ~df[error_flags].any(axis="columns")
#     df = df.loc[no_error_flag].drop(error_flags, axis="columns")

#     # Rename columns
#     # PRSI/earnings ratio
#     return df


# def get_sw_payments(ids: pd.Index, columns: Optional[List] = None) -> pd.DataFrame:
#     """
#     Given a series of IDs, return all available SW payment information for those IDs

#     Parameters
#     ----------
#     ids: pd.Series = None
#         Pandas Series with IDs for lookup.

#     columns: Optional[List] = None
#         Columns from the database to return. Default is all columns.

#     Returns
#     -------
#     df: pd.DataFrame
#     """
#     query = f"('ppsn' in ({set(ids)}))"

#     # print(query)

#     with pd.HDFStore("data/interim/master_data_store.h5", mode="r") as store:
#         df = store.select("/payments", where=query, columns=columns)

#     return df


# %%
