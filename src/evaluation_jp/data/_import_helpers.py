# %%
from typing import List, Set, Dict, Tuple, Optional

import numpy as np
import pandas as pd

import sqlalchemy as sa

from evaluation_jp.features import nearest_lr_date


engine = sa.create_engine(
    "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
)



def datetime_cols(engine, table_name):
    insp = sa.engine.reflection.Inspector.from_engine(engine)
    column_metadata = insp.get_columns(table_name)
    datetime_cols = [
        col["name"]
        for col in column_metadata
        if type(col["type"]) == sa.sql.sqltypes.DATETIME
    ]
    return datetime_cols


def get_col_list(engine, table_name, columns=None, required_columns=None):
    insp = sa.engine.reflection.Inspector.from_engine(engine)
    column_metadata = insp.get_columns(table_name)
    table_columns = [col["name"] for col in column_metadata]
    if columns is not None:
        ok_columns = (set(columns) | set(required_columns)) & set(table_columns)
    else:
        ok_columns = table_columns
    return [col for col in ok_columns if col not in ["index", "id"]]


def unpack(listlike):
    return ", ".join([str(i) for i in listlike])


def get_parameterized_query(query_text, ids=None):
    params = {}
    if ids is not None:
        query_text += f"""\
            {"WHERE" if "WHERE" not in query_text else "AND"} ppsn in :ids
        """
        params["ids"] = list(set(ids))
        query = sa.sql.text(query_text).bindparams(
            sa.sql.expression.bindparam("ids", expanding=True)
        )
    else:
        query = query_text
    print(query)
    return query, params


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


# TODO Implement get_clusters
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
    pass


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
    col_list = unpack(
        get_col_list(engine, "ists_claims", columns=columns, required_columns=["lr_flag"])
        + ["ppsn"]
    )
    lookup_date = nearest_lr_date(date.normalize(), how="previous").date()
    query_text = f"""\
        SELECT {col_list} 
            FROM ists_claims c
            JOIN ists_personal p
            ON c.personal_id=p.id
            WHERE lr_date = '{lookup_date}'
        """
    if lr_flag:
        query_text += """\
            AND lr_flag is true
        """
    query, params = get_parameterized_query(query_text, ids)
    ists = pd.read_sql(
        query, con=engine, params=params, parse_dates=datetime_cols(insp, "ists_claims"),
    ).drop_duplicates("ppsn", keep="first")

    return (
        ists.dropna(axis=0, how="all")
        .reset_index(drop=True)
        .set_index("ppsn", verify_integrity=True)
    )


# %%
def get_vital_statistics(
    ids: Optional[pd.Index] = None, columns: Optional[List] = None
) -> pd.DataFrame:
    """
    Given an (optional) series of IDs, return a df with vital statistics

    Parameters
    ----------
    ids: Optional[pd.Index]
        IDs for lookup. If None, return all records for date.
    columns: Optional[List]
        Columns from the ISTS claim database to return. Default is all columns.
    Returns
    -------
    df: pd.DataFrame
        Columns returned are given in `columns`
    """
    col_list = unpack(
        get_col_list(engine, "ists_personal", columns=columns, required_columns=["ppsn"])
    )
    query_text = f"""\
        SELECT {col_list} 
            FROM ists_personal
        """
    query, params = get_parameterized_query(query_text, ids)
    print(query)
    ists_personal = pd.read_sql(
        query,
        con=engine,
        params=params,
        parse_dates=datetime_cols(insp, "ists_personal"),
    ).drop_duplicates("ppsn", keep="first")

    return (
        ists_personal.dropna(axis=0, how="all")
        .reset_index(drop=True)
        .set_index("ppsn", verify_integrity=True)
    )


#%%
def get_les_data(
    ids: Optional[pd.Index] = None, columns: Optional[List] = None
) -> pd.DataFrame:
    col_list = unpack(
        get_col_list(engine, "les", columns=columns, required_columns=["ppsn", "start_date"])
    )
    query_text = f"""\
        SELECT {col_list} 
            FROM les
        """
    query, params = get_parameterized_query(query_text, ids)
    les = pd.read_sql(
        query, con=engine, params=params, parse_dates=datetime_cols(insp, "les"),
    )

    # Add calculated columns if needed
    if not columns or (columns and "end_date" in columns):
        les["end_date"] = les["start_date"] + pd.DateOffset(years=1)
    if not columns or (columns and "start_month" in columns):
        les["start_month"] = les["start_date"].dt.to_period("M")
    return les


#%%
def get_jobpath_data(
    ids: Optional[pd.Index] = None, columns: Optional[List] = None
) -> pd.DataFrame:
    required_columns = ["ppsn", "jobpath_start_date"]
    col_list = unpack(get_col_list(engine, "jobpath_referrals", columns, required_columns))
    query_text = f"""\
        SELECT {col_list} 
            FROM jobpath_referrals
        """
    query, params = get_parameterized_query(query_text, ids)
    jobpath = pd.read_sql(
        query,
        con=engine,
        params=params,
        parse_dates=datetime_cols(insp, "jobpath_referrals"),
    )

    # Add calculated columns
    if columns is None or (columns is not None and "jobpath_end_date" in columns):
        jobpath["jobpath_end_date"] = jobpath["jobpath_end_date"].fillna(
            jobpath["jobpath_start_date"] + pd.DateOffset(years=1)
        )
    if columns is None or (columns is not None and "jobpath_start_month" in columns):
        jobpath["jobpath_start_month"] = jobpath["jobpath_start_date"].dt.to_period("M")
    return jobpath


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
def get_sw_payments(
    ids: Optional[pd.Index] = None,
    period: Optional[pd.Period] = None,
    columns: Optional[List] = None,
) -> pd.DataFrame:
    required_columns = ["ppsn"]
    col_list = unpack(get_col_list(engine, "payments", columns, required_columns))
    query_text = f"""\
        SELECT {col_list} 
            FROM payments
        """
    if period:
        query_text += f"""\
            WHERE QTR = '{period}'
        """
    query, params = get_parameterized_query(query_text, ids)
    payments = pd.read_sql(
        query, con=engine, params=params, parse_dates=datetime_cols(insp, "payments"),
    )
    return payments


# %%
