# %%
from typing import List, Optional

import pandas as pd

import sqlalchemy as sa

from evaluation_jp.data.metadata_utils import nearest_lr_date
from evaluation_jp.data.sql_utils import (
    datetime_cols,
    temp_table_connection,
    get_col_list,
    unpack,
    get_parameterized_query,
)


engine = sa.create_engine(
    "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
)


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


# //TODO Refactor common code in get_{data}() functions into helper function

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
        get_col_list(
            engine, "ists_claims", columns=columns, required_columns=["lr_flag"]
        )
        + get_col_list(
            engine, "ists_personal", columns=columns, required_columns=["ppsn"]
        )
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
        query,
        con=engine,
        params=params,
        parse_dates=datetime_cols(engine, "ists_claims")
        + datetime_cols(engine, "ists_personal"),
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
        get_col_list(
            engine, "ists_personal", columns=columns, required_columns=["ppsn"]
        )
    )
    query_text = f"""\
        SELECT {col_list} 
            FROM ists_personal
        """
    query, params = get_parameterized_query(query_text, ids)
    ists_personal = pd.read_sql(
        query,
        con=engine,
        params=params,
        parse_dates=datetime_cols(engine, "ists_personal"),
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
        get_col_list(
            engine, "les", columns=columns, required_columns=["ppsn", "start_date"]
        )
    )
    query_text = f"""\
        SELECT {col_list} 
            FROM les
        """
    query, params = get_parameterized_query(query_text, ids)
    les = pd.read_sql(
        query, con=engine, params=params, parse_dates=datetime_cols(engine, "les"),
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
    col_list = unpack(
        get_col_list(engine, "jobpath_referrals", columns, required_columns)
    )
    query_text = f"""\
        SELECT {col_list} 
            FROM jobpath_referrals
        """
    query, params = get_parameterized_query(query_text, ids)
    jobpath = pd.read_sql(
        query,
        con=engine,
        params=params,
        parse_dates=datetime_cols(engine, "jobpath_referrals"),
    )

    # Add calculated columns
    if columns is None or (columns is not None and "jobpath_end_date" in columns):
        jobpath["jobpath_end_date"] = jobpath["jobpath_end_date"].fillna(
            jobpath["jobpath_start_date"] + pd.DateOffset(years=1)
        )
    if columns is None or (columns is not None and "jobpath_start_month" in columns):
        jobpath["jobpath_start_month"] = jobpath["jobpath_start_date"].dt.to_period("M")
    return jobpath


# %%
def get_sw_payments(
    ids: Optional[pd.Series] = None,
    id_col: Optional[str] = "ppsn",
    period: Optional[pd.Period] = None,
    columns: Optional[List] = None,
) -> pd.DataFrame:
    required_columns = [id_col]
    col_list = get_col_list(engine, "payments", columns, required_columns)
    print(f"Column list is: {col_list}")

    if ids is not None:
        with temp_table_connection(engine, ids, "ids") as con:
            query = f"""\
                SELECT {unpack([f'payments.{col} AS {col}' for col in col_list])}
                FROM payments 
                INNER JOIN temp.ids
                ON payments.{id_col} = temp.ids.{id_col}
                """
            if period:
                query += f"""\
                    WHERE QTR = '{period}'
                """
            df = pd.read_sql(
                query, con=con, parse_dates=datetime_cols(engine, "payments")
            )
    else:
        query = f"""\
            SELECT {col_list} 
                FROM payments
            """
        if period:
            query += f"""\
                WHERE QTR = '{period}'
            """
        df = pd.read_sql(
            query, con=engine, parse_dates=datetime_cols(engine, "payments"),
        )

    return df


# %%
