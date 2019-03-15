# %%
from typing import List, Set, Dict, Tuple, Optional
import datetime as dt
import dateutil.relativedelta as rd

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.data.import_helpers import get_les_data, get_jobpath_data, get_ists_claims


# %%
def restrict_by_age(
    dates_of_birth: pd.Series(pd.Timestamp),
    date: pd.Timestamp,
    min_age: Optional[int] = None,
    max_age: Optional[int] = None,
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

    min_age: Optional[int] = None
        Min (=youngest) possible age, in years. Can be None, which means no min age.

    max_age: Optional[int] = None
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
    codes: pd.Series(str), eligible_code_list: Optional[List]
) -> pd.Series(bool):
    """
    Given a code_series and a list of eligible_codes, return a boolean series 
    that is True for each record in code_series with code in eligible_codes, and False otherwise

    Parameters
    ----------
    codes: pd.Series(str)
        Series of strings (=eligible codes) 

    eligible_code_list: Optional[List] = None
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
        return pd.Series(data=True, index=codes.index)


def restrict_by_duration(
    date: pd.Timestamp,
    claim_start_date: pd.Series(pd.Timestamp),
    min_duration: Optional[int] = 365,
    max_duration: Optional[int] = None,
) -> pd.Series(bool):
    """
    Given a series of durations (in days), a date, and min/max durations, 
    return a boolean series that is True for each duration within the min/max range

    Parameters
    ----------
    date: pd.Timestamp
        Reference date for calculating durations

    claim_start_date: pd.Series(pd.Timestamp)
        Should be datetime series; expected to be episode durations

    min_duration: Optional[int] = None
        Min (=shortest) possible duration, in days. 
        Can be None, which means no min duration.

    max_duration: Optional[int] = None
        Max (=longest) possible duration, in days. 
        Can be None, which means no max duration.

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original durations series.
        True for each record between given min/max durations, False otherwise.
    """

    if min_duration is not None:
        date_of_min_duration = date - rd.relativedelta(days=min_duration)
        at_least_min_duration = claim_start_date <= date_of_min_duration
    else:
        at_least_min_duration = pd.Series(data=True, index=claim_start_date.index)

    if max_duration is not None:
        date_of_max_duration = date - rd.relativedelta(days=max_duration)
        less_than_max_duration = date_of_max_duration < claim_start_date
    else:
        less_than_max_duration = pd.Series(data=True, index=claim_start_date.index)

    return at_least_min_duration & less_than_max_duration


def on_les(
    date: pd.Timestamp,
    id_series: pd.Series,
    les_duration: pd.DateOffset = pd.DateOffset(years=1),
) -> pd.Series:
    """
    Given a date and an id_series, return a series which is True for every record with a LES start in this period

    Parameters
    ----------
    date: pd.Period
        Period of interest. Can also be pd.Timestamp.
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    les_duration:
        Pandas DateOffset representing the imputed duration of a LES episode.
        Default is 1 year - need to implement code for any other length!

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    les_df = get_les_data(columns=["ppsn", "start_date", "end_date"])
    on_or_after_start = les_df["start_date"] <= date
    on_or_before_end = date <= les_df["end_date"]

    all_on_les_on_date = les_df["ppsn"].loc[on_or_after_start & on_or_before_end]
    on_les_on_date = id_series.isin(all_on_les_on_date)
    return on_les_on_date


def any_previous_jobpath(
    date: pd.Timestamp, id_series: pd.Series, how: str = "both"
) -> pd.Series:
    """
    Given a date and an id_series, return a series which is 
    True for every record with any previous JobPath history and False otherwise

    Parameters
    ----------
    date: pd.Period
        Period of interest. Can also be pd.Timestamp.
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    how: str = 'both'
        Flag for how to find JobPath people:
            -- 'both' means check both ISTS and JobPath datasets
            -- 'jobpath_only' means check JobPath only
            -- 'ists_only' means check ISTS only

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    # First get ppsns of all JobPath starters before date from JobPath database
    if how == "both" or how == "jobpath_only":
        jobpath_df = get_jobpath_data(columns=["ppsn", "start_date"])
        jobpath_previous_start = jobpath_df["start_date"] < date
        jobpath_previous_jobpath = (
            jobpath_df["ppsn"].loc[jobpath_previous_start].drop_duplicates()
        )

    # Then get ppsn of everyone with an ISTS JobPath flag at end of previous month
    if how == "both" or how == "ists_only":
        previous_month_date = (
            (date - pd.DateOffset(months=1)).to_period("M").to_timestamp(how="E")
        )
        ists_df = get_ists_claims(previous_month_date)
        ists_previous_jobpath = ists_df["ppsn"].loc[ists_df["JobPath_Flag"] == 1]

    # Transform this pd.Series to a set. Simple if just using one source...
    if how == "jobpath_only":
        previous_jobpath = set(jobpath_previous_jobpath)
    elif how == "ists_only":
        previous_jobpath = set(ists_previous_jobpath)
    # ...but need to get union of JobPath and ISTS series ("|") if using both
    else:
        previous_jobpath = set(jobpath_previous_jobpath) | set(ists_previous_jobpath)

    # Then find the IDs of the people in the given id_series which are in this set
    previous_jobpath_in_id_series = id_series.isin(previous_jobpath)

    return previous_jobpath_in_id_series


def les_starts_this_period(
    period: pd.Period, id_series: pd.Series, period_type: str = "M"
) -> pd.Series:
    """
    Given a time period (or timestamp with period_type), and an id_series,
    return a boolean series which is True for every record with a LES start in this period

    Parameters
    ----------
    date: pd.Period
        Period of interest. Can also be pd.Timestamp.
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    period_type: str
        Pandas period-type-identifying string. Only needed if passing pd.Timestamp not Period.

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    if type(period) != pd.Period:
        period = period.to_period(period_)
    df = get_les_data(columns=["ppsn", "start_month"])
    all_starters_this_period = (
        df["ppsn"].loc[df["start_month"] == period].drop_duplicates()
    )
    started_this_period = id_series.isin(all_starters_this_period)

    return started_this_period


def jobpath_starts_this_period(
    period: pd.Period, id_series: pd.Series, period_type: str = "M"
) -> pd.Series:
    """
    Given a time period (or timestamp with period_type), and an id_series, return a series:
    True for every record with a JobPath start in this period, False otherwise

    Parameters
    ----------
    date: pd.Period
        Period of interest. Can also be pd.Timestamp.
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    period_type: str
        Pandas period-type-identifying string. Only needed if passing pd.Timestamp not Period.

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    if type(period) != pd.Period:
        period = period.to_period(period_type)
        df = get_jobpath_data(columns=["ppsn", "start_date"])
        df["start_period"] = df["start_date"].dt.to_period(period_type)
        all_starters_this_period = (
            df["ppsn"].loc[df["start_period"] == period].drop_duplicates()
        )
    else:
        df = get_jobpath_data(columns=["ppsn", "start_month"])
        all_starters_this_period = (
            df["ppsn"].loc[df["start_month"] == period].drop_duplicates()
        )

    started_this_period = id_series.isin(all_starters_this_period)

    return started_this_period



#%%
