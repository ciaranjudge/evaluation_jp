# %%
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional
from functools import wraps

import pandas as pd

from src.data.import_helpers import (
    get_les_data,
    get_jobpath_data,
    get_ists_claims,
    get_vital_statistics,
)


# %%
ELIGIBILITY_CHECKS = {}


def eligibility(inversed=False):
    def eligibility_checker(checker):
        check = checker.__name__.lstrip("check_")
        ELIGIBILITY_CHECKS[check] = checker

        @wraps(checker)
        def eligibility_checked(*args, **kwargs):
            if inversed:
                return ~checker(*args, **kwargs)
                print("negating!")
            else:
                print("not negating!")
                return checker(*args, **kwargs)
        
        return eligibility_checked
        
    return eligibility_checker


@dataclass
class EligibilityChecker:
    """
    Manage eligibility parameters and generate eligibility status information

    Parameters
    ----------
    age : dict = None
        min : pd.DateOffset
        max : pd.DateOffset

    codes : dict = None
        eligible : Tuple(str)  # Tuple of eligible codes

    claim_duration : dict = None
        min : pd.DateOFfset
        max : pd.DateOffset

    not_on_les : dict = None
        imputed_duration : pd.DateOffset

    les_starts : dict = None
        exclude : bool

    not_current_jobpath : dict = None

    no_previous_jobpath : dict = None



        
    """

    # Expect function ``check_[this]`` to exist for each variable here
    age: dict = None
    codes: dict = None
    duration: dict = None
    les_status: dict = None
    les_starts: dict = None
    jobpath_status: dict = None
    jobpath_starts: dict = None
    jobpath_hold: dict = None

    def __post_init__(self):
        # Set up methods for only the criteria supplied on initialisation
        # Need to pass params dynamically to functions
        self.criteria = {k: v for k, v in self.__dict__.items() if v is not None}
        print(ELIGIBILITY_CHECKS[0].__name__)

    def check_eligibility(self, *args, **kwargs) -> pd.DataFrame:
        # For each method set up post_init, create a boolean pd.Series
        # Then create an overall boolean eligibility series
        # What needs to be passed in here?
        # Create a temp dataframe for stuff that's common to several things?
        f_list = [f"check_{k}" for k in self.criteria.keys()]
        print(f_list)


@eligibility(inversed=True)
def on_first():
    print("who")
    return pd.Series([True, False])


@eligibility()
def check_age(
    date: pd.Timestamp, id_series: pd.Series, checks: dict = None
) -> pd.Series(bool):
    """
    Given a reference date, an ID series, and a min and/or max age (in years), 
    return a boolean series that is True for each date of birth within the min/max range

    Parameters
    ----------
    date: pd.Timestamp
        Reference date for calculating ages

    id_series: pd.Series(pd.Timestamp)
        Should be datetime series; expected to be dates of birth

    checks: dict = None
        min : pd.DateOffset
        max : pd.DateOffset

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series
        True for each record between given min/max ages, False otherwise.
    """

    if checks is None or (
        "min" not in checks.keys() and "max" not in checks.keys()
    ):
        return pd.Series(data=True, index=id_series.index)
    else:
        min_age = checks["min"] if "min" in checks.keys() else None
        max_age = checks["max"] if "max" in checks.keys() else None
        print(f"Restrict by age using min: {min_age} and max: {max_age}")
        source = get_vital_statistics(
            date, id_series, columns=["ppsn", "date_of_birth"]
        )
        dates_of_birth = pd.Series(index=source["ppsn"], data=source["date_of_birth"])

        if min_age is not None:
            min_age_date_of_birth = date - min_age
            at_least_min_age = dates_of_birth <= min_age_date_of_birth
        else:
            at_least_min_age = pd.Series(data=True, index=dates_of_birth.index)

        if max_age is not None:
            max_age_date_of_birth = date - max_age
            under_max_age = max_age_date_of_birth < dates_of_birth
        else:
            under_max_age = pd.Series(data=True, index=dates_of_birth.index)

        return at_least_min_age & under_max_age

# %%
def code_eligible(
    date: pd.Timestamp, codes: pd.Series(str), code_criteria: pd.DataFrame = None
) -> pd.Series(bool):
    """
    Given a series of codes and a date, 
    return True for each record that has an eligible code on that date

    Parameters
    ----------
    date: pd.Timestamp
        Reference date for what codes are eligible. 
        NB this is not implemented yet! Codes are hard-coded below!

    codes: pd.Series(str)
        Series of (people with their) codes

    code_criteria: pd.DataFrame = None
        Not yet implemented!!
        Should be the lookup source for what codes were eligible on date    

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original code_series.
        True for each code in codes where code is in eligible_code_list, False otherwise.
    """

    # Should be got by date lookup from code_criteria but hardcoded for now!
    eligible_code_list = ("UA", "UB")

    print(f"Restrict by code using eligible codes: {eligible_code_list}")

    return codes.isin(eligible_code_list)


def restrict_by_duration(
    date: pd.Timestamp,
    claim_start_dates: pd.Series(pd.Timestamp),
    duration_criteria: pd.DataFrame = None,
) -> pd.Series(bool):
    """
    Given a series of durations (in days), a date, and min/max durations, 
    return a boolean series that is True for each duration within the min/max range

    Parameters
    ----------
    date: pd.Timestamp
        Reference date for calculating durations

    claim_start_dates: pd.Series(pd.Timestamp)
        Should be datetime series; expected to be episode durations

    duration_criteria: pd.DataFrame = None
        Not yet implemented!!
        Should be the lookup source for what durations were eligible on date    

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original durations series.
        True for each record between given min/max durations, False otherwise.
    """
    # Should be got by lookup from age_criteria but hardcoded for now!
    # Also reimplement durations as pd.DateOffset rather than int (=no. of days)
    min_duration: int = 365  # Min (=shortest) possible duration, in days.
    max_duration: int = None

    print(f"Restrict by duration using min: {min_duration}, max: {max_duration}")

    if min_duration is not None:
        date_of_min_duration = date - pd.DatetimeDelta(days=min_duration)
        at_least_min_duration = claim_start_dates <= date_of_min_duration
    else:
        at_least_min_duration = pd.Series(data=True, index=claim_start_dates.index)

    if max_duration is not None:
        date_of_max_duration = date - pd.DatetimeDelta(days=max_duration)
        less_than_max_duration = date_of_max_duration < claim_start_dates
    else:
        less_than_max_duration = pd.Series(data=True, index=claim_start_dates.index)

    return at_least_min_duration & less_than_max_duration


def on_les(
    date: pd.Timestamp, id_series: pd.Series, les_criteria: pd.DataFrame = None
) -> pd.Series:
    """
    Given a date and id_series, return True for every record on LES on that date

    Parameters
    ----------
    date: pd.Period
        Period of interest. Can also be pd.Timestamp.
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    les_criteria:
        Not yet implemented!!
        Should be the lookup source for what durations were eligible on date 

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    # Should be got by lookup from les_criteria but hardcoded for now!
    les_duration: pd.DateOffset = pd.DateOffset(years=1)

    print(f"Find people on LES using duration: {les_duration}")

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
    date: pd.Timestamp
        Lookup date
    
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

    print(f"Find people on JobPath on date {date}")

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
    start_date: pd.Timestamp, id_series: pd.Series, period_type: str = "M"
) -> pd.Series:
    """
    Given a start_date, an id_series, and an optional period_type (defaults to "M")
    return True for every record with a LES start in this period, False otherwise

    Parameters
    ----------
    start_date: pd.Timestamp
        Start of this period
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    period_type: str
        Pandas period-type-identifying string. Default is 'M' (not implemented otherwise!)
    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    period = start_date.to_period(period_type)

    print(f"Get LES starters in period: {period}")

    df = get_les_data(columns=["ppsn", "start_month"])
    all_starters_this_period = (
        df["ppsn"].loc[df["start_month"] == period].drop_duplicates()
    )
    started_this_period = id_series.isin(all_starters_this_period)

    return started_this_period


def jobpath_starts_this_period(
    start_date: pd.Timestamp, id_series: pd.Series, period_type: str = "M"
) -> pd.Series:
    """
    Given a start_date and an id_series, with optional period_type (default is 'M'):
    return True for every record with a JobPath start in this period, False otherwise

    Parameters
    ----------
    start_date: pd.Timestamp
        Start of this period
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    period_type: str
        Pandas period-type-identifying string. Default is 'M'.

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """
    period = start_date.to_period(period_type)

    print(f"Get JobPath starters in period: {period}")

    df = get_jobpath_data(columns=["ppsn", "start_date"])
    df["start_period"] = df["start_date"].dt.to_period(period_type)
    all_starters_this_period = (
        df["ppsn"].loc[df["start_period"] == period].drop_duplicates()
    )
    return id_series.isin(all_starters_this_period)


def jobpath_hold_this_period(
    start_date: pd.Timestamp,
    id_series: pd.Series,
    period_type: str = "M",
    how: str = "end",
) -> pd.Series:
    """
    Given a time period and an id_series, 
    return True for every record with a JobPath hold, False otherwise

    Can choose how to measure JobPath Hold status:
        -- "end" -> at end of this period (only)
        -- "start" -> at start of this period (only)
        -- "both" -> at both start and end of this period

    Parameters
    ----------
    start_date: pd.Timestamp
        Start of this period
    
    id_series: pd.Series
        Pandas Series with IDs for lookup

    period_type: str
        Pandas period-type-identifying string. Default is 'M'.

    how: str = "end"
        Specify how to measure JobPath hold status

    Returns
    -------
    pd.Series(bool):
        Boolean series with same index as original id_series.
    """

    period = start_date.to_period(period_type)

    print(f"Get JobPathHold people in period: {period}")

    if how in ["start", "both"]:
        start_data = get_ists_claims(
            period.to_timestamp(how="S"), columns=["ppsn", "JobPathHold"]
        )
        jobpath_hold_at_start = (
            start_data["ppsn"].loc[start_data["JobPathHold"] == 1].drop_duplicates()
        )
    if how in ["end", "both"]:
        end_data = get_ists_claims(
            period.to_timestamp(how="E"), columns=["ppsn", "JobPathHold"]
        )
        jobpath_hold_at_end = (
            end_data["ppsn"].loc[end_data["JobPathHold"] == 1].drop_duplicates()
        )
    if how == "both":
        jobpath_hold = set(jobpath_hold_at_start) & set(jobpath_hold_at_end)
    elif how == "start":
        jobpath_hold = jobpath_hold_at_start
    elif how == "end":
        jobpath_hold = jobpath_hold_at_end

    return id_series.isin(jobpath_hold)


#%%
