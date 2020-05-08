# %%
import collections
import abc
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

import numpy as np
import pandas as pd

from evaluation_jp.data import get_ists_claims, get_les_data, get_jobpath_data


# %%
class NearestKeyDict(collections.UserDict):
    """Dictionary for looking up nearest key to a given key.
    Designed mainly for date lookups but can work for any keys implementing <= >=

    Based on https://stackoverflow.com/a/3387975/13088500
    """

    def __init__(self, data={}, how="max_before"):
        self.data = data
        self.how = how

    def __getitem__(self, key):
        return self.data[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.data[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.data[self.__keytransform__(key)]

    def __keytransform__(self, key):
        if self.how != "min_after":
            if len(candidate_keys := [k for k in sorted(self.data) if k <= key]):
                return max(candidate_keys)
            else:
                raise KeyError(f"No data key found before {key}")
        else:
            if len(candidate_keys := [k for k in sorted(self.data) if k >= key]):
                return min(candidate_keys)
            else:
                raise KeyError(f"No data key found after {key}")


@dataclass
class SetupStep(abc.ABC):
    # Parameters

    # Setup method
    @abc.abstractmethod
    def run(self, data_id, data=None):
        """Do something and return data"""
        pass


@dataclass
class SetupSteps:
    """Ordered sequence of setup steps, each represented by a dataclass
    Each dataclass should have a run(data) method
    """

    steps: List[SetupStep]

    def run(self, data_id=None, data: pd.DataFrame = None):
        for step in self.steps:
            data = step.run(data_id, data=data)
        return data


@dataclass
class LiveRegisterPopulation(SetupStep):
    """Get `columns` about people on Live Register on `data_id.date`
    Assume this is for EvaluationSlice, so data_id.date exists and is a date
    """
    # Parameters
    columns: List[str]

    # Setup method
    def run(self, data_id):
        return get_ists_claims(data_id.date, columns=self.columns)

@dataclass
class AgeEligible(SetupStep):
    """Add a boolean column that is True if age between min and max eligible. 
    """
    date_of_birth_col: str
    min_eligible: Dict[str, int] = None
    max_eligible: Dict[str, int] = None

    def run(self, data_id, data):
        date = data_id.date
        dates_of_birth = data[self.date_of_birth_col]
        if self.min_eligible:
            min_age = pd.DateOffset(**self.min_eligible)
            min_date_of_birth = date - min_age
            ge_min_eligible = dates_of_birth <= min_date_of_birth
        else:
            ge_min_eligible = pd.Series(data=True, index=data.index)

        if self.max_eligible:
            max_age = pd.DateOffset(**self.max_eligible)
            max_date_of_birth = date - max_age
            lt_max_eligible = max_date_of_birth < dates_of_birth
        else:
            lt_max_eligible = pd.Series(data=True, index=data.index)

        data["age_eligible"] = ge_min_eligible & lt_max_eligible
        return data


# def check_code(
#     date: pd.Timestamp, ids: pd.Index, eligible_codes: Tuple[str] = None
# ) -> pd.Series(bool):
#     """
#     Given a reference date, an ID series, and a tuple of codes,
#     return True for each id that has an eligible code on that date, else False

#     Parameters
#     ----------
#     date: pd.Timestamp
#         Reference date for lookup of ids

#     ids : pd.Index
#         Need unique IDs - should be pd.Index but will work with list or set

#     eligible : Tuple(str)
#         Tuple of eligible codes

#     Returns
#     -------
#     pd.Series(bool)
#         Boolean series with original id index, True for each id with eligible code
#     """

#     if (eligible_codes is not None) and (len(eligible_codes) > 0):
#         print(f"Check codes using eligible: {eligible_codes}")
#         code_data = get_ists_claims(date, ids, columns=["lr_code"]).squeeze(
#             axis="columns"
#         )
#         return code_data.isin(eligible_codes)
#     else:
#         return pd.Series(data=True, index=ids)


# def check_duration(
#     date: pd.Timestamp,
#     ids: pd.Index,
#     min_duration: pd.DateOffset = None,
#     max_duration: pd.DateOffset = None,
# ) -> pd.Series(bool):
#     """
#     Given a reference date, an ID series, and checks for min and/or max duration,
#     return True for each id that has an eligible code on that date, else False

#     Parameters
#     ----------
#     date: pd.Timestamp
#         Reference date for lookup of ids and calculating duration

#     ids : pd.Index
#         Need unique IDs - should be pd.Index but will work with list or set

#     min_duration : pd.DateOffset = None

#     max_duration : pd.DateOffset = None

#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original durations series.
#         True for each record between given min/max durations, False otherwise.
#     """
#     if (min_duration is not None) or (max_duration is not None):
#         print(f"Check duration using min: {min_duration} and max: {max_duration}")
#         claim_start_dates = get_ists_claims(
#             date, ids, columns=["clm_comm_date"]
#         ).squeeze(axis="columns")

#     if min_duration is not None:
#         date_of_min_duration = date - min_duration
#         at_least_min_duration = claim_start_dates <= date_of_min_duration
#     else:
#         at_least_min_duration = pd.Series(data=True, index=claim_start_dates.index)

#     if max_duration is not None:
#         date_of_max_duration = date - max_duration
#         less_than_max_duration = date_of_max_duration < claim_start_dates
#     else:
#         less_than_max_duration = pd.Series(data=True, index=claim_start_dates.index)

#     return at_least_min_duration & less_than_max_duration


# def on_les(
#     date: pd.Timestamp,
#     ids: pd.Index,
#     episode_duration: pd.DateOffset = pd.DateOffset(years=1),
# ) -> pd.Series:
#     """
#     Given a date and id_series, return True for every record on LES on that date

#     Parameters
#     ----------
#     date: pd.Timestamp
#         Reference date for lookup of ids

#     ids : pd.Index
#         Need unique IDs - should be pd.Index but will work with list or set

#     les_episode_duration : pd.DateOffset

#     Returns
#     -------
#     pd.Series(bool)
#         Boolean series with same index as original id_series.
#     """

#     if episode_duration is not None:
#         print(f"Find people on LES using duration: {episode_duration}")
#         les = get_les_data(columns=["ppsn", "start_date"])
#         gte_start = les["start_date"] <= date
#         lte_end = date <= les["start_date"] + episode_duration
#         les.loc[gte_start & lte_end, "on_les"] = True
#         on_les_on_date = (
#             pd.pivot_table(les, values="on_les", index="ppsn", aggfunc=np.any)
#             .squeeze(axis="columns")
#             .reindex(ids, fill_value=False)
#         )
#         return on_les_on_date
#     else:
#         return pd.Series(data=False, index=ids)


# def on_jobpath(
#     date: pd.Timestamp,
#     ids: pd.Index,
#     episode_duration: pd.DateOffset = pd.DateOffset(years=1),
#     use_jobpath_data: bool = True,
#     use_ists_data: bool = True,
#     combine: str = "either",  # Can be "either" or "both"
# ) -> pd.Series:
#     """
#     Given a date and an id_series, return a series which is
#     True for every record with any previous JobPath history and False otherwise

#     Parameters
#     ----------
#     date : pd.Timestamp
#         Lookup date

#     id_series : pd.Series
#         Pandas Series with IDs for lookup

#     episode_duration: pd.DateOffset = pd.DateOffset(years=1)

#     use_jobpath_data: bool = True

#     use_ists_data: bool = True

#     combine: str = "either"
#         Can be "either" or "both"

#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original id_series.
#     """
#     if use_jobpath_data or use_ists_data:
#         print(f"Find people on JobPath on {date} assuming duration {episode_duration}")

#         # First get ppsns of all JobPath starters before date from JobPath database
#         if use_jobpath_data:
#             jobpath = get_jobpath_data(columns=["ppsn", "start_date"])
#             gte_start = jobpath["start_date"] <= date
#             lte_end = date <= jobpath["start_date"] + episode_duration
#             jobpath.loc[gte_start & lte_end, "on_jobpath"] = True
#             on_jobpath_jobpath = (
#                 pd.pivot_table(
#                     jobpath, values="on_jobpath", index="ppsn", aggfunc=np.any
#                 )
#                 .squeeze(axis="columns")
#                 .reindex(ids, fill_value=False)
#             )
#         else:
#             on_jobpath_jobpath = pd.Series(data=False, index=ids)

#         # Then get ppsn of everyone with an ISTS JobPath flag at end of previous month
#         if use_ists_data:
#             on_jobpath_ists = get_ists_claims(
#                 date, ids, columns=["JobPath_Flag"]
#             ).squeeze(axis="columns")
#         else:
#             on_jobpath_ists = pd.Series(data=False, index=ids)

#         if use_jobpath_data and use_ists_data and combine == "both":
#             return on_jobpath_jobpath & on_jobpath_ists
#         else:  # combine == "either" or only one source in use
#             return on_jobpath_jobpath | on_jobpath_ists


# def les_starts(date: pd.Timestamp, ids: pd.Index, period_type: str = "M") -> pd.Series:
#     """
#     Given a start_date, an id_series, and an optional period_type (defaults to "M")
#     return True for every record with a LES start in this period, False otherwise

#     Parameters
#     ----------
#     date: pd.Timestamp
#         Start of this period

#     ids: pd.Series
#         Pandas Series with IDs for lookup

#     period_type: str = 'M'
#         Pandas period-type-identifying string. Default is 'M'.
#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original id_series.
#     """
#     period = date.to_period(period_type)
#     print(f"Get LES starters in period: {period}")
#     les = get_les_data(columns=["ppsn", "start_date"])
#     gte_period_start = period.start_time <= les["start_date"]
#     lte_period_end = les["start_date"] <= period.end_time
#     les.loc[gte_period_start & lte_period_end, "on_les"] = True
#     les_starts = (
#         pd.pivot_table(les, values="on_les", index="ppsn", aggfunc=np.any)
#         .squeeze(axis="columns")
#         .reindex(ids, fill_value=False)
#     )
#     return les_starts


# def jobpath_starts(
#     date: pd.Timestamp,
#     ids: pd.Index,
#     period_type: str = "M",
#     use_jobpath_data: bool = True,
#     use_ists_data: bool = True,
#     combine: str = "either",  # Can be "either" or "both"
# ) -> pd.Series:
#     """
#     Given a start_date and an id_series, with optional period_type (default is 'M'):
#     return True for every record with a JobPath start in this period, False otherwise

#     Parameters
#     ----------
#     start_date: pd.Timestamp
#         Start of this period

#     id_series: pd.Series
#         Pandas Series with IDs for lookup

#     period_type: str
#         Pandas period-type-identifying string. Default is 'M'.

#     use_jobpath_data: bool = True

#     use_ists_data: bool = False

#     combine: str = "either"
#         Can be "either" or "both"

#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original id_series.
#     """

#     if use_jobpath_data:
#         period = date.to_period(period_type)
#         print(f"Get JobPath starters in period: {period}")
#         jobpath = get_jobpath_data(columns=["ppsn", "start_date"])
#         gte_period_start = period.start_time <= jobpath["start_date"]
#         lte_period_end = jobpath["start_date"] <= period.end_time
#         jobpath.loc[gte_period_start & lte_period_end, "on_jobpath"] = True
#         jobpath_jobpath_starts = (
#             pd.pivot_table(jobpath, values="on_jobpath", index="ppsn", aggfunc=np.any)
#             .squeeze(axis="columns")
#             .astype(bool)
#             .reindex(ids, fill_value=False)
#         )
#     else:
#         jobpath_jobpath_starts = pd.Series(data=False, index=ids)

#     if use_ists_data:
#         # Not on at start but on at end
#         not_ists_jobpath_at_start = ~on_jobpath(
#             date,
#             ids,
#             episode_duration=pd.DateOffset(years=1),
#             use_jobpath_data=False,
#             use_ists_data=True,
#         )
#         ists_jobpath_at_end = on_jobpath(
#             date + pd.DateOffset(months=1),
#             ids,
#             episode_duration=pd.DateOffset(years=1),
#             use_jobpath_data=False,
#             use_ists_data=True,
#         )
#         ists_jobpath_starts = not_ists_jobpath_at_start & ists_jobpath_at_end
#     else:
#         ists_jobpath_starts = pd.Series(data=False, index=ids)

#     if use_jobpath_data and use_ists_data and combine == "both":
#         return jobpath_jobpath_starts & ists_jobpath_starts
#     else:  # combine == "either" or only one source in use
#         return jobpath_jobpath_starts | ists_jobpath_starts


# def jobpath_hold(
#     date: pd.Timestamp, ids: pd.Series, period_type: str = "M", when: str = "end"
# ) -> pd.Series:
#     """
#     Given a time period and an id_series,
#     return True for every record with a JobPath hold, False otherwise

#     Parameters
#     ----------
#     start_date: pd.Timestamp
#         Start of this period

#     id_series: pd.Series
#         Pandas Series with IDs for lookup

#     period_type: str
#         Pandas period-type-identifying string. Default is 'M'.

#     when: str = "end"
#         Specify when to measure JobPath hold status.
#         Possible values are "start", "end", "both", "either".

#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original id_series.
#     """

#     period = date.to_period(period_type)

#     print(f"Get JobPathHold people in period: {period}")

#     if when in ["start", "both", "either"]:
#         jobpath_hold_at_start = (
#             get_ists_claims(period.to_timestamp(when="S"), ids, columns=["JobPathHold"])
#             .squeeze(axis="columns")
#             .astype(bool)
#         )
#     if when in ["end", "both", "either"]:
#         jobpath_hold_at_end = (
#             get_ists_claims(period.to_timestamp(when="E"), ids, columns=["JobPathHold"])
#             .squeeze(axis="columns")
#             .astype(bool)
#         )

#     if when == "both":
#         jobpath_hold = jobpath_hold_at_start & jobpath_hold_at_end
#     if when == "either":
#         jobpath_hold = jobpath_hold_at_start | jobpath_hold_at_end
#     elif when == "start":
#         jobpath_hold = jobpath_hold_at_start
#     elif when == "end":
#         jobpath_hold = jobpath_hold_at_end

#     return jobpath_hold


# def on_lr(
#     date: pd.Timestamp, ids: pd.Series, period_type: str = "M", when: str = "end"
# ) -> pd.Series:
#     """
#     Given a time period and an id_series,
#     return True for every record with an on_lr flag, False otherwise

#     Parameters
#     ----------
#     start_date: pd.Timestamp
#         Start of this period

#     id_series: pd.Series
#         Pandas Series with IDs for lookup

#     period_type: str
#         Pandas period-type-identifying string. Default is 'M'.

#     when: str = "end"
#         Specify time(s) at which to measure status.
#         Possible values are "start", "end", "both", "either".

#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original id_series.
#     """

#     period = date.to_period(period_type)

#     print(f"Get LR status of people in period: {period}")

#     if when in ["start", "both", "either"]:
#         on_lr_at_start = (
#             get_ists_claims(period.to_timestamp(how="S"), ids, columns=["on_lr"])
#             .squeeze(axis="columns")
#             .astype(bool)
#         )
#     if when in ["end", "both", "either"]:
#         on_lr_at_end = (
#             get_ists_claims(period.to_timestamp(how="E"), ids, columns=["on_lr"])
#             .squeeze(axis="columns")
#             .astype(bool)
#         )

#     if when == "both":
#         on_lr = on_lr_at_start & on_lr_at_end
#     if when == "either":
#         on_lr = on_lr_at_start | on_lr_at_end
#     elif when == "start":
#         on_lr = on_lr_at_start
#     elif when == "end":
#         on_lr = on_lr_at_end

#     return on_lr


# def programme_starts(
#     date: pd.Timestamp,
#     ids: pd.Index,
#     period_type: str = "M",
#     programmes: Tuple[str] = ("jobpath_starts", "les_starts"),
#     combine: str = "any",
#     jobpath_use_jobpath_data: bool = True,
#     jobpath_use_ists_data: bool = True,
#     jobpath_combine: str = "either",  # Can be "either" or "both"
# ) -> pd.Series:
#     """
#     Given a start_date and an id_series, with optional period_type (default is 'M'):
#     return True for every record with a JobPath start in this period, False otherwise

#     Parameters
#     ----------
#     start_date : pd.Timestamp
#         Start of this period

#     id_series : pd.Series
#         Pandas Series with IDs for lookup

#     period_type : str
#         Pandas period-type-identifying string. Default is 'M'.

#     programmes : Tuple[str] = ("jobpath_starts", "les_starts")
#         Programmes to check starts for

#     combine : str = "any"
#         How to combine programme start information

#     jobpath_use_jobpath_data: bool = True

#     jobpath_use_ists_data: bool = False

#     jobpath_combine: str = "either"
#         Can be "either" or "both"

#     Returns
#     -------
#     pd.Series(bool):
#         Boolean series with same index as original id_series.
#     """
#     if "jobpath" in programmes:
#         jobpath_starters = jobpath_starts(
#             date,
#             ids,
#             period_type=period_type,
#             use_jobpath_data=jobpath_use_jobpath_data,
#             use_ists_data=jobpath_use_ists_data,
#             combine=jobpath_combine,
#         )
#     else:
#         jobpath_starters = pd.Series(data=False, index=ids)

#     if "les" in programmes:
#         les_starters = les_starts(date, ids, period_type=period_type)
#     else:
#         les_starters = pd.Series(data=False, index=ids)

#     return jobpath_starters | les_starters
