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


def dates_between_durations(
    dates: pd.Series,
    ref_date: pd.Timestamp,
    min_duration: pd.DateOffset = None,
    max_duration: pd.DateOffset = None,
) -> pd.Series:
    """Return boolean series for records with dates between specified limits.
    True if `min_duration` < (`ref_date` - `date`) < `max_duration`, False otherwise.
    Assume all dates are in the past.
    """
    if min_duration:
        latest_possible_date = ref_date - min_duration
        # If duration is longer, date must be earlier!
        ge_min_duration = dates <= latest_possible_date
    else:
        ge_min_duration = pd.Series(data=True, index=dates.index)

    if max_duration:
        earliest_possible_date = ref_date - max_duration
        # If duration is longer, date must be earlier!
        lt_max_duration = earliest_possible_date < dates
    else:
        lt_max_duration = pd.Series(data=True, index=dates.index)

    return ge_min_duration & lt_max_duration


class DataIDKeyMissing(KeyError):
    """Can't find field in a data ID object.
    """

    pass


def ref_date_from_id(data_id, how="Start"):
    if data_id.date:
        return data_id.date
    elif data_id.time_period:
        return data_id.time_period.to_timestamp(how=how)
    else:
        raise DataIDKeyMissing


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


# //TODO Implement StartingPopulation for TreatmentPeriod
@dataclass
class StartingPopulation(SetupStep):
    starting_pop_col: str = None
    starting_pop_val: str = None

    def run(self, data_id, data):
        return data

@dataclass
class LiveRegisterPopulation(SetupStep):
    """Get `columns` about people on Live Register on `data_id.date`.
    With initial data, restrict generated dataset to data[starting_pop_col]
    """

    # Parameters
    columns: List[str]
    eligible_col: str = None

    # Setup method
    def run(self, data_id, data=None):
        if data is None:
            data = get_ists_claims(ref_date_from_id(data_id), columns=self.columns)
        else:
            live_register_population = get_ists_claims(
                ref_date_from_id(data_id), columns=self.columns
            )
            starting_pop = data.loc[
                data[self.starting_pop_col] == self.starting_pop_include_val
            ]
            data = live_register_population.loc[
                live_register_population.index.intersection(starting_pop.index)
            ]
        data = data.drop({"lr_flag"}, axis="columns")
        return data


@dataclass
class AgeEligible(SetupStep):
    """Add bool "age_eligible" col to `data`. True if `min_eligible` <= `date_of_birth_col` < `max_eligible`
    `min_eligible` and `max_eligible` are both optional.
    """

    date_of_birth_col: str
    min_eligible: Dict[str, int] = None
    max_eligible: Dict[str, int] = None

    def run(self, data_id, data):

        if self.min_eligible:
            min_age = pd.DateOffset(**self.min_eligible)
        else:
            min_age = None

        if self.max_eligible:
            max_age = pd.DateOffset(**self.max_eligible)
        else:
            max_age = None

        data["age_eligible"] = dates_between_durations(
            dates=data[self.date_of_birth_col],
            ref_date=ref_date_from_id(data_id),
            min_duration=min_age,
            max_duration=max_age,
        )
        return data


@dataclass
class ClaimCodeEligible(SetupStep):
    """Add bool "claim_code_eligible" col to `data`. True if `code_col` in `eligible codes`, False otherwise
    """

    code_col: str
    eligible_codes: list = None

    def run(self, data_id, data):
        if self.eligible_codes:
            claim_code_eligible = data[self.code_col].isin(self.eligible_codes)
        else:
            claim_code_eligible = pd.Series(data=True, index=data.index)
        data["claim_code_eligible"] = claim_code_eligible
        return data


@dataclass
class ClaimDurationEligible(SetupStep):
    """Add bool "claim_duration_eligible" col to `data`.
    True if `claim_start_col` , False otherwise
    """

    claim_start_col: str
    min_eligible: Dict[str, int] = None
    max_eligible: Dict[str, int] = None

    def run(self, data_id, data):

        if self.min_eligible:
            min_duration = pd.DateOffset(**self.min_eligible)
        else:
            min_duration = None

        if self.max_eligible:
            max_duration = pd.DateOffset(**self.max_eligible)
        else:
            max_duration = None

        data["claim_duration_eligible"] = dates_between_durations(
            dates=data[self.claim_start_col],
            ref_date=ref_date_from_id(data_id),
            min_duration=min_duration,
            max_duration=max_duration,
        )
        return data


def open_episodes_on_ref_date(
    episodes,
    ref_date,
    start_date_col="start_date",
    end_date_col="end_date",
    id_cols=None,
):
    """Given dataframe of `episodes`, return boolean series for all episodes open on ref_date
    """
    episodes["open_on_ref_date"] = (episodes[start_date_col] <= ref_date) & (
        ref_date <= episodes[end_date_col]
    )
    if id_cols:
        episodes_by_id = pd.pivot_table(
            episodes, values="open_on_ref_date", index=id_cols, aggfunc=np.any
        )
        return episodes_by_id["open_on_ref_date"]
    else:
        # Assume episodes.index is already what's required by caller
        return episodes["open_on_ref_date"]


@dataclass
class OnLES(SetupStep):
    """Given a data_id and data, return True for every record on LES on data_id reference date
    """

    assumed_episode_length: Dict[str, int]
    how: str = None  # Can be "start" or "end" for periods. Leave as None for slices.

    def run(self, data_id, data):
        les = get_les_data(columns=["start_date"])
        les["end_date"] = les["start_date"] + pd.DateOffset(
            **self.assumed_episode_length
        )
        open_episodes = open_episodes_on_ref_date(
            episodes=les, ref_date=ref_date_from_id(data_id, how), id_cols="ppsn",
        )
        out_colname = f"on_les_{how}" if how else "on_les"
        data[out_colname] = (
            open_episodes.loc[open_episodes.index.intersection(data.index)]
            .reindex(data.index)
            .fillna(False)
        )

        return data


@dataclass
class OnJobPath(SetupStep):
    assumed_episode_length: Dict[str, int]
    use_jobpath_operational_data: bool = True
    use_ists_claim_data: bool = False
    ists_jobpath_flag_col: str = None
    combine_data: str = None  # "either" or "both"

    def run(self, data_id, data):
        if self.use_jobpath_operational_data:
            jobpath_operational = get_jobpath_data(
                columns=["jobpath_start_date", "jobpath_end_date"]
            )
            jobpath_operational["jobpath_end_date"] = jobpath_operational[
                "jobpath_end_date"
            ].fillna(
                value=jobpath_operational["jobpath_start_date"]
                + pd.DateOffset(**self.assumed_episode_length)
            )
            open_episodes = open_episodes_on_ref_date(
                episodes=jobpath_operational,
                ref_date=ref_date_from_id(data_id),
                start_date_col="jobpath_start_date",
                end_date_col="jobpath_end_date",
                id_cols="ppsn",
            )
            on_jobpath_operational = (
                open_episodes.loc[open_episodes.index.intersection(data.index)]
                .reindex(data.index)
                .fillna(False)
            )
        else:
            on_jobpath_operational = pd.Series(data=False, index=data.index)

        # Then get ppsn of everyone with an ISTS JobPath flag at end of previous month
        if self.use_ists_claim_data:
            on_jobpath_ists = data[self.ists_jobpath_flag_col]
        else:
            on_jobpath_ists = pd.Series(data=False, index=data.index)

        if (
            self.use_jobpath_operational_data
            and self.use_ists_claim_data
            and self.combine_data == "both"
        ):
            data["on_jobpath"] = on_jobpath_operational & on_jobpath_ists
        else:  # combine == "either" or only one source in use
            data["on_jobpath"] = on_jobpath_operational | on_jobpath_ists

        return data


# //TODO Add JobPathStarts
# # # def jobpath_starts(
# # #     date: pd.Timestamp,
# # #     ids: pd.Index,
# # #     period_type: str = "M",
# # #     use_jobpath_data: bool = True,
# # #     use_ists_data: bool = True,
# # #     combine: str = "either",  # Can be "either" or "both"
# # # ) -> pd.Series:
# # #     """
# # #     Given a start_date and an id_series, with optional period_type (default is 'M'):
# # #     return True for every record with a JobPath start in this period, False otherwise

# # #     Parameters
# # #     ----------
# # #     start_date: pd.Timestamp
# # #         Start of this period

# # #     id_series: pd.Series
# # #         Pandas Series with IDs for lookup

# # #     period_type: str
# # #         Pandas period-type-identifying string. Default is 'M'.

# # #     use_jobpath_data: bool = True

# # #     use_ists_data: bool = False

# # #     combine: str = "either"
# # #         Can be "either" or "both"

# # #     Returns
# # #     -------
# # #     pd.Series(bool):
# #         Boolean series with same index as original id_series.
# #     """

# #     if use_jobpath_data:
# #         period = date.to_period(period_type)
# #         print(f"Get JobPath starters in period: {period}")
# #         jobpath = get_jobpath_data(columns=["ppsn", "start_date"])
# #         gte_period_start = period.start_time <= jobpath["start_date"]
# #         lte_period_end = jobpath["start_date"] <= period.end_time
# #         jobpath.loc[gte_period_start & lte_period_end, "on_jobpath"] = True
# #         jobpath_jobpath_starts = (
# #             pd.pivot_table(jobpath, values="on_jobpath", index="ppsn", aggfunc=np.any)
# #             .squeeze(axis="columns")
# #             .astype(bool)
# #             .reindex(ids, fill_value=False)
# #         )
# #     else:
# #         jobpath_jobpath_starts = pd.Series(data=False, index=ids)

# #     if use_ists_data:
# #         # Not on at start but on at end
# #         not_ists_jobpath_at_start = ~on_jobpath(
# #             date,
# #             ids,
# #             episode_duration=pd.DateOffset(years=1),
# #             use_jobpath_data=False,
# #             use_ists_data=True,
# #         )
# #         ists_jobpath_at_end = on_jobpath(
# #             date + pd.DateOffset(months=1),
# #             ids,
# #             episode_duration=pd.DateOffset(years=1),
# #             use_jobpath_data=False,
# #             use_ists_data=True,
# #         )
# #         ists_jobpath_starts = not_ists_jobpath_at_start & ists_jobpath_at_end
# #     else:
# #         ists_jobpath_starts = pd.Series(data=False, index=ids)

# #     if use_jobpath_data and use_ists_data and combine == "both":
# #         return jobpath_jobpath_starts & ists_jobpath_starts
# #     else:  # combine == "either" or only one source in use
# #         return jobpath_jobpath_starts | ists_jobpath_starts

# //TODO Add JobPathHold
# # def jobpath_hold(
# #     date: pd.Timestamp, ids: pd.Series, period_type: str = "M", when: str = "end"
# # ) -> pd.Series:
# #     """
# #     Given a time period and an id_series,
# #     return True for every record with a JobPath hold, False otherwise

# #     Parameters
# #     ----------
# #     start_date: pd.Timestamp
# #         Start of this period

# #     id_series: pd.Series
# #         Pandas Series with IDs for lookup

# #     period_type: str
# #         Pandas period-type-identifying string. Default is 'M'.

# #     when: str = "end"
# #         Specify when to measure JobPath hold status.
# #         Possible values are "start", "end", "both", "either".

# #     Returns
# #     -------
# #     pd.Series(bool):
# #         Boolean series with same index as original id_series.
# #     """

# #     period = date.to_period(period_type)

# #     print(f"Get JobPathHold people in period: {period}")

# #     if when in ["start", "both", "either"]:
# #         jobpath_hold_at_start = (
# #             get_ists_claims(period.to_timestamp(when="S"), ids, columns=["JobPathHold"])
# #             .squeeze(axis="columns")
# #             .astype(bool)
# #         )
# #     if when in ["end", "both", "either"]:
# #         jobpath_hold_at_end = (
# #             get_ists_claims(period.to_timestamp(when="E"), ids, columns=["JobPathHold"])
# #             .squeeze(axis="columns")
# #             .astype(bool)
# #         )

# #     if when == "both":
# #         jobpath_hold = jobpath_hold_at_start & jobpath_hold_at_end
# #     if when == "either":
# #         jobpath_hold = jobpath_hold_at_start | jobpath_hold_at_end
# #     elif when == "start":
# #         jobpath_hold = jobpath_hold_at_start
# #     elif when == "end":
# #         jobpath_hold = jobpath_hold_at_end

# #     return jobpath_hold


# //TODO Recursive processing of nested dict with "all", "any" and "not" as keys
# Values should be names of dataframe boolean columns

@dataclass
class EligiblePopulation(SetupStep):
    eligibility_criteria: dict

    def run(self, data_id, data):
        eligibility_cols = []
        negative_cols = []
        for key, value in self.eligibility_criteria.items():
            if value is False:
                eligibility_cols += [f"not_{key}"]
                negative_cols += [f"not_{key}"]
                data[f"not_{key}"] = ~data[key]
            else:
                eligibility_cols += [key]
        data["eligible_population"] = data[eligibility_cols].all(axis="columns")
        data = data.drop(negative_cols, axis="columns")
        return data

        # data["evaluation_population"] = data[]

