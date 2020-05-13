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


def ref_date_from_id(data_id, how="Start"):
    if isinstance(data_id, pd.Timestamp):
        return data_id
    else:
        try:
            return data_id.date
        except AttributeError:
            return data_id.time_period.to_timestamp(how=how)


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
    """Get `columns` about people on Live Register on `data_id.date`.
    With initial data, restrict generated dataset to data[starting_pop_col]
    """

    # Parameters
    columns: List[str]
    starting_pop_col: str = None  # Must be bool!

    # Setup method
    def run(self, data_id, data=None):
        if data is None:
            data = get_ists_claims(ref_date_from_id(data_id), columns=self.columns)
        else:
            live_register_population = get_ists_claims(
                ref_date_from_id(data_id), columns=self.columns
            )
            starting_pop = data.loc[data[self.starting_pop_col]]
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
            episodes=les, ref_date=ref_date_from_id(data_id, self.how), id_cols="ppsn",
        )
        out_colname = f"on_les_at_{self.how}" if self.how else "on_les"
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


@dataclass
class JobPathStartedEndedSamePeriod(SetupStep):
    def run(self, data_id, data):
        start = ref_date_from_id(data_id, how="start")
        end = ref_date_from_id(data_id, how="end")

        jobpath = get_jobpath_data(columns=["jobpath_start_date", "jobpath_end_date"])
        starts = jobpath["jobpath_start_date"].between(start, end)
        ends = jobpath["jobpath_end_date"].between(start, end)
        started_and_ended_by_id = (
            pd.pivot_table(
                jobpath[starts & ends],
                values="jobpath_start_date",
                index="ppsn",
                aggfunc=np.any,
            )
            .squeeze(axis="columns")
            .astype(bool)
        )
        data["jobpath_started_and_ended"] = (
            started_and_ended_by_id.loc[
                started_and_ended_by_id.index.intersection(data.index)
            ]
            .reindex(data.index)
            .fillna(False)
        )
        return data


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


@dataclass
class JobPathStarts(SetupStep):
    use_jobpath_operational_data: bool = True
    use_ists_claim_data: bool = False
    ists_jobpath_flag_col: str = None
    combine_data: str = None  # "either" or "both"

    def run(self, data_id, data):
        start = ref_date_from_id(data_id, how="start")
        end = ref_date_from_id(data_id, how="end")

        if self.use_jobpath_operational_data:
            jobpath_operational = get_jobpath_data(columns=["jobpath_start_date"])
            started = jobpath_operational[
                jobpath_operational["jobpath_start_date"].between(start, end)
            ]
            starts_by_id = (
                pd.pivot_table(
                    started, values="jobpath_start_date", index="ppsn", aggfunc=np.any
                )
                .squeeze(axis="columns")
                .astype(bool)
            )
            operational_jobpath_starts = (
                starts_by_id.loc[starts_by_id.index.intersection(data.index)]
                .reindex(data.index)
                .fillna(False)
            )
        else:
            operational_jobpath_starts = pd.Series(data=False, index=data.index)

        if self.use_ists_claim_data:
            on_jobpath = OnJobPath(
                assumed_episode_length={"years": 1},
                use_jobpath_operational_data=False,
                use_ists_claim_data=True,
                ists_jobpath_flag_col="JobPath_Flag",
            )
            ists_jobpath_start = on_jobpath.run(start, data.copy())["on_jobpath"]
            ists_jobpath_end = on_jobpath.run(end, data.copy())["on_jobpath"]
            ists_jobpath_starts = ~ists_jobpath_start & ists_jobpath_end
        else:
            ists_jobpath_starts = pd.Series(data=False, index=data.index)

        if (
            self.use_jobpath_operational_data
            and self.use_ists_claim_data
            and self.combine_data == "both"
        ):
            data["jobpath_starts"] = operational_jobpath_starts & ists_jobpath_starts
        else:  # combine == "either" or only one source in use
            data["jobpath_starts"] = operational_jobpath_starts | ists_jobpath_starts

        return data


@dataclass
class EvaluationGroup(SetupStep):
    eligible_col: str
    treatment_col: str
    treatment_label: str = "T"
    control_label: str = "C"

    def run(self, data_id, data):
        eligible = data[self.eligible_col]
        data.loc[eligible, "evaluation_group"] = self.control_label
        eligible_and_treatment = data[self.eligible_col] & data[self.treatment_col]
        data.loc[eligible_and_treatment, "evaluation_group"] = self.treatment_label

        return data


@dataclass
class StartingPopulation(SetupStep):
    starting_pop_col: str = None
    starting_pop_label: str = None

    def run(self, data_id, data):
        if self.starting_pop_col in data.columns:
            data["eligible_population"] = (
                data[self.starting_pop_col] == self.starting_pop_label
            )
        data = data[["eligible_population"]]

        return data
