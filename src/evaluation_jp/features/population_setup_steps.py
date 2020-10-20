# %%
from dataclasses import dataclass
from typing import List, Dict

import numpy as np
import pandas as pd


from evaluation_jp import (
    ColumnsByType,
    SetupStep,
    dates_between_durations,
    get_edw_customer_details,
    get_ists_claims,
    get_les_data,
    get_jobpath_data,
    get_edw_customer_details,
)


@dataclass
class LiveRegisterPopulation(SetupStep):
    """Get `columns` about people on Live Register on `data_id.date`.
    With initial data, restrict generated dataset to data[starting_pop_col]
    """

    # Parameters
    lookup_columns_by_type: ColumnsByType
    starting_pop_col: str = None  # Must be bool!

    # Setup method
    def run(self, data_id, data=None):
        if data is None:
            data = get_ists_claims(
                data_id.reference_date(),
                columns=self.lookup_columns_by_type.data_columns,
            )
        else:
            data = data[self.starting_pop_col]
            live_register_population = get_ists_claims(
                data_id.reference_date(),
                columns=self.lookup_columns_by_type.data_columns,
            )
            live_register_population["on_live_register"] = True
            data = pd.merge(
                data,
                live_register_population,
                how="left",
                left_index=True,
                right_index=True,
            )
        data = self.lookup_columns_by_type.set_datatypes(
            data.drop(["lr_flag"], axis="columns")
        )

        return data


@dataclass
class CustomerDetails(SetupStep):
    lookup_columns: List = None
    data_not_found_col: str = "customer_data_not_found"

    def run(self, data_id, data):

        customer_details = get_edw_customer_details(
            ppsns_to_lookup=data.index.to_series(),
            lookup_columns=self.lookup_columns,
            reference_date=data_id.reference_date(),
        )
        # Merge data with customer_details - make sure index is still ok
        augmented_data = pd.merge(
            data,
            customer_details.set_index("ppsn"),
            how="left",
            left_index=True,
            right_index=True,
        )
        # Find missing EDW records and add "insufficient_data" flag
        augmented_data[self.data_not_found_col] = np.where(
            augmented_data["client_gender"].isnull(), True, False
        )
        return augmented_data


@dataclass
class AgeEligible(SetupStep):
    """Add bool "age_eligible" col to `data`. True if `min_eligible` <= `date_of_birth_col` < `max_eligible`
    `min_eligible` and `max_eligible` are both optional.
    """

    date_of_birth_col: str
    output_flag_col: str = "age_eligible"
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

        data[self.output_flag_col] = dates_between_durations(
            dates=data[self.date_of_birth_col],
            ref_date=data_id.reference_date(),
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

    # //TODO Remove data_id safely from function signature
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
            ref_date=data_id.reference_date(),
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
            episodes=les, ref_date=data_id.reference_date(self.how), id_cols="ppsn",
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
                ref_date=data_id.reference_date(),
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
        start = data_id.reference_date(how="start")
        end = data_id.reference_date(how="end")

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
        try:
            data["jobpath_started_and_ended"] = (
                started_and_ended_by_id.loc[
                    started_and_ended_by_id.index.intersection(data.index)
                ]
                .reindex(data.index)
                .fillna(False)
            )
        except ValueError:
            data["jobpath_started_and_ended"] = False
        return data


# //TODO Recursive processing of nested dict with "all", "any" and "not" as keys
# Values should be names of dataframe boolean columns
# # # "any", "all", "not"
# # # "any", "all" need list
# # # "not" needs single thing


# # from collections.abc import Mapping, Sequence

# # # For every item in in_list:
# # # Should be either dict or str
# # # If str, add to out_list
# # # If dict, populate out_list item with resolution of recursive call


# # for key, value in logic_dict.itens():
# #     if key == "not":
# #         # Is it a single thing? Then negate it!
# #         if isinstance(value, str):
# #             print(f"Create col not_{value}")
# #             return f"not_{value}"

# #         # Raise an error if there's a list being not-ed
# #         elif isinstance(value, Sequence):
# #             print(f"Can't not {value}")

# #         # Recurse if it's a dict being not-ed
# #         elif isinstance(value, Mapping):
# #             print(f"Need to recurse this {value}")

# #     if key in ["any", "all"]:
# #         # Is it a single thing? Then
# #         pass


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
        start = data_id.reference_date(how="start")
        end = data_id.reference_date(how="end")

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
# //TODO add test for JobPath referral
@dataclass
class JobPathReferrals(SetupStep):
    
    def run(self, data_id, data):
        start = data_id.reference_date(how="start")
        end = data_id.reference_date(how="end")

        jobpath_operational = get_jobpath_data(columns=["referral_date"])
        referred = jobpath_operational[
            jobpath_operational["referral_date"].between(start, end)
        ]
        referrals_by_id = (
            pd.pivot_table(
                referred, values="referral_date", index="ppsn", aggfunc=np.any
            )
            .squeeze(axis="columns")
            .astype(bool)
        )
        operational_jobpath_referrals = (
            referrals_by_id.loc[referrals_by_id.index.intersection(data.index)]
            .reindex(data.index)
            .fillna(False)
        )
    
        data["jobpath_referral"] = operational_jobpath_referrals
        return data




@dataclass
class EvaluationGroup(SetupStep):
    eligible_col: str
    treatment_col: str
    treatment_label: str = "T"
    control_label: str = "C"

    def run(self, data_id=None, data=None):
        eligible = data[self.eligible_col]
        data.loc[eligible, "evaluation_group"] = self.control_label
        eligible_and_treatment = data[self.eligible_col] & data[self.treatment_col]
        data.loc[eligible_and_treatment, "evaluation_group"] = self.treatment_label

        return data


@dataclass
class StartingPopulation(SetupStep):
    eligible_from_pop_slice_col: str = None
    eligible_from_previous_period_col: str = None
    starting_pop_label: str = None

    def run(self, data_id=None, data=None):
        if self.eligible_from_previous_period_col in data.columns:
            data["starting_population"] = (
                data[self.eligible_from_previous_period_col] == self.starting_pop_label
            )
        else:
            data["starting_population"] = data[self.eligible_from_pop_slice_col]
        # Just keep "starting_population" column
        # Restrict to records where "starting_population" is True
        data = data.loc[data["starting_population"] == True, ["starting_population"]]

        return data
