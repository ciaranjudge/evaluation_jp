# %%
# Standard library
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, List, Set, Dict, Tuple, Optional, Union

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data.import_helpers import get_clusters, get_ists_claims
from evaluation_jp.data.persistence_helpers import (
    get_name,
    populate,
    save_data,
    load_data,
)
from evaluation_jp.features.selection_helpers import EligibilityChecker, EligibilityCheckManager
from evaluation_jp.features.metadata_helpers import lr_reporting_date

# from evaluation_jp.models.period import EvaluationPeriod

# %%
@dataclass
class EvaluationSlice:
    """
    Dataclass to manage state and eligibility criteria for evaluation slices

    Parameters
    ---------- 
    start_date: pd.Timestamp
        The start of the period that this object refers to

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")

    name_prefix: str = None
        Prefix to be added to logical name of object

    rebuild_all: bool = False
        If True, this will call create_dataframe on this object and all its children
        If False, this object and its children will first try to load existing data

    min_age: int = None
        Minimum age for eligibility

    max_age: int = 62
        Maximum age for eligibility

    is_on_lr: bool = True
        Flag that is True if only including LR cases in dataframe

    eligible_codes: Tuple[str] = ["UA", "UB"]
        List of codes which are eligible for treatment

    min_duration_days: int = 365
        Minimum duration of claim for eligibility

    Methods
    -------
    make_logical_name()

    setup_dataframe()

    save_dataframe()

    load_dataframe()

    setup_children()

    """

    ### -- Class variables -- ###
    TYPE_NAME: ClassVar[str] = "slice"
    # DATA_INDEXES: ClassVar[]

    ### -- Parameters set on __init__ -- ###
    ## Slice
    period: pd.Period
    root: Tuple[str] = (".",)
    name_prefix: str = None
    rebuild_all: bool = False
    clustering_eligibility_checker: EligibilityCheckManager = None
    evaluation_eligibility_checker: EligibilityCheckManager = None
    ## Outcomes
    outcome_start_date: pd.Timestamp = None
    outcome_end_date: pd.Timestamp = None
    ## Periods
    period_freq: str = "M"
    period_name_prefix: str = None
    last_period_date: pd.Timestamp = None
    period_eligibility_checker: EligibilityCheckManager = None

    ### -- Other attributes set __post_init__ -- ###
    ## Boilerplate
    name: str = field(init=False)
    path: Path = field(init=False)
    ## Data
    data: Dict[str, pd.DataFrame] = field(init=False)
    ## Periods managed by slice
    # periods: Dict[pd.Period, EvaluationPeriod] = field(init=False)

    ### -- Methods -- ###
    def __post_init__(self):
        self.name = get_name(
            self.TYPE_NAME,
            self.period.to_timestamp(how="S"),
            self.period.freq,
            # self.prefix,
        )
        self.data = {}
        self.get_seed_pop()
        self.get_clustered()
        self.get_eligiblity_checked_clustered()
        self.get_eligible_clustered()
        # self.setup_periods()

    @populate
    def get_seed_pop(self, index=["ppsn"]):
        """
        Set up initial pop: people on LR at period start with claim and personal info

        """
        # First get Live Register claims...
        self.data["seed_pop"] = get_ists_claims(
            self.period.to_timestamp(how="S"),
            lr_flag=True,  
            columns=["lr_code", "clm_comm_date"],
        )


    @populate
    def get_clustered(self):
        """
        Create clusters for population, starting from seed_pop

        Here's one we've prepared earlier...
        """
        clusters = get_clusters(self.period.to_timestamp(how="S"))
        self.data["clustered"] = pd.merge(
            left=self.data["seed_pop"],
            right=clusters,
            left_index=True,
            right_index=True,
        )

    @populate
    def get_eligiblity_checked_clustered(self):
        """
        Get eligibility dimensions 

        """
        eligiblity_checked = self.evaluation_eligibility_checker.get_eligibility(
            self.period.to_timestamp(how="S"), self.data["clustered"].index
        )
        self.data["eligibility_checked_clustered"] = pd.concat(
            [self.data["clustered"], eligiblity_checked],
            axis="columns",
            join_axes=[self.data["clustered"].index],
        )

    @populate
    def get_eligible_clustered(self):
        eligible = self.data["eligibility_checked_clustered"]["eligible"] == True
        self.data["eligible_clustered"] = self.data[
            "eligibility_checked_clustered"
        ].loc[eligible]

    # def setup_periods(self):
    #     """
    #     Create dict of {Period: EvaluationPeriod} for each period up to last_period_date

    #     Default frequency is monthly ('M') but can be quarterly ('Q') or even weekly('W')

    #     Parameters
    #     ----------
    #     freq: str = 'M'
    #         Frequency of periods to be created. Default is monthly ('M').
    #         Quarterly ('Q') and weekly ('W') are possible.

    #     """
    #     period_range = pd.period_range(
    #         start=self.period.to_timestamp(how="S"), end=self.last_period_date, freq=self.period_freq
    #     )
    #     self.periods = {}
    #     for index, period in enumerate(period_range):
    #         if index == 0:
    #             seed = self.data["eligible_clustered_pop"]
    #         else:
    #             seed = self.periods[period_range[index - 1]].data["eligible_clustered_pop"]
    #         self.periods[period] = EvaluationPeriod(
    #             ## Period
    #             start_date=period.to_timestamp("S"),
    #             logical_root=tuple(list(self.logical_root) + [self.logical_name]),
    #             seed_population=seed,
    #             rebuild_all=self.rebuild_all,
    #             freq=self.period_freq,
    #             evaluation_eligibility_flags=self.period_evaluation_eligibility_flags,
    #             ## Outcomes
    #             outcome_start_date=period.to_timestamp("S") + pd.DateOffset(months=1),
    #             outcome_end_date=self.outcome_end_date,
    #         )


# def main():
# '''creates a useful class and does something with it for our
# module.'''
# useful = UsefulClass()
# print(useful)
# if __name__ == "__main__":
# main()


# Need to add:
# Counterfactual start date
# Treatment period type
# Number of treatment periods to create




#%%
