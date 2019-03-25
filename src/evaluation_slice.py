# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from src.data.import_helpers import get_clusters, get_vital_statistics, get_ists_claims
from src.data.persistence_helpers import populate, save_data, load_data
from src.features.selection_helpers import EligibilityChecker
from src.features.metadata_helpers import lr_reporting_date

# from src.evaluation_period import EvaluationPeriod

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

    ### -- Parameters set on init -- ###
    ## Slice
    start_date: pd.Timestamp
    freq: str = "Q"
    logical_root: Tuple[str] = (".",)
    name_prefix: str = None
    rebuild_all: bool = False
    cluster_eligibility_flags: Tuple[str] = None  # Needed for reclustering
    evaluation_eligibility_flags: Tuple[str] = ()
    is_on_lr: bool = True  # Should be part of flags!!

    ## Outcomes
    outcome_start_date: pd.Timestamp = None
    outcome_end_date: pd.Timestamp = None

    ## Periods
    period_freq: str = "M"
    last_period_date: pd.Timestamp = None
    period_evaluation_eligibility_flags: Tuple[str] = ()

    ### -- Other attributes -- ###
    logical_name: str = field(init=False)
    data: dict = field(default=None, init=False)
    periods: dict = field(init=False)

    ### Methods

    def __post_init__(self):
        self.start_date = self.start_date.normalize()
        self.setup_logical_name()
        self.data = {}
        self.seed_pop()
        self.clustered_pop()
        # self.eligible_clustered_pop()
        # self.setup_periods()

    def setup_logical_name(self):
        f_name_prefix = f"{self.name_prefix}__" if self.name_prefix else ""
        f_start_date = self.start_date.to_period(freq=self.freq).strftime('%F-Q%q')
        self.logical_name = f"{f_name_prefix}slice_{f_start_date}"

    @populate
    def seed_pop(self):
        """
        Set up initial pop: people on LR on start_date with claim and personal info

        """
        # First get vital statistics...
        self.data["seed_pop"] = get_vital_statistics(self.start_date)

        # ...then merge with ISTS claims weekly database
        ists_columns = ["ppsn", "lr_code", "clm_comm_date"]
        self.data["seed_pop"] = pd.merge(
            left=self.data["seed_pop"],
            right=get_ists_claims(
                self.start_date, lr_flag=self.is_on_lr, columns=ists_columns
            ),
            on="ppsn",
        )

    @populate
    def clustered_pop(self):
        """
        Create clusters for population, starting from seed_pop

        """
        self.data["clustered_pop"] = pd.merge(
            left=self.data["seed_pop"], right=get_clusters(self.start_date), on="ppsn"
        )

    # @populate
    # def eligible_clustered_pop(self):
    #     # Generate boolean columns for restrictions - keep all rows for reporting
    #     # In each case, True means eligible, False not eligible
    #     eligible_clustered_pop = 
    #     self.data["population"]["age_eligible"] = restrict_by_age(
    #         self.start_date, self.data["population"]["date_of_birth"]
    #     )
    #     self.data["population"]["code_eligible"] = restrict_by_code(
    #         self.start_date, self.data["population"]["lr_code"]
    #     )
    #     self.data["population"]["duration_eligible"] = restrict_by_duration(
    #         self.start_date, self.data["population"]["clm_comm_date"]
    #     )
    #     # Find everyone who *is* on LES
    #     # then use ~ ('not') to switch True and False
    #     self.data["population"]["not_on_les"] = ~on_les(
    #         self.start_date, self.data["population"]["ppsn"]
    #     )
    #     # Find everyone who *has* ever started JobPath
    #     # then use ~ to switch True and False
    #     self.data["population"]["no_previous_jobpath"] = ~any_previous_jobpath(
    #         self.start_date, self.data["population"]["ppsn"]
    #     )
    #     # Only people with True for all tests are JobPath eligible!
    #     self.data["population"]["eligible"] = self.data["population"][
    #         list(self.evaluation_eligibility_flags)
    #     ].all(axis="columns")

    def setup_periods(self):
        """
        Create dict of {Period: EvaluationPeriod} for each period up to last_period_date

        Default frequency is monthly ('M') but can be quarterly ('Q') or even weekly('W')

        Parameters
        ----------
        freq: str = 'M'
            Frequency of periods to be created. Default is monthly ('M').
            Quarterly ('Q') and weekly ('W') are possible. 
        
        """
        period_range = pd.period_range(
            start=self.start_date, end=self.last_period_date, freq=self.period_freq
        )
        self.periods = {}
        for index, period in enumerate(period_range):
            if index == 0:
                seed = self.data["eligible_clustered_pop"]
            else:
                seed = self.periods[period_range[index - 1]].data["eligible_clustered_pop"]
            self.periods[period] = EvaluationPeriod(
                ## Period
                start_date=period.to_timestamp("S"),
                logical_root=tuple(list(self.logical_root) + [self.logical_name]),
                seed_population=seed,
                rebuild_all=self.rebuild_all,
                freq=self.period_freq,
                evaluation_eligibility_flags=self.period_evaluation_eligibility_flags,
                ## Outcomes
                outcome_start_date=period.to_timestamp("S") + pd.DateOffset(months=1),
                outcome_end_date=self.outcome_end_date,
            )


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
