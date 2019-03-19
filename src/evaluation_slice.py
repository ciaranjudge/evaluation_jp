# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from src.data.import_helpers import get_clusters, get_vital_statistics, get_ists_claims
from src.features.selection_helpers import (
    restrict_by_age,
    restrict_by_code,
    restrict_by_duration,
    on_les,
    any_previous_jobpath,
    jobpath_starts_this_period,
    les_starts_this_period,
)
from src.features.metadata_helpers import lr_reporting_date

# from src.evaluation_period import EvaluationPeriod
from src.evaluation_class import EvaluationClass

# %%
@dataclass
class EvaluationSlice(EvaluationClass):
    """
    Dataclass to manage state and eligibility criteria for evaluation slices

    Parameters
    ---------- 
    start_date: pd.Timestamp
        The start of the period that this object refers to

    end_date: pd.Timestamp
        The end of the period that this object refers to

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")

    seed_dataframe: pd.DataFrame = None
        Dataframe needed to enable object to set itself up correctly

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

    ### Class variables
    object_type: ClassVar[str] = "slice"
    dataframe_names: ClassVar[Tuple[str]] = (
        "population",
        "eligible",
    )  # NB needs trailing comma!

    ### Parameters set on init
    ## Inherited from EvaluationClass
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    # logical_root: Tuple[str] = (".", )
    # seed_population: pd.DataFrame = None
    # name_prefix: str = None
    # rebuild_all: bool = False

    ## Specific to EvaluationSlice
    eligibility_flags: Tuple[str] = (
        "age_eligible",
        "code_eligible",
        "duration_eligible",
        "not_on_les",
        "no_previous_jobpath",
    )
    slice_freq: str = "Q"
    period_freq: str = "M"
    min_age: int = None
    max_age: int = 62
    is_on_lr: bool = True
    eligible_codes: Tuple[str] = ("UA", "UB")
    min_duration_days: int = 365

    ### Other attributes
    ## Inherited from EvaluationClass
    # logical_name: str = field(init=False)
    # dataframes: dict = field(default=None, init=False)

    ### Private attributes
    ## Inherited from EvaluationClass
    # _start_date: pd.Timestamp = field(init=False, repr=False)
    # _end_date: pd.Timestamp = field(init=False, repr=False)

    ### Methods
    def __post_init__(self):
        super().__post_init__()
        self.setup_children()

    def setup_dataframe(self, dataframe_name: str):
        """
        Wrapper for setup of individual dataframes. 

        Parameters
        ----------
        dataframe_name: str
            The name of the dataframe to be set up. 
            This is the key for this dataframe in the dataframes dict.
        
        """
        if dataframe_name == "population":
            self.setup_population()

        if dataframe_name == "eligible":
            self.setup_eligible()

    def setup_population(self):
        """
        Set up population: each row is a treatable person, with eligibility flags
        
        """
        # Merge first with ISTS vital statistics...
        self.dataframes["population"] = pd.merge(
            left=get_clusters(self.start_date),
            right=get_vital_statistics(self.start_date),
            on="ppsn",
        )
        # ...then with ISTS claims weekly database...
        ists_columns = ["ppsn", "lr_code", "clm_comm_date"]
        self.dataframes["population"] = pd.merge(
            left=self.dataframes["population"],
            right=get_ists_claims(
                self.start_date, lr_flag=self.is_on_lr, columns=ists_columns
            ),
            on="ppsn",
        )

        # Generate boolean columns for restrictions - keep all rows for reporting
        # In each case, True means eligible, False not eligible
        self.dataframes["population"]["age_eligible"] = restrict_by_age(
            self.dataframes["population"]["date_of_birth"],
            self.start_date,
            max_age=self.max_age,
        )
        self.dataframes["population"]["code_eligible"] = restrict_by_code(
            self.dataframes["population"]["lr_code"], self.eligible_codes
        )
        self.dataframes["population"]["duration_eligible"] = restrict_by_duration(
            self.start_date,
            self.dataframes["population"]["clm_comm_date"],
            min_duration=365,
        )
        # Find everyone who *is* on LES
        # then use ~ ('not') to switch True and False
        self.dataframes["population"]["not_on_les"] = ~on_les(
            self.start_date, self.dataframes["population"]["ppsn"]
        )
        # Find everyone who *has* ever started JobPath
        # then use ~ to switch True and False
        self.dataframes["population"]["no_previous_jobpath"] = ~any_previous_jobpath(
            self.start_date, self.dataframes["population"]["ppsn"]
        )
        # Only people with True for all tests are JobPath eligible!
        self.dataframes["population"]["eligible"] = self.dataframes["population"][
            list(self.eligibility_flags)
        ].all(axis="columns")

    def setup_children(self) -> dict:
        """
        Create dict of {Period: EvaluationPeriod} for each period up to end_date

        Default frequency is monthly ('M') but can be quarterly ('Q') or even weekly('W')

        Parameters
        ----------
        end_date: pd.Timestamp
            The date up to which periods should be created. Round downwards.

        freq: str = 'M'
            Frequency of periods to be created. Default is monthly ('M').
            Quarterly ('Q') and weekly ('W') are possible. 
        
        """
        periods = pd.period_range(
            start=self.start_date, end=self.end_date, freq=self.period_freq
        )
        # Parameter like 'rebuild_all: bool = False'
        # if rebuild_all:
        #    Pass dataframe to each period when instantiating
        for period in periods:
            print(period)


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
