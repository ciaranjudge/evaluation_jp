# %%
# Standard library
import datetime as dt
import calendar
from dataclasses import dataclass, field
from typing import List, Set, Dict, Tuple, Optional
from pathlib import Path

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
from src.evaluation_period import EvaluationPeriod
from src.evaluation_class import EvaluationClass

# %%
@dataclass
class EvaluationSlice(EvaluationClass):
    """
    Dataclass to manage state and eligibility criteria for evaluation slices

    Parameters
    ---------- 
    parent_path: Path
        Folder for persistent storage of this EvaluationSlice and all its children

    date: pd.Timestamp
        Date of EvaluationSlice and associated cluster creation

    name: str = None
        The name used for persistent storage of this object. 
        By default, starts as None and set to "slice_{formatted slice date}" in post_init

    pop_dataframe: pd.DataFrame = None
        Dataframe containing necessary information about this EvaluationSlice

    rebuild_all: bool = False
        If True, this will call create_dataframe on this object and all its children
        If False, this object and its children will first try to load existing data.

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
    setup_pop_dataframe()

    save_dataframe()

    load_dataframe()

    setup_evaluation_periods()

    """

    ### Parameters
    ## Inherited from EvaluationClass
    # parent_path: Path
    # date: pd.Timestamp
    name: str = None  # inherited but setting default None here
    # pop_dataframe: pd.DataFrame = None

    ## Specific to EvaluationSlice
    min_age: int = None
    max_age: int = 62
    is_on_lr: bool = True
    eligible_codes: Tuple[str] = ("UA", "UB")
    min_duration_days: int = 365

    ### Methods
    def __post_init__(self):
        if self.name is None:
            self.name = f"slice_{self.date.strftime('%Y-%m-%d')}"
        super().__post_init__()

    def setup_data(self):
        """
        Set up a dataframe where each row is a person in this slice, with eligibility flags
        
        """

        # Merge first with ISTS vital statistics...
        self.pop_dataframe = pd.merge(
            left=get_clusters(self.date),
            right=get_vital_statistics(self.date).drop("lr_date", axis="columns"),
            on="ppsn",
        )
        # ...then with ISTS claims weekly database...
        ists_columns = ["ppsn", "lr_code", "clm_comm_date"]
        self.pop_dataframe = pd.merge(
            left=self.pop_dataframe,
            right=get_ists_claims(
                self.date, lr_flag=self.is_on_lr, columns=ists_columns
            ),
            on="ppsn",
        )

        # Generate boolean columns for restrictions - keep original dataframe for reporting
        # In each case, True means eligible, False not eligible
        eligibility_flags = [
            "age_is_eligible",
            "code_is_eligible",
            "duration_is_eligible",
            "not_on_les",
            "no_previous_jobpath",
        ]
        self.pop_dataframe["age_is_eligible"] = restrict_by_age(
            self.pop_dataframe["date_of_birth"], self.date, max_age=self.max_age
        )
        self.pop_dataframe["code_is_eligible"] = restrict_by_code(
            self.pop_dataframe["lr_code"], self.eligible_codes
        )
        self.pop_dataframe["duration_is_eligible"] = restrict_by_duration(
            self.date, self.pop_dataframe["clm_comm_date"], min_duration=365
        )
        # Find everyone who *does* seem to be on LES and use ~ to switch True and False
        self.pop_dataframe["not_on_les"] = ~on_les(self.date, self.pop_dataframe["ppsn"])

        # Find everyone who *has* ever started JobPath and use ~ to switch True and False
        self.pop_dataframe["no_previous_jobpath"] = ~any_previous_jobpath(
            self.date, self.pop_dataframe["ppsn"]
        )
        # Only people with True for all tests are JobPath eligible!
        self.pop_dataframe["eligible"] = self.pop_dataframe[eligibility_flags].all(
            axis="columns"
        )

    def setup_evaluation_periods(end_date: pd.Timestamp, freq: str = "M") -> dict:
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
        # Use pd.date_range(start=self.date, end=end_date, freq=freq) 
        # Parameter like 'rebuild_all: bool = False'
        # if rebuild_all:
        #    Pass dataframe to each period when instantiating
        pass


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
