# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from src.evaluation_slice import EvaluationSlice
from src.data.import_helpers import get_earnings, get_sw_payments, get_ists_claims

# %%
@dataclass
class EvaluationModel:
    """
    Controls setup, parameters and reporting for EvaluationSlice and EvaluationPeriod objects
    
    Parameters
    ----------
    # start_date: pd.Timestamp
    #     The start of the period that this object refers to

    # logical_root: Tuple[str]
    #     Ancestors of this object starting from root (".")

    # name_prefix: str = None
    #     Prefix to be added to logical name of object

    # rebuild_all: bool = False
    #     If True, this will call create_dataframe on this object and all its children
    #     If False, this object and its children will first try to load existing data

    Attributes
    ----------
    logical_name: str 
        Logical name of this object, created as part of __post_init__ setup

    data: dict
        Dictionary of the dataframes used by this object.
        Names must be == dataframe_names.

    Methods
    -------
    setup_logical_name()
      
    """

    ### -- Parameters set on init -- ###
    ## Model
    start_date: pd.Timestamp
    logical_root: Tuple[str] = (".",)
    name_prefix: str = ""
    rebuild_all: bool = False

    ## Outcomes
    outcome_start_date: pd.Timestamp = None
    outcome_end_date: pd.Timestamp = None

    ## Slices
    slice_freq: str = "Q"
    last_slice_date: pd.Timestamp = None
    # slice_cluster_eligibility_flags: Tuple[str] = None  # Needed for reclustering
    slice_evaluation_eligibility_flags: Tuple[str] = ()

    ## Periods
    period_freq: str = "M"
    last_period_date: pd.Timestamp = pd.Timestamp("2017-01-01")
    period_evaluation_eligibility_flags: Tuple[str] = ()

    ## Lookup dataframes (NB not yet implemented and should just be part of data{})
    # age_criteria: pd.DataFrame = None
    # code_criteria: pd.DataFrame = None

    ### -- Other attributes -- ###
    logical_name: str = field(init=False)
    data: dict = field(default=None, init=False)
    slices: dict = field(init=False)

    ### Methods
    def __post_init__(self):
        self.start_date = self.start_date.normalize()
        self.setup_logical_name()
        self.setup_slices()
        # Get union of slice participants once slices are set up
        # Set up 

    def setup_logical_name(self):
        f_name_prefix = f"{self.name_prefix}__" if len(self.name_prefix) > 0 else ""
        f_start_date = self.start_date.strftime("%Y-%m-%d")
        self.logical_name = f"{f_name_prefix}model_{f_start_date}"


    def setup_slices(self):
        slice_range = pd.period_range(
            start=self.start_date, end=self.last_slice_date, freq=self.slice_freq
        )
        self.slices = {}
        for s in slice_range:
            self.slices[s] = EvaluationSlice(
                ## Slice
                start_date=s.to_timestamp("S"),
                logical_root=tuple(list(self.logical_root) + [self.logical_name]),
                rebuild_all=self.rebuild_all,
                # cluster_eligibility_flags=self.slice_cluster_eligibility_flags,
                evaluation_eligibility_flags=self.slice_evaluation_eligibility_flags,
                ## Outcomes
                outcome_start_date=s.to_timestamp("S") + pd.DateOffset(months=3),
                outcome_end_date=self.outcome_end_date,
                ## Periods
                last_period_date=self.last_period_date,
                period_freq=self.period_freq,
                period_evaluation_eligibility_flags=self.period_evaluation_eligibility_flags,
            )






#%%
