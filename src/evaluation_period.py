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
)
from src.features.metadata_helpers import lr_reporting_date
from src.evaluation_class import EvaluationClass

# %%
@dataclass
class EvaluationPeriod(EvaluationClass):
    """
    Dataclass to manage evaluation treatment periods: 
        -- set up population and divide into T and C groups
        -- add features for matching/weighting model
        -- tune model
        -- run tuned model and create scores
        -- create counterfactual weights for groups affected by counterfactual start date
        -- add outcome reporting data
        -- create weighted outcomes
        -- create visualisations

    ## Implement counterfactual details and weighting details as decorators!

    Parameters
    ----------
    parent_path: Path
        Path to parent object persistent location

    date: pd.Period
        Period covered by this EvaluationTreatmentPeriod
        Default frequency is monthly ('M'). 
        Can do quarterly ('Q') but other periods not implemented in post_init name setup

    name: str = None
        The name used for persistent storage of this object. 
        By default, starts as None and set to "slice_{formatted slice date}" in post_init

    pop_dataframe: pd.DataFrame = None
        Dataframe containing necessary information about this object.

    rebuild_all: bool = False
        If True, this will call setup_pop_dataframe on this object and all its children;
        If False, this object and its children will first try to load existing data.

    is_on_lr: bool = True
        Flag that is True if only including LR cases in pop_dataframe.
        Reference date is end of this evaluation period.

    eligible_codes: Tuple[str] = ("UA", "UB")
        List of codes which are eligible for treatment.
        Reference date is end of this evaluation period.

    Methods
    ----------
    setup_pop_dataframe()

    save_dataframe()

    load_dataframe()

    setup_evaluation_periods()
    
    """

    ### Parameters

    ## Inherited from EvaluationClass
    # parent_path: Path
    date: pd.Period # inherited but changing typehint here
    name: str = None  # inherited but setting default None here
    # pop_dataframe: pd.DataFrame = None
    # rebuild_all: bool = False

    ## Specific to EvaluationPeriod
    is_on_lr: bool = True
    eligible_codes: Tuple[str] = ("UA", "UB")
    exclude_on_les: bool = False
    exclude_jobpath_hold: bool = False 


    def __post_init__(self):
        if name is None:
            if self.date.freq == 'M':
                period_str = self.date.__str__().replace('-', 'M')  # "2016-01" -> "2016M01"
            elif self.date.freq == 'Q':
                period_str = self.date.__str__()  # Already like "2016Q1" by default
            else:
                period_str = self.date.to_timestamp().strftime('%Y-%m-%d')
            self.name = f"period_{period_str}"
        super().__post_init__()

    def setup_pop_dataframe(self, seed_dataframe: Optional[pd.Dataframe] = None):
        """
        Set up a dataframe where each row is a person in this period, with eligibility flags
        
        Requires a starting dataframe - normally use pop_dataframe passed in at instantiaion
        ...but can specify a seed_dataframe here.

        Parameters
        ----------
        seed_dataframe: Optional[pd.Dataframe] = None
            If specified, use this dataframe instead of pop_dataframe
        """
        if seed_dataframe is not None:
            pop_dataframe = seed_dataframe
        
        
        # Exclude LES starters this period
        # Exclude LR leavers
        # Exclude code changers
        # Exclude JP cancelled this period



    # Add T and C group labels



    # Add features for matching/weighting model



    # Tune model
        # Persist tuning parameters and results



    # run tuned model and create scores
        # Persist results



    # create counterfactual weights for groups affected by counterfactual start date
        # Need to create and manage counterfactual weights for all possible periods
        # Call correct set of weights for given outcome set


    # Add outcome reporting data



    # create weighted outcomes



    # create visualisations


    

# # %%
# period = pd.Period(es.date, freq="M")
# persist_folder = es.persist_folder / f"period_{period}"
# print(persist_folder)
# etp = EvaluationTreatmentPeriod(period=period, persist_folder=persist_folder)

