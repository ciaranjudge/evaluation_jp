# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from src.data.import_helpers import get_ists_claims
from src.features.selection_helpers import (
    restrict_by_code,
    restrict_by_duration,
    on_les,
    jobpath_starts_this_period,
    les_starts_this_period,
    jobpath_hold_this_period,
)
from src.features.metadata_helpers import lr_reporting_date

# from src.evaluation_period import EvaluationPeriod
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
    start_date: pd.Timestamp
        The start of the period that this object refers to

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")


    seed_population: pd.DataFrame = None
        Dataframe needed to enable object to set itself up correctly

    name_prefix: str = None
        Prefix to be added to logical name of object

    rebuild_all: bool = False
        If True, this will call create_dataframe on this object and all its children
        If False, this object and its children will first try to load existing data

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

    ### Class variables
    object_type: ClassVar[str] = "period"
    dataframe_names: ClassVar[Tuple[str]] = (
        "population",
        "eligible",
    )  # NB needs trailing comma!

    ### Parameters set on init
    ## Inherited from EvaluationClass
    # start_date: pd.Timestamp
    # logical_root: Tuple[str] = (".", )
    # seed_population: pd.DataFrame = None
    # name_prefix: str = None
    # rebuild_all: bool = False
    # outcome_start_date: pd.Timestamp = None
    # outcome_end_date: pd.Timestamp = None

    ## Specific to EvaluationPeriod
    freq: str = "M"
    evaluation_eligibility_flags: Tuple[str] = (
        "on_lr",
        "code_eligible",
        "duration_eligible",
        "not_jobpath_hold",
    )
    is_on_lr: bool = True

    ### Other attributes
    ## Inherited from EvaluationClass
    # logical_name: str = field(init=False)
    # dataframes: dict = field(default=None, init=False)
    _period: pd.Period = field(init=False)
    _end_date: pd.Timestamp = field(init=False)

    def __post_init__(self):
        self._period = self.start_date.to_period(freq="M")
        self._end_date = self._period.to_timestamp(how="E")
        super().__post_init__()

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
        # This should be the seed_population created when the object was set up
        self.dataframes["population"] = self.seed_population.drop(
            ["lr_code", "clm_comm_date"], axis="columns"
        )

        # Generate boolean columns for restrictions - keep all rows for reporting
        # In each case, True means eligible, False not eligible

        # Need ISTS data for LR status, duration, and code
        # Left join with ists data to get LR information for period end
        ists_columns = ["ppsn", "lr_code", "clm_comm_date", "lr_flag"]
        self.dataframes["population"] = pd.merge(
            left=self.dataframes["population"],
            right=get_ists_claims(
                self._end_date, lr_flag=self.is_on_lr, columns=ists_columns
            ),
            how="left",
            on="ppsn",
        )
        # Live Register status comes from ISTS lr_flag - just tidying up here
        self.dataframes["population"]["on_lr"] = self.dataframes["population"][
            "lr_flag"
        ].astype("bool")
        self.dataframes["population"] = self.dataframes["population"].drop(
            "lr_flag", axis="columns"
        )
        # Exclude code changers
        self.dataframes["population"]["code_eligible"] = restrict_by_code(
            self.start_date, self.dataframes["population"]["lr_code"]
        )
        # Exclude people who left the Live Register during the period
        self.dataframes["population"]["duration_eligible"] = restrict_by_duration(
            self.start_date, self.dataframes["population"]["clm_comm_date"]
        )
        # Exclude people with a 'JobPathHold' flag
        self.dataframes["population"]["not_jobpath_hold"] = ~jobpath_hold_this_period(
            self.start_date, id_series=self.dataframes["population"]["ppsn"]
        )

        # Only people with True for all tests are JobPath eligible!
        self.dataframes["population"]["eligible"] = self.dataframes["population"][
            list(self.evaluation_eligibility_flags)
        ].all(axis="columns")

    # Add labels for JobPath starts
    # self.dataframes["population"]

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

