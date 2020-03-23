# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from evaluation_jp.models.slices import SliceParams, SliceManager
from evaluation_jp.models.periods import PeriodParams, PeriodManager

from evaluation_jp.data.persistence_helpers import (
    get_name,
    get_path,
    populate,
    save_data,
    load_data,
)



@dataclass
class EvaluationModel:

    # Init parameters
    slice_params: SliceParams
    period_params: PeriodParams
    # outcome_params: OutcomeParams

    # Attributes
    data: pd.DataFrame = None
    slices: SliceManager = None

    def add_slices(self, rules_by_date, freq):
        self.slices = SliceManager(self.slice_params)

    # TODO Create background and outcome data (self.data) for slices.population
    # def add_population_data():

    def add_periods(self, rules_by_date, freq):
        for _date, _slice in self.slices.items():
            _slice.add_periods(
                self.period_params.rules_by_date, self.period_params.periods(_date)
            )

    # TODO Run weighting algorithm for periods

    # TODO Back-propagations of weights through periods

    # TODO Add outcomes with weighting


em = EvaluationModel(start=pd.Timestamp("2016-01-01"), end=pd.Timestamp("2016-05-20"))
# %%
rules_by_date = NearestKeyDict(
    {pd.Timestamp("2016-01-01"): "fruitbat", pd.Timestamp("2017-01-01"): "tapir"}
)






# # %%
# @dataclass
# class EvaluationModel:
#     """
#     Controls setup, parameters and reporting for EvaluationSlice and EvaluationPeriod objects
    
#     Parameters
#     ----------
#     # start_date: pd.Timestamp
#     #     The start of the period that this object refers to

#     # root: Tuple[str]
#     #     Ancestors of this object starting from root (".")

#     # prefix: str = None
#     #     Prefix to be added to logical name of object

#     # rebuild_all: bool = False
#     #     If True, this will call create_dataframe on this object and all its children
#     #     If False, this object and its children will first try to load existing data

#     Attributes
#     ----------
#     name: str 
#         Logical name of this object, created as part of __post_init__ setup

#     data: dict
#         Dictionary of the dataframes used by this object.
#         Names must be == dataframe_names.

#     Methods
#     -------
    
      
#     """

#     ### -- Class variables -- ###
#     TYPE_NAME: ClassVar[str] = "model"

#     ### -- Parameters set on init -- ###
#     ## Model
#     start_date: pd.Timestamp
#     root: Tuple[str] = (".",)
#     prefix: str = None
#     rebuild_all: bool = False

#     ## Outcomes
#     outcome_start_date: pd.Timestamp = None
#     outcome_end_date: pd.Timestamp = None

#     ## Slices
#     slice_prefix: str = None
#     slice_freq: str = "Q"
#     last_slice_date: pd.Timestamp = None
#     slice_clustering_eligibility_checker: EligibilityCheckManager = None
#     slice_evaluation_eligibility_checker: EligibilityCheckManager = None

#     ## Periods
#     period_prefix: str = None
#     period_freq: str = "M"
#     last_period_date: pd.Timestamp = None
#     period_eligibility_checker: EligibilityCheckManager = None

#     ### -- Other attributes -- ###
#     ## Boilerplate
#     name: str = field(init=False)
#     # path: Path = field(init=False)
#     ## Data
#     data: Dict[str, pd.DataFrame] = field(init=False)
#     ## Slices managed by Manager
#     slices: Dict[pd.Period, EvaluationSlice] = field(init=False)

#     ### -- Methods -- ###
#     def __post_init__(self):
#         self.start_date = self.start_date.normalize()
#         self.name = get_name(self.TYPE_NAME, self.start_date, prefix=self.prefix)
#         self.path = get_path(self.root, self.name)
#         self.setup_slices()
#         # Get union of slice participants once slices are set up
#         # Set up shared data and make available to slices

#     def setup_slices(self):
#         slice_range = pd.period_range(
#             start=self.start_date, end=self.last_slice_date, freq=self.slice_freq
#         )
#         self.slices = {}
#         for s in slice_range:
#             self.slices[s] = EvaluationSlice(
#                 ## Slice
#                 period=s,
#                 root=tuple(list(self.root) + [self.name]),
#                 prefix=self.slice_prefix,
#                 rebuild_all=self.rebuild_all,
#                 clustering_eligibility_checker=self.slice_clustering_eligibility_checker,
#                 evaluation_eligibility_checker=self.slice_evaluation_eligibility_checker,
#                 ## Outcomes
#                 outcome_start_date=s.to_timestamp(how="E"),
#                 outcome_end_date=self.outcome_end_date,
#                 ## Periods
#                 period_freq=self.period_freq,
#                 period_prefix=self.period_prefix,
#                 last_period_date=self.last_period_date,
#                 period_eligibility_checker=self.period_eligibility_checker,
#             )

