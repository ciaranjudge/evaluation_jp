# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data.persistence_helpers import PersistenceManager
from evaluation_jp.models.slices import SliceManager
from evaluation_jp.models.periods import PeriodManager

# from evaluation_jp.data.persistence_helpers import (
#     get_name,
#     get_path,
#     populate,
#     save_data,
#     load_data,
# )



@dataclass
class EvaluationModel:

    # Init parameters
    name: str
    persistence_manager: PersistenceManager = None
    slice_manager: SliceManager = None
    period_manager: PeriodManager = None
    # outcome_manager: OutcomeManager = None

    # Attributes
    data: pd.DataFrame = None
    slices: dict = None

    def add_slices(self):
        self.slices = self.slice_manager.run()

    # TODO Create background and outcome data (self.data) for slices.population
    # def add_population_data():
        #population = set().union(*(s.data.index for s in slices.values()))

    def add_periods(self):
        for _date, _slice in self.slices.items():
            _slice.add_periods(
                period_manager=self.period_manager, start=_date
            )

    # TODO Run weighting algorithm for periods

    # TODO Back-propagations of weights through periods

    # TODO Add outcomes with weighting

