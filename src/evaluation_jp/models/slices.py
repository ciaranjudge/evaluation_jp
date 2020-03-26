# %%
# Standard library
from dataclasses import dataclass, field, InitVar
from typing import List, Set

# External packages
import pandas as pd

# Local packages
from evaluation_jp.models.periods import PeriodManager
from evaluation_jp.features.setup_steps import NearestKeyDict, SetupSteps


@dataclass
class EvaluationSlice:
    # Init only
    setup_steps: InitVar[SetupSteps]
    date: InitVar[pd.Timestamp]

    # Set up post-init
    data: pd.DataFrame = field(init=False)
    periods: PeriodManager = None

    def __post_init__(self, setup_steps, date):
        self.data = setup_steps.run(date=date)

    # # TODO Add add_periods code
    def add_periods(self, period_manager: PeriodManager, start: pd.Timestamp):
        self.periods = period_manager.run(start, self.data)


@dataclass
class SliceManager:
    setup_steps_by_date: dict
    start: InitVar[pd.Timestamp]
    end: InitVar[pd.Timestamp]
    freq: InitVar[str] = "QS"

    date_range: pd.DatetimeIndex = field(init=False)

    def __post_init__(self, start, end, freq):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)
        self.date_range = pd.date_range(start=start, end=end, freq=freq)

    def run(self):
        slices = {
            date: EvaluationSlice(self.setup_steps_by_date[date], date)
            for date in self.date_range()
        }
        # TODO Move this to models.py
        population = set().union(*(s.data.index for s in slices.values()))
        return(slices, population)

