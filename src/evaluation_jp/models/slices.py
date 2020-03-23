# %%
# Standard library
import collections
from dataclasses import dataclass, field, InitVar
from typing import List, Set

# External packages
import pandas as pd

# Local packages
from evaluation_jp.models.periods import PeriodParams, PeriodManager
from evaluation_jp.features.setup_steps import SetupSteps, NearestKeyDict


@dataclass
class EvaluationSlice:
    setup_steps: InitVar[SetupSteps]
    date: InitVar[pd.Timestamp]

    data: pd.DataFrame = field(init=False)
    # periods: PeriodManager = field(init=False)

    def __post_init__(self, setup_steps, date):
        self.data = setup_steps.setup(date)

    # TODO Add add_periods code
    def add_periods(self, period_params: PeriodParams, start: pd.Timestamp):
        self.periods = PeriodManager(self.data.index, period_params, start)


@dataclass
class SliceParams:
    setup_steps_by_date: NearestKeyDict
    start: pd.Timestamp
    end: pd.Timestamp
    freq: str = "QS"

    def dates(self):
        return pd.date_range(start=self.start, end=self.end, freq=self.freq)


class SliceManager(collections.UserDict):
    def __init__(self, slice_params):
        self.data = {
            date: EvaluationSlice(slice_params.rules_by_date[date], date)
            for date in slice_params.dates()
        }
        self.population = set().union(*(s.data.index for s in self.data.values()))

