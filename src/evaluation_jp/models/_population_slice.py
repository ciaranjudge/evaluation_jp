# %%
# Standard library
from dataclasses import dataclass, field, InitVar
from typing import List, Set

# External packages
import pandas as pd

# Local packages
from evaluation_jp.features import NearestKeyDict, SetupSteps


@dataclass
class PopulationSlice:
    # Parameters
    date: pd.Timestamp

    # Init only
    setup_steps: InitVar[SetupSteps]

    # Set up post-init
    data: pd.DataFrame = field(init=False)
    # treatment_periods: dict = None

    def __post_init__(self, setup_steps):
        self.data = setup_steps.run(date=self.date)


@dataclass
class PopulationSliceGenerator:
    setup_steps_by_date: dict
    start: InitVar[pd.Timestamp]
    end: InitVar[pd.Timestamp]
    freq: InitVar[str] = "QS"

    date_range: pd.DatetimeIndex = field(init=False)

    def __post_init__(self, start, end, freq):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)
        self.date_range = pd.date_range(start=start, end=end, freq=freq)

    def __call__(self):
        for date in self.date_range:
            population_slice = PopulationSlice(
                date=date, setup_steps=self.setup_steps_by_date[date]
            )
            yield population_slice
