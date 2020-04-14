# %%
# Standard library
from dataclasses import dataclass, field, InitVar
from typing import List, Set

# External packages
import pandas as pd

# Local packages
from evaluation_jp.models import TreatmentPeriodManager
from evaluation_jp.features import NearestKeyDict, SetupSteps


@dataclass
class PopulationSlice:
    # Init only
    setup_steps: InitVar[SetupSteps]

    # Parameters
    date: pd.Timestamp

    # Set up post-init
    data: pd.DataFrame = field(init=False)
    treatment_periods: dict = None

    def __post_init__(self, setup_steps, date):
        self.data = setup_steps.run(date=date)

    def add_treatment_periods(self, treatment_period_manager: TreatmentPeriodManager):
        self.treatment_periods = {
            treatment_period.time_period: treatment_period
            for treatment_period in treatment_period_manager.generate_treatment_periods(
                start=self.date, population=self.data
            )
        }


@dataclass
class PopulationSliceManager:
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
            date: PopulationSlice(self.setup_steps_by_date[date], date)
            for date in self.date_range
        }
        return slices
