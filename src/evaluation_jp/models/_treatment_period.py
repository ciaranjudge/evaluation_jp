# %%
# Standard library
from dataclasses import dataclass, field, InitVar

# External packages
import pandas as pd

# Local packages
from evaluation_jp.features import NearestKeyDict, SetupSteps
from evaluation_jp.models import PopulationSlice


@dataclass
class TreatmentPeriod:
    # Attributes
    population_slice_date: pd.Timestamp
    time_period: pd.Period

    # Init only
    setup_steps: InitVar[SetupSteps]
    init_data: InitVar[pd.DataFrame]

    # Set up post-init
    data: pd.DataFrame = field(init=False)

    def __post_init__(self, setup_steps, init_data):
        self.data = self.setup(setup_steps, init_data)

    def setup(self, setup_steps, init_data):
        return setup_steps(
            date=self.time_period.to_timestamp(), data=init_data
        )


@dataclass
class TreatmentPeriodGenerator:
    setup_steps_by_date: dict
    end: pd.Period
    freq: str = "M"

    # TODO Make this a property that can be set at any time
    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def treatment_period_range(self, start):
        return pd.period_range(start=start, end=self.end, freq=self.freq)

    def __call__(self, population_slice):
        init_data = population_slice.data
        for time_period in self.treatment_period_range(population_slice.date):
            treatment_period = TreatmentPeriod(
                population_slice_date=population_slice.date,
                time_period=time_period,
                setup_steps=self.setup_steps_by_date[time_period.to_timestamp()],
                init_data=init_data
            )
            yield treatment_period
            # Use survivors from previous period as pop for next period
            init_data = treatment_period.data
