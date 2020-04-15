# %%
# Standard library
from dataclasses import dataclass, field, InitVar

# External packages
import pandas as pd

# Local packages
from evaluation_jp.features import NearestKeyDict, SetupSteps


@dataclass
class TreatmentPeriod:
    # Attributes
    time_period: pd.Period

    # Init only
    setup_steps: InitVar[SetupSteps]
    population: InitVar[pd.Index]

    # Set up post-init
    data: pd.DataFrame = field(init=False)

    def __post_init__(self, setup_steps, population):
        self.data = self.setup(setup_steps, population)

    def setup(self, setup_steps, population):
        return setup_steps.run(
            date=self.time_period.to_timestamp(), data=population
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

    def __call__(self, start, population):
        for time_period in self.treatment_period_range(start):
            treatment_period = TreatmentPeriod(
                time_period=time_period,
                setup_steps=self.setup_steps_by_date[time_period.to_timestamp()],
                population=population,
            )
            yield treatment_period
            # Use survivors from previous period as pop for next period
            population = treatment_period.data
