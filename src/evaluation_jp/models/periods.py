# %%
# Standard library
from dataclasses import dataclass, field, InitVar

# External packages
import pandas as pd

# Local packages
from evaluation_jp.features.setup_steps import NearestKeyDict, SetupSteps


@dataclass
class EvaluationPeriod:
    # Init only
    setup_steps: InitVar[SetupSteps]
    period: InitVar[pd.Period]
    population: InitVar[pd.Index]

    # Set up post-init
    data: pd.DataFrame = field(init=False)

    def __post_init__(self, setup_steps, period, population):
        self.data = setup_steps.run(date=period.to_timestamp(), data=population)


@dataclass
class PeriodManager:
    setup_steps_by_date: dict
    end: pd.Period
    freq: str = "M"

    # TODO Make this a property that can be set at any time
    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def period_range(self, start):
        return pd.period_range(start=start, end=self.end, freq=self.freq)

    def run(self, start, population):
        periods = {}
        for period in self.period_range(start):
            periods[period] = EvaluationPeriod(
                self.setup_steps_by_date[period.to_timestamp()],
                period,
                population,
            )
            # Use survivors from previous period as pop for next period
            population = periods[period].data
        return periods
