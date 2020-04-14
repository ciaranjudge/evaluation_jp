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
        for _period in self.period_range(start):
            evaluation_period = EvaluationPeriod(
                self.setup_steps_by_date[_period.to_timestamp()],
                _period,
                population,
            )
            periods[_period] = evaluation_period
            # Use survivors from previous period as pop for next period
            population = evaluation_period.data

        return periods
