# %%
# Standard library
import collections
from dataclasses import dataclass, field, InitVar
from typing import List, Set

# External packages
import pandas as pd

# Local packages


@dataclass
class EvaluationPeriod:
    population: InitVar[pd.Index]
    rules: InitVar[dict]
    period: InitVar[pd.Timestamp]

    data: pd.DataFrame = field(init=False)

    def __post_init__(self, population, rules, period):
        # TODO Run rules at setup
        self.data = pd.DataFrame(population)


@dataclass
class PeriodParams:
    end: pd.Period
    freq: str = "M"
    rules_by_date: dict = None

    def periods(self, start):
        return pd.period_range(start=start, end=self.end, freq=self.freq)


class PeriodManager(collections.UserDict):
    def __init__(self, population, period_params, start):
        for period in period_params.periods(start):
            self.data[period] = EvaluationPeriod(
                population, period_params.rules_by_date[period.to_timestamp()], period
            )
            # Use survivors from previous period as pop for next period
            population = self.data[period].data.index
