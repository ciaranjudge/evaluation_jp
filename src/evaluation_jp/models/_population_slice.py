# %%
# Standard library
from dataclasses import dataclass, field, InitVar, asdict
from typing import List, Set, Dict

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data import ModelDataHandler
from evaluation_jp.features import NearestKeyDict, SetupSteps


@dataclass(frozen=True)
class PopulationSliceID:
    date: pd.Timestamp


@dataclass
class PopulationSlice:
    # Parameters
    id: PopulationSliceID

    # Init only
    setup_steps: InitVar[SetupSteps]
    data_handler: InitVar[ModelDataHandler] = None

    # Set up post-init
    data: pd.DataFrame = field(init=False)

    @property
    def class_name(self):
        return type(self).__name__

    def __post_init__(self, setup_steps, data_handler=None):
        if data_handler is not None:
            self.data = data_handler.run(
                data_type=self.class_name,
                data_id=self.id,
                setup_steps=setup_steps,
            )
        else:
            self.data = setup_steps.run(data_id=self.id)


@dataclass
class PopulationSliceGenerator:

    # Init only
    start: InitVar[pd.Timestamp] 
    end: InitVar[pd.Timestamp]
    freq: InitVar[str] = "QS"

    # Attributes
    setup_steps_by_date: NearestKeyDict = None

    date_range: pd.DatetimeIndex = field(init=False)

    def __post_init__(self, start, end, freq):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)
        self.date_range = pd.date_range(start=start, end=end, freq=freq)

    def run(self, data_handler=None):
        for date in self.date_range:
            population_slice = PopulationSlice(
                id=PopulationSliceID(date),
                setup_steps=self.setup_steps_by_date[date],
                data_handler=data_handler,
            )
            yield population_slice


# %%
