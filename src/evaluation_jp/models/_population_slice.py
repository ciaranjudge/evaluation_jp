# %%
# Standard library
from dataclasses import dataclass, field, InitVar
from typing import List, Set, Dict

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data import ModelDataHandler
from evaluation_jp.features import NearestKeyDict, SetupSteps


@dataclass(frozen=True)
class PopulationSliceID:
    date: pd.Timestamp


# //TODO Refactor PopulationSlice and TreatmentPeriod into subclasses of EvaluationData
@dataclass
class PopulationSlice:
    # Parameters
    id: PopulationSliceID
    setup_steps: InitVar[SetupSteps]
    data_handler: InitVar[ModelDataHandler] = None
    columns_by_type: InitVar[dict] = None
    index_columns_by_type: InitVar[dict] = None

    data: pd.DataFrame = field(init=False)

    @property
    def class_name(self):
        return type(self).__name__

    def __post_init__(
        self,
        setup_steps,
        data_handler=None,
        columns_by_type=None,
        index_columns_by_type=None,
    ):
        if data_handler is not None:
            self.data = data_handler.run(
                data_type=self.class_name,
                data_id=self.id,
                setup_steps=setup_steps,
                columns_by_type=columns_by_type,
                index_columns_by_type=index_columns_by_type,
            )
        else:
            self.data = setup_steps.run(data_id=self.id)


# //TODO Refactor PopulatinSliceGenerator, TreatmentPeriodGenerator into subclasses of EvaluationDataGenerator
@dataclass
class PopulationSliceGenerator:

    # Init only
    start: InitVar[pd.Timestamp]
    end: InitVar[pd.Timestamp]
    freq: InitVar[str] = "QS"

    # Attributes
    setup_steps_by_date: NearestKeyDict = None
    columns_by_type: dict = None
    index_columns_by_type: dict = None

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
                columns_by_type=self.columns_by_type,
                index_columns_by_type=self.index_columns_by_type,
            )
            yield population_slice


# %%
