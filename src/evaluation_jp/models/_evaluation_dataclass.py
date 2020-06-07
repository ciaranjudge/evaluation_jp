# %%
# Standard library
from dataclasses import dataclass, field, InitVar
from typing import List, Set, Dict
import abc

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data import ModelDataHandler, ModelDataHandlerError
from evaluation_jp.features import NearestKeyDict, SetupSteps


@dataclass
class DataID(abc.ABC):
    pass


@dataclass(frozen=True)
class PopulationSliceID(EvaluationDataclassID):
    date: pd.Timestamp


# //TODO Refactor PopulationSlice and TreatmentPeriod into subclasses of EvaluationData
@dataclass
class EvalutionDataclass:
    # Init parameters
    data_name: str
    data_id: EvaluationDataclassID = None
    setup_steps: SetupSteps = None
    columns_by_type: dict = None
    index_columns_by_type: dict = None

    # Set up with .run()
    data: pd.DataFrame = None

    def run(self, init_data=None, data_handler=None):
        if data_handler is not None:
            try:
                self.data = data_handler.read(
                    data_type=self.data_name,  # Get class name
                    columns_by_type=self.columns_by_type,
                    index_columns_by_type=self.index_columns_by_type,
                )
            except ModelDataHandlerError:
                self.data = self.setup_steps.run(data_id=self.data_id, init_data=init_data)
                # //TODO Check that data has correct columns, index after setup_steps.run()
                data_handler.write(
                    data_type=self.class_name,
                    data=self.data,
                    columns_by_type=self.columns_by_type,
                    index_columns_by_type=self.index_columns_by_type,
                )
        else:
            self.data = self.setup_steps.run(data_id=self.data_id, init_data=init_data)


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
