# %%
# Standard library
from dataclasses import dataclass, field, InitVar


# External packages
import pandas as pd

# Local packages
from evaluation_jp.data import DataHandler
from evaluation_jp.features import NearestKeyDict, SetupSteps
from evaluation_jp.models import PopulationSliceID, PopulationSlice

# TODO Use dataclass metadata to implement choice of id attrs for TreatmentPeriod and EvaluationSlice
@dataclass(frozen=True)
class TreatmentPeriodID:
    population_slice_id: PopulationSliceID
    time_period: pd.Period


@dataclass
class TreatmentPeriod:
    # Attributes
    id: TreatmentPeriodID
    setup_steps: InitVar[SetupSteps]
    init_data: InitVar[pd.DataFrame]
    data_handler: InitVar[DataHandler] = None
    index_col: str = None

    # Set up post-init
    data: pd.DataFrame = field(init=False)

    @property
    def class_name(self):
        return type(self).__name__

    def __post_init__(self, setup_steps, init_data, data_handler=None):
        if data_handler is not None:
            self.data = data_handler.run(
                data_type=self.class_name,
                data_id=self.id,
                setup_steps=setup_steps,
                init_data=init_data,
            )
        else:
            self.data = setup_steps.run(data_id=self.id, data=init_data)


# TODO TreatmentPeriodGenerator starting at different times (e.g. slice + 1 year)
@dataclass
class TreatmentPeriodGenerator:
    setup_steps_by_date: dict = None
    end: pd.Period = None
    freq: str = "M"
    columns_by_type: dict = None
    index_columns_by_type: dict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def treatment_period_range(self, start):
        return pd.period_range(start=start, end=self.end, freq=self.freq)

    def run(self, population_slice, data_handler=None):
        init_data = population_slice.data.copy()
        for time_period in self.treatment_period_range(population_slice.id.date):
            treatment_period = TreatmentPeriod(
                id=TreatmentPeriodID(
                    population_slice_id=population_slice.id, time_period=time_period
                ),
                setup_steps=self.setup_steps_by_date[time_period.to_timestamp()],
                init_data=init_data,
                data_handler=data_handler,
                columns_by_type=self.columns_by_type,
                index_columns_by_type=self.index_columns_by_type,
            )
            yield treatment_period
            # Use survivors from previous period as pop for next period
            init_data = treatment_period.data.copy()
