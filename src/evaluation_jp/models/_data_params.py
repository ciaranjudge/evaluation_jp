# Standard library
from dataclasses import dataclass
from typing import ClassVar, TYPE_CHECKING
import abc

# External packages
import pandas as pd

# Local packages
from evaluation_jp.features.nearest_key_dict import NearestKeyDict


def duplicated(item_list):
    return [x for n, x in enumerate(item_list) if x in item_list[:n]]


class DuplicatedItemsError(Exception):
    """There shouldn't be duplicated column names in a dataframe!
    """

    pass


@dataclass
class DataParams(abc.ABC):
    type_name: ClassVar[str]

    columns_by_type: dict
    index_columns_by_type: dict = None

    def __post_init__(self):
        if (duplicates := duplicated(self.all_columns)) :
            raise DuplicatedItemsError(f"Found duplicated columns {duplicates}")

    @abc.abstractmethod
    def setup_steps(self, data_id=None):
        pass

    @property
    def all_columns(self):
        return set(self.columns_by_type) | set(
            self.index_columns_by_type if self.index_columns_by_type is not None else []
        )

    def verify_column_names(self, column_names):
        if (duplicates := duplicated(column_names)) :
            raise DuplicatedItemsError(f"Found duplicated columns {duplicates}")

        expected_column_names = set(self.all_columns)
        if set(column_names) == expected_column_names:
            return True
        else:
            error = "Columns do not match expected columns!\n"
            if set(column_names) - set(expected_column_names) != set():
                error += f"Unexpected columns: {set(column_names) - set(expected_column_names)}\n"
            if set(expected_column_names) - set(column_names) != set():
                error += f"Missing columns: {set(expected_column_names) - set(column_names)}\n"
            raise DuplicatedItemsError(error)

    def set_datatypes(self, data: pd.DataFrame, include_index=True):
        data = data.reset_index()
        self.verify_column_names(data.columns)
        for col, dtype in self.columns_by_type.items():
            data[col] = data[col].astype(dtype)
        if self.index_columns_by_type is not None:
            for index_col, dtype in self.index_columns_by_type.items():
                data[index_col] = data[index_col].astype(dtype)
            data = data.set_index(list(self.index_columns_by_type))
        return data


@dataclass
class PopulationSliceDataParams(DataParams):

    type_name: ClassVar[str] = "population_slice"
    setup_steps_by_date: NearestKeyDict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def setup_steps(self, data_id):
        return self.setup_steps_by_date[data_id.date]


@dataclass
class TreatmentPeriodDataParams(DataParams):

    type_name: ClassVar[str] = "treatment_period"
    setup_steps_by_date: NearestKeyDict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def setup_steps(self, data_id):
        return self.setup_steps_by_date[data_id.time_period.to_timestamp()]


# // 