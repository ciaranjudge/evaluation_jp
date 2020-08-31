# Standard library
from dataclasses import dataclass
from typing import ClassVar
import abc

# External packages
import pandas as pd

# Local packages
from evaluation_jp.features.nearest_key_dict import NearestKeyDict


def duplicated(item_list):
    return set([x for n, x in enumerate(list(item_list)) if x in item_list[:n]])


class DuplicatedItemsError(Exception):
    """There shouldn't be duplicated data and index column names in a dataframe!
    """

    pass


@dataclass
class ColumnsByType:
    data_columns_by_type: dict
    index_columns_by_type: dict = None

    def __post_init__(self):
        """Check for duplicated columns between data_columns and index_columns
        NB A single Python dict won't allow duplicated keys, so can't have duplicates
        inside data_columns_by_type or index_columns_by_type
        """
        if self.index_columns is not None:
            if (duplicates := duplicated(list(self.data_columns) + list(self.index_columns))) :
                raise DuplicatedItemsError(f"Found duplicated columns {duplicates}")

    @property
    def data_columns(self):
        return set(self.data_columns_by_type)

    @property
    def index_columns(self):
        if self.index_columns_by_type is not None:
            return set(self.index_columns_by_type)
        else: 
            return None

    @property
    def all_columns(self):
        if self.index_columns:
            return self.data_columns | self.index_columns
        else:
            return self.data_columns

    @property
    def datetime_data_columns(self):
        return set([k for k, v in self.data_columns_by_type.items() if v == "datetime64"])

    @property
    def datetime_index_columns(self):
        if self.index_columns:
            return set([k for k, v in self.index_columns_by_type.items() if v == "datetime64"])
        else:
            return None

    @property
    def datetime_all_columns(self):
        if self.datetime_index_columns:
            return self.datetime_data_columns | self.datetime_index_columns
        else:
            return self.datetime_data_columns

    def check_column_names(self, column_names: list):
        if (duplicates := duplicated(column_names)) :
            raise DuplicatedItemsError(f"Found duplicated columns {duplicates}")

        if set(column_names) == self.all_columns:
            return True
        else:
            error = "Columns do not match expected columns!\n"
            if set(column_names) - self.all_columns != set():
                error += (
                    f"Unexpected columns: {set(column_names) - set(self.all_columns)}\n"
                )
            if set(self.all_columns) - set(column_names) != set():
                error += (
                    f"Missing columns: {set(self.all_columns) - set(column_names)}\n"
                )
            raise DuplicatedItemsError(error)

    def set_datatypes(self, data: pd.DataFrame):
        self.check_column_names(data.columns)
        if not data.empty:
            for col, dtype in self.data_columns_by_type.items():
                data[col] = data[col].astype(dtype)
        if self.index_columns_by_type is not None:
            data = data.reset_index()
            self.check_column_names(data.columns)
            if not data.empty:
                for index_col, dtype in self.index_columns_by_type.items():
                    data[index_col] = data[index_col].astype(dtype)
            data = data.set_index(list(self.index_columns_by_type))
        return data


@dataclass
class DataParams(abc.ABC):
    
    table_name: ClassVar[str]
    columns_by_type: ColumnsByType

    @abc.abstractmethod
    def setup_steps(self, data_id=None):
        pass


@dataclass
class PopulationSliceDataParams(DataParams):

    table_name: ClassVar[str] = "population_slice"
    setup_steps_by_date: NearestKeyDict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def setup_steps(self, data_id):
        return self.setup_steps_by_date[data_id.date]


@dataclass
class TreatmentPeriodDataParams(DataParams):

    table_name: ClassVar[str] = "treatment_period"
    setup_steps_by_date: NearestKeyDict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def setup_steps(self, data_id):
        return self.setup_steps_by_date[data_id.time_period.to_timestamp()]


# //

