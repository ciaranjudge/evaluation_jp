# Standard library
from dataclasses import dataclass
from typing import ClassVar, TYPE_CHECKING
import abc

# External packages
import pandas as pd

# Local packages
from evaluation_jp import NearestKeyDict

if TYPE_CHECKING:
    from evaluation_jp import SetupSteps


def duplicated(item_list):
    return set([x for n, x in enumerate(list(item_list)) if x in item_list[:n]])


class DuplicatedItemsError(Exception):
    """There shouldn't be duplicated data and index column names in a dataframe!
    """

    pass

def check_columns(expected: set, actual: list):
    if (duplicates := duplicated(actual)) :
        raise DuplicatedItemsError(f"Found duplicated columns {duplicates}")

    if set(actual) == expected:
        return True
    else:
        error = "Columns do not match expected columns!\n"
        if set(actual) - expected != set():
            error += (
                f"Unexpected columns: {set(actual) - expected}\n"
            )
        if expected - set(actual) != set():
            error += (
                f"Missing columns: {expected - set(actual)}\n"
            )
        raise DuplicatedItemsError(error)


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

    def check_data_column_names(self, column_names: list):
        return check_columns(set(self.data_columns), column_names)

    def check_index_column_names(self, column_names: list):
        return check_columns(set(self.index_columns), column_names)    

    def set_datatypes(self, data: pd.DataFrame):
        if not data.empty:
            for col, dtype in self.data_columns_by_type.items():
                data[col] = data[col].astype(dtype)
        if self.index_columns_by_type is not None:
            self.check_index_column_names(data.index.names)
            data = data.reset_index()
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
    def get_setup_steps(self, data_id=None):
        pass


@dataclass
class PopulationSliceDataParams(DataParams):

    table_name: ClassVar[str] = "population_slices"
    setup_steps_by_date: NearestKeyDict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def get_setup_steps(self, data_id):
        return self.setup_steps_by_date[data_id.reference_date()]


@dataclass
class TreatmentPeriodDataParams(DataParams):

    table_name: ClassVar[str] = "treatment_periods"
    setup_steps_by_date: NearestKeyDict = None

    def __post_init__(self):
        self.setup_steps_by_date = NearestKeyDict(self.setup_steps_by_date)

    def get_setup_steps(self, data_id, how="start"):
        return self.setup_steps_by_date[data_id.reference_date(how)]


@dataclass
class SWPaymentsDataParams(DataParams):

    table_name: ClassVar[str] = "sw_payments"
    setup_steps: list

    def get_setup_steps(self):
        return self.setup_steps


@dataclass
class EarningsDataParams(DataParams):

    table_name: ClassVar[str] = "earnings"
    setup_steps: list

    def get_setup_steps(self):
        return self.setup_steps
