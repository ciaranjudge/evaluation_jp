# %%
# Standard library
from typing import List, Set, Dict, Tuple, Optional, Callable, Union
from pathlib import Path
from functools import wraps
from dataclasses import dataclass, field, InitVar

# External packages
import pandas as pd
import sqlalchemy as sa
import sqlalchemy_utils

class ModelDataHandlerError(Exception):
    """Generic exception handler for ModelDataHandler
    """
    pass

class TableNotFoundError(ModelDataHandlerError):
    """Couldn't find the specified table!
    """
    pass

class DataNotFoundError(ModelDataHandlerError):
    """Table doesn't contain requested data!
    """
    pass



# TODO ABC and concrete classes for various database and file-based storage options
# ? Should constructor work with partial type + path + name or full data_path?
@dataclass
class ModelDataHandler:
    """Manages storage and retrieval of model data.
    For now, assume backend is a database with sqlalchemy connection.
    
    """

    data_path: InitVar[str]

    engine: sa.engine.Engine = field(init=False)

    def __post_init__(self, data_path):
        """Convert data_path into instantiated data connection
        """
        # TODO Add exception handling if data connection can't be set up
        self.engine = sa.create_engine(data_path)

    def table_exists(self, table_type):
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        if table_type in insp.get_table_names():
            return True
        else:
            return False

    def read(self, table_type, data_id):
        """Load dataframe from records in `table` matching `id`
        """
        if self.table_exists(table_type):
            query = f"""\
                SELECT * 
                    FROM {table_type}
                """
            for key, value in data_id.items():
                query += f"""\
                    {"WHERE" if "WHERE" not in query else "AND"} {key} = '{value}''
                """
            data = pd.read_sql(query, con="engine").drop(data_id.keys(), axis="columns")
            if not data.empty:
                return data
            else:
                raise DataNotFoundError
        else:
            raise TableNotFoundError

        # TODO Implement read from archive (in memory and into live database)

    def _delete(self, table_type, data_id):
        # If the table exists, delete any previous rows with this data_id
        if self.table_exists(table_type):
            delete_query = f"""\
                DELETE FROM {table_type}
                    {[f"WHERE {key}='{value}'" for key, value in data_id.items()]}
                """
            with self.engine.connect() as conn:
                conn.execute(delete_query)

    def _write_live(self, table_type, data_id, data):
        self._delete(table_type, data_id)
        for key, value in data_id.items():
            data[key] = value
        data.to_sql(table_type, con=self.engine, if_exists="append")

    # TODO Implement _write_archive()

    def write(self, table_type, data_id, data):
        self._write_live(table_type, data_id, data)

    def __call__(self, table_type, data_id, setup_steps=None, initial_data=None):
        """Given a valid table name (`population_data`, `population_slices`, `treatment_periods`)
        ...does the table exist? If not, create it!
        Given the table exists, can the ID of this item be found?
        """
        try:
            data = self.read(table_type, data_id)
        except ModelDataHandlerError:
            data = setup_steps(data_id, initial_data)
            self.write(table_type, data_id, data)
        return data


    # TODO Implement an alternate constructor to copy existing
    # @classmethod
    # def copy_existing(cls, old_data_path, new_data_path, rebuild_all):
    #     # Make copy of old database at new_data_path
    #     pass
