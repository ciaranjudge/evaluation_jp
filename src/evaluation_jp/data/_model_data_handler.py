# %%
# Standard library
from collections import OrderedDict
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional, Callable, Union
from pathlib import Path
from functools import wraps
from dataclasses import dataclass, field, InitVar, asdict

# External packages
import pandas as pd
import sqlalchemy as sa
import sqlalchemy_utils

from evaluation_jp.data import datetime_cols


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


def is_number(s):
    try:
        float(str(s))
        return True
    except ValueError:
        return False


def sql_format(thing):
    if is_number(thing):
        return thing
    elif isinstance(thing, datetime):
        return thing.date()
    else:
        return str(thing)


def sql_where_format(thing):
    if is_number(thing):
        return thing
    else:
        return f"'{sql_format(thing)}'"


def sql_where_clause_from_dict(dictionary):
    where_clause = ""
    first = True
    for key, value in dictionary.items():
        if first:
            where_clause += f"WHERE {key} = {sql_where_format(value)}"
        else:
            where_clause += f"\n    AND {key} = {sql_where_format(value)}"
        first = False
    return where_clause


def flatten(d, sep="_"):
    """Flatten a dict.
    Based on https://gist.github.com/jhsu98/188df03ec6286ad3a0f30b67cc0b8428
    """

    obj = OrderedDict()

    def recurse(t, parent_key=""):

        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)

    return obj


# TODO ABC and concrete classes for various database and file-based storage options


@dataclass
class ModelDataHandler:
    """Manages storage and retrieval of model data.
    For now, assume backend is a database with sqlalchemy connection.
    
    """

    # Feed in the parts of the database URL:
    # dialect+driver://username:password@host:port/database
    # From https://docs.sqlalchemy.org/en/13/core/engines.html

    data_path: InitVar[str] = None
    database_type: InitVar[str] = None
    username: InitVar[str] = None
    password: InitVar[str] = None
    database_location: InitVar[str] = None
    database_name: InitVar[str] = None

    engine: sa.engine.Engine = field(init=False)

    def __post_init__(
        self,
        data_path,
        database_type,
        username,
        password,
        database_location,
        database_name,
    ):
        """Convert data_path into instantiated data connection
        """
        # TODO Add exception handling if data connection can't be set up
        if data_path:
            self.engine = sa.create_engine(data_path)
        else:
            if database_type == "sqlite":
                connection_string = (
                    f"sqlite:///{Path(database_location)}/{database_name}.db"
                )
            # TODO Implement connection strings for MSSQL and other databases
            self.engine = sa.create_engine(connection_string)

    def table_exists(self, data_type):
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        if data_type in insp.get_table_names():
            return True
        else:
            return False

    def read(self, data_type, data_id):
        """Load dataframe from records in `table` matching `id`
        """
        if self.table_exists(data_type):
            query = f"""\
                SELECT * 
                    FROM {data_type}
                """
            sql_data_id = {
                f"data_id_{key}": value for key, value in flatten(data_id).items()
            }
            query += sql_where_clause_from_dict(sql_data_id)
            data = pd.read_sql(
                query,
                con=self.engine,
                parse_dates=datetime_cols(self.engine, data_type),
            ).drop(list(sql_data_id) + ["index"], axis="columns")
            if not data.empty:
                return data
            else:
                raise DataNotFoundError
        else:
            raise TableNotFoundError

        # TODO Implement read from archive (in memory and into live database)

    def _delete(self, data_type, data_id):
        # If the table exists, delete any previous rows with this data_id
        if self.table_exists(data_type):
            query = f"""\
                DELETE FROM {data_type}
            """
            sql_data_id = {
                f"data_id_{key}": value for key, value in flatten(data_id).items()
            }
            query += sql_where_clause_from_dict(sql_data_id)
            print(query)
            with self.engine.connect() as conn:
                conn.execute(query)

    def _write_live(self, data_type, data_id, data):
        self._delete(data_type, data_id)
        for key, value in flatten(data_id).items():
            data[f"data_id_{key}"] = sql_format(value)

        data.to_sql(data_type, con=self.engine, if_exists="append")
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        if len(list(flatten(data_id))) > 1:
            data_id_indexes = list(flatten(data_id)) + [list(flatten(data_id))]
        else:
            data_id_indexes = list(flatten(data_id))
        for idx in data_id_indexes:
            idx = idx if isinstance(idx, list) else [idx]
            if idx not in [d["column_names"] for d in insp.get_indexes(data_type)]:
                query = f"""\
                    CREATE INDEX idx_{'_'.join(i for i in idx)}
                    ON {data_type} ({', '.join(i for i in idx)})
                    """
                with self.engine.connect() as conn:
                    conn.execute(query)

    # TODO Implement _write_archive()

    def write(self, data_type, data_id, data):
        self._write_live(data_type, data_id, data.copy())

    def run(self, data_type, data_id, setup_steps=None, init_data=None):
        """Given a valid table name (`population_data`, `population_slices`, `treatment_periods`)
        ...does the table exist? If not, create it!
        Given the table exists, can the ID of this item be found?
        """
        try:
            data = self.read(data_type, data_id)
        except ModelDataHandlerError:
            data = setup_steps.run(data_id, init_data)
            self.write(data_type, data_id, data)
        return data

    # TODO Implement an alternate constructor to copy existing
    # ? Use alembic ?
    # @classmethod
    # def copy_existing(cls, old_data_path, new_data_path, rebuild_all):
    #     # Make copy of old database at new_data_path
    #     pass
