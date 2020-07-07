# %%
# Standard library
from pathlib import Path
from dataclasses import dataclass, InitVar
import abc

# from __future__ import annotations
from typing import TYPE_CHECKING

# External packages
import pandas as pd
import sqlalchemy as sa

# Local packages
from evaluation_jp.data.sql_utils import sql_format, sql_where_clause_from_dict

# if TYPE_CHECKING:
from evaluation_jp import DataID, DataParams


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


# //TODO ABC and concrete classes for various database and file-based storage options

# //TODO Switch to jinja for SQL templating


class ModelDataHandler(abc.ABC):
    pass


@dataclass
class SQLDataHandler(ModelDataHandler):
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
    location: InitVar[str] = None
    name: InitVar[str] = None

    engine: sa.engine.Engine = None

    def __post_init__(
        self, data_path, database_type, username, password, location, name,
    ):
        """Convert data_path into instantiated data connection
        """
        # //TODO Add exception handling if data connection can't be set up
        if data_path:
            self.engine = sa.create_engine(data_path)
        elif database_type == "sqlite":
            connection_string = f"sqlite:///{Path(location)}/{name}.db"
            # //TODO Implement connection strings for MSSQL and other databases
            self.engine = sa.create_engine(connection_string)

    def table_exists(self, data_type):
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        if data_type in insp.get_table_names():
            return True
        else:
            return False

    def read(self, data_params: DataParams, data_id: DataID = None):
        """Load dataframe from records in `table` matching `id`
        """
        if self.table_exists(data_params.type_name):
            query = f"""\
                SELECT * 
                    FROM {data_params.type_name}
                """
            if data_id is not None:
                sql_data_id = {
                    f"data_id_{key}": value
                    for key, value in data_id.as_flattened_dict().items()
                }
                query += sql_where_clause_from_dict(sql_data_id)
            else:
                sql_data_id = []

            # //TODO Fix datatypes especially bool and categorical!
            date_cols = [
                k for k, v in data_params.columns_by_type.items() if v == "datetime64"
            ]
            data = pd.read_sql(
                query,
                con=self.engine,
                parse_dates=date_cols,
                index_col=data_params.index_columns_by_type.keys(),
            ).drop(list(sql_data_id), axis="columns")
            if not data.empty:
                return data
            else:
                raise DataNotFoundError
        else:
            raise TableNotFoundError

        # //TODO Implement read from archive (in memory and into live database)

    def _delete(self, data_params, data_id=None):
        # If the table exists, delete any previous rows with this data_id
        table_name = data_params.type_name
        if self.table_exists(table_name):
            query = f"""\
                DELETE FROM {table_name}
            """
            if data_id is not None:
                sql_data_id = {
                    f"data_id_{key}": value
                    for key, value in data_id.as_flattened_dict().items()
                }
                query += sql_where_clause_from_dict(sql_data_id)
            with self.engine.connect() as conn:
                conn.execute(query)

    def _add_data_id_indexes(self, data_id, table_name):
        data_id_cols = [f"data_id_{col}" for col in data_id.as_flattened_dict()]
        if len(data_id_cols) > 1:
            data_id_indexes = data_id_cols + data_id_cols
        else:
            data_id_indexes = data_id_cols
        for idx in data_id_indexes:
            idx = idx if isinstance(idx, list) else [idx]
            try:
                query = f"""\
                    CREATE INDEX idx_{'_'.join(i for i in idx)}
                    ON {table_name} ({', '.join(i for i in idx)})
                    """
                with self.engine.connect() as conn:
                    conn.execute(query)
            except:
                pass

    def write(self, data, data_params, data_id=None, use_index=True):
        table_name = data_params.type_name
        if use_index:
            data = data.reset_index()
        for col in data.columns:
            if "period" in str(data[col].dtype):
                data[col] = data[col].astype(str)

        if data_id is not None:
            self._delete(table_name, data_id)
            for key, value in data_id.as_flattened_dict().items():
                data[f"data_id_{key}"] = sql_format(value)

            data.to_sql(table_name, con=self.engine, if_exists="append", index=False)
            self._add_data_id_indexes(data_id, table_name)
        else:
            data.to_sql(table_name, con=self.engine, if_exists="replace", index=False)

    # //TODO Implement _write_archive()

def populate(
    data_params: DataParams,
    data_id: DataID = None,
    init_data: pd.DataFrame = None,
    data_handler: ModelDataHandler = None,
    rebuild: bool = False,
):
    if data_handler is not None:
        if not rebuild:
            try:
                data = data_handler.read(data_params, data_id)
                data = data_params.set_datatypes(data)
            except ModelDataHandlerError:
                rebuild = True
        if rebuild:
            data = data_params.setup_steps.run(data_id=data_id, data=init_data)
            data = data_params.set_datatypes(data)
            data_handler.write(data, data_params, data_id)
    else:
        data = data_params.setup_steps.run(data_id=data_id, data=init_data)
        data = data_params.set_datatypes(data)
    return data


#     # TODO //Implement an alternate constructor to copy existing
#     # ? Use alembic ?
#     # @classmethod
#     # def copy_existing(cls, old_data_path, new_data_path, rebuild_all):
#     #     # Make copy of old database at new_data_path
#     #     pass


