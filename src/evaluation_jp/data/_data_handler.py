# %%
# Standard library
import abc
from dataclasses import dataclass, field
from pathlib import Path


# External packages
import pandas as pd
from pandas.errors import EmptyDataError
import pysnooper
import sqlalchemy as sa

# Local packages
from evaluation_jp.data.sql_utils import *
from evaluation_jp import DataID, DataParams


class DataHandlerError(Exception):
    """Generic exception handler for DataHandler
    """

    pass


class TableNotFoundError(DataHandlerError):
    """Couldn't find the specified table!
    """

    pass


class DataNotFoundError(DataHandlerError):
    """Table doesn't contain requested data!
    """

    pass


# //TODO ABC and concrete classes for various database and file-based storage options

# //TODO Switch to jinja for SQL templating

# //TODO Declare all tables to be used by model in advance at model runtime.
# Create needed indexes in advance not on the fly.
# Does this lock in to SQL though? Need to do it without this.


class DataHandler(abc.ABC):
    @abc.abstractmethod
    def read(self, data_params: DataParams, data_id: DataID = None):
        pass

    @abc.abstractmethod
    def write(self, data, data_params, data_id=None, use_index=True):
        pass


@dataclass
class SQLDataHandler(DataHandler):
    """Manages storage and retrieval of model data.
    For now, assume backend is a database with sqlalchemy connection.
    
    """

    engine: sa.engine.Engine
    model_schema: str

    def __post_init__(self):
        """Create the database schema model_schema using engine if it doesn't exist.
        # ...unless using sqlite engine, in which case do nothing.
        """
        if not self.engine.dialect.name == "sqlite":
            if self.model_schema not in self.engine.dialect.get_schema_names(
                self.engine
            ):
                self.engine.execute(sa.schema.CreateSchema(self.model_schema))

    def table_exists(self, table_name):
        if self.engine.dialect.name == "sqlite":
            table_name = f"{self.model_schema}.{table_name}"
            schema = "main"
        else:
            table_name = table_name
            schema = self.model_schema
        if table_name in sa.inspect(self.engine).get_table_names(schema):
            return True
        else:
            return False

    def read(self, data_params: DataParams, data_id: DataID = None):
        """Loads dataframe from records in data_params.table_name with optional data_id.
        Fixes dtypes based on data_params.columns_by_type.
        """
        if self.table_exists(data_params.table_name):
            # SQLite doesn't really understand schemas - it just thinks they're in table name.
            # But it can't cope with a dot in table name unless wrapped in [ ]
            if self.engine.dialect.name == "sqlite":
                query = f"""\
                    SELECT * 
                        FROM [{self.model_schema}.{data_params.table_name}]
                    """
            else:
                query = f"""\
                    SELECT * 
                        FROM {self.model_schema}.{data_params.table_name}
                    """
            if sql_data_id := get_sql_data_id(data_id):
                query += sql_where_clause_from_dict(sql_data_id)

            data = pd.read_sql(
                query,
                con=self.engine,
                parse_dates=data_params.columns_by_type.datetime_all_columns,
                index_col=data_params.columns_by_type.index_columns,
            ).drop(list(sql_data_id), axis="columns")
            if not data.empty:
                return data_params.columns_by_type.set_datatypes(data)
            else:
                raise DataNotFoundError
        else:
            raise TableNotFoundError
    
    @pysnooper.snoop()
    def _delete(self, table_name, sql_data_id=None):
        # If the table exists, delete any previous rows with this data_id
        if self.table_exists(table_name):
            if self.engine.dialect.name == "sqlite":
                query = f"""\
                    DELETE FROM [{self.model_schema}.{table_name}]
                """
            else:
                query = f"""\
                    DELETE FROM {self.model_schema}.{table_name}
                """
            if sql_data_id:
                query += sql_where_clause_from_dict(sql_data_id)
            with self.engine.connect() as conn:
                conn.execute(query)

    def _add_data_id_indexes(self, table_name, sql_data_id):
        if sql_data_id:
            data_id_cols = list(sql_data_id.items())
            if len(data_id_cols) > 1:
                data_id_indexes = data_id_cols + data_id_cols
            else:
                data_id_indexes = data_id_cols
            for idx in data_id_indexes:
                idx = idx if isinstance(idx, list) else [idx]
                try:
                    if self.engine.dialect.name == "sqlite":
                        query = f"""\
                            CREATE INDEX idx_{'_'.join(i for i in idx)}
                            ON [{self.model_schema}.{table_name}] ({', '.join(i for i in idx)})
                            """
                    else:
                        query = f"""\
                            CREATE INDEX idx_{'_'.join(i for i in idx)}
                            ON {self.model_schema}.{table_name} ({', '.join(i for i in idx)})
                            """
                    with self.engine.connect() as conn:
                        conn.execute(query)
                except:
                    pass

    def write(self, data, data_params, data_id=None, use_index=True):
        """Write dataframe `data` to `data_params.table_name`. 
        If there's a data_id, add the data_id fields as columns in the table.
        If data already exists with same data_params.table_name and optional data_id, overwrite it.
        If use_index is True, add the index columns of data to the table.
        Raise EmptyDataError if passed an empty dataframe.
        """
        sqldata = data.copy()

        if sqldata.empty:
            raise EmptyDataError

        if use_index:
            sqldata = sqldata.reset_index()

        for col in data.columns:
            if "period" in str(data[col].dtype):
                sqldata[col] = sqldata[col].astype(str)

        if self.engine.dialect.name == "sqlite":
            table_name = f"{self.model_schema}.{data_params.table_name}"
            schema = "main"
        else:
            table_name = data_params.table_name
            schema = self.model_schema

        if sql_data_id := get_sql_data_id(data_id):
            self._delete(table_name, sql_data_id)
            for k, v in sql_data_id.items():
                sqldata[k] = v
            sqldata.to_sql(
                table_name,
                schema=schema,
                con=self.engine,
                if_exists="append",
                index=False,
            )
            self._add_data_id_indexes(table_name, sql_data_id)
        else:
            sqldata.to_sql(
                table_name,
                schema=schema,
                con=self.engine,
                if_exists="replace",
                index=False,
            )


def populate(
    data_params: DataParams,
    data_id: DataID = None,
    initial_data: pd.DataFrame = None,
    data_handler: DataHandler = None,
    rebuild: bool = False,
):
    # //TODO Write a docstring for populate()!!
    """
    """
    if data_handler is not None and not rebuild:
        try:
            data = data_handler.read(data_params, data_id)
        except DataHandlerError:
            rebuild = True
    if data_handler is None or rebuild:
        data = data_params.setup_steps(data_id).run(data_id=data_id, data=initial_data)
        data = data_params.columns_by_type.set_datatypes(data)
        if data_handler is not None:
            data_handler.write(
                data,
                data_params,
                data_id,
                use_index=data_params.columns_by_type.index_columns,
            )
    return data


#     # TODO //Implement an alternate constructor to copy existing
#     # ? Use alembic ?
#     # @classmethod
#     # def copy_existing(cls, old_data_path, new_data_path, rebuild_all):
#     #     # Make copy of old database at new_data_path
#     #     pass
