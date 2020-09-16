# Standard library
from datetime import datetime
from typing import List
from contextlib import contextmanager
from pathlib import Path
from urllib import parse

# External packages
import pandas as pd
import pyodbc
import sqlalchemy as sa


def sqlserver_engine(
    server: str, database: str,
):
    """Quickly create SQLAlchemy engine for SQL Server: just specify server and database.
    {ODBC Driver 17 for SQL Server} is the modern Microsoft-provided ODBC driver:
    https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server
    "TRUSTED_CONNECTION" set to "YES" to allow Active Directory authentication.
    """

    odbc_params = {
        "DRIVER": "{ODBC Driver 17 for SQL Server}",
        "SERVER": server,
        "DATABASE": database,
        "TRUSTED_CONNECTION": "YES",
    }
    try:
        formatted_odbc_params = parse.quote_plus(
            ";".join(f"{key}={value}" for key, value in odbc_params.items())
        )
        engine = sa.create_engine(
            f"mssql+pyodbc:///?odbc_connect={formatted_odbc_params}",
            fast_executemany=True,
        )
        engine.connect()
    except sa.exc.InterfaceError:  # Need to add the right error type here
        odbc_params["DRIVER"] = "{SQL Server}"
        formatted_odbc_params = parse.quote_plus(
            ";".join(f"{key}={value}" for key, value in odbc_params.items())
        )
        engine = sa.create_engine(
            f"mssql+pyodbc:///?odbc_connect={formatted_odbc_params}",
            fast_executemany=False,
        )
    return engine


def sqlite_engine(
    path: Path, database: str,
):
    """Quickly create SQLAlchemy engine for SQLite: just specify path and database.
    """
    return sa.create_engine(f"sqlite:///{Path(path)}/{database}.db")


# con can be either Connection or Engine to facilitate nesting
@contextmanager
def temp_table_connection(
    connectable: sa.engine.base.Connectable,
    frame: pd.DataFrame,
    table: str,
    schema: str = None,
):
    """Context manager to add temp table `frame` to temp `table` in `connectable` 
    `connectable` can be either an Engine or an existing Connection.
    If using with MSSQL, connectable must point to tempdb, with executemany=True.
    And with MSSQL, table name must start with '##'!!!
    """

    # Set up connection from connectable - needed if it's an Engine
    with connectable.connect() as con:
        # Setup
        if con.dialect.name == "sqlite":
            rows = ", ".join(
                [str(row) for row in frame.to_records(index=False).tolist()]
            )
            queries = [
                f"DROP TABLE IF EXISTS {table}",
                f"CREATE TEMP TABLE {table}({', '.join(frame.columns)})",
                f"INSERT INTO {table} VALUES {rows}",
            ]
            for query in queries:
                con.execute(query)

        elif con.dialect.name == "mssql":
            insp = sa.inspect(con)
            if table in insp.get_table_names(schema="dbo"):
                con.execute(f"DROP TABLE {table}")
            frame.to_sql(
                table, con=con, schema=schema, if_exists="replace", index=False,
            )

        yield con

        # Cleanup
        con.execute(f"DROP TABLE {table}")


def get_col_list(engine, table_name, columns=None, required_columns=None):
    insp = sa.inspect(engine)
    column_metadata = insp.get_columns(table_name)
    table_columns = [col["name"] for col in column_metadata]
    if columns is not None:
        ok_columns = (set(columns) | set(required_columns)) & set(table_columns)
    else:
        ok_columns = table_columns
    return [col for col in ok_columns if col not in ["index", "id"]]


def unpack(listlike):
    return ", ".join([str(i) for i in listlike])


def get_parameterized_query(query_text, ids=None):
    params = {}
    if ids is not None:
        query_text += f"""\
            {"WHERE" if "WHERE" not in query_text else "AND"} ppsn in :ids
        """
        params["ids"] = list(set(ids))
        query = sa.sql.text(query_text).bindparams(
            sa.sql.expression.bindparam("ids", expanding=True)
        )
    else:
        query = query_text
    return query, params


def datetime_cols(engine, table_name) -> List:
    insp = sa.inspect(engine)
    column_metadata = insp.get_columns(table_name)
    datetime_cols = [
        col["name"]
        for col in column_metadata
        if type(col["type"]) == sa.sql.sqltypes.DATETIME
    ]
    return datetime_cols


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


def sql_clause_format(thing):
    if is_number(thing):
        return thing
    else:
        return f"'{sql_format(thing)}'"


def sql_where_clause_from_dict(dictionary):
    where_clause = ""
    first = True
    for key, value in dictionary.items():
        if first:
            where_clause += f"WHERE {key} = {sql_clause_format(value)}"
        else:
            where_clause += f"\n    AND {key} = {sql_clause_format(value)}"
        first = False
    return where_clause


def where_and():
    first = True
    while True:
        if first:
            yield "WHERE"
        else:
            yield "AND"


def get_sql_data_id(data_id=None):
    """Create id column for each element of a data_id, to allow SQL queries to find data with that ID.
    """
    if data_id is not None:
        return {
            f"data_id_{k}": sql_format(v)
            for k, v in data_id.as_flattened_dict().items()
        }
    else:
        return {}

