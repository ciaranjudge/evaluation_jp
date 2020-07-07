from datetime import datetime
from typing import List
from contextlib import contextmanager

import sqlalchemy as sa


@contextmanager
def temp_ids_con(engine: sa.engine.base.Engine, ids: set):
    """Create database connection that makes temp.ids available as single column temp table
    """

    with engine.connect() as con:
        rows = ", ".join([f"({sql_clause_format(id)})" for id in ids])
        queries = [
            "DROP TABLE IF EXISTS temp.ids",
            "CREATE TEMP TABLE temp.ids(id INTEGER)",
            f"INSERT INTO temp.ids(id) VALUES {rows}",
        ]
        for query in queries:
            con.execute(query)
        yield con


def get_col_list(engine, table_name, columns=None, required_columns=None):
    insp = sa.engine.reflection.Inspector.from_engine(engine)
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
    insp = sa.engine.reflection.Inspector.from_engine(engine)
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



