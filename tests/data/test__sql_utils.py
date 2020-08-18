import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import sqlalchemy as sa

from evaluation_jp.data.sql_utils import *

np.random.seed(0)


@pytest.fixture
def df_100_x_4():
    return pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))

@pytest.fixture
def df_100000_x_4():
    return pd.DataFrame(np.random.randint(0, 100_000, size=(100_000, 4)), columns=list("ABCD"))


def test__sqlserver_engine():
    server = "CSKMA0400\\STATS1"
    database = "tempdb"

    engine = sqlserver_engine(server, database)

    assert engine.dialect.name == "mssql"


def test__sqlite_engine(tmpdir):
    path = tmpdir
    database = "test_db"

    engine = sqlite_engine(path, database)

    assert engine.dialect.name == "sqlite"


def test__temp_table__sqlite_small(tmpdir, df_100_x_4):
    path = tmpdir
    database = "test_db"

    engine = sqlite_engine(path, database)

    df = df_100_x_4

    with temp_table(engine, df, "test_table") as con:
        results = pd.read_sql_table("test_table", con, schema="temp")
    
    assert_frame_equal(df.describe(), results.describe())


def test__temp_table__sqlite_big(tmpdir, df_100000_x_4):
    path = tmpdir
    database = "test_db"

    engine = sqlite_engine(path, database)

    df = df_100000_x_4

    with temp_table(engine, df, "test_table") as con:
        results = pd.read_sql_table("test_table", con, schema="temp")
    
    assert_frame_equal(df.describe(), results.describe())


def test__temp_table__sqlserver_small(tmpdir, df_100_x_4):
    server = "CSKMA0400\\STATS1"
    database = "tempdb"

    engine = sqlserver_engine(server, database)

    df = df_100_x_4

    with temp_table(engine, df, "test_table") as con:
        results = pd.read_sql_table("test_table", con)
    
    assert_frame_equal(df.describe(), results.describe())



def test__temp_table__sqlserver_big(tmpdir, df_100000_x_4):
    server = "CSKMA0400\\STATS1"
    database = "tempdb"

    engine = sqlserver_engine(server, database)

    df = df_100000_x_4

    with temp_table(engine, df, "test_table") as con:
        results = pd.read_sql_table("test_table", con)
    
    print(results.describe())
    print(df.describe())
    assert_frame_equal(df.describe(), results.describe())