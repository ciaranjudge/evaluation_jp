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
    try:
        engine.connect()
        assert True
    except:
        assert False


def test__sqlite_engine(tmpdir):
    path = tmpdir
    database = "test_db"

    engine = sqlite_engine(path, database)

    assert engine.dialect.name == "sqlite"


def test__temp_table_connection__sqlite_small(tmpdir, df_100_x_4):
    path = tmpdir
    database = "test_db"

    engine = sqlite_engine(path, database)

    df = df_100_x_4

    with temp_table_connection(engine, df, "test_table") as con:
        results = pd.read_sql_table("test_table", con, schema="temp")
    
    assert_frame_equal(df.describe(), results.describe())


def test__temp_table_connection__sqlite_big(tmpdir, df_100000_x_4):
    path = tmpdir
    database = "test_db"

    engine = sqlite_engine(path, database)

    df = df_100000_x_4

    with temp_table_connection(engine, df, "test_table") as con:
        results = pd.read_sql_table("test_table", con, schema="temp")
    
    assert_frame_equal(df.describe(), results.describe())


def test__temp_table_connection__sqlserver_small(tmpdir, df_100_x_4):
    server = "CSKMA0400\\STATS1"
    database = "tempdb"

    engine = sqlserver_engine(server, database)

    df = df_100_x_4

    with temp_table_connection(engine, df, "##test_table") as con:
        query = """select * from ##test_table"""
        results = pd.read_sql(query, con)
    
    assert_frame_equal(df.describe(), results.describe())



def test__temp_table_connection__sqlserver_big(tmpdir, df_100000_x_4):
    server = "CSKMA0400\\STATS1"
    database = "tempdb"

    engine = sqlserver_engine(server, database)

    df = df_100000_x_4

    with temp_table_connection(engine, df, "##test_table") as con:
        query = """select * from ##test_table"""
        results = pd.read_sql(query, con)
    
    print(results.describe())
    print(df.describe())
    assert_frame_equal(df.describe(), results.describe())


def test__temp_table_connection__sqlserver_small_PA1(tmpdir, df_100_x_4):
    server = "CSGPC-BPRD-SQ06\\PA1"
    database = "tempdb"

    engine = sqlserver_engine(server, database)

    df = df_100_x_4

    with temp_table_connection(engine, df, "##test_table") as con:
        query = """select * from ##test_table"""
        results = pd.read_sql(query, con)
    
    print(results.describe())
    print(df.describe())
    assert_frame_equal(df.describe(), results.describe())

def test__datetime_cols():
    engine = sa.create_engine(
        "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
    )
    test__inputs = ["les", "ists_personal", "jobpath_referrals"]
    results = {
        table_name: set(datetime_cols(engine, table_name))
        for table_name in test__inputs
    }
    expected = {
        "les": set(["start_date"]),
        "ists_personal": set(["date_of_birth"]),
        "jobpath_referrals": set(
            [
                "referral_date",
                "jobpath_start_date",
                "jobpath_end_date",
                "cancellation_date",
                "pause_date",
                "resume_date",
                "ppp_agreed_date",
            ]
        ),
    }
    assert results == expected