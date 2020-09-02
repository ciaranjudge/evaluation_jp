from pathlib import Path
from IPython.display import display

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import sqlalchemy as sa

from evaluation_jp import (
    ColumnsByType,
    SQLDataHandler,
    populate,
    PopulationSliceID,
    PopulationSliceDataParams,
    SetupStep,
    SetupSteps,
    TreatmentPeriodID,
    TreatmentPeriodDataParams,
)
from evaluation_jp.data.sql_utils import *



class TestStep(SetupStep):
    def run(self, data=None, data_id=None):
        return pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )


test_setup_steps_by_date = {pd.Timestamp("2016-01-01"): SetupSteps([TestStep])}

population_slice_data_params = PopulationSliceDataParams(
    columns_by_type=ColumnsByType(
        data_columns_by_type={col: "int32" for col in list("ABCD")}
    ),
    setup_steps_by_date=test_setup_steps_by_date,
)
population_slice_id = PopulationSliceID(date=pd.Timestamp("2016-01-01"))


@pytest.mark.parametrize("sql_dialect", ["sqlite", "mssql"])
def test__SQLDataHandler__init(tmpdir, sql_dialect):
    """Simple test to make sure everything gets initiated correctly
    """
    if sql_dialect == "sqlite":
        engine = sqlite_engine(tmpdir, "test_db")
    if sql_dialect == "mssql":
        engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")

    data_handler = SQLDataHandler(engine=engine, model_schema="test_schema")

    assert data_handler.engine.dialect.name == sql_dialect
    assert data_handler.model_schema == "test_schema"


# +/- use_index = 12 tests


@pytest.mark.parametrize("sql_dialect", ["sqlite", "mssql"])
def test__DataHandler__write_new__PopulationSlice__with_data_id__no_index(
    tmpdir, sql_dialect
):
    """Write dataframe with *new* PopulationSlice data_id to SQL test_schema.population_slice
    """
    data_params = population_slice_data_params
    data_id = population_slice_id

    schema = "test_schema"

    if sql_dialect == "sqlite":
        engine = sqlite_engine(tmpdir, "test")
        sql_table_name = f"{schema}.{data_params.table_name}"
        sql_schema = "main"
    if sql_dialect == "mssql":
        engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")
        sql_table_name = data_params.table_name
        sql_schema = schema

    with engine.connect() as con:
        # Setup
        data_handler = SQLDataHandler(engine=con, model_schema=schema)

        # Write
        data = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        data_handler.write(data, data_params, data_id, use_index=False)

        # Read back data from SQL following write

        df = pd.read_sql_table(sql_table_name, con=con, schema=sql_schema)

    # Results should be same as original data passed to data_handler.write()
    results = df.loc[df["data_id_date"] == str(data_id.date.date())].drop(
        ["data_id_date"], axis="columns"
    )
    assert_frame_equal(data.describe(), results.describe())


@pytest.mark.parametrize("sql_dialect", ["sqlite", "mssql"])
def test__DataHandler__write_overwrite__PopulationSlice__with_data_id__no_index(
    tmpdir, sql_dialect
):
    """Write dataframe with *existing* PopulationSlice data_id to SQL test_schema.population_slice
    """
    data_params = population_slice_data_params
    data_id = population_slice_id

    schema = "test_schema"

    if sql_dialect == "sqlite":
        engine = sqlite_engine(tmpdir, "test")
        sql_table_name = f"{schema}.{data_params.table_name}"
        sql_schema = "main"
    if sql_dialect == "mssql":
        engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")
        sql_table_name = data_params.table_name
        sql_schema = schema

    with engine.connect() as con:
        # Setup data_handler
        data_handler = SQLDataHandler(engine=con, model_schema=schema)

        # First write
        data_1 = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        data_handler.write(data_1, data_params, data_id, use_index=False)

        # Second write
        data_2 = pd.DataFrame(
            np.random.randint(0, 200, size=(200, 4)), columns=list("ABCD")
        )
        data_handler.write(data_2, data_params, data_id, use_index=False)

        # Read back data from SQL following write

        df = pd.read_sql_table(sql_table_name, con=con, schema=sql_schema)

    # Results should be same as (2nd time around) data passed to data_handler.write()
    results = df.loc[df["data_id_date"] == str(data_id.date.date())].drop(
        ["data_id_date"], axis="columns"
    )
    assert_frame_equal(data_2.describe(), results.describe())


@pytest.mark.parametrize("sql_dialect", ["sqlite", "mssql"])
def test__DataHandler__read_existing__PopulationSlice__with_data_id__no_index(
    tmpdir, sql_dialect
):
    """Read dataframe with *existing* PopulationSlice data_id from SQL test_schema.population_slice
    """
    data_params = population_slice_data_params
    data_id = population_slice_id

    schema = "test_schema"

    if sql_dialect == "sqlite":
        engine = sqlite_engine(tmpdir, "test")
    if sql_dialect == "mssql":
        engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")

    with engine.connect() as con:
        # Setup
        data_handler = SQLDataHandler(engine=con, model_schema=schema)

        # Write
        data = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        data_handler.write(data, data_params, data_id, use_index=False)

        # Read
        results = data_handler.read(data_params, data_id)

    assert_frame_equal(data.describe(), results.describe())


# //TODO test read when table doesn't exist
# //TODO test read when table exists but data_id doesn't exist
# //TODO test index creation


def test__populate__no_data_handler():
    data_params = population_slice_data_params
    data_id = population_slice_id
    results = populate(data_params, data_id)
    assert results.shape == (100, 4)
    assert set(results.columns) == set(list("ABCD"))


# @pytest.mark.parametrize("data_params", "data_id", "init_data")
# @pytest.mark.parametrize("rebuild", [True, False])
# @pytest.mark.parametrize("overwrites")
@pytest.mark.parametrize("sql_dialect", ["sqlite", "mssql", "no_database"])
def test__populate__new(tmpdir, sql_dialect):
    pass
