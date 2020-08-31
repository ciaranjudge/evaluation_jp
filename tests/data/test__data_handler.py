from pathlib import Path
from IPython.display import display

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import sqlalchemy as sa

from evaluation_jp import (
    ColumnsByType,
    DataHandler,
    SQLDataHandler,
    PopulationSliceID,
    PopulationSliceDataParams,
    TreatmentPeriodID,
    TreatmentPeriodDataParams,
)
from evaluation_jp.data.sql_utils import *


# def test__SQLDataHandler__init__sqlite(tmpdir):
#     """Simple test to make sure everything gets initiated correctly
#     """

#     engine = sqlite_engine(tmpdir, "test_db")

#     data_handler = SQLDataHandler(engine=engine, model_schema="test_schema")

#     assert data_handler.engine.dialect.name == "sqlite"
#     assert data_handler.model_schema == "test_schema"


def test__SQLDataHandler__init__sqlserver(tmpdir):
    """Simple test to make sure everything gets initiated correctly
    """
    engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")

    data_handler = SQLDataHandler(engine=engine, model_schema="test_schema")

    assert data_handler.engine.dialect.name == "mssql"
    assert data_handler.model_schema == "test_schema"


# * No vs simple vs complex data_id, sqlserver vs sqlite, +/- use_index = 12 tests


population_slice_data_params = PopulationSliceDataParams(
    columns_by_type=ColumnsByType(
        data_columns_by_type={col: "int32" for col in list("ABCD")}
    ),
    setup_steps_by_date=None,
)
population_slice_id = PopulationSliceID(date=pd.Timestamp("2016-01-01"))


def test__DataHandler__write_new__PopulationSlice__with_data_id__no_index__sqlserver():
    """Write dataframe with *new* PopulationSlice data_id to SQL test_schema.population_slice
    """
    engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")
    data_params = population_slice_data_params
    data_id = population_slice_id
    with engine.connect() as con:
        # Setup
        data_handler = SQLDataHandler(engine=con, model_schema="test_schema")

        # Write
        data = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        data_handler.write(data, data_params, data_id, use_index=False)

        # Read back data from SQL following write
        df = pd.read_sql_table("population_slice", con=con, schema="test_schema")

    # Results should be same as original data passed to data_handler.write()
    results = df.loc[df["data_id_date"] == str(data_id.date.date())].drop(
        ["data_id_date"], axis="columns"
    )
    assert_frame_equal(data.describe(), results.describe())


def test__DataHandler__write_overwrite__PopulationSlice__with_data_id__no_index__sqlserver():
    """Write dataframe with *existing* PopulationSlice data_id to SQL test_schema.population_slice
    """
    engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")
    data_params = population_slice_data_params
    data_id = population_slice_id

    with engine.connect() as con:
        # Setup data_handler
        data_handler = SQLDataHandler(engine=con, model_schema="test_schema")

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

        # Read back data from SQL following second write
        df = pd.read_sql_table("population_slice", con=con, schema="test_schema")

    # Results should be same as (2nd time around) data passed to data_handler.write()
    results = df.loc[df["data_id_date"] == str(data_id.date.date())].drop(
        ["data_id_date"], axis="columns"
    )
    assert_frame_equal(data_2.describe(), results.describe())


def test__DataHandler__read_existing__PopulationSlice__with_data_id__no_index__sqlserver():
    """Read dataframe with *existing* PopulationSlice data_id from SQL test_schema.population_slice
    """
    engine = sqlserver_engine("CSKMA0400\\STATS1", "tempdb")
    data_params = population_slice_data_params
    data_id = population_slice_id
    with engine.connect() as con:
        # Setup
        data_handler = SQLDataHandler(engine=con, model_schema="test_schema")

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

# # def test__DataHandler__read(fixture__population_slice, tmpdir):

# #     data_path = f"sqlite:///{tmpdir}/test_DataHandler.db"
# #     population_slice = fixture__population_slice
# #     data_handler = DataHandler(data_path)
# #     data_handler.write(
# #         data_type=population_slice.class_name,
# #         data_id=population_slice.id,
# #         data=population_slice.data,
# #         index=False,
# #     )
# #     display(population_slice.data)
# #     results = data_handler.read(
# #         data_type=population_slice.class_name, data_id=population_slice.id,
# #     )
# #     display(results)
# #     assert results.shape == population_slice.data.shape


# # # def test__DataHandler__run__new(
# # #     fixture__setup_steps_by_date, fixture__population_slice_generator, tmpdir
# # # ):
# # #     data_path = f"sqlite:///{tmpdir}/test_DataHandler.db"
# # #     data_handler = DataHandler(data_path)
# # #     population_slice_generator = fixture__population_slice_generator
# # #     results = {
# # #         population_slice.id: population_slice
# # #         for population_slice in population_slice_generator.run(data_handler)
# # #     }
# # #     key = PopulationSliceID(date=pd.Timestamp("2016-07-01", freq="QS-JAN"))

# # #     assert results[key].data.shape == (90, 5,)


# # # def test__DataHandler__run__existing(
# # #     fixture__setup_steps_by_date, fixture__population_slice_generator, tmpdir
# # # ):
# #     data_path = f"sqlite:///{tmpdir}/test_DataHandler.db"
# #     data_handler = DataHandler(data_path)
# #     population_slice_generator = fixture__population_slice_generator
# #     # First iteration should run setup_steps then write to storage
# #     first_population_slices = {
# #         population_slice.id: population_slice
# #         for population_slice in population_slice_generator.run(data_handler)
# #     }
# #     # Second iteration should just read from storage
# #     second_population_slices = {
# #         population_slice.id: population_slice
# #         for population_slice in population_slice_generator.run(data_handler)
# #     }
# #     key = PopulationSliceID(date=pd.Timestamp("2016-07-01", freq="QS-JAN"))

# #     assert (
# #         len(first_population_slices[key].data)
# #         == len(second_population_slices[key].data)
# #     )

