from pathlib import Path
from IPython.display import display

import numpy as np
import pandas as pd
import sqlalchemy as sa

from evaluation_jp.data import ModelDataHandler, datetime_cols
from evaluation_jp.models import PopulationSlice, PopulationSliceID


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


def test__ModelDataHandler__init(tmpdir):
    """Simple test to make sure everything gets initiated correctly
    """
    results = ModelDataHandler(database_type="sqlite", location=tmpdir, name="test")
    assert results.engine.name == sa.create_engine(f"sqlite:///{tmpdir}/test_ModelDataHandler.db").name


def test__ModelDataHandler__write__new(fixture__population_slice, tmpdir):
    """Given a population_slice instance that's not saved, save it correctly.
    -- fixture__population_slice returns a population_slice with data.shape (10, 5).
    -- data_handler.write() saves this to table PopulationSlice, adding date column.
    -- Reading the PopulationSlice table back directly from database, dropping date...
    -- ...should return a df with the same shape as population_slice.data
    """
    data_path = f"sqlite:///{tmpdir}/test_ModelDataHandler.db"
    population_slice = fixture__population_slice
    data_handler = ModelDataHandler(data_path)
    data_handler.write(
        data_type=population_slice.class_name,
        data_id=population_slice.id,
        data=population_slice.data,
    )
    engine = sa.create_engine(data_path)
    df = pd.read_sql("PopulationSlice", con=engine)
    results = df.loc[df["data_id_date"] == str(population_slice.id.date.date())].drop(
        ["data_id_date"], axis="columns"
    )
    display(results)
    display(population_slice.data)
    assert results.shape == population_slice.data.shape


def test__ModelDataHandler__write__overwrite(fixture__population_slice, tmpdir):
    """Given a population_slice that overwrites an old one, save it correctly.
    """
    data_path = f"sqlite:///{tmpdir}/test_ModelDataHandler.db"
    population_slice = fixture__population_slice
    data_handler = ModelDataHandler(data_path)
    # Write first version of data
    data_handler.write(
        data_type=population_slice.class_name,
        data_id=population_slice.id,
        data=population_slice.data,
    )
    # Change the data - it's ok for now to assume same number of columns!
    population_slice.data = pd.DataFrame(
        np.random.randint(100, 200, size=(20, 4)), columns=list("ABCD")
    )
    population_slice.data["date"] = pd.date_range("2016-01-01", periods=8, freq="QS")[0]

    # Now write the changed data to database
    data_handler.write(
        data_type=population_slice.class_name,
        data_id=population_slice.id,
        data=population_slice.data,
    )

    engine = sa.create_engine(data_path)
    df = pd.read_sql("PopulationSlice", con=engine)
    display(df)
    display(df.info())
    results = df.loc[df["data_id_date"] == population_slice.id.date].drop(
        ["data_id_date"], axis="columns"
    )

    assert results.shape == population_slice.data.shape


def test__ModelDataHandler__read(fixture__population_slice, tmpdir):

    data_path = f"sqlite:///{tmpdir}/test_ModelDataHandler.db"
    population_slice = fixture__population_slice
    data_handler = ModelDataHandler(data_path)
    data_handler.write(
        data_type=population_slice.class_name,
        data_id=population_slice.id,
        data=population_slice.data,
        index=False,
    )
    display(population_slice.data)
    results = data_handler.read(
        data_type=population_slice.class_name, data_id=population_slice.id,
    )
    display(results)
    assert results.shape == population_slice.data.shape


def test__ModelDataHandler__run__new(
    fixture__setup_steps_by_date, fixture__population_slice_generator, tmpdir
):
    data_path = f"sqlite:///{tmpdir}/test_ModelDataHandler.db"
    data_handler = ModelDataHandler(data_path)
    population_slice_generator = fixture__population_slice_generator
    results = {
        population_slice.id: population_slice
        for population_slice in population_slice_generator.run(data_handler)
    }
    key = PopulationSliceID(date=pd.Timestamp("2016-07-01", freq="QS-JAN"))

    assert results[key].data.shape == (90, 5,)


def test__ModelDataHandler__run__existing(
    fixture__setup_steps_by_date, fixture__population_slice_generator, tmpdir
):
    data_path = f"sqlite:///{tmpdir}/test_ModelDataHandler.db"
    data_handler = ModelDataHandler(data_path)
    population_slice_generator = fixture__population_slice_generator
    # First iteration should run setup_steps then write to storage
    first_population_slices = {
        population_slice.id: population_slice
        for population_slice in population_slice_generator.run(data_handler)
    }
    # Second iteration should just read from storage
    second_population_slices = {
        population_slice.id: population_slice
        for population_slice in population_slice_generator.run(data_handler)
    }
    key = PopulationSliceID(date=pd.Timestamp("2016-07-01", freq="QS-JAN"))

    assert (
        len(first_population_slices[key].data)
        == len(second_population_slices[key].data)
    )


# # def test__ModelDataHandler__run__existing_rebuild(
# #     fixture__population_slice,
# # ):
# #     """Given a constructor for an *existing* population_slice, run constructor and save it
# #     -- Correctly determine that population_slice exists
# #     -- Delegate to constructor...
# #     -- ... and save created dataframe in right place.
# #     """
# #     pass


# # def test__ModelDataHandler__run__population_slice__existing_norebuild(
# #     fixture__population_slice,
# # ):
# #     """
# #     """
# #     pass
