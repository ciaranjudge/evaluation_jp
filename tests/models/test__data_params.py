from contextlib import contextmanager

from pytest import raises
import numpy as np
import pandas as pd
from hypothesis import given
from hypothesis.extra.pandas import column, data_frames
import hypothesis.strategies as st

# from hypothesis.strategies import text


from evaluation_jp import (
    ColumnsByType,
    DuplicatedItemsError,
    PopulationSliceID,
    PopulationSliceDataParams,
    TreatmentPeriodID,
    TreatmentPeriodDataParams,
)


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))


def test__ColumnsByType__init_with_duplicated_items():
    """Error correctly thrown if initiated with duplicate columns
    """
    with raises(DuplicatedItemsError):
        columns_by_type = ColumnsByType(
            data_columns_by_type={
                "a": "bool",
                "b": "datetime64[ns]",
                "c": str,
                "d": int,
            },
            index_columns_by_type={"c": "bool", "d": "datetime64[ns]", "e": str,},
        )


def test__ColumnsByType__check_column_names__duplicates_should_fail():
    """Check_column_names throws error when passed duplicates
    """
    columns_by_type = ColumnsByType(data_columns_by_type={"a": "bool", "b": str})
    with raises(DuplicatedItemsError):
        columns_by_type.check_column_names(["a", "a", "b"])


def test__ColumnsByType__check_column_names__noduplicates_should_pass():
    """Check_column_names throws error when passed duplicates
    """
    columns_by_type = ColumnsByType(data_columns_by_type={"a": "bool", "b": str})
    with not_raises(DuplicatedItemsError):
        columns_by_type.check_column_names(["a", "b"])


def test__ColumnsByType__check_column_names__extra_columns_error():
    """Check_column_names throws error correctly naming extra columns
    """
    columns_by_type = ColumnsByType(data_columns_by_type={"a": "bool"})
    with raises(DuplicatedItemsError) as excinfo:
        columns_by_type.check_column_names(["a", "b"])
    assert "Unexpected columns: {'b'}" in str(excinfo.value)


def test__ColumnsByType__check_column_names__missing_columns_error():
    """Check_column_names throws error correctly naming extra columns
    """
    columns_by_type = ColumnsByType(data_columns_by_type={"a": "bool", "b": str})
    with raises(DuplicatedItemsError) as excinfo:
        columns_by_type.check_column_names(["a"])
    assert "Missing columns: {'b'}" in str(excinfo.value)


# //TODO set_datatypes works with nultindex


dfs = data_frames(
    columns=[
        column(name="boolean_col", dtype=int),
        column(name="datetime_col", dtype="datetime64[ns]"),
        column(name="string_col", elements=st.text(alphabet=st.characters())),
        column(
            name="int_col",
            dtype=float,
            elements=st.floats(
                min_value=-1_000_000, max_value=1_000_000, allow_nan=False,
            ),
        ),
        column(name="float_col", dtype=bool),
    ]
)


@given(dfs)
def test__ColumnsByType__set_datatypes(test_df):

    columns_by_type = ColumnsByType(
        data_columns_by_type={
            "boolean_col": "bool",
            "datetime_col": "datetime64[ns]",
            "string_col": str,
            "int_col": int,
            "float_col": float,
        }
    )
    if not len(test_df.index):
        # ignore empty datasets as dtype is impossible to infer from serialized
        return
    results = columns_by_type.set_datatypes(test_df)
    print(results.dtypes)
    assert list(str(dtype) for dtype in results.dtypes.values) == [
        "bool",
        "datetime64[ns]",
        "object",
        "int32",
        "float64",
    ]


def test__DataParams__

# @pytest.fixture
# def fixture__psdp_simple(fixture__RandomPopulation, fixture__SampleFromPopulation):
#     psdp_simple = PopulationSliceDataParams()
#     setup_steps = SetupSteps(
#         [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
#     )
#     population_slice = PopulationSlice(
#         id=PopulationSliceID(date=pd.Timestamp("2016-01-01")), setup_steps=setup_steps,
#     )
#     return psdp_simple


# # @pytest.fixture
# # def fixture__treatment_period(
# #     fixture__random_date_range_df,
# #     fixture__population_slice,
# #     fixture__SampleFromPopulation,
# # ):
# #     setup_steps = SetupSteps([fixture__RandomPopulation()])
# #     treatment_period = TreatmentPeriod(
# #         id=TreatmentPeriodID(
# #             population_slice_id=fixture__population_slice.id,
# #             time_period=pd.Period("2016Q1"),
# #         ),
# #         setup_steps=setup_steps,
# #         init_data=fixture__random_date_range_df,
# #     )
# #     return treatment_period


# # # @pytest.mark.parametrize(
# # #     "data_params, data_id, init_data, data_handler, expected",
# # #     [
# # #         pytest.param(
# # #             datetime(2001, 12, 12), datetime(2001, 12, 11), timedelta(1), id="forward"
# # #         ),
# # #         pytest.param(
# # #             datetime(2001, 12, 11), datetime(2001, 12, 12), timedelta(-1), id="backward"
# # #         ),
# # #     ],
# # # )


# # @pytest.mark.parametrize(
# #     "data_params, data_id, expected",
# #     [
# #         pytest.param(
# #             datetime(2001, 12, 12), datetime(2001, 12, 11), timedelta(1), id="forward"
# #         ),
# #     ],
# # )
# # def test__PopulationSliceDataParams(fixture__setup_steps_by_date):
# #     psid = PopulationSliceID(date=pd.Timestamp("2016-01-01"))
# #     psdp =

# #     # * setup_steps correctly returned for given date
# #     # * all_columns correctly returned
# #     # * test with multi-index!


# #     # * Don't need to test the setup_steps
# #     # * Do need to generate lots of different dataframes to check columns and type conversion
# #     assert True


# # # def test__TreatmentPeriodID():
# # #     results = TreatmentPeriodID(
# # #         population_slice=PopulationSliceID(date=pd.Timestamp("2016-01-01")),
# # #         time_period=pd.Period("2016-01"),
# # #     )
# # #     assert results.as_flattened_dict() == {
# # #         "population_slice_date": pd.Timestamp("2016-01-01"),
# # #         "time_period": pd.Period("2016-01"),
# # #     }


# # def test__TreatmentPeriod(
# #     fixture__population_slice,
# #     fixture__SampleFromPopulation,
# # ):
# #     setup_steps = SetupSteps([fixture__SampleFromPopulation(0.9),])
# #     results = TreatmentPeriod(
# #         id=TreatmentPeriodID(
# #             population_slice_id=fixture__population_slice.id,
# #             time_period=pd.Period("2016Q1"),
# #         ),
# #         setup_steps=setup_steps,
# #         init_data=fixture__population_slice.data,
# #     )
# #     assert results.data.shape == (9, 5)

