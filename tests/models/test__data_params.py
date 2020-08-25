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
    SetupStep,
    SetupSteps,
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

# #     # * setup_steps correctly returned for given date
# #     # * all_columns correctly returned

class DummyStep1(SetupStep):
    def run(self, data=None, data_id=None):
        return("Dummy step 1")

class DummyStep2(SetupStep):
    def run(self, data=None, data_id=None):
        return("Dummy step 2")

dummy_setup_steps_by_date = {
    pd.Timestamp("2016-01-01"): SetupSteps(
        [DummyStep1]
    ),
    pd.Timestamp("2017-01-01"): SetupSteps(
        [DummyStep2]
    ),
}


def test__PopulationSliceDataParams():

    ps_data_params = PopulationSliceDataParams(
        columns_by_type=ColumnsByType(
            data_columns_by_type={col: "int32" for col in list("ABCD")}
        ),
        setup_steps_by_date=dummy_setup_steps_by_date
    )

    ps_id_1= PopulationSliceID(date=pd.Timestamp("2016-01-01"))
    results_1 = ps_data_params.setup_steps(ps_id_1)
    assert results_1.run() == "Dummy step 1"

    ps_id_2= PopulationSliceID(date=pd.Timestamp("2018-01-01"))
    results_2 = ps_data_params.setup_steps(ps_id_2)
    assert results_2.run() == "Dummy step 2"

    assert ps_data_params.columns_by_type.check_column_names(list("ABCD"))


def test__TreatmentPeriodDataParams():

    tp_data_params = TreatmentPeriodDataParams(
        columns_by_type=ColumnsByType(
            data_columns_by_type={col: "int32" for col in list("ABCD")}
        ),
        setup_steps_by_date=dummy_setup_steps_by_date
    )

    tp_id_1= TreatmentPeriodID(
        population_slice=PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        time_period=pd.Period("2016-01"),
    )
    results_1 = tp_data_params.setup_steps(tp_id_1)
    assert results_1.run() == "Dummy step 1"

    tp_id_2= TreatmentPeriodID(
        population_slice=PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        time_period=pd.Period("2018-01"),
    )
    results_2 = tp_data_params.setup_steps(tp_id_2)
    assert results_2.run() == "Dummy step 2"

    assert tp_data_params.columns_by_type.check_column_names(list("ABCD"))

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





# #     # * test with multi-index!


# #     # * Don't need to test the setup_steps



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

