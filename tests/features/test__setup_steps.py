import numpy as np
import pandas as pd
import pytest

from evaluation_jp.features import (
    SetupStep,
    SetupSteps,
    LiveRegisterPopulation,
    AgeEligible,
    ClaimCodeEligible,
    ClaimDurationEligible,
)
from evaluation_jp.models import PopulationSliceID

# TODO test__NearestKeyDict()


def test__SetupStep(fixture__RandomPopulation):
    results = fixture__RandomPopulation()
    assert isinstance(results, SetupStep)


def test__SetupSteps(fixture__RandomPopulation, fixture__SampleFromPopulation):
    ss = SetupSteps([fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),])
    results = ss.run()
    assert results.shape == (10, 5)


# TODO Parameterize this properly
def test__SetupSteps_with_data_and_data_id(
    fixture__random_date_range_df,
    fixture__RandomPopulation,
    fixture__SampleFromPopulation,
):
    ss = SetupSteps([fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),])
    results = ss.run(
        data_id={"date": pd.Timestamp("2016-04-01")}, data=fixture__random_date_range_df
    )
    assert results.shape == (10, 5)


def test__LiveRegisterPopulation(fixture__population_slice):
    """Check that number of people on LR == official total per CSO, and correct columns generated
    """
    live_register_population = LiveRegisterPopulation(
        columns=[
            "lr_code",
            "clm_comm_date",
            "JobPath_Flag",
            "JobPathHold",
            "date_of_birth",
            "sex",
        ]
    )
    results = live_register_population.run(data_id=fixture__population_slice.id)
    assert results.shape == (321373, 7)


@pytest.fixture
def fixture__date_of_birth_df():
    date_range = pd.date_range(start="1940-01-01", end="1999-12-31", periods=30)
    date_of_birth_df = pd.DataFrame(pd.Series(date_range, name="date_of_birth"))
    return date_of_birth_df


def test__AgeEligible__lt_max(fixture__date_of_birth_df):
    lt_max = AgeEligible(date_of_birth_col="date_of_birth", max_eligible={"years": 60})
    results = lt_max.run(
        PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        data=fixture__date_of_birth_df,
    )
    # 22 out of 30 records have date_of_birth more than 60 years before date
    # Should be 2 columns in results df (date_of_birth and age_eligible)
    assert results.loc[results["age_eligible"]].shape == (22, 2)


def test__AgeEligible__ge_min(fixture__date_of_birth_df):
    ge_min = AgeEligible(date_of_birth_col="date_of_birth", min_eligible={"years": 25})
    results = ge_min.run(
        PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        data=fixture__date_of_birth_df,
    )
    # 22 out of 30 records have date_of_birth more than 60 years before date
    # Should be 2 columns in results df (date_of_birth and age_eligible)
    assert results.loc[results["age_eligible"]].shape == (25, 2)


def test__ClaimCodeEligible():
    data = pd.DataFrame({"lr_code": ["UA", "UB", "UC", "UD", "UE", "UA2", "UB2", "UC2", "UD2", "UE2"]})
    eligible = ClaimCodeEligible(code_col="lr_code", eligible_codes=["UA", "UB"])
    results = eligible.run(PopulationSliceID(date=pd.Timestamp("2016-01-01")), data=data)
    assert results.loc[results["claim_code_eligible"]].shape == (2, 2)


@pytest.fixture
def fixture__claim_duration_df():
    date_range = pd.date_range(start="2000-01-01", end="2015-12-31", periods=30)
    claim_duration_df = pd.DataFrame(pd.Series(date_range, name="clm_comm_date"))
    return claim_duration_df


def test__ClaimDurationEligible__lt_max(fixture__claim_duration_df):
    eligible = ClaimDurationEligible(claim_start_col="clm_comm_date", max_eligible={"years": 5})
    results = eligible.run(
        PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        data=fixture__claim_duration_df,
    )
    assert results.loc[results["claim_duration_eligible"]].shape == (10, 2)


def test__ClaimDurationEligible__ge_min(fixture__claim_duration_df):
    eligible = ClaimDurationEligible(claim_start_col="clm_comm_date", min_eligible={"years": 1})
    results = eligible.run(
        PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        data=fixture__claim_duration_df,
    )
    assert results.loc[results["claim_duration_eligible"]].shape == (28, 2)


