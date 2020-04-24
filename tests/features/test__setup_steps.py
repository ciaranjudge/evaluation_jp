import numpy as np
import pandas as pd

from evaluation_jp.features import SetupStep, SetupSteps, LiveRegister

# TODO test__NearestKeyDict()


def test__SetupStep(fixture__RandomPopulation):
    results = fixture__RandomPopulation()
    assert isinstance(results, SetupStep)


def test__SetupSteps(fixture__RandomPopulation, fixture__SampleFromPopulation):
    ss = SetupSteps([fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),])
    results = ss()
    assert results.shape == (10, 5)


# TODO Parameterize this properly
def test__SetupSteps_with_data_and_date(
    fixture__random_date_range_df, fixture__RandomPopulation, fixture__SampleFromPopulation
):
    ss = SetupSteps([fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),])
    results = ss(date=pd.Timestamp("2016-04-01"), data=fixture__random_date_range_df)
    assert results.shape == (10, 5)


def test__LiveRegister__init():
    """Check that a LiveRegister object is initiated correctly with column list
    """
    results = LiveRegister(columns=["lr_flag", "ppsn"])
    assert results.columns == ["lr_flag", "ppsn"]




