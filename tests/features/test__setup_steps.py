import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps

# TODO test__NearestKeyDict()


def test__SetupStep(fixture__RandomPopulation):
    results = fixture__RandomPopulation()
    assert isinstance(results, SetupStep)


def test__SetupSteps(fixture__RandomPopulation, fixture__SampleFromPopulation):
    ss = SetupSteps([fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),])
    results = ss.run()
    assert results.shape == (10, 5)


# TODO Parameterize this properly
def test__SetupSteps_with_data_and_date(
    fixture__random_date_range_df, fixture__RandomPopulation, fixture__SampleFromPopulation
):
    ss = SetupSteps([fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),])
    results = ss.run(date=pd.Timestamp("2016-04-01"), data=fixture__random_date_range_df)
    assert results.shape == (10, 5)
