import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps

# TODO test_NearestKeyDict()


def test_SetupStep(fixture_RandomPopulation):
    results = fixture_RandomPopulation()
    assert isinstance(results, SetupStep)


def test_SetupSteps(fixture_RandomPopulation, fixture_SampleFromPopulation):
    ss = SetupSteps([fixture_RandomPopulation(), fixture_SampleFromPopulation(0.1),])
    results = ss.run()
    assert results.shape == (10, 4)


# TODO Parameterize this properly
def test_SetupSteps_with_data_and_date(
    fixture_random_date_range_df, fixture_RandomPopulation, fixture_SampleFromPopulation
):
    ss = SetupSteps([fixture_RandomPopulation(), fixture_SampleFromPopulation(0.1),])
    results = ss.run(date=pd.Timestamp("2016-04-01"), data=fixture_random_date_range_df)
    assert results.shape == (10, 5)
