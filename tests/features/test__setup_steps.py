import numpy as np
import pandas as pd

from evaluation_jp.features import SetupStep, SetupSteps, LiveRegisterPopulation

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
