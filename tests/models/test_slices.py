from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps
from evaluation_jp.models.periods import EvaluationPeriod, PeriodManager
from evaluation_jp.models.slices import EvaluationSlice


def test__EvaluationSlice(fixture_RandomPopulation, fixture_SampleFromPopulation):
    setup_steps = SetupSteps(
        [fixture_RandomPopulation(), fixture_SampleFromPopulation(0.1),]
    )
    results = EvaluationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
    assert results.data.shape == (10, 4)


def test__EvaluationSlice__add_periods(
    fixture_RandomPopulation, fixture_SampleFromPopulation, fixture_setup_steps_by_date
):
    setup_steps = SetupSteps(
        [fixture_RandomPopulation()]
    )
    results = EvaluationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
    results.add_periods(
        period_manager=PeriodManager(fixture_setup_steps_by_date, end=pd.Period("2018Q4")),
        start=pd.Timestamp("2016-01-01")
    )
    assert results.periods[pd.Period('2016-06', 'M')].data.shape == (53, 4)


# def test_SliceManager():
#     assert True
