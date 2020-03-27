from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps
from evaluation_jp.models.periods import EvaluationPeriod, PeriodManager
from evaluation_jp.models.slices import EvaluationSlice, SliceManager


def test__EvaluationSlice(fixture_RandomPopulation, fixture_SampleFromPopulation):
    setup_steps = SetupSteps(
        [fixture_RandomPopulation(), fixture_SampleFromPopulation(0.1),]
    )
    results = EvaluationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
    assert results.data.shape == (10, 5)


def test__EvaluationSlice__add_periods(
    fixture_RandomPopulation, fixture_SampleFromPopulation, fixture_setup_steps_by_date
):
    setup_steps = SetupSteps([fixture_RandomPopulation()])
    results = EvaluationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
    results.add_periods(
        period_manager=PeriodManager(
            fixture_setup_steps_by_date, end=pd.Period("2017Q4")
        ),
        start=pd.Timestamp("2016-01-01"),
    )
    assert results.periods[pd.Period("2016-06", "M")].data.shape == (53, 5)


def test_SliceManager(fixture_setup_steps_by_date):
    slice_manager = SliceManager(
        setup_steps_by_date=fixture_setup_steps_by_date,
        start=pd.Timestamp("2016-01-01"),
        end=pd.Timestamp("2017-12-31"),
    )
    results = slice_manager.run()
    assert results[pd.Timestamp("2016-07-01 00:00:00", freq="QS-JAN")].data.shape == (
        90,
        5,
    )
    assert slice_manager.date_range.equals(
        pd.date_range(
            start=pd.Timestamp("2016-01-01"), end=pd.Timestamp("2017-12-31"), freq="QS"
        )
    )
