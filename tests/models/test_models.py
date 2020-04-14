from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps
from evaluation_jp.models.periods import EvaluationPeriod, PeriodManager
from evaluation_jp.models.slices import EvaluationSlice, SliceManager
from evaluation_jp.models.models import EvaluationModel


def test__EvaluationModel(fixture__slice_manager, fixture__setup_steps_by_date):
    slice_manager = fixture__slice_manager

    results = EvaluationModel(name="test_model", slice_manager=slice_manager)
    assert results.slice_manager.setup_steps_by_date == fixture__setup_steps_by_date


def test__EvaluationModel__add_slices(fixture__slice_manager,):
    evaluation_model = EvaluationModel(
        name="test_model", slice_manager=fixture__slice_manager
    )
    evaluation_model.add_slices()
    results = evaluation_model.slices[
        pd.Timestamp("2016-07-01 00:00:00", freq="QS-JAN")
    ]
    assert results.data.shape == (90, 5,)


def test__EvaluationModel__add_periods(fixture__slice_manager, fixture__period_manager):
    evaluation_model = EvaluationModel(
        slice_manager=fixture__slice_manager, period_manager=fixture__period_manager
    )
    evaluation_model.add_slices()
    evaluation_model.add_periods()
    results = evaluation_model.slices[
        pd.Timestamp("2016-01-01 00:00:00", freq="QS-JAN")
    ].periods[pd.Period("2016-06", "M")]
    assert results.data.shape == (48, 5)


# def test___EvaluationModel__add_periods(
#     fixture__RandomPopulation, fixture__SampleFromPopulation, fixture__setup_steps_by_date
# ):
#     setup_steps = SetupSteps(
#         [fixture__RandomPopulation()]
#     )
#     results = EvaluationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
#     results.add_periods(
#         period_manager=PeriodManager(fixture__setup_steps_by_date, end=pd.Period("2018Q4")),
#         start=pd.Timestamp("2016-01-01")
#     )
#     assert results.periods[pd.Period('2016-06', 'M')].data.shape == (53, 4)
