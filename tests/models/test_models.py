from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps
from evaluation_jp.models.periods import TreatmentPeriod, TreatmentPeriodGenerator
from evaluation_jp.models.slices import PopulationSlice, PopulationSliceGenerator
from evaluation_jp.models.models import EvaluationModel


def test__EvaluationModel(fixture__population_slice_generator, fixture__setup_steps_by_date):
    population_slice_generator = fixture__population_slice_generator

    results = EvaluationModel(name="test_model", population_slice_generator=population_slice_generator)
    assert results.population_slice_generator.setup_steps_by_date == fixture__setup_steps_by_date


def test__EvaluationModel__add_slices(fixture__population_slice_generator,):
    evaluation_model = EvaluationModel(
        name="test_model", population_slice_generator=fixture__population_slice_generator
    )
    evaluation_model.add_slices()
    results = evaluation_model.slices[
        pd.Timestamp("2016-07-01 00:00:00", freq="QS-JAN")
    ]
    assert results.data.shape == (90, 5,)


def test__EvaluationModel__add_periods(fixture__population_slice_generator, fixture__treatment_period_generator):
    evaluation_model = EvaluationModel(
        population_slice_generator=fixture__population_slice_generator, treatment_period_generator=fixture__treatment_period_generator
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
#     results = PopulationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
#     results.add_periods(
#         treatment_period_generator=TreatmentPeriodGenerator(fixture__setup_steps_by_date, end=pd.Period("2018Q4")),
#         start=pd.Timestamp("2016-01-01")
#     )
#     assert results.periods[pd.Period('2016-06', 'M')].data.shape == (53, 4)
