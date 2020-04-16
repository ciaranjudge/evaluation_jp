from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.models import (
    EvaluationModel,
    PopulationSliceGenerator,
    TreatmentPeriodGenerator,
)


def test__EvaluationModel(
    fixture__population_slice_generator,
    fixture__treatment_period_generator,
    fixture__setup_steps_by_date,
):
    results = EvaluationModel(
        population_slice_generator=fixture__population_slice_generator,
        treatment_period_generator=fixture__treatment_period_generator,
    )
    assert (
        results.population_slice_generator.setup_steps_by_date
        == fixture__setup_steps_by_date
    )


def test__EvaluationModel__add_slices(fixture__population_slice_generator,):
    evaluation_model = EvaluationModel(
        population_slice_generator=fixture__population_slice_generator,
    )
    evaluation_model.add_population_slices()
    results = evaluation_model.population_slices[
        pd.Timestamp("2016-07-01 00:00:00", freq="QS-JAN")
    ]
    assert results.data.shape == (90, 5,)


def test__EvaluationModel__add_periods(
    fixture__population_slice_generator, fixture__treatment_period_generator
):
    evaluation_model = EvaluationModel(
        population_slice_generator=fixture__population_slice_generator,
        treatment_period_generator=fixture__treatment_period_generator,
    )
    evaluation_model.add_population_slices()
    evaluation_model.add_treatment_periods()
    results = evaluation_model.treatment_periods[
        (pd.Timestamp("2016-01-01", freq="QS-JAN"), pd.Period("2016-06", "M"))
    ]
    assert results.data.shape == (48, 5)


# # def test__PopulationSlice__add_periods(
# #     fixture__RandomPopulation,
# #     fixture__treatment_period_generator,
# #     fixture__setup_steps_by_date,
# # ):
# #     setup_steps = SetupSteps([fixture__RandomPopulation()])
# #     results = PopulationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
# #     results.add_treatment_periods(
# #         treatment_period_generator=fixture__treatment_period_generator
# #     )
# #     assert results.treatment_periods[pd.Period("2016-06", "M")].data.shape == (53, 5)
