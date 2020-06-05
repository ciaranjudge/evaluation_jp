from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.models import (
    EvaluationModel,
    PopulationSliceID,
    PopulationSliceGenerator,
    TreatmentPeriodID,
    TreatmentPeriodGenerator,
)
from evaluation_jp.data import ModelDataHandler


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
        PopulationSliceID(pd.Timestamp("2016-07-01 00:00:00", freq="QS-JAN"))
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
        TreatmentPeriodID(
            population_slice_id=PopulationSliceID(
                pd.Timestamp("2016-01-01", freq="QS-JAN")
            ),
            time_period=pd.Period("2016-06", "M"),
        )
    ]
    assert results.data.shape == (48, 5)


def test__EvaluationModel__add_longitudinal_data(
    fixture__population_slice_setup_steps, tmpdir,
):

    population_slice_generator = PopulationSliceGenerator(
        setup_steps_by_date={
            pd.Timestamp("2016-01-01"): fixture__population_slice_setup_steps
        },
        start=pd.Timestamp("2016-01-01"),
        end=pd.Timestamp("2016-03-31"),
        index_col="ppsn",
    )

    data_handler = ModelDataHandler(
        database_type="sqlite",
        location=tmpdir,
        name="jobpath_evaluation",
    )

    evaluation_model = EvaluationModel(
        data_handler=data_handler,
        population_slice_generator=population_slice_generator,
    )
    evaluation_model.add_population_slices()

    evaluation_model.add_longitudinal_data()
    results = evaluation_model.longitudinal_data

    assert set(results.columns) == set(
        [
            "earnings_per_week_worked",
            "employment_earnings",
            "prsi_weeks",
            "sw_education_training",
            "sw_employment_supports",
            "sw_family_children",
            "sw_illness_disability",
            "sw_other",
            "sw_unemployment",
        ]
    )
    population = evaluation_model.total_eligible_population
    quarters = results.reset_index().get("quarter").unique()
    assert len(population) <= len(results) <= (len(quarters) * len(population))
