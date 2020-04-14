from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features import SetupSteps
from evaluation_jp.models import (
    TreatmentPeriod,
    TreatmentPeriodGenerator,
    PopulationSlice,
    PopulationSliceGenerator,
)


def test__PopulationSlice(fixture__RandomPopulation, fixture__SampleFromPopulation):
    setup_steps = SetupSteps(
        [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
    )
    results = PopulationSlice(date=pd.Timestamp("2016-01-01"), setup_steps=setup_steps,)
    assert results.data.shape == (10, 5)


def test__PopulationSlice__add_periods(
    fixture__RandomPopulation,
    fixture__treatment_period_generator,
    fixture__setup_steps_by_date,
):
    setup_steps = SetupSteps([fixture__RandomPopulation()])
    results = PopulationSlice(setup_steps=setup_steps, date=pd.Timestamp("2016-01-01"))
    results.add_treatment_periods(
        treatment_period_generator=fixture__treatment_period_generator
    )
    assert results.treatment_periods[pd.Period("2016-06", "M")].data.shape == (53, 5)


def test__PopulationSliceGenerator(
    fixture__setup_steps_by_date, fixture__population_slice_generator
):
    population_slice_generator = fixture__population_slice_generator
    results = {
        population_slice.date: population_slice
        for population_slice in population_slice_generator()
    }
    assert results[pd.Timestamp("2016-07-01", freq="QS-JAN")].data.shape == (90, 5,)
    assert population_slice_generator.date_range.equals(
        pd.date_range(
            start=pd.Timestamp("2016-01-01"), end=pd.Timestamp("2017-12-31"), freq="QS"
        )
    )
