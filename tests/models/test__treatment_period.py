from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features import SetupStep, SetupSteps
from evaluation_jp.models import (
    TreatmentPeriod,
    TreatmentPeriodGenerator,
    PopulationSlice,
)


def test__TreatmentPeriod(
    fixture__random_date_range_df,
    fixture__RandomPopulation,
    fixture__SampleFromPopulation,
):
    setup_steps = SetupSteps(
        [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
    )
    results = TreatmentPeriod(
        population_slice_date=pd.Timestamp("2016-01-01"),
        time_period=pd.Period("2016Q1"),
        setup_steps=setup_steps,
        init_data=fixture__random_date_range_df,
    )
    assert results.data.shape == (10, 5)


def test__TreatmentPeriodGenerator(
    fixture__treatment_period_generator, fixture__RandomPopulation
):
    treatment_period_generator = fixture__treatment_period_generator
    population_slice = PopulationSlice(
        date=pd.Timestamp("2016-01-01"),
        setup_steps=SetupSteps([fixture__RandomPopulation()]),
    )
    results = {
        treatment_period.time_period: treatment_period
        for treatment_period in treatment_period_generator(population_slice)
    }
    assert results[pd.Period("2016-02", "M")].data.shape == (81, 5)
