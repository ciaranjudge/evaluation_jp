from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features import SetupStep, SetupSteps
from evaluation_jp.models import TreatmentPeriod, TreatmentPeriodManager


def test__TreatmentPeriod(
    fixture__random_date_range_df,
    fixture__RandomPopulation,
    fixture__SampleFromPopulation,
):
    setup_steps = SetupSteps(
        [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
    )
    results = TreatmentPeriod(
        time_period=pd.Period("2016Q1"),
        setup_steps=setup_steps,
        population=fixture__random_date_range_df,
    )
    assert results.data.shape == (10, 5)


def test__TreatmentPeriodManager(
    fixture__treatment_period_manager, fixture__RandomPopulation
):
    treatment_period_manager = fixture__treatment_period_manager
    random_population = fixture__RandomPopulation()
    results = {
        treatment_period.time_period: treatment_period
        for treatment_period in treatment_period_manager.generate_treatment_periods(
            start=pd.Timestamp("2016-01-01"), population=random_population.run()
        )
    }
    assert results[pd.Period("2016-02", "M")].data.shape == (81, 5)
