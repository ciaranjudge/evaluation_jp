from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps
from evaluation_jp.models.periods import EvaluationPeriod, PeriodManager


def test__EvaluationPeriod(
    fixture__random_date_range_df, fixture__RandomPopulation, fixture__SampleFromPopulation
):
    setup_steps = SetupSteps(
        [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
    )
    results = EvaluationPeriod(
        setup_steps=setup_steps,
        period=pd.Period("2016Q1"),
        population=fixture__random_date_range_df,
    )
    assert results.data.shape == (10, 5)


def test__PeriodManager(fixture__period_manager, fixture__RandomPopulation):
    period_manager = fixture__period_manager
    random_population = fixture__RandomPopulation()
    results = period_manager.run(
        start=pd.Timestamp("2016-01-01"), population=random_population.run()
    )
    assert results[pd.Period('2016-02', 'M')].data.shape == (81, 5)


