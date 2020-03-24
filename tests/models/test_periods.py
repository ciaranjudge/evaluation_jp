from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps
from evaluation_jp.models.periods import EvaluationPeriod, PeriodManager


def test_EvaluationPeriod(
    fixture_random_date_range_df, fixture_RandomPopulation, fixture_SampleFromPopulation
):
    setup_steps = SetupSteps(
        [fixture_RandomPopulation(), fixture_SampleFromPopulation(0.1),]
    )
    results = EvaluationPeriod(
        setup_steps=setup_steps,
        period=pd.Period("2016Q1"),
        population=fixture_random_date_range_df,
    )
    assert results.data.shape == (10, 5)


def test_PeriodManager(fixture_setup_steps_by_date, fixture_RandomPopulation):
    period_manager = PeriodManager(
        setup_steps_by_date=fixture_setup_steps_by_date, end=pd.Timestamp("2018-12-31")
    )
    random_population = fixture_RandomPopulation()
    results = period_manager.run(
        start=pd.Timestamp("2016-01-01"), population=random_population.run()
    )
    print(results)
    assert results[pd.Period('2016-06', 'M')].data.shape == (53, 4)


