# from dataclasses import dataclass

# import numpy as np
# import pandas as pd

# from evaluation_jp.features import SetupStep, SetupSteps
# from evaluation_jp.models import (
#     TreatmentPeriodID,
#     TreatmentPeriod,
#     TreatmentPeriodGenerator,
#     PopulationSlice,
# )

# def test__PopulationSliceID(fixture__RandomPopulation, fixture__SampleFromPopulation):
#     setup_steps = SetupSteps(
#         [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
#     )
#     results = PopulationSlice(
#         id=PopulationSliceID(date=pd.Timestamp("2016-01-01")), setup_steps=setup_steps,
#     )
#     assert results.data.shape == (10, 5)

# def test__TreatmentPeriod(
#     fixture__population_slice,
#     fixture__SampleFromPopulation,
# ):
#     setup_steps = SetupSteps([fixture__SampleFromPopulation(0.9),])
#     results = TreatmentPeriod(
#         id=TreatmentPeriodID(
#             population_slice_id=fixture__population_slice.id,
#             time_period=pd.Period("2016Q1"),
#         ),
#         setup_steps=setup_steps,
#         init_data=fixture__population_slice.data,
#     )
#     assert results.data.shape == (9, 5)


# def test__TreatmentPeriodGenerator(
#     fixture__treatment_period_generator, fixture__population_slice
# ):
#     treatment_period_generator = fixture__treatment_period_generator
#     population_slice = fixture__population_slice
#     results = {
#         treatment_period.id: treatment_period
#         for treatment_period in treatment_period_generator.run(population_slice)
#     }
#     assert results[
#         TreatmentPeriodID(
#             population_slice_id=population_slice.id, time_period=pd.Period("2016-02")
#         )
#     ].data.shape == (8, 5)
