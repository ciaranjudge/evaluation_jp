# from dataclasses import dataclass

# import numpy as np
# import pandas as pd

# from evaluation_jp.features import SetupSteps
# from evaluation_jp.models import (
#     PopulationSliceID,
#     PopulationSlice,
#     PopulationSliceGenerator,
# )


# def test__PopulationSlice(fixture__RandomPopulation, fixture__SampleFromPopulation):
#     setup_steps = SetupSteps(
#         [fixture__RandomPopulation(), fixture__SampleFromPopulation(0.1),]
#     )
#     results = PopulationSlice(
#         id=PopulationSliceID(date=pd.Timestamp("2016-01-01")), setup_steps=setup_steps,
#     )
#     assert results.data.shape == (10, 5)


# def test__PopulationSliceGenerator(
#     fixture__setup_steps_by_date, fixture__population_slice_generator
# ):
#     population_slice_generator = fixture__population_slice_generator
#     results = {
#         population_slice.id: population_slice
#         for population_slice in population_slice_generator.run()
#     }
#     key = PopulationSliceID(date=pd.Timestamp("2016-07-01", freq="QS-JAN"))

#     assert results[key].data.shape == (90, 5,)
#     assert population_slice_generator.date_range.equals(
#         pd.date_range(
#             start=pd.Timestamp("2016-01-01"), end=pd.Timestamp("2017-12-31"), freq="QS"
#         )
#     )
