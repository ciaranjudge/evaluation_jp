# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data import ModelDataHandler
from evaluation_jp.models import PopulationSliceGenerator, TreatmentPeriodGenerator


@dataclass
class EvaluationModel:

    # Init parameters
    data_handler: ModelDataHandler = None
    population_slice_generator: PopulationSliceGenerator = None
    treatment_period_generator: TreatmentPeriodGenerator = None
    # outcome_manager: OutcomeManager = None

    # Attributes - set up post init
    data: pd.DataFrame = None
    population_slices: dict = None
    treatment_periods: dict = None

    def add_population_slices(self):
        self.population_slices = {
            population_slice.id: population_slice
            for population_slice in self.population_slice_generator.run(self.data_handler)
        }

    # TODO Create background and outcome data (self.data) for slices.population
    # def add_population_data():
    # population = set().union(*(s.data.index for s in slices.values()))

    def add_treatment_periods(self):
        self.treatment_periods = {}
        for population_slice in self.population_slices.values():
            for t_period in self.treatment_period_generator.run(
                population_slice, self.data_handler
            ):
                self.treatment_periods[t_period.id] = t_period

    # TODO Run weighting algorithm for periods

    # TODO Back-propagations of weights through periods

    # TODO Add outcomes with weighting
