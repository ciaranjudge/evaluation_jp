# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd

# Local packages
from evaluation_jp.data import PersistenceManager
from evaluation_jp.models import PopulationSliceGenerator
from evaluation_jp.models import TreatmentPeriodGenerator


@dataclass
class EvaluationModel:

    # Init parameters
    persistence_manager: PersistenceManager = None
    population_slice_generator: PopulationSliceGenerator = None
    treatment_period_generator: TreatmentPeriodGenerator = None
    # outcome_manager: OutcomeManager = None

    # Attributes - set up post init
    data: pd.DataFrame = None
    population_slices: dict = None

    def add_population_slices(self):
        self.population_slices = {
            population_slice.date: population_slice
            for population_slice in self.population_slice_generator()
        }

    # TODO Create background and outcome data (self.data) for slices.population
    # def add_population_data():
    # population = set().union(*(s.data.index for s in slices.values()))

    def add_treatment_periods(self):
        for population_slice in self.population_slices.values():
            population_slice.add_treatment_periods(
                treatment_period_generator=self.treatment_period_generator
            )

    # TODO Run weighting algorithm for periods

    # TODO Back-propagations of weights through periods

    # TODO Add outcomes with weighting
