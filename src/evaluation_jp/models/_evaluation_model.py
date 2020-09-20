# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional

# External packages
import pandas as pd
from tqdm import tqdm

# Local packages
from evaluation_jp import (
    DataHandler,
    SQLDataHandler,
    populate,
    PopulationSliceDataParams,
    PopulationSliceID,
    PopulationSliceIDGenerator,
    TreatmentPeriodDataParams,
    TreatmentPeriodID,
    TreatmentPeriodIDGenerator,
    sqlserver_engine,
)

# //TODO Read EvaluationModel parameters from yml file
@dataclass
class EvaluationModel:

    # Init parameters
    population_slice_data_params: PopulationSliceDataParams
    population_slice_id_generator: PopulationSliceIDGenerator
    treatment_period_data_params: TreatmentPeriodDataParams
    treatment_period_id_generator: TreatmentPeriodIDGenerator
    # outcome_generator: OutcomeGenerator = None
    data_handler: DataHandler = None

    # Attributes - set up post init
    # longitudinal_data: pd.DataFrame = None
    population_slices: dict = None
    treatment_periods: dict = None

    def add_population_slices(self):
        self.population_slices = {}
        for id in tqdm([id for id in self.population_slice_id_generator()]):
            self.population_slices[id] = populate(
                data_params=self.population_slice_data_params,
                data_id=id,
                data_handler=self.data_handler,
                rebuild=False,
            )

    @property
    def total_population(self):
        return set().union(
            *(
                population_slice.data.index
                for population_slice in self.population_slices.values()
            )
        )

    @property
    def total_eligible_population(self):
        return set().union(
            *(
                population_slice.data[
                    population_slice.data["eligible_population"] == True
                ].index
                for population_slice in self.population_slices.values()
            )
        )

    def add_longitudinal_data(self):
        # try:
        #     self.longitudinal_data = self.data_handler.read(
        #         data_type="longitudinal_data", index_col=["ppsn", "quarter"],
        #     )
        # except DataHandlerError:
        #     self.longitudinal_data = pd.merge(
        #         quarterly_earnings(self.total_eligible_population),
        #         quarterly_sw_payments(self.total_eligible_population),
        #         how="outer",
        #         left_index=True,
        #         right_index=True,
        #     )
        #     self.data_handler.write(
        #         data_type="longitudinal_data", data=self.longitudinal_data, index=True
        #     )
        pass

    def add_treatment_periods(self):
        self.treatment_periods = {}
        if self.population_slices is None:
            raise ValueError("Can't add treatment periods without population slices!")

        for slice_id, slice_data in tqdm(self.population_slices.items()):
            print(f"Adding treatment periods for population slice {slice_id}")
            # Data for first period is population slice data
            initial_data = slice_data
            for id in tqdm([id for id in self.treatment_period_id_generator(slice_id)]):
                self.treatment_periods[id] = populate(
                    data_params=self.treatment_period_data_params,
                    data_id=id,
                    initial_data=initial_data,
                    data_handler=self.data_handler,
                    rebuild=False,
                )
                # Data for each period after first is inital data for next one
                initial_data = self.treatment_periods[id]

       

    # //TODO Run weighting algorithm for periods

    # //TODO Back-propagations of weights through periods

    # //TODO Add outcomes with weighting
