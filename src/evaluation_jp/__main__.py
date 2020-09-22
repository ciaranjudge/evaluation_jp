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


if __name__ == "__main__":
    evaluation_model = EvaluationModel(
        data_handler=SQLDataHandler(
            engine=sqlserver_engine("CSKMA0400\\STATS1", "evaluation_jp"),
            model_schema="report_2020",
        ),
        population_slice_data_params=PopulationSliceDataParams(
            setup_steps_by_date={
                pd.Timestamp("2016-01-01"): SetupSteps(
                    steps=[
                        LiveRegisterPopulation(
                            lookup_columns_by_type=ColumnsByType(
                                data_columns_by_type={
                                    "lr_code": "category",
                                    "clm_comm_date": "datetime64",
                                    "JobPath_Flag": "boolean",
                                    "JobPathHold": "boolean",
                                    "date_of_birth": "datetime64",
                                },
                                index_columns_by_type={"ppsn": str},
                            )
                        ),
                        AgeEligible(
                            date_of_birth_col="date_of_birth",
                            max_eligible={"years": 60},
                        ),
                        ClaimCodeEligible(
                            code_col="lr_code", eligible_codes=["UA", "UB"]
                        ),
                        ClaimDurationEligible(
                            claim_start_col="clm_comm_date", min_eligible={"years": 1}
                        ),
                        OnLES(assumed_episode_length={"years": 1}),
                        OnJobPath(
                            assumed_episode_length={"years": 1},
                            use_jobpath_operational_data=True,
                            use_ists_claim_data=False,
                        ),
                        EligiblePopulation(
                            eligibility_criteria={
                                "age_eligible": True,
                                "claim_code_eligible": True,
                                "claim_duration_eligible": True,
                                "on_les": False,
                                "on_jobpath": False,
                            }
                        ),
                    ]
                )
            },
            columns_by_type={
                "JobPath_Flag": "boolean",
                "clm_comm_date": "datetime64",
                "lr_code": "category",
                "date_of_birth": "datetime64",
                "age_eligible": "boolean",
                "claim_code_eligible": "boolean",
                "claim_duration_eligible": "boolean",
                "on_les": "boolean",
                "on_jobpath": "boolean",
                "eligible_population": "boolean",
            },
            index_colums_by_type={"ppsn": "string"},
        ),
        treatment_period_generator=TreatmentPeriodGenerator(
            end=pd.Period("2017-12"),
            freq="M",
            setup_steps_by_date={
                pd.Timestamp("2016-01-01"): SetupSteps(
                    steps=[
                        StartingPopulation(
                            eligible_from_pop_slice_col="eligible_population",
                            eligible_from_previous_period_col="evaluation_group",
                            starting_pop_label="C",
                        ),
                        LiveRegisterPopulation(
                            columns_by_type={
                                "lr_code": "category",
                                "clm_comm_date": "datetime64",
                                "JobPath_Flag": "boolean",
                                "JobPathHold": "boolean",
                            },
                            starting_pop_col="eligible_population",
                        ),
                        ClaimCodeEligible(
                            code_col="lr_code", eligible_codes=["UA", "UB"]
                        ),
                        ClaimDurationEligible(
                            claim_start_col="clm_comm_date", min_eligible={"years": 1}
                        ),
                        OnLES(assumed_episode_length={"years": 1}, how="start"),
                        OnLES(assumed_episode_length={"years": 1}, how="end"),
                        JobPathStartedEndedSamePeriod(),
                        EligiblePopulation(
                            eligibility_criteria={
                                "on_live_register": True,
                                "claim_code_eligible": True,
                                "claim_duration_eligible": True,
                                "on_les_at_start": False,
                                "on_les_at_end": False,
                                "JobPathHold": False,
                                "jobpath_started_and_ended": False,
                            }
                        ),
                        JobPathStarts(),
                        EvaluationGroup(
                            eligible_col="eligible_population",
                            treatment_col="jobpath_starts",
                            treatment_label="T",
                            control_label="C",
                        ),
                    ]
                )
            },
            columns_by_type={
                "starting_population": "boolean",
                "on_live_register": "boolean",
                "JobPathHold": "boolean",
                "lr_code": "category",
                "JobPath_Flag": "boolean",
                "clm_comm_date": "datetime64",
                "claim_code_eligible": "boolean",
                "claim_duration_eligible": "boolean",
                "on_les_at_start": "boolean",
                "on_les_at_end": "boolean",
                "jobpath_started_and_ended": "boolean",
                "eligible_population": "boolean",
                "jobpath_starts": "boolean",
                "evaluation_group": "category",
            },
            index_colums_by_type={"ppsn": "string"},
        ),
        # outcome_generator = OutcomeGenerator(
        #     outcome_start_date=pd.Timestamp("2016-02-01"),
        #     outcome_end_date=pd.Timestamp("2019-02-01"),
        # )
    )
