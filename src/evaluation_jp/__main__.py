# %%
## Standard library


## External packages
import pandas as pd

## Local packages
from evaluation_jp.models import (
    EvaluationModel,
    PopulationSliceGenerator,
    TreatmentPeriodGenerator,
)
from evaluation_jp.features import (
    SetupSteps,
    StartingPopulation,
    LiveRegisterPopulation,
    AgeEligible,
    ClaimCodeEligible,
    ClaimDurationEligible,
    OnLES,
    OnJobPath,
    JobPathStartedEndedSamePeriod,
    EligiblePopulation,
    JobPathStarts,
    EvaluationGroup,
)
from evaluation_jp.data import ModelDataHandler


evaluation_model = EvaluationModel(
    data_handler=ModelDataHandler(
        database_type="sqlite",
        location="//cskma0294/f/Evaluations/JobPath",
        name="jobpath_evaluation",
    ),
    population_slice_generator=PopulationSliceGenerator(
        start=pd.Timestamp("2016-01-01"),
        end=pd.Timestamp("2017-12-31"),
        freq="QS",
        setup_steps_by_date={
            pd.Timestamp("2016-01-01"): SetupSteps(
                steps=[
                    LiveRegisterPopulation(
                        columns_by_type={
                            "lr_code": "category",
                            "clm_comm_date": "datetime64",
                            "JobPath_Flag": "boolean",
                            "date_of_birth": "datetime64",
                        }
                    ),
                    AgeEligible(
                        date_of_birth_col="date_of_birth", max_eligible={"years": 60}
                    ),
                    ClaimCodeEligible(code_col="lr_code", eligible_codes=["UA", "UB"]),
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
                    ClaimCodeEligible(code_col="lr_code", eligible_codes=["UA", "UB"]),
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
    ),
    # outcome_generator = OutcomeGenerator(
    #     outcome_start_date=pd.Timestamp("2016-02-01"),
    #     outcome_end_date=pd.Timestamp("2019-02-01"),
    # )
)


evaluation_model.add_population_slices()
evaluation_model.add_treatment_periods()
