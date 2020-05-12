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
    EligiblePopulation,
)
from evaluation_jp.data import ModelDataHandler


evaluation_model = EvaluationModel(
    data_handler=ModelDataHandler(
        database_type="sqlite",
        location="//cskma0294/f/Evaluations/JobPath",
        name="test",
    ),
    population_slice_generator=PopulationSliceGenerator(
        start=pd.Timestamp("2016-01-01"),
        end=pd.Timestamp("2017-12-31"),
        freq="QS",
        setup_steps_by_date={
            pd.Timestamp("2016-01-01"): SetupSteps(
                steps=[
                    LiveRegisterPopulation(
                        columns=[
                            "lr_code",
                            "clm_comm_date",
                            "JobPath_Flag",
                            "JobPathHold",
                            "date_of_birth",
                        ]
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
                        starting_pop_col="evaluation_group", starting_pop_val="C"
                    ),
                    LiveRegisterPopulation(
                        columns=["lr_code", "clm_comm_date", "JobPath_Flag", "JobPathHold",],
                        starting_pop_col="eligible_population",
                    ),
                    ClaimCodeEligible(code_col="lr_code", eligible_codes=["UA", "UB"]),
                    ClaimDurationEligible(
                        claim_start_col="clm_comm_date", min_eligible={"years": 1}
                    ),
                    OnLES(assumed_episode_length={"years": 1}, how="start"),
                    OnLES(assumed_episode_length={"years": 1}, how="end"),
                    EligiblePopulation(
                        eligibility_criteria={
                            "claim_code_eligible": True,
                            "claim_duration_eligible": True,
                            "on_les_at_start": False,
                            "on_les_at_end": False,
                            "JobPathHold": False,
                        }
                    ),
                    # *"eligible_population" -> JobPath starts -> "evaluation_group" = "T" else "C"
                    # ?What to do about JP starts that finish within same period?
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
