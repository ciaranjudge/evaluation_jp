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
    LiveRegisterPopulation,
    AgeEligible,
    ClaimCodeEligible,
    ClaimDurationEligible,
    OnLES,
    OnJobPath,
    OnLiveRegister,
)
from evaluation_jp.data import ModelDataHandler


em = EvaluationModel(
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
                    # EvaluationEligible(
                    #     eligibility_criteria={
                    #         "age_eligible": True,
                    #         "claim_code_eligible": True,
                    #         "claim_duration_eligible": True,
                    #         "on_les": False,
                    #         "on_jobpath": False,
                    #     }
                    # ),
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
                    # * Get LR data again!
                    # on_lr={"period_type": period_freq, "when": "end"},
                    # code={"eligible_codes": ("UA", "UB")},
                    # duration={"min_duration": pd.DateOffset(years=1)},
                    # # not_on_les={"episode_duration": pd.DateOffset(years=1)},
                    # not_on_jobpath={
                    #     "episode_duration": pd.DateOffset(years=1),
                    #     "use_jobpath_data": True,
                    #     "use_ists_data": False,
                    #     "combine": "either",
                    # },
                    # not_jobpath_hold={"period_type": period_freq, "how": "end"},
                    # not_les_starts={"period_type": period_freq}
                ]
            )
        },
    ),
    # outcome_generator = OutcomeGenerator(
    # outcome_start_date=pd.Timestamp("2016-02-01"),
    # outcome_end_date=pd.Timestamp("2019-02-01"),
    # )
)
