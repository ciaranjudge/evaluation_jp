# %%
## Standard library
from typing import ClassVar, List, Set, Dict, Tuple, Optional

## External packages
import pandas as pd

## Local packages
from evaluation_jp.models.model import EvaluationModel
from evaluation_jp.features.selection_helpers import EligibilityChecker, EligibilityCheckManager

slice_freq = 'Q'
period_freq = 'M'


slice_evaluation_eligibility_checker = EligibilityCheckManager(
    checks_by_startdate={
        pd.Timestamp("2016-01-01"): EligibilityChecker(
            age={"max_age": pd.DateOffset(years=60)},
            on_lr={"when": "start"},
            code={"eligible_codes": ("UA", "UB")},
            duration={"min_duration": pd.DateOffset(years=1)},
            not_on_les={"episode_duration": pd.DateOffset(years=1)},
            not_on_jobpath={
                "episode_duration": pd.DateOffset(years=1),
                "use_jobpath_data": True,
                "use_ists_data": False,
                "combine": "either",
            },
        ),
    }
)
period_eligibility_checker = EligibilityCheckManager(
    checks_by_startdate={
        pd.Timestamp("2016-01-01"): EligibilityChecker(
            on_lr={"period_type": period_freq, "when": "end"},
            code={"eligible_codes": ("UA", "UB")},
            duration={"min_duration": pd.DateOffset(years=1)},
            # not_on_les={"episode_duration": pd.DateOffset(years=1)},
            not_on_jobpath={
                "episode_duration": pd.DateOffset(years=1),
                "use_jobpath_data": True,
                "use_ists_data": False,
                "combine": "either",
            },
            not_jobpath_hold={"period_type": period_freq, "how": "end"},
            # not_les_starts={"period_type": period_freq}
        ),
    }
)

em = EvaluationModel(
    ## Model
    start_date=pd.Timestamp("2016-01-01"),
    name_prefix="initial_report",
    rebuild_all=False,
    ## Outcomes
    outcome_start_date=pd.Timestamp("2016-02-01"),
    outcome_end_date=pd.Timestamp("2019-02-01"),
    ## Slices
    slice_name_prefix=None,
    slice_freq=slice_freq, 
    last_slice_date=pd.Timestamp("2016-12-31"), 
    slice_clustering_eligibility_checker=None,
    slice_evaluation_eligibility_checker=slice_evaluation_eligibility_checker, 
    ## Periods
    period_name_prefix=None,
    period_freq=period_freq,
    last_period_date=pd.Timestamp("2016-12-31"), 
    period_eligibility_checker=period_eligibility_checker,
)

