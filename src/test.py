# %%
# Standard library
# import datetime as dt
# from dataclasses import dataclass, field
# from typing import List, Set, Dict, Tuple, Optional
from pathlib import Path

# External packages
import pandas as pd

# Local packages
# from src.data.import_helpers import get_clusters, get_vital_statistics, get_ists_claims
# from src.features.selection_helpers import (
#     restrict_by_age,
#     restrict_by_code,
#     restrict_by_duration,
#     on_les,
#     any_previous_jobpath,
#     jobpath_starts_this_period,
#     les_starts_this_period,
# )
# from src.features.metadata_helpers import lr_reporting_date
from src.evaluation_period import EvaluationPeriod
from src.evaluation_slice import EvaluationSlice

# from src.evaluation_class import EvaluationClass

# %%
p = pd.Period("2016-01-01", "Y")
es = EvaluationSlice(p.to_timestamp(how="S"), p.to_timestamp(how="E"), (".", "test1"))

# %%
es.dataframes["population"].head()


#%%
ep = EvaluationPeriod(
    start_date=es.start_date,
    end_date=es.start_date + pd.DateOffset(months=1),
    seed_population=es.dataframes["eligible"],
)


# %%
pm = es.start_date.to_period("M")
#%%
from src.features.selection_helpers import jobpath_hold_this_period

jp_hold = jobpath_hold_this_period(
    es.start_date, es.dataframes["population"]["ppsn"], how="both"
)


#%%
p.asfreq("M")


#%%
pd.period_range(p, p, freq="M")

#%%
eligibility_flags = [
    "age_eligible",
    "code_eligible",
    "duration_eligible",
    "not_on_les",
    "no_previous_jobpath",
    "eligible",
]
test_e = (
    es.dataframes["population"]
    .copy()
    .loc[es.dataframes["population"]["eligible"] == True]
    .drop(eligibility_flags, axis="columns")
)

#%%
