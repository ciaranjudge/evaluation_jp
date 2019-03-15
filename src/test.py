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
from src.features.metadata_helpers import lr_reporting_date
from src.evaluation_period import EvaluationPeriod
from src.evaluation_slice import EvaluationSlice
# from src.evaluation_class import EvaluationClass
# %%
date = pd.Timestamp("2016-01-01")
p = Path() / "data" / "processed" / "test_model"

# %%
es = EvaluationSlice(date, p)


#%%
import inspect
inspect.getmembers(Path)


#%%
