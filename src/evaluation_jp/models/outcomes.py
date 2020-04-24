# %%
# Standard library
import collections
from dataclasses import dataclass, field, InitVar
from typing import List, Set, MutableMapping

# External packages
import pandas as pd

# Local packages
from evaluation_jp.models.model_helpers import NearestKeyDict
from evaluation_jp.data.import_helpers import get_ists_claims


@dataclass
class OutcomeParams:
    end: pd.Period
    freq: str = "QS"