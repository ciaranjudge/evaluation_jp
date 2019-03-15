# %%
# Standard library
import datetime as dt
import calendar
from dataclasses import dataclass, field
from typing import List, Set, Dict, Tuple, Optional
from pathlib import Path

# External packages
import pandas as pd

# Local packages
from src.features.metadata_helpers import lr_reporting_date

# %%
@dataclass
class EvaluationModel:
    """
    One-liner

    Extended summary

    Parameters
    ----------
    num1 : int
        First number to add
    num2 : int
        Second number to add

    Attributes
    ----------

    Methods
    -------


    Examples
    --------

    """

    descriptive_name: str
    # df: pd.DataFrame


