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

    ### Parameters
    ## Inherited from EvaluationClass
    # period_covered: pd.Period
    logical_root: Tuple[str] = (".")  # Add default value here
    # name_prefix: str = None
    # seed_dataframe: pd.DataFrame = None
    # rebuild_all: bool = False

    ### Other attributes
    ## Inherited from EvaluationClass
    # logical_name: str = field(init=False)
    # dataframe_names: Tuple[str] = field(default=(), init=False)
    # dataframes: dict = field(default=None, init=False)  

