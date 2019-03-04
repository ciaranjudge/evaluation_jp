
# %%
import datetime as dt
import calendar

from dataclasses import dataclass, field
import pandas as pd

from src.data.import_helpers import get_clusters, get_vital_statistics
from src.features.metadata_helpers import lr_reporting_date

# %%
@dataclass
class TreatmentPeriod:
    pass


@dataclass
class EvaluationSlice:
    """
    Manage candidate treatment data
    Initialise with or without everything being already set up!

    Setup:
        Given eligibility criteria and a population, create eligible-for-treatment dataframe
        Add earnings, employment status, payments, statuses...
        Create TreatmentPeriod list

    """

    date: pd.Timestamp
    # clean_df_path: Path
    # data: pd.DataFrame = field(init=False)
    



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

    title: str
    df: pd.DataFrame

    # data\interim\jobpath.db


# %%

df = pd.DataFrame([[1, 1], [2, 2]])
em = EvaluationModel("hello world", df)


# def main():
# '''creates a useful class and does something with it for our
# module.'''
# useful = UsefulClass()
# print(useful)
# if __name__ == "__main__":
# main()

# %%
help(lr_reporting_date)

#%%
