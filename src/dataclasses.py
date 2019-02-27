# %%
from dataclasses import dataclass
import pandas as pd

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

# %%

df = pd.DataFrame([[1,1,], [2,2]])
em = EvaluationModel("hello world", df)






# def main():
# '''creates a useful class and does something with it for our
# module.'''
# useful = UsefulClass()
# print(useful)
# if __name__ == "__main__":
# main()