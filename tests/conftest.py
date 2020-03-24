from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps

np.random.seed(0)


@dataclass
class RandomPopulation(SetupStep):
    # Parameters
    data: pd.DataFrame = pd.DataFrame(
        np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
    )

    # Setup method
    def run(self, date=None, data=None):
        if data is None:
            return self.data
        else:
            if date is not None:
                return data.loc[data["date"] == date]
            else:
                return data


@pytest.fixture
def fixture_RandomPopulation():
    return RandomPopulation


@dataclass
class SampleFromPopulation(SetupStep):
    # Parameters
    frac: float

    # Setup method
    def run(self, date=None, data=None):
        return data.sample(frac=self.frac, replace=True, random_state=0)


@pytest.fixture
def fixture_SampleFromPopulation():
    return SampleFromPopulation


@pytest.fixture
def fixture_random_date_range_df():
    # Random df for each date in date range then append
    dates = pd.date_range("2016-01-01", periods=8, freq="QS")
    data = None
    for date in dates:
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        df["date"] = date
        if data is None:
            data = df
        else:
            data = data.append(df, ignore_index=True)
    return data


@pytest.fixture
def fixture_setup_steps_by_date():
    return {
        pd.Timestamp("2016-01-01"): SetupSteps([SampleFromPopulation(frac=0.9)]),
        pd.Timestamp("2017-01-01"): SetupSteps([SampleFromPopulation(frac=0.8)]),
    }

# @pytest.fixture
# def fixture_starting_population(fixture_random_date_range_df):
#     data = 


# import pandas as pd
# import pytest
# import sqlalchemy as sa

# @pytest.fixture(scope="module")
# engine = sa.create_engine(
#     "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
# )
# insp = sa.engine.reflection.Inspector.from_engine(engine)
