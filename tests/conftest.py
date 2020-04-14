from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from evaluation_jp.features import SetupStep, SetupSteps
from evaluation_jp.models import PopulationSliceGenerator, TreatmentPeriodGenerator

np.random.seed(0)


@dataclass
class RandomPopulation(SetupStep):
    # Parameters
    data: pd.DataFrame = pd.DataFrame(
        np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
    )
    data["date"] = pd.date_range("2016-01-01", periods=8, freq="QS")[0]

    # Setup method
    def run(self, date=None, data=None):
        # Generate data if none has been passed in
        if data is None:
            return self.data
        else:
        # If data and date both passed in, look up that date in the data
            if date is not None:
                df = data.loc[data["date"] == date]
                if not df.empty:
                    return df
        # Default is just give back data that's been passed in
        return data


@pytest.fixture
def fixture__RandomPopulation():
    return RandomPopulation


@dataclass
class SampleFromPopulation(SetupStep):
    # Parameters
    frac: float

    # Setup method
    def run(self, date=None, data=None):
        return data.sample(frac=self.frac, replace=True, random_state=0)


@pytest.fixture
def fixture__SampleFromPopulation():
    return SampleFromPopulation


@pytest.fixture
def fixture__random_date_range_df():
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
def fixture__setup_steps_by_date():
    return {
        pd.Timestamp("2016-01-01"): SetupSteps(
            [RandomPopulation(), SampleFromPopulation(frac=0.9),]
        ),
        pd.Timestamp("2017-01-01"): SetupSteps(
            [RandomPopulation(), SampleFromPopulation(frac=0.8),]
        ),
    }


@pytest.fixture
def fixture__population_slice_generator(fixture__setup_steps_by_date):
    population_slice_generator = PopulationSliceGenerator(
        setup_steps_by_date=fixture__setup_steps_by_date,
        start=pd.Timestamp("2016-01-01"),
        end=pd.Timestamp("2017-12-31"),
    )
    return population_slice_generator


@pytest.fixture
def fixture__treatment_period_generator(fixture__setup_steps_by_date):
    treatment_period_generator = TreatmentPeriodGenerator(
        setup_steps_by_date=fixture__setup_steps_by_date, end=pd.Timestamp("2017-12-31")
    )
    return treatment_period_generator

# @pytest.fixture
# def fixture__starting_population(fixture__random_date_range_df):
#     data =


# import pandas as pd
# import pytest
# import sqlalchemy as sa

# @pytest.fixture(scope="module")
# engine = sa.create_engine(
#     "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
# )
# insp = sa.engine.reflection.Inspector.from_engine(engine)
