from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.features.setup_steps import SetupStep, SetupSteps

# TODO Tidy test classes somewhere else - NB they are shared with tests for slices, periods
# TODO test_NearestKeyDict()

@dataclass
class RandomPopulation(SetupStep):
    # Parameters
    data: pd.DataFrame = pd.DataFrame(
        np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
    )

    # Setup method
    def setup(self, data=None, date=None):
        if data is None:
            return self.data
        else:
            if date is not None:
                return data.loc[data["date"] == date]
            else:
                return data


@dataclass
class SampleFromPopulation(SetupStep):
    # Parameters
    frac: float

    # Setup method
    def setup(self, data, date=None):
        return data.sample(frac=self.frac, replace=True, random_state=1)


def test_SetupStep():
    rp = RandomPopulation()
    assert isinstance(rp, SetupStep)


def test_setup_steps():
    ss = SetupSteps([RandomPopulation(), SampleFromPopulation(0.1)])
    assert ss.setup().shape == (10, 4)


def test_setup_steps_with_data_and_date():
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

    ss = SetupSteps([RandomPopulation(), SampleFromPopulation(0.1)])
    assert ss.setup(data, pd.Timestamp("2016-04-01")).shape == (10, 5)
