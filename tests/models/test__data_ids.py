from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp import (
    SetupSteps,
    PopulationSliceID,
    PopulationSliceIDGenerator,
    TreatmentPeriodID,
    TreatmentPeriodIDGenerator,
)


def test__PopulationSliceID():
    results = PopulationSliceID(date=pd.Timestamp("2016-01-01"))
    assert results.as_flattened_dict() == {"date": pd.Timestamp("2016-01-01")}


def test__TreatmentPeriodID():
    results = TreatmentPeriodID(
        population_slice=PopulationSliceID(date=pd.Timestamp("2016-01-01")),
        time_period=pd.Period("2016-01"),
    )
    assert results.as_flattened_dict() == {
        "population_slice_date": pd.Timestamp("2016-01-01"),
        "time_period": pd.Period("2016-01"),
    }


def test__PopulationSliceIDGenerator():
    psid_generator = PopulationSliceIDGenerator(
        start=pd.Timestamp("2016-01-01"), end=pd.Timestamp("2017-12-31")
    )
    results = [psid for psid in psid_generator()]

    assert len(results) == 8
    assert results[7].as_flattened_dict() == {"date": pd.Timestamp("2017-10-01")}


def test__TreatmentPeriodIDGenerator():
    tpid_generator = TreatmentPeriodIDGenerator(end=pd.Timestamp("2017-12-31"))
    psid = PopulationSliceID(date=pd.Timestamp("2016-01-01"))
    results = [tpid for tpid in tpid_generator(psid)]

    assert len(results) == 24
    assert results[11].as_flattened_dict() == {
        "population_slice_date": pd.Timestamp("2016-01-01"),
        "time_period": pd.Period("2016-12"),
    }