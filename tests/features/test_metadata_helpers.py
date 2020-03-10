import numpy as np
import pandas as pd
from evaluation_jp.features.metadata_helpers import nearest_lr_date, lr_reporting_date

print(nearest_lr_date(pd.Timestamp("2016-01-06")))

# TODO test nearest_lr_date() with how == ["previous", "next", ""]

# From answer #25 here: https://stackoverflow.com/questions/50559078/generating-random-dates-within-a-given-range-in-pandas
def random_dates(start, end, n=10):
    np.random.seed(0)
    start_u = start.value // 10 ** 9
    end_u = end.value // 10 ** 9

    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit="s")


def test_lr_reporting_date():
    start = pd.to_datetime("2012-01-01")
    end = pd.to_datetime("2020-03-01")
    test_inputs = random_dates(start, end)
    test_outputs = [lr_reporting_date(date) for date in test_inputs]
    # Manually generated!
    expected_datelist = [
        "2018-08-31",
        "2016-02-26",
        "2015-10-30",
        "2016-04-29",
        "2017-06-30",
        "2018-03-30",
        "2018-02-23",
        "2016-09-30",
        "2018-08-31",
        "2014-11-28"
    ]
    expected_outputs = [pd.Timestamp(date) for date in expected_datelist]
    assert test_outputs == expected_outputs

