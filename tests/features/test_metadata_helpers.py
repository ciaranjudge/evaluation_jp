import pandas as pd
from evaluation_jp.features.metadata_helpers import nearest_lr_date, lr_reporting_date

print(nearest_lr_date(pd.Timestamp("2016-01-06")))

# TODO Create list of input and output dates
# TODO test nearest_lr_date() with how == ["previous", "next", ""]
# TODO test lr_reporting_date()
# Use list comprehension to generate test outputs
# test_outputs = [nearest_lr_date(date) for date in test_inputs]

  