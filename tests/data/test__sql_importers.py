import pandas as pd

# # Without column names
# ppsns = pd.DataFrame(["9190014F", "6754687B"], columns=["ppsn"])
# df = get_edw_customer_details(ppsns, reference_date=pd.Timestamp("2016-01-01"))

# # With column names
# df = get_edw_customer_details(
#     ppsns,
#     reference_date=pd.Timestamp("2016-01-01"),
#     lookup_columns=["pps_no", "nationality_country_name"],
# )

