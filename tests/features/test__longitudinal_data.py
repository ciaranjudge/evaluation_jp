# import pandas as pd
# import sqlalchemy as sa

# from evaluation_jp.features import quarterly_earnings, quarterly_sw_payments


# def test__quarterly_earnings():
#     test_ids = pd.Index(["6892436U", "5051366B", "6049367W", "5092934S", "8420262S",])
#     results = quarterly_earnings(ids=test_ids)
#     print(results)
#     assert set(results.columns) == set(
#         ["earnings_per_week_worked", "employment_earnings", "prsi_weeks"]
#     )
#     quarters = results.reset_index().get("quarter").unique()
#     assert set(quarters) == set(pd.period_range(start="2005Q1", end="2018Q4", freq="Q"))
#     assert len(quarters) <= len(results) <= (len(quarters) * len(test_ids))


# def test__quarterly_sw_payments():
#     test_ids = pd.Index(["6892436U", "5051366B", "6049367W", "5092934S", "8420262S",])
#     results = quarterly_sw_payments(ids=test_ids)
#     print(results)
#     assert set(results.columns) <= set(
#         [
#             "sw_education_training",
#             "sw_employment_supports",
#             "sw_family_children",
#             "sw_illness_disability",
#             "sw_other",
#             "sw_unemployment",
#         ]
#     )
#     quarters = results.reset_index().get("quarter").unique()
#     assert set(quarters) == set(pd.period_range(start="2010Q3", end="2019Q3", freq="Q"))


