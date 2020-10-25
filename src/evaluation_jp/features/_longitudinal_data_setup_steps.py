# %%

import collections
from dataclasses import dataclass

import numpy as np
import pandas as pd
from evaluation_jp import SetupStep, get_earnings_contributions_data, get_sw_payments

# from evaluation_jp.features import SetupStep, SetupSteps


# //TODO LR status by quarter (as share)

# //TODO Add earnings (quarterised!) to longitudinal data
# Get all earnings data for everyone (if possible) - break into chunks if needed
#
# ids = evaluation_model.total_eligible_population


# def quarterly_earnings(ids):

#     earnings = get_earnings(
#         ids=ids, columns=["ppsn", "wies", "EARNINGS_AMT", "CON_YEAR",],
#     )

#     # df.groupby('grouping column').agg({'aggregating column': 'aggregating function'})
#     # Pivot table with [ppsn, year] as index, sum of €, sum of WIES, count of employments by year
#     # //TODO Add employment count, PRSI class, NACE codes
#     # //TODO Add 2019 employment data
#     earnings_by_id_year = (
#         earnings.groupby(["ppsn", "CON_YEAR"])
#         .agg({"EARNINGS_AMT": "sum", "wies": "sum"})
#         .fillna(0)
#     )

#     earnings_by_id_year.columns = ["employment_earnings", "prsi_weeks"]
#     earnings_by_id_year["prsi_weeks"] = earnings_by_id_year["prsi_weeks"].clip(upper=52)
#     earnings_by_id_year["earnings_per_week_worked"] = (
#         earnings_by_id_year["employment_earnings"] / earnings_by_id_year["prsi_weeks"]
#     )

#     # Brutally quarterize to index [ppsn, quarter]
#     # As well as first df, create 3 more, adding timedelta of 1 quarter to each record
#     # Then append dfs together
#     setup_q = earnings_by_id_year.copy()
#     setup_q[["employment_earnings", "prsi_weeks"]] = (
#         setup_q[["employment_earnings", "prsi_weeks"]] / 4
#     )
#     setup_q = setup_q.reset_index()

#     for i in range(1, 5):
#         quarter = pd.PeriodIndex(year=setup_q["CON_YEAR"], quarter=i, freq="Q")
#         d_df = setup_q.drop("CON_YEAR", axis="columns")
#         d_df["quarter"] = quarter
#         if i == 1:
#             earnings_by_id_quarter = d_df
#         else:
#             earnings_by_id_quarter = earnings_by_id_quarter.append(d_df)

#     return earnings_by_id_quarter.set_index(["ppsn", "quarter"])


# Identify key types of SW payment: Unemployment, ED&Train, I&D, other weekly, all other
# Pivot table with [ppsn, quarter] as index, sum of € by payment type as columns


def invert_dict(dict_of_lists):

    output_dict = {}
    for k, v in dict_of_lists.items():
        if isinstance(v, collections.Iterable) and not isinstance(v, str):
            for item in v:
                output_dict[item] = k
        else:
            output_dict[v] = k
    return output_dict


@dataclass
class QuarterlySWPayments(SetupStep):
    def run(self, data_id=None, data: pd.Series = None):
        columns = ["SCHEME_TYPE", "AMOUNT", "QTR"]
        payments = get_sw_payments(ids=data, columns=columns)

        code_dict = {
            "education_training": ["TLA", "SLO"],
            "family_children": [
                "CB",
                "BSCFA",
                "OPFP1",
                "CARER",
                "FIS",
                "BTWFD",
                "MAT",
                "PAT",
            ],
            "illness_disability": ["DA", "IB", "DCA", "INVP", "OIB", "DB"],
            "unemployment": ["JA", "JB"],
            "other": [
                "SWAER",
                "SWA",
                "HHB",
                "RCG",
                "WCG",
                "DSBLP",
                "DPA",
                "PRSI",
                "GPC",
                "REDUN",
                "HHB1",
                "WCP",
                "GPNC",
                "CARB",
                "WNCP",
                "SPC",
                "ABI",
                "SPNCP",
                "BPP",
                "OPFP2",
                "ECS",
                "DSBLG",
                "JOBSP",
                "HSB",
                "DRASC",
                "HAID",
                "DWB",
                "WPG",
                "FF",
                "RA",
                "WPGNC",
                "DRASI",
                "MCS",
                "MEDC",
                "INSOL",
                "DENB",
                "DWA",
                "DRASD",
                "APB",
                "MEDSB",
                "UnkSc",
                "BG",
                "OPTB",
                "DRASN",
            ],
            "employment_supports": [
                "BTW",
                "INTN",
                "FARMA",
                "PTJA",
                "PCB",
                "YESS",
                "WSS",
            ],
        }
        payments["function"] = payments["SCHEME_TYPE"].map(invert_dict(code_dict))
        payments["quarter"] = pd.to_datetime(payments["QTR"]).dt.to_period(freq="Q")
        payments = (
            payments.drop(["SCHEME_TYPE", "QTR"], axis="columns")
            .groupby(["ppsn", "quarter", "function"])
            .agg({"AMOUNT": "sum"})
            .reset_index()
            .rename({"AMOUNT": "value"}, axis="columns")
        )
        # payments.columns = [f"sw_{col}" for col in payments.columns.droplevel(0)]

        return payments


# %%
@dataclass
class EarningsByIDYearClass(SetupStep):
    columns: list
    start_year: int
    end_year: int

    def run(self, data_id=None, data: pd.Series = None):

        earnings = get_earnings_contributions_data(
            ppsns_to_lookup=data,
            lookup_columns=self.columns,
            start_year=self.start_year,
            end_year=self.end_year,
        )
        earnings = earnings.drop_duplicates()

        # *Simplify PRSI classes
        # Input table has detailed PRSI classes, but only need first letter for broad class
        earnings["contribution_class"] = earnings["contribution_class"].str[0]
        class_A = earnings["contribution_class"] == "A"
        class_S = earnings["contribution_class"] == "S"
        # Only need A vs S vs O[ther]
        earnings["contribution_class"] = np.where(
            class_A | class_S, earnings["contribution_class"], "O",
        )

        # *Remove outliers where earnings are too large compared to PRSI paid
        inverse_prsi_earnings = earnings["employee_prsi_amount"] / 0.04  # 4% PRSI rate
        raw_earnings = earnings["earnings_amount"]
        # Guess thresholds of €500/week, and earnings more than twice as high as expected
        earnings.loc[
            (raw_earnings > 500 * 52) & (raw_earnings > inverse_prsi_earnings * 2),
            "earnings_amount",
        ] = inverse_prsi_earnings

        # *Data cleaning based on CSO Morgan O'Donnell
        # Exclude less than 2 working weeks per year and less than 500/year incom
        # find outliers based on interquartile range and log of weekly earnings
        # earnings.loc[
        #     class_A & (earnings["wies"] >= 2) & (earnings["earnings_amount"] > 500),
        #     "weekly_log",
        # ] = np.log(earnings["earnings_per_week"])

        # third_quantile = np.quantile(earnings["weekly_log"], 0.75)
        # first_quantile = np.quantile(earnings["weekly_log"], 0.25)
        # log_IQR = third_quantile - first_quantile
        # outlier_cutoff = third_quantile + (3 * log_IQR)
        # lower_cutoff = first_quantile - (3 * log_IQR)
        # high_outliers = earnings[earnings["weekly_log"] >= outlier_cutoff]
        # low_outliers = earnings[earnings["weekly_log"] <= lower_cutoff]
        # earnings_no_outliers = earnings[earnings["weekly_log"] < outlier_cutoff]
        # earnings_no_outliers = earnings_no_outliers[
        #     earnings_no_outliers["weekly_log"] > lower_cutoff
        # ]

        # TODO Use history for outlier detection

        # * Transform to earnings_by_id_year_class
        # Assume an employment exists for each record where earnings > 0
        # Use this for counting employments per person in groupby below
        earnings["employments"] = np.where(earnings["earnings_amount"] > 1, 1, 0)
        earnings_by_id_year_class = (
            earnings.groupby(["ppsn", "year", "contribution_class"])
            .agg(
                {
                    "wies": "sum",
                    "earnings_amount": "sum",
                    "total_prsi_amount": "sum",
                    "employee_prsi_amount": "sum",
                    "employments": "sum",
                }
            )
            .fillna(0)
            .reset_index()
        )

        # *Restrict PRSI WIES to 52
        earnings_by_id_year_class["wies"] = earnings_by_id_year_class["wies"].clip(
            upper=52
        )

        # *Calculate earnings per week worked
        earnings_by_id_year_class["earnings_per_week"] = 0
        # Avoid dividing by zero
        earnings_by_id_year_class.loc[
            earnings_by_id_year_class["wies"] > 0, "earnings_per_week"
        ] = (
            earnings_by_id_year_class["earnings_amount"]
            / earnings_by_id_year_class["wies"]
        )

        return earnings_by_id_year_class


def annualise_sw_payments(sw_payments):
    pass


def features_from_earnings_sw_payments(
    eligible_population: pd.Series,
    all_earnings: pd.DataFrame,
    all_sw_payments: pd.DataFrame,
) -> pd.DataFrame:
    """Create features suitable for IPW (before SKL preprocessing) from earnings and payments data 
    For each person:
    - log(total_income) over 3 to 5 years before treatment period (or ranking)
    - for each year in that period, difference from average income over whole period
    - share of total income by type (earnings A, S, O; sw_payment broad functional categories)
    - for each year in overall period, difference from average income share by income share type
    """

    # *Restrict to eligible population
    eligible_population = (
        eligible_population.to_frame(name="ppsn")
        if isinstance(eligible_population, pd.Series)
        else eligible_population
    )
    earnings = pd.merge(
        all_earnings, eligible_population, how="inner", left_on="ppsn", right_on="ppsn"
    )
    sw_payments = pd.merge(
        all_sw_payments,
        eligible_population,
        how="inner",
        left_on="ppsn",
        right_on="ppsn",
    )

    # *Annualise payments data
    sw_payments["year"] = sw_payments["quarter"].dt.year


    # *Standardise columns for payments to ["ppsn", "year", "type", "value"] {"function": "type"}

    # *Standardise columns for earnings in same way {"earnings_amount": "value", "contribution_class": "type"}

    # *Pivot table to create total income series by person by year

    # *Broadcast total income by person by year to each person-year-type, and use to construct income shares

    # *Pivot income share column to make one column for each type by person-year

    # *Merge total income and income share dataframes

    # *

    # *Total income distribution
    # *Share of each income type
    # *Slope of income change

    # pd.Index(q.array).asfreq("Y")


# # %%


# data = pd.Series(list(evaluation_model.total_population), name="ppsn")

# ec = EarningsByIDYearClass(columns=[
#     "contribution_class",
#     "wies",
#     "earnings_amount",
#     "employee_prsi_amount",
#     "total_prsi_amount",
# ], start_year=2013, end_year=2019)

# earnings = ec.run(data=data)


# # %%


# # %%
# evaluation_model.sw_payments.head()

# # %%
# evaluation_model.earnings.head()

# %%
eligible_population = pd.Series(list(evaluation_model.total_eligible_population), name="ppsn")
eligible_population
# %%
