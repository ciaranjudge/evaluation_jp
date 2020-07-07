from dataclasses import dataclass

import numpy as np
import pandas as pd

from evaluation_jp.data import get_earnings, get_sw_payments, get_ists_claims
# from evaluation_jp.features import SetupStep, SetupSteps


# //TODO LR status by quarter (as share)

# //TODO Add earnings (quarterised!) to longitudinal data
# Get all earnings data for everyone (if possible) - break into chunks if needed
#
# ids = evaluation_model.total_eligible_population


def quarterly_earnings(ids):

    earnings = get_earnings(
        ids=ids, columns=["ppsn", "NO_OF_CONS", "EARNINGS_AMT", "CON_YEAR",],
    )

    # df.groupby('grouping column').agg({'aggregating column': 'aggregating function'})
    # Pivot table with [ppsn, year] as index, sum of €, sum of WIES, count of employments by year
    # //TODO Add employment count, PRSI class, NACE codes
    # //TODO Add 2019 employment data
    earnings_by_id_year = (
        earnings.groupby(["ppsn", "CON_YEAR"])
        .agg({"EARNINGS_AMT": "sum", "NO_OF_CONS": "sum"})
        .fillna(0)
    )

    earnings_by_id_year.columns = ["employment_earnings", "prsi_weeks"]
    earnings_by_id_year["prsi_weeks"] = earnings_by_id_year["prsi_weeks"].clip(upper=52)
    earnings_by_id_year["earnings_per_week_worked"] = (
        earnings_by_id_year["employment_earnings"] / earnings_by_id_year["prsi_weeks"]
    )

    # Brutally quarterize to index [ppsn, quarter]
    # As well as first df, create 3 more, adding timedelta of 1 quarter to each record
    # Then append dfs together
    setup_q = earnings_by_id_year.copy()
    setup_q[["employment_earnings", "prsi_weeks"]] = (
        setup_q[["employment_earnings", "prsi_weeks"]] / 4
    )
    setup_q = setup_q.reset_index()

    for i in range(1, 5):
        quarter = pd.PeriodIndex(year=setup_q["CON_YEAR"], quarter=i, freq="Q")
        d_df = setup_q.drop("CON_YEAR", axis="columns")
        d_df["quarter"] = quarter
        if i == 1:
            earnings_by_id_quarter = d_df
        else:
            earnings_by_id_quarter = earnings_by_id_quarter.append(d_df)

    return earnings_by_id_quarter.set_index(["ppsn", "quarter"])


# Identify key types of SW payment: Unemployment, ED&Train, I&D, other weekly, all other
# Pivot table with [ppsn, quarter] as index, sum of € by payment type as columns
def quarterly_sw_payments(ids):
    columns = ["ppsn", "SCHEME_TYPE", "AMOUNT", "QTR"]
    payments = get_sw_payments(ids=ids, columns=columns)

    # //TODO SW code list to SQL table
    code_list = {
        "JA": "unemployment",
        "SWAER": "other",
        "CB": "family_children",
        "BSCFA": "family_children",
        "OPFP1": "family_children",
        "JB": "unemployment",
        "TLA": "education_training",
        "SWA": "other",
        "DA": "illness_disability",
        "IB": "illness_disability",
        "CARER": "family_children",
        "BTW": "employment_supports",
        "SLO": "education_training",
        "FIS": "family_children",
        "HHB": "other",
        "INTN": "employment_supports",
        "BTWFD": "family_children",
        "DCA": "illness_disability",
        "RCG": "other",
        "WCG": "other",
        "MAT": "family_children",
        "DSBLP": "other",
        "DPA": "other",
        "INVP": "illness_disability",
        "PRSI": "other",
        "FARMA": "employment_supports",
        "OIB": "illness_disability",
        "PTJA": "employment_supports",
        "GPC": "other",
        "REDUN": "other",
        "HHB1": "other",
        "WCP": "other",
        "GPNC": "other",
        "PAT": "family_children",
        "CARB": "other",
        "WNCP": "other",
        "SPC": "other",
        "ABI": "other",
        "SPNCP": "other",
        "BPP": "other",
        "OPFP2": "other",
        "ECS": "other",
        "DSBLG": "other",
        "JOBSP": "other",
        "HSB": "other",
        "DRASC": "other",
        "HAID": "other",
        "PCB": "employment_supports",
        "DWB": "other",
        "WPG": "other",
        "FF": "other",
        "RA": "other",
        "WPGNC": "other",
        "DRASI": "other",
        "MCS": "other",
        "MEDC": "other",
        "INSOL": "other",
        "YESS": "employment_supports",
        "DENB": "other",
        "DB": "illness_disability",
        "WSS": "employment_supports",
        "DWA": "other",
        "DRASD": "other",
        "APB": "other",
        "MEDSB": "other",
        "UnkSc": "other",
        "BG": "other",
        "OPTB": "other",
        "DRASN": "other",
    }
    payments["function"] = payments["SCHEME_TYPE"].map(code_list)
    payments["quarter"] = pd.to_datetime(payments["QTR"]).dt.to_period(freq="Q")
    payments = (
        payments.drop(["SCHEME_TYPE", "QTR"], axis="columns")
        .groupby(["ppsn", "quarter", "function"])
        .agg({"AMOUNT": "sum"})
        .unstack(level="function")
    )
    payments.columns = [f"sw_{col}" for col in payments.columns.droplevel(0)]

    return payments

    # //TODO Defeat outliers! Add feature scaling!

    # *Total income distribution
    # *Share of each income type
    # *Slope of income change


# pd.Index(q.array).asfreq("Y")
