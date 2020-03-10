import pandas as pd
import sqlalchemy as sa

from evaluation_jp.data.import_helpers import (
    get_datetime_cols,
    get_col_list,
    # unpack,
    # compose_query,
    # get_clusters,
    get_ists_claims,
    get_les_data,
    get_jobpath_data,
    # get_earnings,
    # get_payments,
)

engine = sa.create_engine(
    "sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db", echo=False
)
insp = sa.engine.reflection.Inspector.from_engine(engine)

# TODO Move data to fixtures


def test_get_datetime_cols():
    test_inputs = ["les", "ists_personal", "jobpath_referrals"]
    results = {
        table_name: set(get_datetime_cols(table_name)) for table_name in test_inputs
    }
    expected = {
        "les": set(["start_date"]),
        "ists_personal": set(["date_of_birth"]),
        "jobpath_referrals": set(
            [
                "referral_date",
                "jobpath_start_date",
                "jobpath_end_date",
                "cancellation_date",
                "pause_date",
                "resume_date",
                "ppp_agreed_date",
            ]
        ),
    }
    assert results == expected


# TODO Parameterised test with and without actual list of columns
def test_get_col_list():
    test_inputs = ["les", "ists_claims"]
    results = {
        table_name: set(get_col_list(table_name, columns=None))
        for table_name in test_inputs
    }
    expected = {
        "les": set(["client_group", "ppsn", "start_date"]),
        "ists_claims": set(
            [
                "lr_code",
                "lr_flag",
                "lls_code",
                "clm_reg_date",
                "clm_comm_date",
                "location",
                "CLM_STATUS",
                "CLM_SUSP_DTL_REAS_CODE",
                "CDAS",
                "ada_code",
                "JobPath_Flag",
                "JobPathHold",
                "PERS_RATE",
                "ADA_AMT",
                "CDA_AMT",
                "MEANS",
                "EMEANS",
                "NEMEANS",
                "NET_FLAT",
                "FUEL",
                "RRA",
                "WEEKLY_RATE",
                "Recip_flag",
                "lr_date",
                "personal_id",
            ]
        ),
    }
    assert results == expected


# TODO test unpack()

# TODO test compose_query()

# TODO test get_clusters()

# TODO test get_ists_claims()


def test_get_les_data():
    test_inputs = pd.Index(
        ["6892436U", "5051366B", "6049367W", "5092934S", "8420262S",]
    )
    results = (
        get_les_data(ids=test_inputs, columns=["ppsn", "end_date"])
        .drop_duplicates("ppsn", keep="first")
        .set_index("ppsn")
        .sort_index()
    )
    expected = (
        pd.DataFrame(
            {
                "ppsn": {
                    0: "6892436U",
                    1: "5051366B",
                    2: "6049367W",
                    3: "5092934S",
                    4: "8420262S",
                },
                "end_date": {
                    0: pd.Timestamp("2018-01-02 00:00:00"),
                    1: pd.Timestamp("2018-01-03 00:00:00"),
                    2: pd.Timestamp("2018-01-03 00:00:00"),
                    3: pd.Timestamp("2018-01-03 00:00:00"),
                    4: pd.Timestamp("2018-01-03 00:00:00"),
                },
            }
        )
        .set_index("ppsn")
        .sort_index()
    )

    assert results.equals(expected)


def test_get_jobpath_data():
    test_sample = pd.read_sql(
        "select ppsn, jobpath_start_date from jobpath_referrals",
        con=engine,
        parse_dates=["jobpath_start_date"],
    ).drop_duplicates("ppsn", keep="first").sample(n=100, random_state=0)
    test_inputs = pd.Index(test_sample["ppsn"].tolist())
    results = (
        get_jobpath_data(ids=test_inputs, columns=["ppsn", "jobpath_start_date"])
        .drop_duplicates("ppsn", keep="first")
        .set_index("ppsn")
        .sort_index()
    )
    expected = (
        test_sample.drop_duplicates("ppsn", keep="first").set_index("ppsn").sort_index()
    )

    assert results.equals(expected)


# TODO test get_earnings()
# TODO test get_payments()


# returned_df = get_ists_claims(
#      pd.Timestamp("2020-01-03"),
#     lr_flag=True,
#     # columns=["lr_code", "clm_comm_date", 'lr_flag'],
#     ids=["0070688N", "0200098K"],
# )
# returned_df.describe()

