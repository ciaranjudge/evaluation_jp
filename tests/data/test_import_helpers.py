from evaluation_jp.data.import_helpers import (
    get_datetime_cols,
    get_col_list,
    # get_clusters,
    get_ists_claims,
    get_les_data,
    get_jobpath_data,
    #     get_earnings,
    #     get_payments,
)


def test_get_datetime_cols():
    test_inputs = ["les", "ists_personal", "jobpath_referrals"]
    test_outputs = {
        table_name: set(get_datetime_cols(table_name)) for table_name in test_inputs
    }
    expected_outputs = {
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
    assert test_outputs == expected_outputs

# TODO Parameterised test with and without actual list of columns
# TODO Move data to fixtures
def test_get_col_list():
    test_inputs = ["les", "ists_claims"]
    test_outputs = {
        table_name: set(get_col_list(table_name, columns=None))
        for table_name in test_inputs
    }
    expected_outputs = {
        "les": set(['client_group', 'ppsn', 'start_date']),
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
    assert test_outputs == expected_outputs


# # TODO test get_clusters()
# # TODO test get_ists_claims()
def test_get_les_data():
    pass


# # TODO test get_jobpath_data()
# TODO test get_earnings()
# TODO test get_payments()


# returned_df = get_ists_claims(
#     pd.Timestamp("2020-01-03"),
#     lr_flag=True,
#     # columns=["lr_code", "clm_comm_date", 'lr_flag'],
#     ids=["0070688N", "0200098K"],
# )
# returned_df.describe()

