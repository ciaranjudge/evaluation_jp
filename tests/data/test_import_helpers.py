from evaluation_jp.data.import_helpers import get_datetime_cols, get_ists_claims


def test_get_datetime_cols():
    test_inputs = ["les", "ists_personal", "jobpath_referrals"]
    test_outputs = {
        table_name: set(get_datetime_cols(table_name)) for table_name in test_inputs
    }
    expected_outputs = {
        "les": set(["start_date", "start_week"]),
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


# TODO test get_clusters()
# TODO test get_ists_claims()
# TODO test get_les_data()
# TODO test get_jobpath_data()
# TODO test get_earnings()
# TODO test get_payments()


# returned_df = get_ists_claims(
#     pd.Timestamp("2020-01-03"),
#     lr_flag=True,
#     # columns=["lr_code", "clm_comm_date", 'lr_flag'],
#     ids=["0070688N", "0200098K"],
# )
# returned_df.describe()

