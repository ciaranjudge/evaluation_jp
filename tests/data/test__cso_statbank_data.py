from evaluation_jp.data import cso_statbank_data

# //TODO Make CSO statbank access code work behind firewall
def test__cso_statbank_data__LRM02():
    """LR total for test month should be same as published on CSO website
    """

    data = cso_statbank_data(
        table="LRM02", dimensions=["Age Group", "Sex", "Month", "Statistic",]
    )

    # Look up test data on CSO website
    # https://www.cso.ie/en/releasesandpublications/er/lr/liveregistermarch2020/
    test_month = "2020M03"
    total_live_register = 205_209

    results = data.loc[
        (data["Month"] == test_month)
        & (data["Age Group"] == "All ages")
        & (data["Sex"] == "Both sexes")
        & (data["Statistic"] == "Persons on the Live Register (Number)")
    ]
    assert int(results["Value"]) == total_live_register


def test__cso_statbank_data__QLF18():
    """Labour force total for test quarter should be same as published on CSO website
    """

    data = cso_statbank_data(
        table="QLF18", dimensions=["Age Group", "Sex", "Quarter", "Statistic",]
    )

    # Look up test data on CSO website
    # https://www.cso.ie/en/releasesandpublications/er/lfs/labourforcesurveylfsquarter42019/
    test_quarter = "2019Q4"
    total_labour_force = 2_471_700

    results = data.loc[
        (data["Quarter"] == test_quarter)
        & (data["Age Group"] == "15 years and over")
        & (data["Sex"] == "Both sexes")
        & (
            data["Statistic"]
            == "Person aged 15 years and over in the Labour Force (Thousand)"
        )
    ]
    # Multiply by 1,000 here to match the number on CSO release
    assert int(results["Value"] * 1_000) == total_labour_force
