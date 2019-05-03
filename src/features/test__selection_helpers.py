#%%
"""Do not run this unless rebuilding test dataset in next cell!!"""
# import pandas as pd
# from src.data.import_helpers import get_ists_claims, get_vital_statistics, get_les_data

# date = pd.Timestamp("2016-01-01")
# %time lr =get_ists_claims(date, lr_flag=True, columns=["lr_code", "clm_comm_date"])
# %time vs = get_vital_statistics(date, ids=lr.index)
# seed = pd.merge(left=lr, right=vs, left_index=True, right_index=True)
# seed.reset_index().to_feather("data/interim/seed.feather")


#%%
import pandas as pd
# from src.data.import_helpers import get_ists_claims, get_vital_statistics
from src.features.selection_helpers import (
    check_age, 
    check_code, 
    check_duration, 
    on_les, 
    on_jobpath, 
    les_starts, 
    jobpath_starts, 
    jobpath_hold,
    EligibilityChecker, 
    EligibilityCheckManager
)
test = pd.read_feather("data/interim/seed.feather").set_index("ppsn")
date = pd.Timestamp("2016-01-01")
ids = test.index

#%%
d1 = pd.Timestamp("2016-01-01")
d2 = d1 + pd.DateOffset(days=300)
d3 = d2 + pd.DateOffset(days=43)
d4 = d3 + pd.DateOffset(days=95)
ec = EligibilityCheckManager(
    checkers_by_startdate={
        pd.Timestamp("2016-01-01"): EligibilityChecker(
            # age={"max_age": pd.DateOffset(years=60)}, 
            # code={"eligible_codes": ("UA", "UB")}, 
            # duration={"min_duration": pd.DateOffset(years=1)},
            not_on_les={"episode_duration": pd.DateOffset(years=1)},
            # not_on_jobpath={
            #     "episode_duration": pd.DateOffset(years=1),
            #     "use_jobpath_data": True,
            #     "use_ists_data": False,
            #     "combine": "either"
            # },
            # not_jobpath_hold={"period_type": "M", "how": "end"},
            # not_les_starts={"period_type": "M"}
            # not_jobpath_starts={
            #     "period_type": "M",
            #     "use_jobpath_data": False,
            #     "use_ists_data": True,
            #     "combine": "either"
            # }
        ),
    pd.Timestamp("2017-01-01"): EligibilityChecker(
            # age={"max_age": pd.DateOffset(years=60)}, 
            # code={"eligible_codes": ("UA", "UB")},  
            not_on_jobpath={
                "episode_duration": pd.DateOffset(years=1),
                "use_jobpath_data": True,
                "use_ists_data": False,
                "combine": "either"
            },
        ),     
    }
)
e1 = ec.get_eligibility(d1, ids)
e2 = ec.get_eligibility(d2, ids)
e3 = ec.get_eligibility(d3, ids)
e4  = ec.get_eligibility(d4, ids)

# %%
%time ca = get_age(date, ids, max_age=pd.DateOffset(years=60))
test["test_col"] = ca
print(ca.value_counts())
test.head(50)

# %%
%time cd = get_code(date, ids, eligible_codes=("UA", "UB"))
test["test"] = cd
print(cd.value_counts())
test.head(50)


# %%
%time dr = get_duration(date, ids, min_duration=pd.DateOffset(years=1))
test["test"] = dr
print(dr.value_counts())
test.head(50)

# # %%
%time ol = get_on_les(date, ids, episode_duration=pd.DateOffset(years=1))
test["test"] = ol
print(ol.value_counts())
test.head(50)

# %%
oj = get_on_jobpath(
    date + pd.DateOffset(months=1), 
    ids, 
    episode_duration=pd.DateOffset(years=1), 
    use_jobpath_data=True,
    use_ists_data=False,
    combine="either"
)
test["test"] = oj
print(oj.value_counts())
test.head(50)

# # %%
ls = get_les_starts(
    date, 
    ids, 
    period_type="M"
)
ls.value_counts()
# %%
jh = get_jobpath_hold(date, ids, period_type="M", how="end")
jh.value_counts()

# %%
js = get_jobpath_starts(
    date, 
    ids, 
    period_type="M",
    use_jobpath_data=False,
    use_ists_data=True,
    combine="either"
)
js.value_counts()


#%%
