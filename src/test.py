# %%
# Standard library
# import datetime as dt
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional
from pathlib import Path

# External packages
import pandas as pd

# Local packages
# from src.data.import_helpers import get_clusters, get_vital_statistics, get_ists_claims, get_employment_data
# from src.data.persistence_helpers import populate

# from src.features.selection_helpers import (
#     restrict_by_age,
#     restrict_by_code,
#     restrict_by_duration,
#     on_les,
#     any_previous_jobpath,
#     jobpath_starts_this_period,
#     les_starts_this_period,
# )
# from src.features.metadata_helpers import lr_reporting_date
# from src.evaluation_period import EvaluationPeriod
# from src.evaluation_slice import EvaluationSlice
from src.evaluation_model import EvaluationModel

# %%
## Slices
slice_evaluation_eligibility_flags: Tuple[str] = (
    # "on_lr"  # implied by cluster flag selection
    "age_eligible",
    "code_eligible",
    "duration_eligible",
    "not_on_les",
    "no_previous_jobpath",
)
## Periods
period_eligibility_flags: Tuple[str] = (
    "on_lr",
    "code_eligible",
    "duration_eligible",
    "not_jobpath_hold",
)

# %%
em = EvaluationModel(
    ## Model
    start_date=pd.Timestamp("2016-01-01"),
    name_prefix="deasp_wp_2019-Mar",
    rebuild_all=False,
    ## Outcomes
    outcome_start_date=pd.Timestamp("2016-04-01"),
    outcome_end_date=pd.Timestamp("2019-04-01"),
    ## Slices
    last_slice_date=pd.Timestamp("2016-12-31"),
    slice_freq="Q",
    # slice_cluster_eligibility_flags=slice_cluster_eligibility_flags,
    slice_evaluation_eligibility_flags=slice_evaluation_eligibility_flags,
    ## Periods
    last_period_date=pd.Timestamp("2016-12-31"),
    period_freq="M",
    period_evaluation_eligibility_flags=period_eligibility_flags,
)


#%%
em.logical_name


#%%
pop = em.slices[pd.Period("2016Q1")].dataframes["population"]

#%%
pop2 = pop.copy().loc[pop["eligible"] == True]

#%%
pop2.info()

#%%
pop["eligible"].value_counts()

#%%


# %%

#%%
@dataclass
class Team:
    on_first: str = "Who"
    on_second: str = "What"
    on_third: str = "I Don't Know"
    rebuild_all: bool = False
    data: dict = None
    rebuild_all: bool = False
    logical_root: Tuple[str] = (".",)
    logical_name: str = "who"

    def __post_init__(self):
        self.data = {}

    def you_throw_the_ball_to_who(self, who_picks_it_up: bool = False):
        print("Naturally.")
        if who_picks_it_up is True:
            print("Sometimes his wife picks it up.")

    @populate
    def team_data(self):
        self.data["team_data"] = pd.DataFrame(
            data=[[1, 2], [3, 4]], columns=["who", "what"]
        )


t = Team()


#%%
t.team_data()


#%%
td = load_data(data_name="team_data", logical_root=(".",), logical_name="who")

#%%
t.logical_name

#%%
# slice_evaluation_eligibility_flags: Tuple[str] = (
#     # "on_lr"  # implied by cluster flag selection
#     "age_eligible",
#     "code_eligible",
#     "duration_eligible",
#     "not_on_les",
#     "no_previous_jobpath",
# )
e_checks = {"age_eligible": {min_age: None, max_age: 62}, "code_eligible": {}}


# %%
arp = pd.Period()#


# %%
def fluffy():
    x = 
    print("yes")

fl = "f" + "luffy"


#%%
eval(fl+"()")

#%%
p = Path("D:/repos/evaluation_jp/data/processed/deasp_wp_2019-Mar__model_2016-01-01/model_2016-Q1/model_2016-Q1_seed_pop.feather")
test_data = pd.read_feather(p)

#%%
test_data.info()


#%%


ppsn_index = pd.Index(test_data["ppsn"])


#%%
type(ppsn_index)

#%%
set(ppsn_index)

#%%
ppsn_index.shape


#%%
test_data["ppsn"].describe()

#%%
import pandas as pd
from src.features.selection_helpers import check_age, on_first
ca = check_age(
    pd.Timestamp("2016-01-01"), 
    id_series=test_data["ppsn"], 
    checks={"max": pd.DateOffset(years=62)}
)

#%%
