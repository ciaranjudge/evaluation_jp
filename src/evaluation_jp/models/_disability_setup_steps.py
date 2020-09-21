

# Use setup steps as model, write function to estimate shares from population slice, taking DA recipiency x (param) months subsequent to:
# 1. start v referred and no start
# 2. Start v control (not referred)
# 3. Referred v control
# df.loc[(df['column_name'] >= A) & (df['column_name'] <= B)]

# dummy data step
# 
from evaluation_jp.data.sql_importers import *
from evaluation_jp import get_ists_claims, get_les_data, get_jobpath_data, ColumnsByType

import pandas as pd
from pathlib import Path
import numpy as np

dummy_data_folder=Path("//cskma0294/F/Evaluations/JobPath/dummy_data")
dummy_data_file="jp_dummy_data_dis.csv"
dummy = pd.read_csv(dummy_data_folder/dummy_data_file, parse_dates=True)
dummy['population_slice']=pd.to_datetime(dummy['population_slice'])
dummy['treatment_period']=pd.to_datetime(dummy['treatment_period'])	

dummy['age']=np.random.randint(18, 60, size=8) 
dummy['duration']=np.random.randint(365, 5_000, size=8) 
def non-starters_v_referred:
# df.loc[(df['column_name'] >= A) & (df['column_name'] <= B)]
#see JobPath setup steps - incorporate start within three months (relate to SDs of distribution) - does the person start within three months of referral? WWLD_importers and population_setup_steps: JobPath starts
#setup one: in some time period, was there a referral effect? Date of referral and use same logic as JobPath start
#setup two: within JobPath setup steps, take logic of 'within these two dates, was there a JobPath start' - one date being a referral date and the second being x months/weeks after the referral?
ref_ctrl= np.where((dummy['JP_start']==0) & (dummy['JP_refer'] == 1), True, False)
dummy=dummy.loc[dummy['JP_refer'] == 1]]
start_ref = np.where((dummy['JP_start']==1), True, False)
#referred and will start, in the normal time frame
@dataclass
class StartingPopulation(SetupStep):
        data = data.loc[data["starting_population"]
#in some time period, was there a referral effect? Date of referral and use same logic as JobPath start
@dataclass
class JobPathReferrals(SetupStep):

data["jobpath_referral"] = operational_jobpath_referrals
        return data
#either use the amended get_jobPath_referral_data function below or use the JobPathReferrals SetupStep

def get_jobpath_referral_data(
    ids: Optional[pd.Index] = None, columns: Optional[List] = None
) -> pd.DataFrame:
    required_columns = ["ppsn", "jobpath_start_date", "referral_date"]
    col_list = unpack(
        get_col_list(engine, "jobpath_referrals", columns, required_columns)
    )
    query_text = f"""\
        SELECT {col_list} 
            FROM jobpath_referrals
        """
    query, params = get_parameterized_query(query_text, ids)
    jobpath = pd.read_sql(
        query,
        con=engine,
        params=params,
        parse_dates=datetime_cols(engine, "jobpath_referrals"),
    )

    # Add calculated columns
    if columns is None or (columns is not None and "jobpath_end_date" in columns):
        jobpath["jobpath_end_date"] = jobpath["jobpath_end_date"].fillna(
            jobpath["jobpath_start_date"] + pd.DateOffset(years=1)
        )
    if columns is None or (columns is not None and "jobpath_start_month" in columns):
        jobpath["jobpath_start_month"] = jobpath["jobpath_start_date"].dt.to_period("M")
    return jobpath



def dates_between_durations(
    dates: pd.Series,
    ref_date: pd.Timestamp,
    min_duration: pd.DateOffset = None,
    max_duration: pd.DateOffset = None,
) -> pd.Series:
    """Return boolean series for records with dates between specified limits.
    True if `min_duration` < (`ref_date` - `date`) < `max_duration`, False otherwise.
    Assume all dates are in the past.
    """
    if min_duration:
        latest_possible_date = ref_date - min_duration
        # If duration is longer, date must be earlier!
        ge_min_duration = dates <= latest_possible_date
    else:
        ge_min_duration = pd.Series(data=True, index=dates.index)

    if max_duration:
        earliest_possible_date = ref_date - max_duration
        # If duration is longer, date must be earlier!
        lt_max_duration = earliest_possible_date < dates
    else:
        lt_max_duration = pd.Series(data=True, index=dates.index)

    return ge_min_duration & lt_max_duration

dates_between_durations(dates: JobPath_starts,    ref_date: pd.Timestamp,
    min_duration: pd.DateOffset = None,
    max_duration: pd.DateOffset = None,
)

def exit_to_da(df, interval, criteria):
"""function to establish DA recipiency at some interval, comparing this share for mutually exclusive cohorts""" 
        df: input df(starting_population)
        interval: number of weeks to check status 
        criteria: 1/0 values for  two cohorts from three interim cols (start_ref, start_ctrl, ref_ctrl)
    read df(population slice ID),
    df=df.apply(criteria)
    interval 
    share=df ['sw_pay_status']=='illness and disability ' /len(df),
    for df in all population slices, print share
return (df)

generate ten rows of df to get it to work - population date, shift it forward with timedelta, assert statements, setup steps

id, population date, shift=interval, status, sw_pay_status