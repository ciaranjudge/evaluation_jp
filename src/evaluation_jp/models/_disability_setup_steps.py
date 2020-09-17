

# Use setup steps as model, write function to estimate shares from population slice, taking DA recipiency x (param) months subsequent to:
# 1. start v referred and no start
# 2. Start v control (not referred)
# 3. Referred v control

# dummy data step
# start_ref=data['JobPath_start' data['JobPath_ref'] 1,0
# start_ctrl=data['JobPath_start']1,0
# ref_ctrl=data['JobPath_ref']1,0
from src\evaluation_jp\data\sql importers.py, import *

import pandas as pd

from pathlib import Path

dummy_data_folder=Path("//cskma0294/F/Evaluations/JobPath/dummy_data")
dummy_data_file="jp_dummy_data_dis.csv"
dummy = pd.read_csv(dummy_data_folder/dummy_data_file)

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