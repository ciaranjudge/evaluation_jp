import pandas as pd
import datetime
import sqlalchemy as sa
from sqlalchemy import text
from pathlib import Path
import numpy as np
engine = sa.create_engine('sqlite:///\\\\cskma0294\\F\\Evaluations\\data\\wwld.db', echo=False)

#%%
referral_data_folder=Path("//cskma0294/F/Evaluations/JobPath/ReferralData")
referral_import_lookup_file="jobpath_referral_dict.csv"
referral_import_lookup = pd.read_csv(referral_data_folder/referral_import_lookup_file)

referrals_2016_file='2016 Data for Evaluation Run 23072018 V3 - mvmt.xlsx'
referrals_2016_sheet='Revised Status with Paused Data'

referrals_2016= pd.read_excel(
    referral_data_folder/referrals_2016_file, 
    sheet_name=referrals_2016_sheet
    )

#%%
referrals_2017_file='2017 Data for Evaluation 04092018 - mvmt WRONG.xlsx'
referrals_2017_sheet='2017 Data with Pauses'

referrals_2017= pd.read_excel(
    referral_data_folder/referrals_2017_file, 
    sheet_name=referrals_2017_sheet
    )
#%%
dict2016=dict(zip(referral_import_lookup['2016'], referral_import_lookup['label']))
dict2017=dict(zip(referral_import_lookup['2017'], referral_import_lookup['label']))


# %%
r2016=referrals_2016.rename(columns=dict2016)
r2016=r2016[list(referral_import_lookup['label'])]
object_cols = r2016.select_dtypes("object").columns.to_list()
r2016['cancellation_date']=r2016['cancellation_date'].replace({'Not available':''})

for col in [col for col in object_cols if "date" in col.lower()]:
    r2016[col] = pd.to_datetime(r2016[col], infer_datetime_format=True, cache=False)


# %%

r2017=referrals_2017.rename(columns=dict2017)
r2017=r2017[list(referral_import_lookup['label'])]
object_cols = r2017.select_dtypes("object").columns.to_list()

for col in [col for col in object_cols if "date" in col.lower()]:
    r2017[col] = pd.to_datetime(r2017[col], infer_datetime_format=True, cache=False)

#%%
r=r2017.append(r2016)


#to_sql
r=r.set_index('referral_id')

#%%
# Parse dates explicitly if they weren't already parsed by pd.read_sql_query
#%%
r.to_sql('jobpath_referrals', con=engine, if_exists='replace')


# %%
