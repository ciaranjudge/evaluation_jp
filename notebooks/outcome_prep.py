
#%%
import sys
import pandas as pd              # python package for dataframes
import os                        # used to change directory paths
import matplotlib.pyplot as plt  # python package for plotting
import numpy as np
#can't import seaborn
import seaborn as sns #package for plotting
from scipy.stats import norm
from sklearn.preprocessing import StandardScaler
from scipy import stats
from IPython.display import display, HTML  # Make tables pretty
from datetime import datetime
import pandas as pd

#%% [markdown]
# ## Import JobPath data

#%%
path_jp = '//cskma0294/F/Evaluations/JobPath/Python/Analysis/JPOutcomes/'
filename_jp = 'linkedclaims_casuals_2018m04_v2_flat_20160101_with_income_36Vars__7BGM_full_clusters_jp_summary_status.csv'
data_jp_python = pd.read_csv(path_jp+filename_jp, encoding= 'utf-8')


#%%
#for col in data_jp_python.columns: print(col)

#%% [markdown]
# ### Import SAS dataset used for initial results for comparison

#%%
path_sas = '//cskma0294/F/Evaluations/JobPath/'
filename_sas = 'slice1_8_earnings1.sas7bdat'
data_jp_sas = pd.read_sas(path_sas+filename_sas, format = 'sas7bdat', encoding='iso-8859-1')
data_jp_sas.fillna(0, inplace=True)
data_jp_sas.head()


#%%
#for col in data_jp_sas.columns:
#    print("'"+col+"', ", sep="", end="")


#%%
data_jp_python=data_jp_python[~data_jp_python['hist_lr_0'].str.contains('C', case=False)]
data_jp_python=data_jp_python[data_jp_python['age']<60]
data_jp_python=data_jp_python[data_jp_python['jp_flag_before_aw']!= 1]
data_jp_python=data_jp_python[data_jp_python['duration_days_0'] > 299]


#%%
to_keep_jp = ['cluster','ppsn','age',
                'duration_days_0',
                'sex',
                'LM_code_rank_P1',
                'occupation_rank_P1',
                'ada_code_rank_P1',
                'family_flag_rank_P1',
                'marital_status_rank_P1',
                'LM_code_rank_P2',
                'occupation_rank_P2',
                'ada_code_rank_P2',
                'family_flag_rank_P2',
                'marital_status_rank_P2',
                'LM_code_rank_P3',
                'occupation_rank_P3',
                'ada_code_rank_P3',
                'family_flag_rank_P3',
                'marital_status_rank_P3',
                'LM_code_rank_P4',
                'occupation_rank_P4',
                'ada_code_rank_P4',
                'family_flag_rank_P4',
                'marital_status_rank_P4',
                'LM_code_rank_P5',
                'occupation_rank_P5',
                'ada_code_rank_P5',
                'family_flag_rank_P5',
                'marital_status_rank_P5',
                'LM_code_rank_P6',
                'occupation_rank_P6',
                'ada_code_rank_P6',
                'family_flag_rank_P6',
                'marital_status_rank_P6',
                'LM_code_rank_P7',
                'occupation_rank_P7',
                'ada_code_rank_P7',
                'family_flag_rank_P7',
                'LM_code_rank_P8',
                'occupation_rank_P8',
                'ada_code_rank_P8',
                'family_flag_rank_P8',
                'marital_status_rank_P7',
                'marital_status_rank_P8',
                'jp_started_P1',
                'jp_started_P2',
                'jp_started_P3', 'jp_started_P4', 'jp_started_P5', 'jp_started_P6', 
                'jp_started_P7', 'jp_started_P8',
                'Cancellationsubcategory',
                'hist_lr_0',
             ]

data_jp_python=data_jp_python[to_keep_jp]


#%%
data_jp_python.columns


#%%
keep_sas = ['ppsn', 'total_duration_days', 'total_sum_penaltyflag', 'End_Q42015', 'End_Q12016', 'End_Q22016', 'End_Q32016', 'End_Q42016', 'End_Q12017', 'End_Q22017', 'End_Q32017', 'End_Q42017', 'End_Q12018', 'End_Q22018', 'Group1', 'Group2', 'Group3', 'Group4', 'Group5', 'Group6', 'Group7', 'Group8', 'Outcome_24m', 'Earn2012', 'Earn2013', 'Earn2014', 'Earn2015', 'Earn2016', 'Earn2017', 'Earn2018', ]

data_jp=pd.merge(data_jp_python, data_jp_sas[keep_sas], on='ppsn', how='inner')


#%%
data_jp['cancel_flag'] = np.where(data_jp['Cancellationsubcategory'] == "b'nan'", 0, 1)
data_jp.groupby(['jp_started_P1', 'Group1', 'End_Q12016', 'cancel_flag'])['Cancellationsubcategory'].count()


#%%
data_jp.loc[:, 'Duration Bands'] = (data_jp['duration_days_0']).apply(lambda x : "0" if x < 365 else ("1"   if x < 730 else ("2" if x < 1095 else ("3"))))
data_jp['Duration Bands']=data_jp['Duration Bands'].astype(int)


#%%
data_jp.set_index('ppsn', inplace=True)

#%% [markdown]
# ##  Import Earnings data

#%%
path_earnings='//cskma0294/F/Evaluations/JobPath/Python/Data/Earnings/'
filename_earnings = 'new_earnings_11jul2018.csv'
data_earnings = pd.read_csv(path_earnings+filename_earnings, encoding= 'utf-8')
data_earnings.fillna(0, inplace=True)


#%%
#col_list2013_2015= ['Class_A_Earn2013', 'Class_A_Earn2014', 'Class_A_Earn2015', 'Class_S_Earn2013', 'Class_S_Earn2014', 
#                    'Class_S_Earn2015', 'Class_Other_Earn2013', 'Class_Other_Earn2014','Class_Other_Earn2015']

#data_earnings['Sum_2013_2015'] = (data_earnings[col_list2013_2015].sum(axis=1))

# Can't use Class S as it's not available for 2017
#col_list2013 = ['Class_A_Earn2013', 'Class_S_Earn2013', 'Class_Other_Earn2013']
#col_list2014 = ['Class_A_Earn2014', 'Class_S_Earn2014', 'Class_Other_Earn2014']
#col_list2015 = ['Class_A_Earn2015', 'Class_S_Earn2015', 'Class_Other_Earn2015']



#data_earnings['Sum_2013'] = (data_earnings[col_list2013].sum(axis=1))
#data_earnings['Sum_2014'] = (data_earnings[col_list2014].sum(axis=1))
#data_earnings['Sum_2015'] = (data_earnings[col_list2015].sum(axis=1))

#col_list_sum_2013_2015 = ['Sum_2013', 'Sum_2014', 'Sum_2015']

#data_earnings['mean_2013_2015'] = (data_earnings[col_list_sum_2013_2015].mean(axis=1))
column_name_dict={'RSI_NO': 'ppsn'}

for year in range(2012, 2018):
    for cl in ["a", "s", "other"]:
        column_name_dict['Class_'+cl.title()+'_Earn'+str(year)] = 'earn_'+cl+'_'+str(year)
data_earnings.rename(columns=column_name_dict, inplace=True)

keep_cols = []
for key, value in column_name_dict.items():
    keep_cols.append(value)

data_earnings=data_earnings[keep_cols]

for year in range(2012, 2018):
    data_earnings['earn_tot_'+str(year)] = data_earnings['earn_a_'+str(year)] + data_earnings['earn_s_'+str(year)] + data_earnings['earn_other_'+str(year)]

for cl in ["a", "s", "other", "tot"]:
        cols_1215 = ['earn_'+cl+'_'+str(year) for year in range(2012,2016)]
        data_earnings['earn_'+cl+'_mean_1215'] = data_earnings[cols_1215].mean(axis=1)
        cols_1315 = ['earn_'+cl+'_'+str(year) for year in range(2013,2016)]
        data_earnings['earn_'+cl+'_mean_1315'] = data_earnings[cols_1315].mean(axis=1)
        


#%%
data_earnings.set_index('ppsn', inplace=True)


#%%
data_earnings.describe()

#%% [markdown]
# ## Import SW payments data

#%%
path_payments ="\\\cskma0294\\F\\Evaluations\\JobPath"
os.chdir(path_payments)
sw_payments = pd.read_csv('ALL_PPSNS_with_payments.csv')
sw_payments.fillna(0, inplace=True)


#%%
sw_payments['Year'],sw_payments['Q'] = zip(*sw_payments["Quarter"].apply(lambda x: x.split('_')))


#%%
sw_pay_by_year=pd.pivot_table(sw_payments,index=['ppsn', 'Year'], values='total_payment', aggfunc=np.sum)
sw_pay_by_year.reset_index(inplace=True)
#sw_pay_by_year.head()


#%%
sw_pay = sw_pay_by_year.pivot(index="ppsn", columns="Year", values="total_payment")
sw_pay.fillna(0, inplace=True)
newcols = ["sw_pay_"+str(col) for col in range(2013, 2019)]
sw_pay.columns = newcols


#%%
cols_1315 = ['sw_pay_'+str(year) for year in range(2013,2016)]
sw_pay['sw_pay_mean_1315'] = sw_pay[cols_1315].mean(axis=1)


#%%
sw_pay.columns


#%%
sw_pay.describe()

#%% [markdown]
# ## Merge JobPath, Earnings and Payments data
#%% [markdown]
# ### Merge data

#%%
data_jp_earn=pd.merge(data_jp, data_earnings, on='ppsn', how='left')
data_jp_earn_sw=pd.merge(data_jp_earn, sw_pay, on='ppsn', how='left')

#%% [markdown]
# ### Fix missing values in numeric columns

#%%
data_earnings_numeric_cols = list(data_earnings.columns)
if 'ppsn' in data_earnings_numeric_cols:
    data_earnings_numeric_cols.remove("ppsn")

sw_pay_numeric_cols = list(sw_pay.columns)
if 'ppsn' in sw_pay_numeric_cols:
   sw_pay_numeric_cols.remove("ppsn")

numeric_cols = data_earnings_numeric_cols + sw_pay_numeric_cols
numeric_cols.append('duration_days_0')

data_jp_earn_sw[numeric_cols] = data_jp_earn_sw[numeric_cols].fillna(value=0)

#%% [markdown]
# ### And check all are ok

#%%
numeric_nas = dict(data_jp_earn_sw[numeric_cols].isna().sum())
for k, v in numeric_nas.items():
    if v != 0:
        print(k,":",v)


#%%
data_jp_earn_sw.head()

#%% [markdown]
# ### Rename final dataframe as df for convenience

#%%
df = data_jp_earn_sw
df.reset_index(inplace=True)
df.insert(0, 'id', range(len(df)))
df.head()


#%%
# df.drop('ppsn', axis='columns', inplace=True)


#%%
df.to_csv('jp_outcomes.csv')


#%%
# This is where it pops out!
os.getcwd()


#%%
df.describe()




#%%
