#%% [markdown]
# # Weekly Status

#%%
import pandas as pd              # python package for dataframes
import matplotlib.pyplot as plt  # python package for plotting
from datetime import datetime

#%%
in_df = pd.read_csv('\\\\cskma0294\\F\\Evaluations\\JobPath\\\Quarterly_status\\WeeklyStatus.zip')
in_df = in_df.drop(['Purge_ind'], axis=1)
in_df.head()         

#%%
df = pd.melt(in_df, id_vars=['ppsn'], var_name='date', value_name='status')

#%%
df = df[df['date'] != ''].dropna()
df['date'] = [x.strip('StatusEnd_') for x in df['date']]
#%%
df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True).dt.date

#%%
df = df.set_index(['ppsn', 'date'])

#%%
df_dummies = df.head(1000)['status'].str.get_dummies(sep='+')
#%%
df_dummies.head()

#%%
df.to_csv('weekly_status_long.csv')



#%%
df.info()

#%%
