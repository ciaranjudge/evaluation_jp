# %% [markdown]
# # Weekly Status

# %%
import pandas as pd              # python package for dataframes
from datetime import datetime
pd.options.display.max_columns = None
pd.options.display.max_rows = None

# %%
in_df = pd.read_csv(
    '\\\\cskma0294\\F\\Evaluations\\JobPath\\\Quarterly_status\\WeeklyStatus.zip')
in_df = in_df.drop(['Purge_ind'], axis=1)
in_df.head()

# %%
df = pd.melt(in_df, id_vars=['ppsn'], var_name='date', value_name='status')

# %%
df = df[df['date'] != ''].dropna()
df['date'] = [x.strip('StatusEnd_') for x in df['date']]
# %%
df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True).dt.date

# %%
max_simultaneous = df['status'].str.count('\+').max() + 1

# %%
status_cols = ['s' + str(i) for i in range(max_simultaneous)]
df[status_cols] = df['status'].str.split(pat='\+', expand=True)

# %%
df.info()

# %%
df = df.drop('status', axis='columns')

# %%
df.info()

# %%
df = df.set_index(['ppsn', 'date'])

# %%

# %%
df_dummies = (pd.get_dummies(df, prefix='', prefix_sep='')
              .max(level=0, axis=1)
              .astype('bool')
              )

# %%
df_dummies.info()
""" 
# %%
df_with_dummies = pd.concat([df, df_dummies], axis='columns')

# %%
df_with_dummies.info()

# %%
four_concurrent = df_with_dummies.groupby(status_cols).sum()
print(four_concurrent)
# %%
three_concurrent = df_with_dummies.groupby(status_cols[:3]).sum()
print(three_concurrent)

# %%
two_concurrent = df_with_dummies.groupby(status_cols[:2]).sum()
print(two_concurrent)

# %%
df_dummies.columns
 """

# %%




""" df_melt = pd.melt(df,
                  id_vars=['ppsn', 'date'], 
                  var_name='status', 
                  value_name='flag'
                  )

# %%
df_melt.info() """