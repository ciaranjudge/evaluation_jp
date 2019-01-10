# %% [markdown]
# # Weekly Status

# %%
import pandas as pd              # python package for dataframes
from datetime import datetime
pd.options.display.max_columns = None
pd.options.display.max_rows = None

# %%
in_df = pd.read_csv('data/wsd.csv')
in_df.head()


#%%
