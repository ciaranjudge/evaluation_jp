# %%
import pandas as pd
import numpy as np

columns = ["Q1", "Q2", "Q3", "Q4"]
df = pd.DataFrame(np.random.randint(0,2,size=(100, 4)), columns=columns)

for i, now_col in enumerate(columns):
    for j, later_col in enumerate(columns[i+1:]):
        later_starts = (df[now_col] == 0) & (df[later_col] == 1)
        df.loc[later_starts, now_col] = -(j+1)


#%%
df

#%%
