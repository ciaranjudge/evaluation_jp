
# %%
import sys
import pandas as pd  # python package for dataframes
import os  # used to change directory paths
import matplotlib.pyplot as plt  # python package for plotting
import numpy as np

import seaborn as sns  # package for plotting
from scipy.stats import norm
from sklearn.preprocessing import StandardScaler
# from scipy import stats
from IPython.display import display, HTML  # Make tables pretty
from datetime import datetime
import pandas as pd

# %% [markdown]
# ## Import JobPath outcomes data

# %%
df = pd.read_csv('jp_outcomes.zip')


# %%
# Convert floats to ints to make reporting easier
numerics = ['float64']
for col in df.select_dtypes(include=numerics).columns:
    if df[col].isnull().sum() == 0:
        #print(col)
        df[col] = df[col].astype('int')
df.columns

# %% [markdown]
# ## Test vs October SAS results

# %%
df.groupby(["Group1"])["earn_tot_mean_1215", "earn_tot_2017"].mean().astype('int')

# %% [markdown]
# ## Add weights
# %% [markdown]
# Still need to come up with better approach here.
# Experiment first with different weighting approaches, eg
# - By each reporting variable individually
# - By different combinations of reporting variables
# - Should be able to use weighting as a way to eliminate crazy outliers (e.g. the case with mean earnings > €300k)
#
# What exactly is target of weights?

# %%
#df = df[df["Group1"] != 0]


# %%
df[["duration_days_0"]].divide(7).hist(bins=100, log=True)


# %%
df.groupby(["Group1"])["earn_tot_mean_1315"].max() / 52


# %%
df["w_factor"] = df["earn_tot_mean_1315"].divide(52).astype(int).multiply(10 ** 4) + df[
    "duration_days_0"
].divide(7).astype(int)


# %%
df_T = df.loc[df["Group1"] == "T1"]


# %%
df_C = df.loc[df["Group1"] == "C1"]


# %%
df_T["t_centile"], bins = pd.qcut(df_T["w_factor"], 100, retbins=True, labels=False)


# %%
df_T.groupby("t_centile")["w_factor"].describe()


# %%
t_count = df_T["t_centile"].count()
t_centile_weights = df_T.groupby(["t_centile"])["w_factor"].count().divide(t_count)
df_T["weight"] = 1


# %%
df_C["t_centile"] = pd.cut(df_C["w_factor"], bins, labels=range(len(bins) - 1))
c_count = df_C["t_centile"].count()
# df_C.groupby(['t_centile'])['ppsn'].count()
c_t_centile_weights = df_C.groupby(["t_centile"])["t_centile"].count().divide(c_count)
c_weights = t_centile_weights / c_t_centile_weights
df_C["weight"] = df_C["t_centile"].map(c_weights)

# %%
print(len(df_C))
df_C = df_C.dropna(subset=['weight'])
print(len(df_C))

# %%
df = df_T.append(df_C)


# %%
numeric_cols = [col for col in df.columns.tolist() if col.startswith(('earn_', 'sw_pay_'))]
numeric_cols 

# %%
for col in numeric_cols:
    df["w_" + col] = (df[col] * df["weight"])

w_numeric_cols = ['w_'+col for col in numeric_cols]
for col in numeric_cols:
    df[w_numeric_cols] = df[w_numeric_cols].astype(int)

# %%
df["sw_pay_diff"] = df["sw_pay_2017"] - df["sw_pay_mean_1315"]
df["earn_tot_diff"] = df["earn_tot_2017"] - df["earn_tot_mean_1315"]
df["income_diff"] = df["sw_pay_diff"] + df["earn_tot_diff"]

df["w_sw_pay_diff"] = df["w_sw_pay_2017"] - df["w_sw_pay_mean_1315"]
df["w_earn_tot_diff"] = df["w_earn_tot_2017"] - df["w_earn_tot_mean_1315"]
df["w_income_diff"] = df["w_sw_pay_diff"] + df["w_earn_tot_diff"]




# %%
df["w_impact_earn_tot_mean_1315"] = (
    df["w_earn_tot_mean_1315"] - df["earn_tot_mean_1315"]
)
df["w_impact_earn_tot_2017"] = df["w_earn_tot_2017"] - df["earn_tot_2017"]
df["w_impact_earn_tot_diff"] = df["w_earn_tot_diff"] - df["earn_tot_diff"]

df["w_impact_sw_pay_mean_1315"] = df["w_sw_pay_mean_1315"] - df["sw_pay_mean_1315"]
df["w_impact_sw_pay_2017"] = df["w_sw_pay_2017"] - df["sw_pay_2017"]
df["w_impact_sw_pay_diff"] = df["w_sw_pay_diff"] - df["sw_pay_diff"]

df["w_impact_income_diff"] = df["w_income_diff"] - df["income_diff"]

# %% [markdown]
# ## Descriptive statistics

# # %%
# import sklearn.cross_validation.Bootstrap as bootstrap
# import scipy
# for col in w_numeric_cols:
#     print(col, 
#     df.groupby('Group1')[col].apply(
#         lambda x:bootstrap.ci(data=x, statfunction=scipy.mean)))


# %%
df[["w_impact_sw_pay_mean_1315", "w_impact_earn_tot_mean_1315"]].hist(
    bins=100, log=True
)


# %%
df[["w_impact_sw_pay_2017", "w_impact_earn_tot_2017"]].hist(bins=100, log=True)

# %%
df.groupby(["cluster", "Group1"])["w_sw_pay_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_sw_pay_mean_1315"].describe()


# %%
cl_x_g = df.groupby(["cluster", "Group1"])["w_earn_tot_diff"].describe()
cl_x_g

# %%
cl_diffs = pd.pivot_table(cl_x_g, index='cluster', columns='Group1', values='mean')
cl_diffs['diff'] = cl_diffs['T1'] - cl_diffs['C1']
cl_diffs

# %%
df.groupby(["cluster", "Group1"])["earn_tot_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_earn_tot_2017"].describe()

# %%
df.groupby(["cluster", "Group1"])["sw_pay_diff"].describe()

# %%
df['w_income_1315'] = df['w_earn_tot_mean_1315'] + df['w_sw_pay_mean_1315']
df['w_income_2017'] = df['w_earn_tot_2017'] + df['w_sw_pay_2017']
df['income_1315'] = df['earn_tot_mean_1315'] + df['sw_pay_mean_1315']
df['w_income_percentage'] = df['w_income_diff'] / df['w_income_1315']


# %%
mean_income_1315 = df.groupby("Group1")["w_income_1315"].describe()
mean_income_1315

# %%
mean_income_2017 = df.groupby("Group1")["w_income_2017"].describe()
mean_income_2017

# %%
mean_income_change = mean_income_2017['mean'] / mean_income_1315['mean']
mean_income_change

# %%
mean_income_diff = mean_income_2017['mean'] - mean_income_1315['mean']
mean_income_diff

# %%
mean_income_cl_1315 = df.groupby(['cluster', "Group1"])["w_income_1315"].describe()
mean_income_cl_1315

# %%
mean_income_cl_2017 = df.groupby(['cluster', "Group1"])["w_income_2017"].describe()
mean_income_cl_2017

# %%
mean_income_cl_change = mean_income_cl_2017['mean'] / mean_income_cl_1315['mean']
mean_income_cl_change

# %%
mean_earn_tot_1315 = df.groupby("Group1")["w_earn_tot_mean_1315"].describe()
mean_earn_tot_1315

# %%
mean_earn_tot_2017 = df.groupby("Group1")["w_earn_tot_2017"].describe()
mean_earn_tot_2017

# %%
mean_earn_change = mean_earn_tot_2017['mean'] / mean_earn_tot_1315['mean']
mean_earn_change

# %%
df.groupby("Group1")["w_income_diff"].describe()

# %%
df.groupby("Group1")["w_earn_tot_diff"].describe()


# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.boxplot(
    x="Group1", y="w_income_diff", data=df, showmeans=True, palette="Set3"
)
ax.set_yscale("symlog")

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.boxplot(x="Group1", y="w_income_diff", data=df, showmeans=True, palette="Set3")
ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/box_income_diff_by_cluster_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_earn_tot_diff", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig('images/earn_diff_by_cluster_group.png')
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_earn_tot_mean_1315", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/earn_1315_by_cluster_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="earn_tot_mean_1315", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/unweighted_earn_1315_by_cluster_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/earn_1315_by_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/earn_1315_by_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_sw_pay_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/sw_pay_1315_by_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/unweighted_earn_1315_by_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="sw_pay_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/unweighted_sw_pay_1315_by_group.png')
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_earn_tot_2017", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig('images/earn_2017_by_cluster_group.png')
ax.plot()


# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_sw_pay_diff", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig('images/sw_diff_by_cluster_group.png')
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_income_diff", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig('images/income_diff_by_cluster_group.png')
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_income_diff", data=df)
fig = ax.get_figure()
fig.savefig('images/Income_diff_by_group.png')
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="t_centile", y="weight", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig('images/weights_by_centile.png')
ax.plot()



#%%
