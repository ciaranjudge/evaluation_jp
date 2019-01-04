

# %%
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np

import pandas as pd
import seaborn as sns  # package for plotting
from IPython.display import HTML, display  # Make tables pretty

sns.set()


# %% [markdown]
# ## Import outcomes dataset and tidy up

# %%
df = pd.read_csv("jp_outcomes.zip")


# %%
# Convert floats to ints to make reporting easier
numerics = ["float64"]
for col in df.select_dtypes(include=numerics).columns:
    if df[col].isnull().sum() == 0:
        # print(col)
        df[col] = df[col].astype("int")
# for col in df.columns:
#     print(col)

# %%
# Tidy up T and C columns to make processing easier
periods = []
for i in range(1, 9):
    this_period = 'period_'+str(i)
    df[this_period] = df['Group'+str(i)].str[:1]
    periods.append(this_period)

# Eliminate earlier T people from later period C
for i in range(1, 9):
    for j in range(i+1, 9):
        df['period_'+str(j)].loc[df['period_'+str(i)] == 'T'] = 0

# %% [markdown]
# ## Look - a group triangle!

# %%
grouped = df.groupby(periods)['id'].count()
grouped.sort_index(ascending=False, inplace=True)
grouped


# %% [markdown]
# ## Add weights


# %%
# def create_ranks (df, sorting_function):
"""
Adds a ranking column to a dataframe based on a sorting function

Given a ranking function and a dataframe with the columns needed for that
ranking function, this function returns a ranking of all the rows.

Parameters
----------
df : DataFrame
    Dataframe with unique index and columns needed for rank_function
sorting_function : function
    A sorting algorithm which returns a sorted version of a given dataframe

Returns
-------
rank : Series
    A pandas series with a unique rank for every row in df

See Also
--------
# List ranking algorithms here!

"""
# # do the actual stuff
df = df.set_index([
    # 'cluster',
    'earn_tot_mean_1315',
    'duration_days_0',
    'id'])
df.sort_index(inplace=True)
df.reset_index(inplace=True)
df.index.name = 'rank'
# # np.sum(df1.index.duplicated())
df.reset_index(inplace=True)
df.set_index('id', inplace=True)

# return rank

# %%
# def create_bins (df, bin_method, bin_size, bin_number):
"""
Assigns records to bins, given a ranking and specified number or size of bins



Parameters
----------
df: DataFrame
    Must have T vs C column and ranks
bin_method: String
    "size" or "count"
bin_size: int
    Max or min?
bin_count: int
    Max or min


Returns
-------
bin_col: Series

"""
# # do the actual stuff

# %%
df_T = df.loc[df["Group1"] == "T1"].copy()
df_C = df.loc[df["Group1"] == "C1"].copy()

df_T["t_centile"], bins = pd.qcut(
    df_T["rank"], 100, retbins=True, labels=False)

df_C["t_centile"] = pd.cut(df_C["rank"], bins, labels=range(len(bins) - 1))
# return bin_col


# def add_weights ()
"""Create T, C, and X groups for all periods
# In:       Whatever we've got already
# Out:     [periods (1, 0, -)]


# def get_weights_all_periods:
# In:       df [periods] [weighting cols] weighting_score_fn, bin_fn
# Out:     [period weight columns]

# def get_weights:
# In:      period, [weighting_cols], weighting_score_fn, bin_fn
# Out:     weight, t_bin

"""
# 	1. Create ranks
# 	2. Create T and C bins based on ranks
# 	3. Add weights
t_count = df_T["t_centile"].count()
t_centile_weights = df_T.groupby(["t_centile"])[
    "rank"].count().divide(t_count)
df_T["weight"] = 1
c_count = df_C["t_centile"].count()

c_t_centile_weights = df_C.groupby(
    ["t_centile"])["t_centile"].count().divide(c_count)
c_weights = t_centile_weights / c_t_centile_weights
df_C["weight"] = df_C["t_centile"].map(c_weights)

df = df_T.append(df_C)

# %% [markdown]
# ## Create weighted versions of background and outcome columns
# %%
numeric_cols = [
    col for col in df.columns.tolist() if col.startswith(("earn_", "sw_pay_"))
]

for col in numeric_cols:
    df["w_" + col] = df[col] * df["weight"]

w_numeric_cols = ["w_" + col for col in numeric_cols]
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

# %%
# import sklearn.cross_validation.Bootstrap as bootstrap
# import scipy
# for col in w_numeric_cols:
#     print(col,
#     df.groupby('Group1')[col].apply(
#         lambda x:bootstrap.ci(data=x, statfunction=scipy.mean)))

# %% [markdown]
# ### Weighting impact

# %%
df[["w_impact_sw_pay_mean_1315", "w_impact_earn_tot_mean_1315"]].hist(
    bins=100, log=True
)

# %%
f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="t_centile", y="weight", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig("images/weights_by_centile.png")
ax.plot()

# %% [markdown]
# ### Previous earnings analysis

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/earn_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/unweighted_earn_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_earn_tot_mean_1315", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/earn_1315_by_cluster_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="earn_tot_mean_1315", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/unweighted_earn_1315_by_cluster_group.png")
ax.plot()

# %% [markdown]
# ### Previous social welfare payment analysis

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_sw_pay_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/sw_pay_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="sw_pay_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/unweighted_sw_pay_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_sw_pay_mean_1315", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/sw_pay_1315_by_cluster_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="sw_pay_mean_1315", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/sw_pay_1315_by_cluster_group.png")
ax.plot()


# %%

f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="sw_pay_mean_1315", y="earn_tot_mean_1315",
                hue="Group1", data=df)
ax.set_yscale("symlog")
fig = ax.get_figure()
fig.savefig("images/earnings_vs_sw_pay.png")
ax.plot()

# %%

f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="w_sw_pay_mean_1315",
                y="w_earn_tot_mean_1315", hue="Group1", data=df)
ax.set_yscale("symlog")
fig = ax.get_figure()
fig.savefig("images/w_earnings_vs_sw_pay.png")
ax.plot()


# %%
