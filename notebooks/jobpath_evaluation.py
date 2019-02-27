
# %%
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns  # package for plotting


from IPython.display import display, HTML  # Make tables pretty
from datetime import datetime

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
for col in df.columns:
    print(col)


# %% 


# %%
# Tidy up T and C columns to make processing easier
for i in range(1, 9):
    df['period_'+str(i)] = df['Group'+str(i)].str.split([0])

df.groupby('period_3')['period_4'].sum()


k# %% [markdown]
# ## Add weights


# %%
# def create_ranks (df, sorting_function):
"""
Adds a ranking column to a dataframe that has 

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

# %%
df

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
# return bin_col


# def add_weights ()

# Weighting evaluation
# Counterfactual weights


# %%
# Create T, C, and X groups for all periods
# In:       Whatever we've got already
# Out:     [periods (1, 0, -)]


# def get_weights_all_periods:
# In:       df [periods] [weighting cols] weighting_score_fn, bin_fn
# Out:     [period weight columns]

# def get_weights:
# In:      period, [weighting_cols], weighting_score_fn, bin_fn
# Out:     weight, t_bin
# 	1. create_weighting_scores
# 	2. bin_t
# 	3. bin_c
# 	4. add_weights




# %%


# %%
df[["duration_days_0"]].divide(7).hist(bins=100, log=True)


# %%
df.groupby(["Group1"])["earn_tot_mean_1315"].max() / 52


# %%
df["w_factor"] = (df["earn_tot_mean_1315"].divide(52).astype(int).multiply(10 ** 4)) + (
    df["duration_days_0"].divide(7).astype(int)
)


# %%
df_T = df.loc[df["Group1"] == "T1"]


# %%
df_C = df.loc[df["Group1"] == "C1"]


# %%
df_T["t_centile"], bins = pd.qcut(
    df_T["rank"], 100, retbins=True, labels=False)


# %%
df_T.groupby("t_centile")["rank"].describe()


# %%
t_count = df_T["t_centile"].count()
t_centile_weights = df_T.groupby(["t_centile"])[
    "rank"].count().divide(t_count)
df_T["weight"] = 1


# %%
df_C["t_centile"] = pd.cut(df_C["rank"], bins, labels=range(len(bins) - 1))
c_count = df_C["t_centile"].count()
# df_C.groupby(['t_centile'])['ppsn'].count()
c_t_centile_weights = df_C.groupby(
    ["t_centile"])["t_centile"].count().divide(c_count)
c_weights = t_centile_weights / c_t_centile_weights
df_C["weight"] = df_C["t_centile"].map(c_weights)

# %%
print(len(df_C))
df_C = df_C.dropna(subset=["weight"])
print(len(df_C))

# %%
df = df_T.append(df_C)


# %%
numeric_cols = [
    col for col in df.columns.tolist() if col.startswith(("earn_", "sw_pay_"))
]
numeric_cols

# %%
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
# End of weighting
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# %%
# Start of outcome analysis

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
cl_diffs = pd.pivot_table(cl_x_g, index="cluster",
                          columns="Group1", values="mean")
cl_diffs["diff"] = cl_diffs["T1"] - cl_diffs["C1"]
cl_diffs

# %%
df.groupby(["cluster", "Group1"])["earn_tot_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_earn_tot_2017"].describe()

# %%
df.groupby(["cluster", "Group1"])["sw_pay_diff"].describe()

# %%
df["w_income_1315"] = df["w_earn_tot_mean_1315"] + df["w_sw_pay_mean_1315"]
df["w_income_2017"] = df["w_earn_tot_2017"] + df["w_sw_pay_2017"]
df["income_1315"] = df["earn_tot_mean_1315"] + df["sw_pay_mean_1315"]
df["w_income_percentage"] = df["w_income_diff"] / df["w_income_1315"]


# %%
mean_income_1315 = df.groupby("Group1")["w_income_1315"].describe()
mean_income_1315

# %%
mean_income_2017 = df.groupby("Group1")["w_income_2017"].describe()
mean_income_2017

# %%
mean_income_change = mean_income_2017["mean"] / mean_income_1315["mean"]
mean_income_change

# %%
mean_income_diff = mean_income_2017["mean"] - mean_income_1315["mean"]
mean_income_diff

# %%
mean_income_cl_1315 = df.groupby(["cluster", "Group1"])[
    "w_income_1315"].describe()
mean_income_cl_1315

# %%
mean_income_cl_2017 = df.groupby(["cluster", "Group1"])[
    "w_income_2017"].describe()
mean_income_cl_2017

# %%
mean_income_cl_change = mean_income_cl_2017["mean"] / \
    mean_income_cl_1315["mean"]
mean_income_cl_change

# %%
mean_earn_tot_1315 = df.groupby("Group1")["w_earn_tot_mean_1315"].describe()
mean_earn_tot_1315

# %%
mean_earn_tot_2017 = df.groupby("Group1")["w_earn_tot_2017"].describe()
mean_earn_tot_2017

# %%
mean_earn_change = mean_earn_tot_2017["mean"] / mean_earn_tot_1315["mean"]
mean_earn_change

# %%
df.groupby("Group1")["w_income_diff"].describe()

# %%
df.groupby("Group1")["w_earn_tot_diff"].describe()


# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.boxplot(x="Group1", y="w_income_diff", data=df,
                 showmeans=True,)
ax.set_yscale("symlog")

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.boxplot(x="Group1", y="w_income_diff", data=df,
                 showmeans=True)
ax.set_yscale("symlog")
fig = ax.get_figure()
fig.savefig("images/box_income_diff_by_cluster_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_earn_tot_diff", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig("images/earn_diff_by_cluster_group.png")
ax.plot()
# ax.set_yscale('symlog')

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

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/earn_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_earn_tot_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/earn_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_sw_pay_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/sw_pay_1315_by_group.png")
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
ax = sns.barplot(x="Group1", y="sw_pay_mean_1315", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/unweighted_sw_pay_1315_by_group.png")
ax.plot()

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_earn_tot_2017", hue="Group1", data=df)
# ax.set_yscale('symlog')
fig = ax.get_figure()
fig.savefig("images/earn_2017_by_cluster_group.png")
ax.plot()


# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_sw_pay_diff", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig("images/sw_diff_by_cluster_group.png")
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="cluster", y="w_income_diff", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig("images/income_diff_by_cluster_group.png")
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
ax = sns.barplot(x="Group1", y="w_income_diff", data=df)
fig = ax.get_figure()
fig.savefig("images/Income_diff_by_group.png")
ax.plot()
# ax.set_yscale('symlog')

# %%
f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="t_centile", y="weight", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig("images/weights_by_centile.png")
ax.plot()


# %%


#%%


#%%
