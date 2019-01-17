# %%
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np

import pandas as pd
import seaborn as sns  # package for plotting
from IPython.display import HTML, display  # Make tables pretty

from scipy import stats

sns.set()


# %% [markdown]
# ## Import outcomes dataset and tidy up

# %%
df = pd.read_csv("data/jp_outcomes.zip")


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
# Need to use multiindex!
periods = []
for i in range(1, 9):
    this_period = "period_" + str(i)
    df[this_period] = df["Group" + str(i)].str[:1]
    periods.append(this_period)

# Eliminate earlier T people from later period C
for i in range(1, 9):
    for j in range(i + 1, 9):
        df["period_" + str(j)].loc[df["period_" + str(i)] == "T"] = 0

# %%
df.head()

# %%
# Remove outliers
numeric_cols = [
    col for col in df.columns.tolist() if col.startswith(("earn_", "sw_pay_"))
]

# df1 = df[(np.abs(stats.zscore(df[numeric_cols])) < 5).all(axis=1)]

# %% [markdown]
# ## Look - a group triangle!

# %%
grouped = df.groupby(periods)["id"].count()
grouped.sort_index(ascending=False, inplace=True)
grouped


# %% [markdown]
# ## Add weights
# This is just a global sort but needs to be replaced by a per-group classification function
# called at the start of the binning/weighting code

# %%
# Sort all records
df = df.set_index(
    [
        # 'cluster',
        "earn_tot_mean_1315",
        "sw_pay_mean_1315",
        "duration_days_0",
        "id",
    ]
)
df.sort_index(inplace=True)
df.reset_index(inplace=True)
df.index.name = "rank"
# # np.sum(df1.index.duplicated())
df.reset_index(inplace=True)
df.set_index("id", inplace=True)

# %%
def add_weights(df, period):
    # 1. Create scores
    # Currently done above with global ranks but need to replace

    # 2. Create bins based on scores
    # Create a temporary df for T and C groups
    df_T = df.loc[df[period] == "T"].copy()
    df_C = df.loc[df[period] == "C"].copy()

    # Split T group into equal sized bins
    df_T["bin"], bins = pd.qcut(df_T["rank"], 100, retbins=True, labels=False)

    # Put C group into T bins based on T bin edges
    df_C["bin"] = pd.cut(df_C["rank"], bins, labels=range(len(bins) - 1))

    # Exclude unassigned C group members to eliminate outliers
    df_C = df_C.dropna(subset=["bin"])
    df_C["bin"] = df_C["bin"].astype("int")

    # 3. Add weights based on bins
    # Create counts for T and C by bin
    t_bin_counts = pd.Series(df_T.groupby("bin")["rank"].count(), name="t_bin_counts")
    c_bin_counts = pd.Series(df_C.groupby("bin")["rank"].count(), name="c_bin_counts")
    bin_counts = pd.concat([t_bin_counts, c_bin_counts], axis="columns")

    # Divide T by C to get weights for C group
    bin_counts["abs_weight"] = bin_counts["t_bin_counts"] / bin_counts["c_bin_counts"]
    c_total = df_C.shape[0]
    t_total = df_T.shape[0]
    bin_counts["weight"] = bin_counts["abs_weight"] * c_total / t_total

    # All Ts have weight = 1
    df_T["weight"] = 1
    df_T["abs_weight"] = 1

    # Assign C weights based on weights in bin_counts dataframe
    # Have to reset and then set index to avoid losing it!
    df_C = df_C.reset_index()
    df_C = df_C.merge(bin_counts[["abs_weight", "weight"]], how="inner", on="bin")
    df_C = df_C.set_index('id')

    # Append T and C dataframes together
    out_df = df_T.append(df_C)
    out_df = out_df[["weight", "abs_weight"]]

    return out_df

# %%
for period in periods:
    w_df = add_weights(df, period)
    w_df.columns = [period + '__' + str(col) for col in w_df.columns]
    df = pd.concat([df, w_df], axis='columns')

df.info(verbose=True)

# %%
grouped = df.groupby(periods)["rank"].count()
grouped.sort_index(ascending=False, inplace=True)
grouped

# %%
for period in periods:
    aw_col = period + '__abs_weight'
    print(df.groupby(period)[aw_col].sum())

# %% [markdown]
# ## Create weighted versions of background and outcome columns
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

# %% [markdown]
# ### Weighting impact

# %%
df[["w_impact_sw_pay_mean_1315", "w_impact_earn_tot_mean_1315"]].hist(
    bins=100, log=True
)

# %%
f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="bin", y="weight", hue="Group1", data=df)
fig = ax.get_figure()
fig.savefig("images/weights_by_bin.png")
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
sns.scatterplot(x="sw_pay_mean_1315", y="earn_tot_mean_1315", hue="Group1", data=df)
ax.set_yscale("symlog")
fig = ax.get_figure()
fig.savefig("images/earnings_vs_sw_pay.png")
ax.plot()

# %%

f, ax = plt.subplots(dpi=1000)
sns.scatterplot(x="w_sw_pay_mean_1315", y="w_earn_tot_mean_1315", hue="Group1", data=df)
ax.set_yscale("symlog")
fig = ax.get_figure()
fig.savefig("images/w_earnings_vs_sw_pay.png")
ax.plot()


# %%
