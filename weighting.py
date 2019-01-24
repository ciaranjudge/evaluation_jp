# %%
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np

import pandas as pd
import seaborn as sns
from IPython.display import HTML, display

from scipy import stats

sns.set()


# %% [markdown]
# ## Import outcomes dataset and tidy up

# %%
df = pd.read_csv("data/jp_outcomes.csv")


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
periods = pd.PeriodIndex(start="2016Q1", end="2017Q4", freq="Q")
period_list = list(periods.strftime("%YQ%q"))
periods_len = len(period_list)

for i, period in enumerate(reversed(period_list)):

    df[period] = df["Group" + str(periods_len - i)].str[:1]
    cat_map = {"T": 1, "C": 0, "0": -1}
    df[period] = df[period].map(cat_map)
    if i > 0:
        later_periods = period_list[-i:]
        df.loc[df[period] == 1, later_periods] = -2
    # df[period] = df[period].fillna(-1)
    # df[period] = df[period].astype('int')
# %%
numeric_cols = [
    col for col in df.columns.tolist() if col.startswith(("earn_", "sw_pay_"))
]

# %%
grouped = df.groupby(period_list)["id"].count()
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


# %%
def add_weights(df, period):
    # 1. Create scores
    # Currently done above with global ranks but need to replace

    # 2. Create bins based on scores
    # Create a temporary df for T and C groups
    df_T = df.loc[df[period] == 1].copy()
    df_C = df.loc[df[period] == 0].copy()

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
    bin_counts["weight"] = bin_counts["t_bin_counts"] / bin_counts["c_bin_counts"]
    # c_total = df_C.shape[0]
    # t_total = df_T.shape[0]
    # bin_counts["weight"] = bin_counts["abs_weight"] * c_total / t_total

    # All Ts have weight = 1
    df_T["weight"] = 1

    # Assign C weights based on weights in bin_counts dataframe
    # Have to reset and then set index to avoid losing it!
    df_C = df_C.reset_index()
    df_C = df_C.merge(bin_counts[["weight"]], how="inner", on="bin")
    df_C = df_C.set_index("id")

    # Append T and C dataframes together
    out_df = df_T.append(df_C)

    # Select columns to return
    return_cols = ["weight", "bin"]
    out_df = out_df[return_cols]

    # Create multiindex for consistency with groups and counterfactuals
    columns = pd.MultiIndex.from_product(
        [[period], return_cols, ["_"], ["_"]],
        names=["period", "data_type", "cf_cutoff", "cf_period"],
    )
    out_df.columns = columns

    # Finally ready!
    print(f"\n------------------\nPeriod: {period}")
    print(f"T group size:     {df_T['weight'].sum()}")
    print(f"Sum of C weights: {df_C['weight'].sum()}")
    return out_df


# %%
# Create dedicated dataframe for groups, weights, bins, counterfactuals
# Start with periods...
w_df = df[period_list]

# Add multiindex...
columns = pd.MultiIndex.from_product(
    [list(w_df.columns), ["group"], ["_"], ["_"]],
    names=["period", "data_type", "cf_cutoff", "cf_period"],
)
w_df.columns = columns

# Then add weights and bins...
for i, period in enumerate(period_list):
    w_df = pd.concat([w_df, add_weights(df, period)], axis="columns", sort=False)

# %%


# %%
# Create counterfactual weights
# As the ox ploughs!
# First, go forwards, adding CF weight columns for each period
for i, this_period in enumerate(period_list[:-1]):

    this_group = (this_period, "group", "_", "_")
    this_bin = (this_period, "bin", "_", "_")
    this_real_weight = (this_period, "weight", "_", "_")

    later_period_list = period_list[i + 1 :]
    print(f"\n---------\nThis period: {this_period}")
    for j, later_period in enumerate(later_period_list):
        later_group = (later_period, "group", "_", "_")
        later_bin = (later_period, "bin", "_", "_")
        later_real_weight = (later_period, "weight", "_", "_")

        this_later_df = pd.DataFrame(
            w_df[
                (w_df[this_group] == 0)
                & (w_df[later_group].isin([1, 0]))
            ]
        )
        this_later_df = this_later_df[[this_period, later_period]]
        g_this_later_df = this_later_df.groupby([later_group])
        print(f"Later period: {later_period}")
        for name, group in g_this_later_df:
            print(f"Group name: {name}", group[later_real_weight].sum())

        # g_later_group_this_bin = this_later_df.groupby([later_group, this_bin])

        # for name, group in g_later_group_this_bin:
        #     print(name)


# column_index = w_df.columns.get_loc(("2016Q2", "abs_weight", "_", "_"))

# w_df.loc[
#     (w_df[("2016Q1", "group", "_", "_")] == 0) & (w_df[("2016Q2", "group", "_", "_")]
#     == 1)
# ].describe()

# .groupby(("2016Q1", "bin", "_", "_"))[column_index].sum()

# %%


# %% [markdown]
# ## Look - a group triangle!

# %%
grouped = df.groupby(period_list)["rank"].count()
grouped.sort_index(ascending=False, inplace=True)
grouped

# %%

for period in period_list:
    aw_col = period + "__abs_weight"
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
