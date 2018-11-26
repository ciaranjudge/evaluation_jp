
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
indf = pd.read_csv('jp_outcomes.zip')


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

# %%
df[["w_impact_sw_pay_mean_1315", "w_impact_earn_tot_mean_1315"]].hist(
    bins=100, log=True
)


# %%
df[["w_impact_sw_pay_2017", "w_impact_earn_tot_2017"]].hist(bins=100, log=True)


# %%
df.groupby(["cluster", "Group1"])["w_earn_tot_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_sw_pay_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_sw_pay_mean_1315"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_earn_tot_diff"].describe()


# %%
df.groupby(["cluster", "Group1"])["earn_tot_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_earn_tot_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["w_earn_tot_2017"].describe()


# %%
df.groupby(["cluster", "Group1"])["sw_pay_diff"].describe()


# %%
df.groupby(["cluster", "Group1"])["earn_tot_2017"].describe()


# %%
df.groupby("Group1")["w_impact_income_diff"].describe()


# %%
ax = sns.boxplot(
    x="Group1", y="w_earn_tot_diff", data=df, showmeans=True, palette="Set3"
)
ax.set_yscale("symlog")


# %%
ax = sns.barplot(x="cluster", y="w_earn_tot_diff", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="cluster", y="w_earn_tot_2017", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="cluster", y="earn_tot_2017", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="cluster", y="w_sw_pay_diff", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="cluster", y="sw_pay_diff", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="cluster", y="w_income_diff", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="cluster", y="w_income_diff", hue="Group1", data=df)
# ax.set_yscale('symlog')


# %%
ax = sns.barplot(x="Group1", y="income_diff", data=df)
# ax.set_yscale('symlog')


# %%
ee_df = df[df["earn_tot_mean_1315"] > 0]


# %%
ee_df_T = df_T[df_T["earn_tot_mean_1315"] > 0]
ee_df_T[["earn_tot_mean_1315"]].hist(bins=100, log=True)


# %%
sns.scatterplot(
    x="earn_tot_mean_1315", y="w_earn_tot_mean_1315", hue="Group1", data=ee_df
)


# %%
sns.scatterplot(x="earn_tot_2017", y="w_earn_tot_2017", hue="Group1", data=ee_df)


# %%
sns.scatterplot(x="weight", y="w_earn_tot_mean_1215", hue="Group1", data=ee_df)


# %%
sns.scatterplot(x="t_centile", y="w_earn_tot_mean_1315", hue="Group1", data=ee_df)


# %%
# This one should line up neatly T vs C if the centiles have worked correctly!
sns.scatterplot(x="t_centile", y="earn_tot_mean_1315", hue="Group1", data=ee_df)


# %%
# And this one should too (just not as much)
sns.scatterplot(x="t_centile", y="sw_pay_mean_1315", hue="Group1", data=ee_df)


# %%
sns.scatterplot(x="t_centile", y="duration_days_0", hue="Group1", data=ee_df)


# %%
# And this one should too (just not as much)
sns.scatterplot(x="t_centile", y="sw_pay_mean_1315", hue="Group1", data=ee_df)


# %%
sns.scatterplot(x="t_centile", y="w_sw_pay_mean_1315", hue="Group1", data=ee_df)


# %%
sns.countplot(x="t_centile", hue="Group1", data=df)


# %%
sns.barplot(x="t_centile", y="sw_pay_mean_1315", data=df_C)


# %%
sns.barplot(x="centile", y="sw_pay_mean_1315", data=df_T)


# %%
sns.distplot(df_T["duration_days_0"], bins=10)


# %%
df_C["w_duration_days_0"] = (
    (df_C["duration_days_0"] * df_C["weight"]).fillna(0).astype(int)
)
sns.distplot(df_C["w_duration_days_0"], bins=10)


# %%
sns.distplot(df_C["duration_days_0"], bins=10)


# %%
sns.scatterplot(x="t_centile", y="weight", hue="Group1", data=df)


# %%
sns.scatterplot(x="t_centile", y="weight", hue="Group1", data=df)

# %% [markdown]
# ## Treatment Group Earnings and Payment Means

# %%
sns.scatterplot(x="earn_tot_mean_1315", y="sw_pay_mean_1315", data=df_T)


# %%
sns.scatterplot(x="w_earn_tot_mean_1315", y="w_sw_pay_mean_1315", data=df_C)


# %%
df_T.groupby(["cluster"])["earn_tot_mean_1315"].mean()


# %%
df_C.groupby(["cluster"])["w_earn_tot_mean_1315"].mean()


# %%
df_C.groupby(["cluster"])["earn_tot_mean_1315"].mean()


# %%
df_C.groupby(["cluster"])["sw_mean_2013_2015_weight"].mean()


# %%
df_T.groupby(["cluster"])["sw_mean_2013_2015"].mean()


# %%
df_T.groupby(["cluster"])["sw_pay_2017"].mean()


# %%
df_C.groupby(["cluster"])["sw_pay_2017_weight"].mean()


# %%


# %%
df_C["weighted_uplift_SW"] = (
    df_C["sw_pay_2017_weight"] - df_C["sw_mean_2013_2015_weight"]
)


# %%
df_T["unweighted_uplift_SW"] = df_T["sw_pay_2017"] - df_T["sw_mean_2013_2015"]


# %%
df_C.groupby(["cluster"])["weighted_uplift_SW"].mean()


# %%
df_T.groupby(["cluster"])["unweighted_uplift_SW"].mean()


# %%
df_T["sw_mean_2013_2015"].mean()


# %%
df_T.groupby(["cluster"])["emp_2017"].mean()


# %%
df_C.groupby(["cluster"])["emp_2017"].mean()


# %%
df_C.groupby(["cluster"])["emp_2017_weight"].mean()


# %%
df_C.groupby(["cluster"])["emp_mean_weight_2013_2015"].mean()


# %% [markdown]
# weighted uplift


# %%
df_C["weighted_uplift"] = df_C["emp_2017_weight"] - df_C["emp_mean_weight_2013_2015"]


# %%
df_C.groupby(["cluster"])["weighted_uplift"].mean()


# %%
df_C["unweighted_uplift"] = df_C["emp_2017"] - df_C["emp_mean_2013_2015"]


# %%
df_C.groupby(["cluster"])["unweighted_uplift"].mean()


# %%
df_C.groupby(["cluster"])["weighted_uplift"].mean()


# %%
df_T["unweighted_uplift"] = df_T["emp_2017"] - df_T["emp_mean_2013_2015"]


# %%
df_T.groupby(["cluster"])["unweighted_uplift"].mean()


# %%


# %%
df_T["sw_pay_2017"].mean()

# %% [markdown]
# ## P - Values of variables

# %%


# # %%
# from sklearn import datasets, linear_model
# from sklearn.linear_model import LinearRegression
# import statsmodels.api as sm
# from scipy import stats

# x = ["cluster", "age", "Duration Bands", "sex"]
# y = ["jp_started_P1"]
# X = data_final[x]
# Y = data_final[y]

# X2 = sm.add_constant(X)
# est = sm.OLS(Y, X2)
# est2 = est.fit()
# print(est2.summary())

# # %% [markdown]
# # ## Variable importance by logistic

# # %%
# import numpy as np
# from sklearn.linear_model import LogisticRegression

# x = ["cluster", "age", "Duration Bands", "sex"]
# y = ["jp_started_P1"]
# X = data_final[x]
# Y = data_final[y]

# m = LogisticRegression()
# m.fit(X / np.std(X, 0), Y)

# # The estimated coefficients will all be around 1:
# print(m.coef_)

# # %% [markdown]
# # ## Variable importance by Random forest

# # %%
# from sklearn.ensemble import RandomForestRegressor

# model = RandomForestRegressor()
# x = [
#     "cluster",
#     "age",
#     "Duration Bands",
#     "sex",
#     "LM_code_rank_P1",
#     "occupation_rank_P1",
#     "ada_code_rank_P1",
#     "family_flag_rank_P1",
#     "marital_status_rank_P1",
#     "Sum_2012-2015",
# ]
# y = ["jp_started_P1"]

# X = data_final[x]
# Y = data_final[y]
# model.fit(X, Y)
# # display the relative importance of each attribute
# importances = model.feature_importances_
# # Sort it
# print("Sorted Feature Importance:")
# sorted_feature_importance = sorted(zip(importances, list(X)), reverse=True)
# print(sorted_feature_importance)

# # %% [markdown]
# # ## Propensity Scores Calculation for 8 quarters (Logistic Regression)

# # %%
# appended_data = []
# for i in range(1, 9):
#     # variable initialization
#     jp_started = "jp_started_P" + str(i)
#     data_jp_i = "data_jp_i" + str(i)
#     data_jp_i = data_final[
#         (data_final[jp_started] == 1) | (data_final[jp_started] == 0)
#     ]
#     LM_code_rank = "LM_code_rank_P" + str(i)
#     age = "age"
#     sex = "sex"
#     ada_code_rank = "ada_code_rank_P" + str(i)
#     family_flag_rank = "family_flag_rank_P" + str(i)
#     marital_status_rank = "marital_status_rank_P" + str(i)
#     Propensity = "Propensity" + str(i)
#     Probability = "Probability" + str(i)
#     # propensity score calculation
#     from sklearn.linear_model import LogisticRegression

#     names = [age, "Duration Bands", sex]
#     propensity = LogisticRegression()
#     propensity = propensity.fit(data_jp_i[names], data_jp_i[jp_started])
#     pscore = propensity.predict_proba(data_jp_i[names])[
#         :, 1
#     ]  # The predicted propensities by the model
#     data_jp_i[Propensity] = pscore
#     data_jp_i[Probability] = 1 - pscore
#     keep = [Propensity, Probability, age, sex, jp_started]
#     data_jp_i = data_jp_i[keep]
#     appended_data.append(data_jp_i)

# appended_data = pd.concat(appended_data, axis=1)


# # %%
# appended_data.head()


# # %%
# appended_data_final = []
# # Common attributes
# keep1 = ["cluster", "ppsn", "age", "sex", "Duration Bands"]
# df = data_final[(data_final[jp_started] == 1) | (data_final[jp_started] == 0)]
# df = df[keep1]
# appended_data_final = pd.concat([df, appended_data], axis=1)
# appended_data_final.to_csv("propensity_scores.csv")


# # %%


# # %% [markdown]
# # ## Propensity Scores Calculation for 1 quarter (Random Forest)

# # %%
# non_treated_df = data_final[data_final.jp_started_P1 == 0]
# treated_df = data_final[data_final.jp_started_P1 == 1]
# data_jp = pd.concat([treated_df, non_treated_df])
# data_jp = data_jp.reset_index()

# from sklearn.ensemble import RandomForestRegressor

# names2 = [
#     "Sum_2012-2015",
#     "age",
#     "occupation_rank_P1",
#     "Duration Bands",
#     "ada_code_rank_P1",
#     "cluster",
# ]

# propensity2 = RandomForestRegressor()

# propensity2 = propensity2.fit(data_jp[names2], data_jp.jp_started_P1)
# pscore2 = propensity2.predict(
#     data_jp[names2]
# )  # The predicted propensities by the model
# print(pscore2[:5])

# data_jp["Propensity_RF"] = pscore2

# # %% [markdown]
# # ## Propensity Score distribution Graphically

# # %%
# import matplotlib.pyplot as plt


# ax1 = plt.subplot2grid((1, 1), (0, 0))


# data_jp.groupby("jp_started_P1").Propensity.plot(
#     kind="hist",
#     ax=ax1,
#     alpha=0.6,
#     bins=np.arange(min(data_jp["Propensity"]), max(data_jp["Propensity"]), .05),
# )
# data_jp.groupby("jp_started_P1").Propensity.plot(
#     kind="kde", ax=ax1, secondary_y=True, legend=True
# )


# # %%
# import numpy as np
# import pandas as pd
# import seaborn as sns


# unique_vals = data_jp["jp_started_P1"].unique()
# print(unique_vals)

# # Sort the dataframe by target
# # Use a list comprehension to create list of sliced dataframes
# targets = [data_jp.loc[data_jp["jp_started_P1"] == val] for val in unique_vals]

# # Iterate through list and plot the sliced dataframe
# for target in targets:
#     sns.distplot(
#         target[["Propensity"]],
#         hist=False,
#         kde=True,
#         kde_kws={"shade": True, "linewidth": 3},
#     )


# # %%
# data_jp.Propensity.hist(by=data_jp.jp_started_P1)


# # %%
# data_jp.Propensity_RF.hist(by=data_jp.jp_started_P1)


# # %%
# import numpy as np
# import pandas as pd
# import seaborn as sns


# unique_vals = data_jp["jp_started_P1"].unique()
# print(unique_vals)

# # Sort the dataframe by target
# # Use a list comprehension to create list of sliced dataframes
# targets = [data_jp.loc[data_jp["jp_started_P1"] == val] for val in unique_vals]

# # Iterate through list and plot the sliced dataframe
# for target in targets:
#     sns.distplot(
#         target[["Propensity_RF"]],
#         hist=False,
#         kde=True,
#         kde_kws={"shade": True, "linewidth": 3},
#     )

# # %% [markdown]
# # ## Matching of Propensity score between Treatment & Control Group

# # %%
# def Match(groups, propensity, caliper=0.05):

#     # Check inputs
#     if any(propensity < 0) or any(propensity > 1):
#         raise ValueError("Propensity scores must be between 0 and 1")
#     elif not (0 < caliper < 1):
#         raise ValueError("Caliper must be between 0 and 1")
#     elif len(groups) != len(propensity):
#         raise ValueError("groups and propensity scores must be same dimension")
#     elif len(groups.unique()) != 2:
#         raise ValueError("wrong number of groups")

#     # Code groups as 0 and 1
#     groups = groups == groups.unique()[0]
#     N = len(groups)
#     N1 = groups.sum()
#     N2 = N - N1
#     g1, g2 = propensity[groups == 1], (propensity[groups == 0])

#     # Check if treatment groups got flipped - treatment (coded 1) should be the smaller
#     if N1 > N2:
#         N1, N2, g1, g2 = N2, N1, g2, g1

#     # Randomly permute the smaller group to get order for matching
#     morder = np.random.permutation(N1)
#     matches = pd.Series(np.empty(N1))
#     matches[:] = np.NAN
#     for m in morder:
#         dist = abs(g1[m] - g2)
#         if dist.min() <= caliper:
#             matches[m] = dist.idxmin()
#             g2 = g2.drop(matches[m])
#     return matches


# # %%
# # Displaying propensity scores side by side for both the groups

# stuff = Match(data_jp.jp_started_P1, data_jp.Propensity)
# g1, g2 = (
#     data_jp.Propensity[data_jp.jp_started_P1 == 1],
#     data_jp.Propensity[data_jp.jp_started_P1 == 0],
# )
# stuff = stuff.dropna()
# stuff = stuff.astype(int)


# # %%
# line = []
# line1 = []
# for i, v in stuff.iteritems():

#     # Logistic
#     line = [
#         g1[i],
#         g2[v],
#         data_jp["ppsn"][i],
#         data_jp["Duration Bands"][i],
#         data_jp.cluster[i],
#         data_jp.LM_code_rank_P1[i],
#         data_jp.marital_status_rank_P1[i],
#         data_jp.family_flag_rank_P1[i],
#         data_jp["Sum_2012-2015"][i],
#         data_jp["ppsn"][v],
#         data_jp["Duration Bands"][v],
#         data_jp.cluster[v],
#         data_jp.LM_code_rank_P1[v],
#         data_jp.marital_status_rank_P1[v],
#         data_jp.family_flag_rank_P1[v],
#         data_jp["Sum_2012-2015"][v],
#     ]
#     # Random
#     # line=[g1[i],g2[v],data_jp['ppsn'][i],data_jp['Duration Bands'][i],data_jp.cluster[i],data_jp.age[i],data_jp.ada_code_rank_P1[i],data_jp.occupation_rank_P1[i],data_jp['Sum_2012-2015'][i],data_jp['ppsn'][v],data_jp['Duration Bands'][v],data_jp.cluster[v],data_jp.age[v],data_jp.ada_code_rank_P1[v],data_jp.occupation_rank_P1[v],data_jp['Sum_2012-2015'][v]]

#     line1.append(line)

# df = pd.DataFrame(line1)


# # %%
# # df.to_csv('propensity_Random.csv')
# df.to_csv("propensity_logistic.csv")

# # %% [markdown]
# # ### Description of outcome, by cluster

# # %%
# list(data_jp)


# # %%
# print(" ")
# # Crosstab of outcomes, raw numbers
# tab_lab_detoutcome_countsP1 = pd.crosstab(
#     index=data_jp["status_simple_P1"], columns=data_jp["cluster"], margins=True
# ).round(2)

# display(tab_lab_detoutcome_countsP1)
# # Select totals columns
# # totals = tab_lab_detoutcome_counts['All'].map(lambda x: "{:,}".format(x))

# # # Cross tab of outcomes, normalised for each nationality group.
# tab_lab_detoutcome_freq1 = pd.crosstab(
#     index=data_jp["status_simple_P1"], columns=data_jp["cluster"], normalize="columns"
# ).round(2)

# display(tab_lab_detoutcome_freq1)


# tab_lab_detoutcome_freq2 = pd.crosstab(
#     index=data_jp["status_simple_P2"], columns=data_jp["cluster"], normalize="columns"
# ).round(2)

# display(tab_lab_detoutcome_freq2)

# tab_lab_detoutcome_freq3 = pd.crosstab(
#     index=data_jp["status_simple_P3"], columns=data_jp["cluster"], normalize="columns"
# ).round(2)

# display(tab_lab_detoutcome_freq3)

# tab_lab_detoutcome_freq4 = pd.crosstab(
#     index=data_jp["status_simple_P4"], columns=data_jp["cluster"], normalize="columns"
# ).round(2)

# display(tab_lab_detoutcome_freq4)

# tab_lab_detoutcome_freq5 = pd.crosstab(
#     index=data_jp["status_simple_P5"], columns=data_jp["cluster"], normalize="columns"
# ).round(2)

# display(tab_lab_detoutcome_freq5)

# tab_lab_detoutcome_freq6 = pd.crosstab(
#     index=data_jp["status_simple_P6"], columns=data_jp["cluster"], normalize="columns"
# ).round(2)

# display(tab_lab_detoutcome_freq6)


# # # Reorder columns
# # tab_lab_detoutcome_freq = tab_lab_detoutcome_freq[column_order]

# # # Create a row containing totals
# # tab_lab_detoutcome_freq.loc['All'] = tab_lab_detoutcome_freq.sum().round(1)

# # # Join totals and frequency tables
# # joined_table = pd.concat([tab_lab_detoutcome_freq, totals], axis = 1)

# # # rename rows and columns
# # joined_table = joined_table.rename(columns = {'All' : 'N'})
# # joined_table = joined_table.rename({'All' : 'All outcomes'})

# # # output
# # display(joined_table)

# # # spacing for output
# # print(color.BOLD + 'Table 18: ' + color.END +' Labour market outcomes by nationality group' )
# # print('(Source: DEASP Administrative Data)')


# print(" ")
# in_LR = data_jp.loc[data_jp["status_simple_P2"] == "On Live Register"]


# in_LR_by_sex = pd.crosstab(
#     index=in_LR["sex"], columns=in_LR["cluster"], margins=True
# ).round(2)


# display(in_LR_by_sex)
# # in_emp_by_age = in_emp_by_age[column_order]
# # in_LR_by_sex['Total'] = in_LR_by_sex.sum(axis =1)


# # display(in_emp_by_age.applymap(lambda x: "{:,}".format(x)))
# # print(color.BOLD + 'Table X: ' + color.END)


# # display(claims_by_age_nat.astype(int).applymap(lambda x: "{:,}".format(x)))
# # print(color.BOLD + 'Table X: ' + color.END)

# # proportion_employment = in_emp_by_age.div(claims_by_age_nat).mul(100).round(2)

# # display(proportion_employment[:-1])


# # %%

