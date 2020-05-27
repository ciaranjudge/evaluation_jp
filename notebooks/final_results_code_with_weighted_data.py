# %%
#importing necessary libraries and test data
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm
import numpy as np
from stargazer.stargazer import Stargazer
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv ('test_data.csv')
df.info()


# %%
#for tabulating variables
df.income.value_counts()

#removing object
# del result

#adding fixed effects
result = smf.wls(formula = "age_cont ~ gender + outside_duration + C(age)",  data = df, missing = "drop", weights = df["weight"]).fit()
result.params
result.params["outside_duration"]

#robust SEs
result.HC1_se
result.HC1_se["outside_duration"]


# %%
#what to do?
#after importing everything and ready to run models, create empty arrays for coefs and SEs
coefs = []
ses = []

#first model
result = smf.wls(formula = "age_cont ~ outside_duration",  data = df, missing = "drop", weights = df["weight"]).fit(cov_type = "HC1")

#get coefs and SEs
coefs = coefs + [result.params["outside_duration"]]
ses = ses + [result.HC1_se["outside_duration"]]

#second model
result2 = smf.wls(formula = "age_cont ~ gender + outside_duration",  data = df, missing = "drop", weights = df["weight"]).fit(cov_type = "HC1")

#get coefs and SEs
coefs = coefs + [result2.params["outside_duration"]]
ses = ses + [result2.HC1_se["outside_duration"]]

#third model
result3 = smf.wls(formula = "age_cont ~ gender + outside_duration + C(year)",  data = df, missing = "drop", weights = df["weight"]).fit(cov_type = "HC1")

#get coefs and SEs
coefs = coefs + [result3.params["outside_duration"]]
ses = ses + [result3.HC1_se["outside_duration"]]

#fourth model
result4 = smf.wls(formula = "age_cont ~ gender + outside_duration + C(year) + C(education)",  data = df, missing = "drop", weights = df["weight"]).fit(cov_type = "HC1")

#get coefs and SEs
coefs = coefs + [result4.params["outside_duration"]]
ses = ses + [result4.HC1_se["outside_duration"]]

#printing regression results 
stargazer = Stargazer([result, result2, result3, result4])
print(stargazer.render_latex())


# %%
#Logit but first make gender binary
df["gender"] = df["gender"].replace(1, 0)
df["gender"] = df["gender"].replace(2, 1)

res_logit = smf.glm("gender ~ age_cont + outside_duration ", data = df, family = sm.families.Binomial(), var_weights = np.asarray(df['weight'])).fit(cov_type = "HC1")
#I could not find printing function. So, it will be done manually.
print(res_logit.summary())

# %%
#for the visualization purposes
coefs = coefs + [.8*x for x in coefs]
ses = ses + ses
ses = [5*x for x in ses] # this is to make graph better

#getting upper and lower values for 95% CIs as well as labels
upper = list(np.array(coefs) + np.array(ses)*1.96)
lower = list(np.array(coefs) - np.array(ses)*1.96)
quarters = ["Q1-16", "Q2-16", "Q3-16", "Q4-16", "Q1-17", "Q2-17", "Q3-17", "Q4-17" ]
earnings = [4000, 5000, 6000, 4500, 3500, 5000, 5500, 3000]
welfare_payments = [-2000, -1000, -500, -1500, -2500, -1000, -500, -2000]

#creating a dataframe
plot_data = pd.DataFrame({"coefs":coefs, "ses":ses, "upper":upper, "lower":lower, "quarters":quarters, "earnings":earnings, "welfare_payments":welfare_payments})


# %%
#PLOTTING####

# %%
#simple plot
plt.figure(figsize = (12,9))
sns.lineplot(x="quarters", y="coefs", sort = False, data=plot_data)
#plt.title("Line Plot", fontsize = 20)
plt.xlabel("")
plt.ylabel("")
#i think there is no need for title and labels

#%%
#bar CIs
plt.figure(figsize = (12,9))
plt.errorbar(plot_data["quarters"], plot_data["coefs"], yerr = plot_data["ses"]*1.96, fmt = 'k')
plt.ylim([0, .8])
plt.tick_params(axis = 'x', labelsize = 16)
plt.tick_params(axis = 'y', labelsize = 16)
#if you like to change the color, put "o" instead of "k" for fmt
plt.xlabel("")
plt.ylabel("")
plt.title("Errorbar", fontsize=20)
plt.savefig("errorbar.pdf")



# %%
#Shaded area CIs - This one has all functions that I think necessary
plt.figure(figsize = (12,9))
plt.errorbar(x = "quarters", y = "coefs",  data = plot_data, color = "black", yerr = None)
plt.fill_between(quarters, lower, upper, alpha = .2, color = "k")#if you like to change the color, put "o", instead of "k"
plt.xlabel("")
plt.ylabel("")
plt.title("Shaded", fontsize=20)
plt.tick_params(axis = 'x', labelsize = 16)
plt.tick_params(axis = 'y', labelsize = 16)
plt.ylim([0, .8])
plt.savefig("shaded.pdf")

# %%
#line CIs
plt.figure(figsize = (12,9))
plt.plot(plot_data["quarters"], plot_data["coefs"], "k")
plt.plot(plot_data["quarters"], plot_data["upper"], "--k")
plt.plot(plot_data["quarters"], plot_data["lower"], "--k")
plt.xlabel("")
plt.ylabel("")
plt.title("Line", fontsize=20)
plt.tick_params(axis = 'x', labelsize = 16)
plt.tick_params(axis = 'y', labelsize = 16)
plt.ylim([0, .8])
plt.savefig("line.pdf")


# %%
#bar plot
plt.figure(figsize = (12,9))
plt.bar(quarters, plot_data["earnings"], align='center', color = "blue")
plt.bar(quarters, plot_data["welfare_payments"], align='center', color = "red" )
plt.xlabel("")
plt.ylabel("")
plt.title("Difference Bar Plot", fontsize=20)
plt.tick_params(axis = 'x', labelsize = 16)
plt.tick_params(axis = 'y', labelsize = 16)
plt.savefig("difference_bar.pdf")

#text for it will be:- Blue bars denote the difference in earnings from employment between jobpath referrals and non-referrals, red bars denote the difference in welfare payments. In other words, blue bars can be understood as how much more JobPath referrals earn than non-referrals and red bars as how much more non-referrals receive welfare benefits. 

#PATTERN FOR BARS
#Blue denotes the difference in earnings (jobpath - non-jobpath) and red difference in social welfare payments


# %%
#merged error bar
fig, ax1 = plt.subplots()
ax1.errorbar(plot_data["quarters"], plot_data["coefs"], yerr=plot_data["ses"]*1.96, color = "black")
ax1.set_xlabel("")
ax1.set_ylabel("")
ax1.tick_params(axis='x', labelsize=12)
ax1.tick_params(axis='y', labelsize=14)
ax1.set_ylim([0, .8])

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

ax2.bar(quarters, plot_data["earnings"], align='center', color = "blue", alpha = .1)
ax2.bar(quarters, plot_data["welfare_payments"], align='center', color = "red", alpha = .1)
ax2.set_ylim([-10000, 100000])
ax2.axes.get_yaxis().set_ticks([])

fig.tight_layout()  # otherwise the right y-label is slightly clipped


# %%
#merging line bar
fig, ax1 = plt.subplots()
ax1.plot(plot_data["quarters"], plot_data["coefs"], "k")
ax1.plot(plot_data["quarters"], plot_data["upper"], "--k")
ax1.plot(plot_data["quarters"], plot_data["lower"], "--k")
ax1.set_xlabel("")
ax1.set_ylabel("")
ax1.tick_params(axis='x', labelsize=12)
ax1.tick_params(axis='y', labelsize=14)
ax1.set_ylim([0, .8])

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

ax2.bar(quarters, plot_data["earnings"], align='center', color = "blue", alpha = .1)
ax2.bar(quarters, plot_data["welfare_payments"], align='center', color = "red", alpha = .1)
ax2.set_ylim([-10000, 100000])
ax2.axes.get_yaxis().set_ticks([])

fig.tight_layout()  # otherwise the right y-label is slightly clipped


# %%
#merged shaded graph
fig, ax1 = plt.subplots()
ax1.errorbar(x = "quarters", y = "coefs",  data = plot_data, color = "black", yerr = None)
ax1.fill_between(quarters, lower, upper, alpha = .2, color = "k")#if you like to change the color, put "o", instead of "k"
ax1.set_xlabel("")
ax1.set_ylabel("")
ax1.tick_params(axis='x', labelsize=12)
ax1.tick_params(axis='y', labelsize=14)
ax1.set_ylim([0, .8])

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

ax2.bar(quarters, plot_data["earnings"], align='center', color = "blue", alpha = .3)
ax2.bar(quarters, plot_data["welfare_payments"], align='center', color = "red", alpha = .3)
ax2.set_ylim([-10000, 100000])
ax2.axes.get_yaxis().set_ticks([])

fig.tight_layout()  # otherwise the right y-label is slightly clipped

# %%
#black and white for bars

fig, ax1 = plt.subplots()
ax1.errorbar(x = "quarters", y = "coefs",  data = plot_data, color = "black", yerr = None)
ax1.fill_between(quarters, lower, upper, alpha = .2, color = "k")#if you like to change the color, put "o", instead of "k"
ax1.set_xlabel("")
ax1.set_ylabel("")
ax1.tick_params(axis='x', labelsize=12)
ax1.tick_params(axis='y', labelsize=14)
ax1.set_ylim([0, .8])

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

ax2.bar(quarters, plot_data["earnings"], align='center', color = "dimgray")
ax2.bar(quarters, plot_data["welfare_payments"], align='center', color = "lightgray")
ax2.set_ylim([-10000, 100000])
ax2.axes.get_yaxis().set_ticks([])

fig.tight_layout()  # otherwise the right y-label is slightly clipped

#colors and the rest can be easily fixed

# %%
