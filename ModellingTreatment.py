
#%%

import sys
import pandas as pd              # python package for dataframes
import os                        # used to change directory paths
import matplotlib.pyplot as plt  # python package for plotting
import numpy as np
import seaborn as sns #package for plotting
from scipy.stats import norm
from sklearn.preprocessing import StandardScaler
from scipy import stats
from IPython.display import display, HTML  # Make tables pretty
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
from sklearn import preprocessing
from sklearn.linear_model import Lasso
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_val_score
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.naive_bayes import GaussianNB
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import statsmodels.api as sm
#import sklearn.cross_validation.Bootstrap as bootstrap
import scipy
import zipfile
import gzip

#%% [markdown]
# ### import files and merge

#%%
path_data = "\\\cskma0294\\F\\Evaluations\\JobPath"
os.chdir(path_data)
df = pd.read_csv("jp_outcomes.csv")



# Convert floats to ints to make reporting easier
numerics = ["float64"]
for col in df.select_dtypes(include=numerics).columns:
    if df[col].isnull().sum() == 0:
        # print(col)
        df[col] = df[col].astype("int")




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

df.shape


#%%

path_data = "\\\cskma0294\\F\\Evaluations\\JobPath\\Quarterly_status"
os.chdir(path_data)

df_shares = pd.read_csv("shares_2017.zip")
df=pd.merge(df, df_shares, on='ppsn', how='left')

df.head()


#%%
df.drop('Unnamed: 0', axis=1)


#%%
df.set_index('id', drop=True)


#%%
df.drop('Unnamed: 0', axis=1,inplace = True)


#%%
df['earn_tot_mean_1315'].groupby['y']


#%%
#group_ycl=df['earn_tot_mean_1315'].groupby(df['y', 'cluster']).mean()

un=df.groupby(['y', 'cluster'])['earn_tot_mean_1315'].describe()


#%%
un.plot()


#%%
fig, ax = plt.subplots(figsize=(15,7))
df.groupby(['y', 'cluster']).mean()['earn_tot_mean_1315'].unstack().plot(df, kind='barh', ax=None, subplots=False, sharex=None, sharey=False, layout=None, figsize=None, use_index=True, title=None, legend=True)
#data.groupby(['date','type']).count()['amount'].unstack().plot(ax=ax)


#%%
display(group_ycl)


#%%


#%% [markdown]
# ### select features/variables

#%% [markdown]

features =['sw_pay_2013',
            'sw_pay_2014',
            'sw_pay_2015',
            'earn_tot_2012',
            'earn_tot_2013',
            'earn_tot_2014',
            'earn_tot_2015',
            'age', 
            'Duration Bands',
            'family_flag_rank_P1',
            'total_duration_days',
            'Empl_13_15sum',
            'Ed_or_Training_13_15share',
            'LM_WSW_13_15share',
            'LR_13_15sum',
            'WSW_13_15share',
          ]

#%%
### Model function


X=features 
y='2016Q1'
#%%
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=21, stratify=y)

def assign_propensity (df, X, y):
    model_df=df[df[y].isin([0,1])]
    X=model_df[X].values
    y=model_df[y].values
#    pipeline_optimizer = TPOTClassifier()
#   pipeline_optimizer = TPOTClassifier(generations=5, population_size=50, cv=5,
#   random_state=42, verbosity=2)
    logreg = LogisticRegression(C=25.0, dual=False, penalty="l1")
    logreg.fit(X, y)
    coefficients = pd.concat([pd.Series(X),pd.DataFrame(np.transpose(logreg.coef_))], axis = 1)
    model_df['propensity']=logreg.predict_proba(X)[:,1]
    return model_df['propensity'], coefficients

a, c=assign_propensity(df, features, '2016Q1')
#%%
#df2=pd.merge(df, a, on='ppsn', how='left')
df2=pd.concat([df, a],axis = 1)
#%%


logit_roc_auc = roc_auc_score(y, logreg.predict(X))
fpr, tpr, thresholds = roc_curve(y, logreg.predict_proba(X)[:,1])
plt.figure()
plt.plot(fpr,tpr,label="Logistic Regression, auc="+str(auc))
# label='Logistic Regression (area = %0.2f)' % )
plt.plot([0, 1], [0, 1],'r--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver operating characteristic')
plt.legend(loc="lower right")
#plt.savefig('Log_ROC')
plt.show()

## here are the number of obs, the constant term, the goodness of fit, the 
## variance around point estimates etc...
#%%
a=logreg.predict_proba(X)
y_pred_prob = a[:,1]
roc_auc_score(y, y_pred_prob)
print(y_pred_prob)
type(y_pred_prob)
y_pred_prob.shape
pp= pd.Series(y_pred_prob)
print(roc_auc_score)
logreg.score(X, y)
y.mean()

#what share of people are treated? 
# ie if you had to predict, going for no would give better results
# logreg.score and y.mean are getting at this

#%%
#%%

select=df[df['2016Q1']!=-1]
select['y']=df['2016Q1']
#y=['y_1']

select['y'].value_counts()
select=select.set_index('id',drop=True)
select=select.sort_index()
select=select.reset_index()


#%%
select.index.names=['2016Q1_index']


#%%
select.tail()


#%%
#X = data_final.loc[:, data_final.columns != 'y']
y = select.loc[:, select.columns == 'y']

#features =['sw_pay_mean_1315', 'earn_tot_mean_1315', 'age', 'occupation_rank_P1','Duration Bands', 'cluster', 
#  'family_flag_rank_P1', 'WSW_13_15share', 'Ed_or_Training_13_15share', 'LM_WSW_13_15share']
features =['sw_pay_2013',
            'sw_pay_2014',
            'sw_pay_2015',
            'earn_tot_2012',
            'earn_tot_2013',
            'earn_tot_2014',
            'earn_tot_2015',
           'age', 
           #'occupation_rank_P1',
          'Duration Bands',
           #'LM_code_rank_P1',
          #'LM_WSW_13_15share',
           'family_flag_rank_P1',
           #'LM_WSW_13_15share',
           #'hist_lr_0',
           'total_duration_days',
           # 'Duration Bands',
           #'Empl_13_15share',
            'Empl_13_15sum',
 'Ed_or_Training_13_15share',
 #'Ed_or_Training_13_15sum',
 'LM_WSW_13_15share',
 #'LM_WSW_13_15sum',
 #'LR_13_15share',
 'LR_13_15sum',
 'WSW_13_15share',
 #'WSW_13_15sum',
           #'Ed_or_Training_13_15share',
          ]

X = select[features] # to enforce column order


#%%
X.shape

#%% [markdown]
# #### check for missing values

#%%
df.columns[df.isna().any()].tolist()

#%% [markdown]
# #### logistic regression based on TPot range of options

#%%
df.sort_values(['id'])


#%%
logreg = LogisticRegression(C=25.0, dual=False, penalty="l1")
logreg.fit(X, y)


#%%
coefficients = pd.concat([pd.DataFrame(X.columns),pd.DataFrame(np.transpose(logistic.coef_))], axis = 1)
print(coefficients)
logreg.score(X, y)
y.mean()
#what share of people are treated? ie if you had to predict, going for no would give better results


#%%



logit_roc_auc = roc_auc_score(y, logreg.predict(X))
fpr, tpr, thresholds = roc_curve(y, logreg.predict_proba(X)[:,1])
plt.figure()
plt.plot(fpr, tpr, label='Logistic Regression (area = 0.592)' )
plt.plot([0, 1], [0, 1],'r--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver operating characteristic')
plt.legend(loc="lower right")
plt.savefig('Log_ROC')
plt.show()


#%%
a=logreg.predict_proba(X)
y_pred_prob = a[:,1]
roc_auc_score(y, y_pred_prob)
print(y_pred_prob)
type(y_pred_prob)
y_pred_prob.shape
pp= pd.Series(y_pred_prob)


#%%
pp.shape


#%%
pp.tail()


#%%
select['Propensity']=pp


#%%
df=select


#%%
df.tail()

#%% [markdown]
# ##### check of any null values in propensity

#%%
type(df['Propensity'] )
df['Propensity'].isnull().values.any()

#df['Propensity'].isnull().sum().sum() 

#%% [markdown]
# #### retain inital, unweighted, dataset variables to compare

#%%
initial=df[['Propensity','y','age', 'sw_pay_mean_1315', 'duration_days_0', 'earn_tot_mean_1315']]


#%%
initial.shape


#%%
initial.columns[initial.isna().any()].tolist()

#%% [markdown]
# #### calculate odds ratios

#%%
df['o_ratio']= df['y']
df['o_ratio']= df['y'] + (1-df['y'])*df['Propensity']/(1-df['Propensity'])


#%%
average_odds=dict(df['o_ratio'].groupby(df['y']).mean())

df['report_o_ratio']=df['o_ratio']
df.loc[df['y']==0, 'report_o_ratio']=df['report_o_ratio']/average_odds[0]


#%% [markdown]
# #### how does the sume of the weighted control units compare to the sum of the treated units?

#%%
for title, group in df.groupby('y'):
    group.plot(x='age', y='MeanToDate', title=title)


#%%
average_odds_sum=dict(df['o_ratio'].groupby(df['y']).sum())
print(average_odds_sum)


#%%
d=df.pivot_table(index='age', columns='y', values='o_ratio', aggfunc=np.sum)
d.reset_index()
#groupby(['y', 'age'])['o_ratio'].sum()


#%%
#d.plot.hist(bins=10, x='age', y=np.sum, alpha=0.3)

# sns.distplot(d, bins=10, hist=True, kde=False, 
#              rug=False, fit=None, hist_kws=None, kde_kws=None, rug_kws=None, 
#              fit_kws=None, color=None, vertical=False, norm_hist=False, axlabel=None, label=None, ax=None)
from matplotlib.pyplot import hist
hist(df['age'], bins=10)


#%%
#
 
# plot
f, axes = plt.subplots(2, 2, figsize=(7, 7), sharex=True)
sns.distplot( df["age"] , color="skyblue", ax=axes[0, 0])
sns.distplot( df["sw_pay_mean_1315"] , color="olive", ax=axes[0, 1])
sns.distplot( df["earn_other_mean_1315"] , color="gold", ax=axes[1, 0])
sns.distplot( df["duration_days_0"] , color="teal", ax=axes[1, 1])


#%%
df.groupby(['y', 'sw_pay_mean_1315'])['o_ratio'].sum().plot()


#%%
df.groupby(['y', 'earn_tot_mean_1315'])['o_ratio'].sum().plot()

#%% [markdown]
# #### age, social welfare payments, earnings and duration - compare the weighted and unweighted means

#%%
list(df)


#%%
var_to_weight =['total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0',
       'LR_2017share','Ed_or_Training_2017share', 'earn_tot_2012', 'earn_tot_2013', 
       'earn_tot_2014', 'earn_tot_2015', 'earn_tot_2016', 'earn_tot_2017', 'sw_pay_2017', 'sw_pay_2018', 'age']
#'Duration Bands',


for col in var_to_weight:
    df["w_" + col] = df[col] *df['report_o_ratio']

# for v in var_to_weight:
#     df['weighted_'+(v)]=df['v']*df['report_o_ratio']
 


#%%
p1=sns.kdeplot(df['sepal_width'], shade=True, color="r")
p1=sns.kdeplot(df['sepal_length'], shade=True, color="b")
#sns.plt.show()


#%%
df.groupby(['y', 'age'])[o_ratio].sum


#%%
df['r_w_age']=df['w_age']*average_odds[0]
df['tr_w_age']=df['w_age']*average_odds[1]


#%%
#total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0',
unique_vals = df['y'].unique()
print(unique_vals)
df['y'].value_counts()

# # Sort the dataframe by target
# # Use a list comprehension to create list of sliced dataframes
targets = [df.loc[df['y'] == val] for val in unique_vals]
for index, group in enumerate(['Control', 'Treatment']):
    subset=df.loc[df['y'] ==index]
    print(subset.shape)
    sns.kdeplot(subset['age'], kernel='gau', label = group )
plt.legend()
plt.show()


#%%
#total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0',
unique_vals = df['y'].unique()
print(unique_vals)
df['y'].value_counts()

# # Sort the dataframe by target
# # Use a list comprehension to create list of sliced dataframes
targets = [df.loc[df['y'] == val] for val in unique_vals]
for index, group in enumerate(['Control', 'Treatment']):
    subset=df.loc[df['y'] ==index]
    print(subset.shape)
    sns.kdeplot(subset['w_age'], kernel='gau', label = group )
plt.legend()
plt.show()


#%%
#total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0',
unique_vals = df['y'].unique()
print(unique_vals)
df['y'].value_counts()

# # Sort the dataframe by target
# # Use a list comprehension to create list of sliced dataframes
targets = [df.loc[df['y'] == val] for val in unique_vals]
for index, group in enumerate(['Control', 'Treatment']):
    subset=df.loc[df['y'] ==index]
    print(subset.shape)
    sns.distplot(subset['duration_days_0'], label = group )
plt.xlabel("Days")
plt.ylabel("Frequency")
plt.title("Duration of unemployment - those who started v eligible for JobPath")
plt.legend()
plt.savefig('Dur_TC')
plt.show()

#%% [markdown]
# #### Outcomes

#%%
#df.head()


print(df['w_LR_2017share'].groupby(df['y']).mean())
#print(df['w_earn_tot_2017'].groupby(df['y']).mean())


#%%
group_cl=df.groupby(['y', 'cluster'])


#%%
LR=group_cl['w_LR_2017share'].mean()
display(LR.T)


#%%
group_cl['w_earn_tot_2017'].mean().round(0)


#%%
group_cl['w_earn_tot_2017'].median().round(0)


#%%
group_cl['w_sw_pay_2017'].mean().round(2)


#%%
group_cl['w_sw_pay_2017'].mean().round(2)


#%%
group_cl['w_sw_pay_2018'].mean().round(2)


#%%
group_cl['w_sw_pay_2018'].median().round(2)


#%%
df.groupby(['y', 'sex'])['w_earn_tot_2017'].mean()

#df['sw_pay_2018'].median().round(2)

#%% [markdown]
# #### subsets - 1. non-zero earnings in 2017

#%%

#non-z=df.loc[df['w_earn_tot_2017']>0]


non_Z=df[df['w_earn_tot_2017']>0]
non_Z.shape
Z=non_Z.groupby(['y','cluster'])


#%%
#print(non_Z['w_LR_2017share'].groupby(non_Z['y', 'cluster']).mean())


#%%
group_nonZ=non_Z.groupby(['y', 'cluster'])


#%%
Ear=group_nonZ['w_LR_2017share'].mean()
display(Ear)


#%%
Ear=group_nonZ['w_earn_tot_2017'].mean().round(0)
display(Ear)


#%%
Ear=group_nonZ['w_earn_tot_2017'].median().round(0)
display(Ear)


#%%
Ear=group_nonZ['w_earn_tot_2017'].median().round(0)
display(Ear)


#%%
Ear=group_nonZ['sw_pay_2018'].median().round(0)
display(Ear)


#%%

print(non_Z['w_LR_2017share'].groupby(non_Z['y']).mean())
print(non_Z['w_earn_tot_2017'].groupby(non_Z['y']).median().round(0))


#%%
transformed = (ts.groupby(lambda x: x.year)
                 .transform(lambda x: (x - x.mean()) / x.std()))


#%%
transformed = (df.groupby('y').transform(lambda x: (x - x.mean()) / x.std()))


#%%
##### Find those who earned something in each group, T and C; done

#### multiply the count in C group in employment (>0) by readjusted odds ratio, 
##### sum of those weights divided by sum of all C group weights - done

##### present the uplift in earnings - done

##### knock out the non-earners, compare average amount - done

#%% [markdown]
# #### subset 2. remove future treatment

#%%

pure=df[df['2016Q1']!=-2] & df[df['2016Q2']!=-2] & df[df['2016Q3']!=-2]
pure.shape


#%%
pure['2017Q4'].values


#%%




'total_duration_days',
         # 'Duration Bands',
         'sw_pay_mean_1315','earn_tot_mean_1215',duration_days_0''LR_2017share',
'Ed_or_Training_2017share',

'earn_tot_2012',
 'earn_tot_2013',
 'earn_tot_2014',
 'earn_tot_2015',
 'earn_tot_2016',
 'earn_tot_2017',

#%% [markdown]
# #### Balance diagnostics
#%% [markdown]
# #### having generated weighted columns with '_weighted' appended to the var name, generate smd as the mean/std for the original variables and the weighted variables

#%%
#df.groupby("y").std(ddof=0) 


#print(df.groupby(['y'])['age'].apply(np.std))
#print(df.groupby(['y'])['age'].apply(np.mean))
m=df.groupby(['y'])['age'].mean()
m2=df.groupby(['y'])['w_age'].mean()
s=df.groupby(['y'])['age'].std()
s2=df.groupby(['y'])['w_age'].std()
#smd=(m-m2)/((s+s2)/2)
#display(smd)
print(m, s, m2, s2)


#%%

df['weighted_age']=df['age']*df['report_o_ratio']
print(df['age'].groupby(df['y']).mean())
print(df['weighted_age'].groupby(df['y']).mean())


df['weighted_age']=df['age']*df['report_o_ratio']
print(df['age'].groupby(df['y']).mean())
print(df['weighted_age'].groupby(df['y']).mean())

df['weighted_earn']=df['earn_tot_mean_1215']*df['report_o_ratio']
print(df['earn_tot_mean_1215'].groupby(df['y']).mean())
print(df['weighted_earn'].groupby(df['y']).mean())

df['weighted_sw_pay']=df['sw_pay_mean_1315']*df['report_o_ratio']
print(df['sw_pay_mean_1315'].groupby(df['y']).mean())
print(df['weighted_sw_pay'].groupby(df['y']).mean())

df['weighted_dur_0']=df['duration_days_0']*df['report_o_ratio']
print(df['duration_days_0'].groupby(df['y']).mean())
print(df['weighted_dur_0'].groupby(df['y']).mean())


#%%
# is it possible to loop through the columns in the dataframe and calculate a value for smd
# this needs to subtract mean values where y =0 from those where y=1; then add std dev values for both y groups and divide by two
col_list=['total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0','LR_2017share','Ed_or_Training_2017share',
'earn_tot_2012', 'earn_tot_2013', 'earn_tot_2014', 'earn_tot_2015', 'earn_tot_2016', 'earn_tot_2017', 'LR_2017share',
          'Ed_or_Training_2017share']
for col in col_list:
    smd=df.groupby(['y']).apply(np.mean)/df.groupby(['y']).apply(np.std)
            # print(col)
    df[col] = df[col].astype("float")

#%% [markdown]
# existing differences - earnings, age, social welfare payments
# then distribuion of weighted same variables
# Likely to be working and, if so, how much you earn
# histogram centiles - counting people in each centile 
# Median out

#%%



