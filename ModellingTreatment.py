#!/usr/bin/env python
# coding: utf-8

# In[ ]:



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


# ### import files and merge

# In[ ]:


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


# In[ ]:



path_data = "\\\cskma0294\\F\\Evaluations\\JobPath\\Quarterly_status"
os.chdir(path_data)

df_shares = pd.read_csv("shares_2017.zip")
df=pd.merge(df, df_shares, on='ppsn', how='left')


# In[ ]:


#PAYMENTS = pd.read_csv("\\\cskma0294\\F\\Evaluations\\JobPath\\ALL_PPSNS_with_payments.csv")
#PAY.info()
#PAYMENTS.shape
#df=pd.merge(df, PAYMENTS, on='ppsn', how='left')


# #### Dicts for cluster labels and contractors to office code, and lab for ctrl/treat values

# In[ ]:


path_dicts= "\\\cskma0294\\F\\Evaluations\\JobPath\\Python"
os.chdir(path_dicts)


cluster_dicts = pd.read_csv('Cluster Dictionary.csv')

# Create a dictionary mapping cluster values to labels
cluster_dicts = cluster_dicts.set_index('cluster')['cluster name'].to_dict() 

# Apply dictionaries to create new columns of cluster labels
#df['cluster name'] = df['cluster'].map(cluster_dicts)



#print(df['cluster name'].value_counts())


# In[ ]:


path_dicts= "\\\cskma0294\\F\\Evaluations\\JobPath\\Python"
os.chdir(path_dicts)

office_contractor_dicts = pd.read_csv('Local Office Contract Dictionary.csv')

# Create a dictionary mapping local office codes to provider
office_contractor_dicts = office_contractor_dicts.set_index('LO Code')['Contractor '].to_dict() 

# Apply dictionaries to create new columns of provider
slice_df['Contractor'] = slice_df['Claim Office Code'].map(office_contractor_dicts)


# In[ ]:


lab={0:"Control",1:"Treatment"}
    
#df.y.apply(lambda x:lab[x])

#categories = df[category].apply(lambda x: 'weekday' if x == 0 else 'weekend')


# In[ ]:


df.drop('Unnamed: 0', axis=1)


# In[ ]:


df.set_index('id', drop=True)


# In[ ]:


df.drop('Unnamed: 0', axis=1,inplace = True)


# ### select y
# 
# 
# #### adding .values.ravel() would be useful

# In[ ]:


select=df[df['2016Q1']!=-1]
select.shape
#list(select)


# In[ ]:


select=select.rename({'2016Q1':'y'}, axis=1)
#y=['y_1']

select['y'].value_counts()
select=select.set_index('id',drop=True)
select=select.sort_index()
select=select.reset_index()


# In[ ]:


select.index.names=['2016Q1_index']


# ### select features/variables

# In[ ]:


#X = data_final.loc[:, data_final.columns != 'y']
y = select.loc[:, select.columns == 'y']

#features =['sw_pay_mean_1315', 'earn_tot_mean_1315', 'age', 'occupation_rank_P1','Duration Bands', 'cluster', 
#  'family_flag_rank_P1', 'WSW_13_15share', 'Ed_or_Training_13_15share', 'LM_WSW_13_15share']
features =[
    #'sw_pay_2013',
            'sw_pay_2014',
            'sw_pay_2015',
     #       'earn_tot_2012',
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
      #     'total_duration_days',
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


# In[ ]:


print(features)


# In[ ]:


df=select


# ### Cat plots showing variance in means in unweighted covariates (saving to Quarterly_status file)
# 
# ##### Needs some code to map (lab) - labels for control and treat - onto df['y']

# In[ ]:


#plotdf=df['y'].apply(lambda x:lab[x])
g= sns.catplot(x="cluster", y="earn_tot_mean_1315", kind="bar", hue=df['y'].map(lab), data=df, legend=True)
(g.set_axis_labels("cluster", "Earnings 2013-2015"),
#g.set_title("Earnings 2013-2015 - those who started JobPath in Q1 2016")
)
plt.title('Average earnings 2013-2015 - cluster, by control and treatment,in Q1 2016')
#plt.savefig('earn_y_c', dpi=500);
plt.show()


# In[ ]:


g= sns.catplot(x="y", y="earn_tot_mean_1315", kind="bar", data=df)
(g.set_axis_labels("control  /  treatment", "Earnings 2013-2015"),
#g.set_title("Earnings 2013-2015 - those who started JobPath in Q1 2016")
)
#plt.title('Earnings 2013-2015 - cluster, by control and treatment,in Q1 2016')
#plt.savefig('earn_y', dpi=500);
plt.show()


# In[ ]:


g= sns.catplot(x="y", y="w_earn_tot_mean_1315", kind="bar", data=df)
(g.set_axis_labels("control  /  treatment", "Earnings 2013-2015"),
#g.set_title("Earnings 2013-2015 - those who started JobPath in Q1 2016")
)
#plt.title('Earnings 2013-2015 - cluster, by control and treatment,in Q1 2016')
plt.savefig('w_earn_y', dpi=500);
plt.show()


# In[ ]:


g= sns.catplot(x=("y"[{'0':"control", '1':"treatment"}]), y="sw_pay_mean_1315", kind="bar", data=df)
(g.set_axis_labels("control    /     treatment", "social welfare payments, mean 2013-2015"),
#g.set(xticklabels=([]),
#g.set(xticklabels=([{'0':"control", '1':"treatment"}])),
#g.set_xtick(['control', 'treatment']))
#plt.savefig('sw_y', dpi=500)
plt.show()


# In[ ]:


sns.catplot(x="y", y="earn_tot_2017", kind="bar", data=df);


# In[ ]:


sns.catplot(x="y", y="earn_tot_2017", kind="boxen", data=df);


# In[ ]:


select.tail()


# In[ ]:


display(group_ycl)


# In[ ]:


X.shape


# In[ ]:


y.shape


# #### check for missing values

# In[ ]:


df.columns[df.isna().any()].tolist()


# #### logistic regression based on TPot range of options

# In[ ]:


df.sort_values(['id'])


# In[ ]:


logreg = LogisticRegression(C=25.0, dual=False, penalty="l1")
logreg.fit(X, y)


# #### bootstrap function - logistic model - prepared

# In[ ]:


X_train, X_test, y_train, y_test =  train_test_split(X, y, test_size = 0.4, stratify=y)
logreg = LogisticRegression(C=25.0, dual=False, penalty="l1", solver='liblinear')
logreg.fit(X_train, y_train.values.ravel())
#y_pred = logreg.predict(X_test)
y_pred = logreg.predict(X)
#logreg.fit(X, y)


# #### run bootstrap replication - time-consuming to run

# In[ ]:


def single_bootstrap_logreg_rep(X,y):
    """Generate one bootstrap replicate"""
    X_train, X_test, y_train, y_test =  train_test_split(X, y, test_size = 0.4, stratify=y)
    logreg = LogisticRegression(C=25.0, dual=False, penalty="l1", solver='liblinear')
    logreg.fit(X_train, y_train.values.ravel())
    """Apply fitted model to all rows"""
    a=logreg.predict_proba(X)
    y_pred_prob = a[:,1]
    """Repeat with another subset"""
    return y_pred_prob, logreg.coef_.ravel(), logreg.intercept_.ravel()

def draw_bs_reps(X, y, replicates):
    """Draw bootstrap replicates"""
    # Initialize array of replicates: bs_replicates
    bs_replicates = []
    coef_replicates=[]
    intercept_replicates=[]
    # Generate replicates
    for i in range(replicates):
        bs_replicate, coef_replicate, intercept_replicate= single_bootstrap_logreg_rep(X, y)
        bs_replicates.append(bs_replicate)
        coef_replicates.append(coef_replicate)
        intercept_replicates.append(intercept_replicate)
    return np.array(bs_replicates), np.array(coef_replicates), np.array(intercept_replicates) 

#I'm calling the function now....giving predicted probabilities of y, coefficients and intercept values

get_ipython().run_line_magic('time', 'bs_replicates, coef_replicates, intercept_replicates=draw_bs_reps(X, y, 1000)')


# In[ ]:


get_ipython().run_line_magic('time', 'bs_replicate, coef_replicate, intercept_replicate = single_bootstrap_logreg_rep(X, y)')


# #### this generates 1,000 columns of y_pred for each row, 1,000 intercept values and 1,000 coefficient estimates

# In[ ]:


#print(coef_replicates)
#intercept_replicates.shape
#['sw_pay_2014', 'sw_pay_2015', 'earn_tot_2013', 'earn_tot_2014', 'earn_tot_2015', 'age', 'Duration Bands', 'family_flag_rank_P1', 'Empl_13_15sum', 'Ed_or_Training_13_15share', 'LM_WSW_13_15share', 'LR_13_15sum', 'WSW_13_15share'])


# #### convert np array to dataframe and save to csv for y_reps, coef, and intercept

# In[ ]:


path_data="\\\cskma0294\\F\\Evaluations\\JobPath\\Python\\Data\\y_pred"
y_reps=pd.DataFrame(bs_replicates).T
y_reps['mean_y_pred']=y_reps.mean(axis=1)
y_reps.head()
y_reps.to_csv('y_reps.csv', sep=',',index=False)


# In[ ]:


coef=pd.DataFrame(coef_replicates)
coef['mean_coef']=coef.mean(axis=1)
coef.head()
coef.to_csv('coef.csv', sep=',',index=False)


# In[ ]:


intercept=pd.DataFrame(intercept_replicates)
intercept['mean_intercept']=intercept.mean(axis=1)
intercept.head()
intercept.to_csv('coef.csv', sep=',',index=False)


# In[ ]:


np.histogram(coef_replicates, bins=10, range=None, normed=None, weights=None, density=None)
plt.hist(coef_replicates, bins=10)
plt.show()


# ##### non-parametric central tendancy for each group of replicated values

# In[ ]:


plt.hist(intercept_replicates, bins=100)
plt.show()


# In[ ]:


bs_replicates_median=np.median(bs_replicates, axis=0)
bs_replicates_median.mean()


# In[ ]:


bs_replicates_mean.mean()


# In[ ]:


#plt.plot(bs_replicates, np.percentile(bs_replicates, 97.5, axis=0))
#plt.plot(bs_replicates, np.percentile(bs_replicates, 2.5, axis=0))
#plt.show()


# In[ ]:


result.summary()


# In[ ]:


coefficients = pd.concat([pd.DataFrame(X.columns),pd.DataFrame(np.transpose(logreg.coef_))], axis = 1)
print(coefficients)
logreg.score(X, y)
y.mean()
#what share of people are treated? ie if you had to predict, going for no would give better results


# ### Statsmodel rather than sci-kit learn

# In[ ]:


logit = sm.Logit(y, X)

result=logit.fit()


# In[ ]:


result.summary()


# In[ ]:





# In[ ]:





# In[ ]:


model = sm.Logit(y, X).fit()
proba = model.predict(X) # predicted probability


# In[ ]:


print(model.cov_params())


# In[ ]:


cov = model.cov_params()
gradient = (proba * (1 - proba) * X.T).T # matrix of gradients for each observation


# In[ ]:


gradient.describe()


# In[ ]:


std_errors = np.array([np.sqrt(np.dot(np.dot(g, cov), g)) for g in gradient])
c = 1.96 # multiplier for confidence interval
upper = np.maximum(0, np.minimum(1, proba + std_errors * c))
lower = np.maximum(0, np.minimum(1, proba - std_errors * c))

plt.plot(x, proba)
plt.plot(x, lower, color='g')
plt.plot(x, upper, color='g')
plt.show()


# In[ ]:


cov = model.cov_params()
gradient = (proba * (1 - proba) * X.T).T # matrix of gradients for each observation
std_errors = np.array([np.sqrt(np.dot(np.dot(g, cov), g)) for g in gradient])
c = 1.96 # multiplier for confidence interval
upper = np.maximum(0, np.minimum(1, proba + std_errors * c))
lower = np.maximum(0, np.minimum(1, proba - std_errors * c))

plt.plot(x, proba)
plt.plot(x, lower, color='g')
plt.plot(x, upper, color='g')
plt.show()


# ### ROC curve

# In[ ]:





logit_roc_auc = roc_auc_score(y, logreg.predict(X))
fpr, tpr, thresholds = roc_curve(y, logreg.predict_proba(X)[:,1])
plt.figure()
plt.plot(fpr, tpr, label='Logistic Regression (area = %2.4f)' % logit_roc_auc)
#plt.plot(fpr, tpr, label="Logistic regression, auc="+str(logit_roc_auc) )
plt.plot([0, 1], [0, 1],'r--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver operating characteristic')
plt.legend(loc="lower right")
plt.savefig('Log_ROC')
plt.show()


# In[ ]:


a=logreg.predict_proba(X)
y_pred_prob = a[:,1]
roc_auc_score(y, y_pred_prob)
print(y_pred_prob)
type(y_pred_prob)
y_pred_prob.shape
pp= pd.Series(y_pred_prob)


# In[ ]:


pp.shape


# In[ ]:


pp.tail()


# In[ ]:


select['Propensity']=pp


# In[ ]:


df=select


# In[ ]:


df.tail()


# ##### check of any null values in propensity

# In[ ]:


type(df['Propensity'] )
df['Propensity'].isnull().values.any()

#df['Propensity'].isnull().sum().sum() 


# #### calculate odds ratios

# In[ ]:


df['o_ratio']= df['y']
df['o_ratio']= df['y'] + (1-df['y'])*df['Propensity']/(1-df['Propensity'])


# In[ ]:


average_odds=dict(df['o_ratio'].groupby(df['y']).mean())

df['report_o_ratio']=df['o_ratio']
df.loc[df['y']==0, 'report_o_ratio']=df['report_o_ratio']/average_odds[0]



# #### how does the sum of the weighted control units compare to the sum of the treated units?

# In[442]:


#for title, group in df.groupby('y'):
    #group.plot( x='age', y='cluster', title=title)


# ### Demonstrate sum of weights totalling (close to) number of treated units

# In[443]:


average_odds_sum=dict(df['o_ratio'].groupby(df['y']).sum())
print(average_odds_sum)


# In[ ]:


d=df.pivot_table(index='age', columns='y', values='o_ratio', aggfunc=np.sum)
d.reset_index()
#groupby(['y', 'age'])['o_ratio'].sum()


# In[ ]:





# #### age, social welfare payments, earnings and duration - compare the weighted and unweighted means

# In[ ]:


list(df)


# In[ ]:


EARN = pd.read_csv("\\\\cskma0294\\F\\Evaluations\\JobPath\\Earn2012_2017_08FEB2019.csv", sep=';')
#EARN.info() \\cskma0294\F\Evaluations\JobPath
#EARN.head()


# In[ ]:


EARN=EARN.fillna(0)


# In[ ]:


EARN.shape


# In[ ]:


EARN['Earn2017_perweek']=EARN['Earn2017']/EARN['WIES2017']


# ## New rule for outliers
# #### Earnings per week>352 and total PRSI per week <.035 of Earnings per week
# 

# In[ ]:


EARN_outliers=EARN.loc[EARN['Earn2017_perweek']>352]


# In[ ]:


EARN_outliers.shape


# In[ ]:


EARN_outliers=EARN_outliers.loc[EARN_outliers['PRSI2017']/EARN_outliers['Earn2017']<0.035]


# In[ ]:


EARN_outliers.index


# In[ ]:


No_outlier_EARN=EARN.drop(EARN_outliers.index)


# In[ ]:


outlierPPSN=EARN_outliers['ppsn']


# In[ ]:


outlier_df=df.loc[df['ppsn'].isin(outlierPPSN)]


# In[ ]:


outlier_df.shape


# In[ ]:


cleandf=df.drop(outlier_df.index)


# In[ ]:


print(outlier_df.groupby('y').count())


# In[ ]:


cleandf.shape


# In[ ]:


EARN=No_outlier_EARN


# In[ ]:


EARN.shape


# In[ ]:


df=pd.merge(cleandf, EARN, on='ppsn', how='left')


# In[ ]:


df.shape


# ### Drop rogue variables with '_x' suffix, if necessary

# In[ ]:


df.drop(['Earn2012_x', 'Earn2013_x', 'Earn2014_x', 'Earn2015_x', 'Earn2016_x', 'Earn2017_x'], axis=1, inplace=True)


# In[ ]:


col_rename=['Earn2012_y',
 'Earn2013_y',
 'Earn2014_y',
 'Earn2015_y',
 'Earn2016_y',
 'Earn2017_y']
df = df.rename(columns={col: col.split('_')[0] for col in col_rename})
    


# ### Generate weighted variables denoted by 'w_'

# In[ ]:


var_to_weight =['total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0',
       'LR_2017share','Ed_or_Training_2017share', 'earn_tot_mean_1315','sw_pay_2016', 'sw_pay_2017', 'sw_pay_2018',
                 'age',  'Earn2012',
 'Earn2013',
 'Earn2014',
 'Earn2015',
 'Earn2016',
 'Earn2017',
 'WIES2012',
 'WIES2013',
 'WIES2014',
 'WIES2015',
 'WIES2016',
 'WIES2017',
 'PRSI2012',
 'PRSI2013',
 'PRSI2014',
 'PRSI2015',
 'PRSI2016',
 'PRSI2017',
 'Earn2017_perweek',]
#'Duration Bands',


for col in var_to_weight:
    df["w_" + col] = df[col] *df['report_o_ratio']

# for v in var_to_weight:
#     df['weighted_'+(v)]=df['v']*df['report_o_ratio']
 


# #### unsuccessful attempt to show distribution of earnings, unweighted and weighted

# In[ ]:



#df = pd.DataFrame({'x':[1.,2.],'weight':[2,4]})
weighted = sm.nonparametric.KDEUnivariate(df['earn_tot_mean_1315'])
noweight = sm.nonparametric.KDEUnivariate(df['earn_tot_mean_1315'])
weighted.fit(fft=False, weights=df['report_o_ratio'])
noweight.fit()

f, (ax1, ax2) = plt.subplots(1, 2, sharey=True)
ax1.plot(noweight.support, noweight.density)
ax2.plot(weighted.support, weighted.density)

ax1.set_title('No Weight')
ax2.set_title('Weighted')


# In[ ]:


g = sns.FacetGrid(df, hue="y")
g = g.map(sns.distplot, "sw_pay_mean_1315")
# or
#g = g.map(sns.distplot, "variable")
plt.show()


# In[ ]:


p1=sns.kdeplot(df['age'], shade=True, color="r")
p1=sns.kdeplot(df['age'], shade=True, weight='report_o_ratio', color="b")
plt.show()


# In[ ]:


df.groupby(['y', 'age'])[o_ratio].sum


# In[ ]:


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
plt.title('Variation in age distribution ')
plt.savefig('age_y', dpi=500);
plt.show()


# In[ ]:


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
    p1=sns.kdeplot(subset['w_age'], shade=True, color="b", label = group )
    p2=sns.kdeplot(subset['age'], kernel='gau', label = group )
plt.legend()
plt.title('Variation in age distribution ')
#plt.savefig('age_y', dpi=500);
plt.show()


# In[ ]:


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


# In[ ]:


# sns.jointplot("age",  "w_age", data=df, kind='kde')


# #### Matplotlib and Numpy histograms with weights

# In[ ]:


plt.hist(df['earn_tot_mean_1315'], bins=5, density=None, weights=df['report_o_ratio'], histtype='bar', data=df)


# In[ ]:


np.histogram(df['earn_tot_mean_1315'], bins=10, range=None, normed=None, weights=df['report_o_ratio'], density=None)
plt.savefig('hist_y', dpi=500);
#plt.show()


# In[ ]:


df_2 = df.groupby(['cluster','y']).sum()
df_2.reset_index(inplace=True)
sns.barplot(x='y', y='sex', data=df_2);


# In[ ]:



ax = sns.countplot(x="y", hue=df["cluster"].map(cluster_dicts), data=df)
for p in ax.patches:
    height = p.get_height()
    if np.isnan(height):
        height = 0
    ax.text(p.get_x()+p.get_width()/2.,
            height + 3,
            str(int(height)),
            ha="center") 
fig = ax.get_figure()
fig.set_size_inches(10,10)
ax.set_xlabel('Treatment and control cases')
ax.set_ylabel('Count')
ax.set_title('Live Register population, by cluster, and referral to JobPath, by cluster')
plt.savefig('cluster_y', dpi=500);
plt.show()


# In[ ]:


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
plt.title("Duration of unemployment \n - Q1 2016 JobPath starters v those eligible")
plt.legend()
plt.savefig('Dur_TC')
plt.show()


# #### End of failed attempts to visualise

# #### Outcomes

# In[ ]:


print(df['w_sw_pay_2017'].groupby(df['y']).mean())
print(df['w_sw_pay_2018'].groupby(df['y']).mean())
print(df['w_Earn2017'].groupby(df['y']).mean())
print(df['w_PRSI2017'].groupby(df['y']).mean())
print(df['w_WIES2017'].groupby(df['y']).mean())


# #### subsets - 1. non-zero earnings in 2017

# In[ ]:


non_zdf=df.loc[df['w_WIES2017']>0]


# In[ ]:


print(non_zdf['w_Earn2017_perweek'].groupby(non_zdf['y']).describe())


# In[ ]:


non_zdf.shape
#testnon-zdf=df.loc[df['w_earn_tot_2017']>0]


# In[ ]:


pd.options.display.max_rows=None
m=df[['Earn2017','PRSI2017', 'WIES2017', 'y']].loc[df['WIES2017']<52].sort_values('Earn2017', ascending=False).head(100)
m['PRSI_Earn2017_perweek']=m['PRSI2017']/m['Earn2017']
display(m)


# In[ ]:



df['w_PRSI_per_WIES2017']=df['w_PRSI2017']/df['w_WIES2017']


# #### ....now group by cluster and treatment/control

# In[ ]:


group_cl=df.groupby(['y', 'cluster'])


# In[ ]:


group_cl['w_earn_tot_2017'].mean().round(0)


# In[ ]:


group_cl['w_earn_tot_2017'].median().round(0)


# In[ ]:


group_cl['w_sw_pay_2017'].mean().round(2)


# In[ ]:


group_cl['w_sw_pay_2017'].mean().round(2)


# In[ ]:


group_cl['w_sw_pay_2018'].mean().round(2)


# In[ ]:


group_cl['w_WIES2017'].mean().round(2)


# In[ ]:


group_cl['w_Earn_per_WIES2017'].mean().round(2)


# In[ ]:


prsi=group_cl['w_PRSI2017'].mean().round(2)
display(prsi)


# In[ ]:


for title, group in df.groupby(['y']):
    group.plot(x=df['PRSI2017'].mean(), y='cluster', title=title)


# In[ ]:


sns.barplot(x='cluster', y='PRSI2017', hue='y', data=prsi, order=None, hue_order=None, ci=95, n_boot=1000)


# #### Generate subsets, >1 weeek of employment, 13 week, 26 weeks, 39 weeks, 52 weeks

# In[ ]:


One_plus=df[['Earn2017', 'WIES2017','w_Earn2017', 'w_sw_pay_2017', 'w_PRSI2017', 'w_sw_pay_2018', 'w_WIES2017','y']].dropna().loc[df['WIES2017']>=1]

One_plus['w_Earn_per_WIES2017']=One_plus['w_Earn2017']/One_plus['w_WIES2017']


# In[ ]:


Thirteen_plus=df[['Earn2017', 'WIES2017','w_Earn2017', 'w_sw_pay_2017','w_PRSI2017', 'w_sw_pay_2018', 'w_WIES2017','y', 'cluster']].dropna().loc[df['WIES2017']>=13]

Thirteen_plus['w_Earn_per_WIES2017']=Thirteen_plus['w_Earn2017']/Thirteen_plus['w_WIES2017']


# In[ ]:


Twentysix_plus=df[['Earn2017', 'WIES2017','w_Earn2017', 'w_sw_pay_2017','w_PRSI2017', 'w_sw_pay_2018', 'w_WIES2017','y', 'cluster']].dropna().loc[df['WIES2017']>=26]

Twentysix_plus['w_Earn_per_WIES2017']=Twentysix_plus['w_Earn2017']/Twentysix_plus['w_WIES2017']


# In[ ]:


Thirtynine_plus=df[['Earn2017', 'WIES2017','w_Earn2017', 'w_sw_pay_2017','w_sw_pay_2018', 'w_PRSI2017', 'w_WIES2017','y', 'cluster']].dropna().loc[df['WIES2017']>=39]

Thirtynine_plus['w_Earn_per_WIES2017']=Thirtynine_plus['w_Earn2017']/Thirtynine_plus['w_WIES2017']


# In[ ]:


Fiftytwo_plus=df[['Earn2017', 'WIES2017','w_Earn2017', 'w_sw_pay_2017', 'w_sw_pay_2018', 'w_PRSI2017', 'w_WIES2017','y', 'cluster']].dropna().loc[df['WIES2017']>=52]

Fiftytwo_plus['w_Earn_per_WIES2017']=Fiftytwo_plus['w_Earn2017']/Fiftytwo_plus['w_WIES2017']


# In[ ]:


One_plus.groupby(One_plus['y'])['w_Earn_per_WIES2017','w_Earn2017'].describe().T


# In[ ]:


Thirteen_plus.groupby(Thirteen_plus['y'])['w_Earn_per_WIES2017','w_Earn2017'].describe().T


# In[ ]:


Twentysix_plus.groupby(Twentysix_plus['y'])['w_Earn_per_WIES2017','w_Earn2017'].describe().T


# In[ ]:


Thirtynine_plus.groupby(Thirtynine_plus['y'])['w_Earn_per_WIES2017','w_Earn2017'].describe().T


# In[ ]:


Fiftytwo_plus.groupby(Fiftytwo_plus['y'])['w_Earn_per_WIES2017','w_Earn2017'].describe().T


# In[ ]:


group_nonZ=non_zdf.groupby(['y', 'cluster'])


# In[ ]:


group_nonZ_13=Thirteen_plus.groupby(['y', 'cluster'])


# In[ ]:


Result_vars=['w_WIES2017','w_Earn2017', 'w_sw_pay_2017','w_PRSI2017', 'w_sw_pay_2018', 'w_WIES2016','w_Earn2016', 'w_sw_pay_2016','w_PRSI2016',]


# #### Results dataframe

# In[ ]:


Results=pd.DataFrame()


# In[ ]:


clus_y=df.groupby(['y', 'cluster'])


# In[ ]:


Results=clus_y[Result_vars].mean().round(2)


# In[ ]:


Results['w_Earn_per_WIES2017']=group_nonZ_13['w_Earn_per_WIES2017'].mean().round(2)


# In[430]:


#Results['Cluster_size']=
clust_size=clus_y['cluster'].count()
clust_size=clust_size.unstack(level=0)
#idx=pd.IndexSlice
#clust_size.loc[idx[0,:]]=clust_size.loc[1,:]
clust_size[0]=clust_size[1]

Results['clust_size']=clust_size.stack().swaplevel(0,1).sort_index()


# In[432]:


Rdf=Results.reset_index()


# In[436]:


Rdf=Rdf.rename(columns={'y':'Group'})
    #Results.index=Results.index.rename(['Group', 'cluster'])


# In[437]:


Rdf['cluster name'] = Rdf['cluster'].map(cluster_dicts)
lab={0:"Without JobPath",1:"With JobPath"}
    
Rdf['Group name']=Rdf['Group'].map(lab)


# In[438]:


Rdf


# In[ ]:


#transformed = (ts.groupby(lambda x: x.year)
#   ....:                  .transform(lambda x: (x - x.mean()) / x.std()))


# #### subset 2. remove future treatment - removal not working!!

# In[ ]:



pure= df[(df['2016Q2']!=1) & (df['2016Q3']!=1) & (df['2016Q4']!=1) & (df['2017Q1']!=1) & (df['2017Q2']!=1) & (df['2017Q3']!=1)]
pure.shape


# In[ ]:


pure['2017Q4'].values


# #### Balance diagnostics

# #### having generated weighted columns with '_weighted' appended to the var name, generate separate df with just variables unweighted and their mean and standard error,  and the same for the weighted variables

# In[ ]:


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


# ### crude version in cell below, attempt to loop in cell below that

# In[ ]:



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


# In[ ]:


# is it possible to loop through the columns in the dataframe and calculate a value for smd
# this needs to subtract mean values where y =0 from those where y=1; then add std dev values for both y groups and divide by two
col_list=['total_duration_days','sw_pay_mean_1315','earn_tot_mean_1215','duration_days_0','LR_2017share','Ed_or_Training_2017share',
'earn_tot_2012', 'earn_tot_2013', 'earn_tot_2014', 'earn_tot_2015', 'earn_tot_2016', 'earn_tot_2017', 'LR_2017share',
          'Ed_or_Training_2017share']
for col in col_list:
    smd=df.groupby(['y']).apply(np.mean)/df.groupby(['y']).apply(np.std)
            # print(col)
    df[col] = df[col].astype("float")


# existing differences - earnings, age, social welfare payments
# then distribuion of weighted same variables
# Likely to be working and, if so, how much you earn
# histogram centiles - counting people in each centile 
# Median out

# #### Number of people with non-zero earnings on each side
