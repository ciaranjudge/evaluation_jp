#!/usr/bin/env python
# coding: utf-8

# ### Plotting results - Rdf dataset

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


# In[ ]:



Rdf = pd.read_csv("\\\\cskma0294\\F\\Evaluations\\JobPath\\Python\\Data\\Results\\Rdf.csv")


# In[130]:


Rdf.groupby('Group')['w_WIES2017'].mean()
#data.groupby('month')['duration'].sum() 


# In[ ]:


list(Rdf)


# In[ ]:


Rdf_T=Rdf.loc[Rdf['Group']==1]
Rdf_C=Rdf.loc[Rdf['Group']==0]

#Rdf.head()


# In[128]:


Rdf_T.head()


# ### Simple bubble of w_sw_pay_2017 and w_Earn2017 by group

# variables: y, w_age, age, mean social welfare payments, mean earnings 13-15, 

# In[131]:


ax1=sns.lmplot(data=Rdf_T, x="w_sw_pay_2017", y="w_Earn2017", hue="cluster", palette="Set1", fit_reg=False, legend=True, legend_out=True,)

plt.xlim(0,14000)
plt.ylim(0,14000)
ax2=sns.lmplot(data=Rdf_C, x="w_sw_pay_2017", y="w_Earn2017", hue="Group name", palette="Set2", fit_reg=False, legend=True, legend_out=True, ax2=ax1)
plt.xlim(0,14000)
plt.ylim(0,14000)
plt.show()


# In[ ]:


g = sns.lmplot(x='w_sw_pay_2017', y='w_Earn2017', data=Rdf, fit_reg=False, hue='cluster', lowess=True, scatter_kws={'alpha': 0.5, "s": 200}, legend=True, markers=['s', 'o', "<","<","P","d","*"])


# In[138]:


x = Rdf['w_sw_pay_2017']
y = Rdf['w_Earn2017']
z = Rdf['clust_size']


# In[149]:


# add iso
# Change color with c and alpha. I map the color to the X axis value.
#split Rdf into 0 and 1 in group
#plt.scatter(x, y, s=z,  cmap="Blues", alpha=0.7, edgecolors="grey", linewidth=3)
# different alpha settings, two plots ax=1, ax=2, palette by cluster going from near to far from labour market, same for each
# Add titles (main and on axis)
ax1=plt.scatter(x, y,s=z, alpha=0.7, edgecolors="grey", linewidth=3)
ax2=plt.scatter(x, y, s=z, alpha=0.3, edgecolors="grey", linewidth=3, ax=ax1)
plt.xlabel("Social welfare payments, 2017")
plt.ylabel("Earnings from employment, 2017")
plt.title("Social welfare pay and earnings from employment, with and without JobPath")
plt.xlim(0,14000)
plt.ylim(0,14000)

#plt.legend()
plt.show()


# In[ ]:



minima = min( Rdf_T['cluster'])
maxima = max( Rdf_T['cluster'])

norm = matplotlib.colors.Normalize(vmin=minima, vmax=maxima, clip=True)
mapper = cm.ScalarMappable(norm=norm, cmap=cm.Greys_r)

for v in Rdf_T['cluster']:
    print(mapper.to_rgba(v))
    
ax1 = Rdf_T.plot(kind='scatter', x='w_sw_pay_2017', y='w_Earn2017', s=Rdf_T['clust_size'], color=c)    
ax2 = Rdf_C.plot(kind='scatter', x='w_sw_pay_2017', y='w_Earn2017', s=Rdf_C['clust_size'], color=c, ax=ax1)    
xlim(0,14000)
ylim(0,14000)
plt.legend
plt.show()


# In[ ]:


x = Rdf_T['w_sw_pay_2017']
y = Rdf_T['w_Earn2017']
x1 = Rdf_C['w_sw_pay_2017']
y1 = Rdf_C['w_Earn2017']


# In[101]:


Rdf_T.head()


# In[103]:



minima = min( Rdf_T['cluster'])
maxima = max( Rdf_T['cluster'])

norm = matplotlib.colors.Normalize(vmin=minima, vmax=maxima, clip=True)
mapper = cm.ScalarMappable(norm=norm, cmap=cm.Greys_r)

for v in Rdf_T['cluster']:
    print(mapper.to_rgba(v))
    
ax1 = Rdf_T.plot(kind='scatter', x='w_sw_pay_2017', y='w_Earn2017', s=Rdf_T['clust_size'], color=c)    
#ax2 = Rdf_C.plot(kind='scatter', x='w_sw_pay_2017', y='w_Earn2017', s=Rdf_C['clust_size'], color=c, ax=ax1)    
xlim(0,14000)
ylim(0,14000)
plt.legend
plt.show()


# In[108]:


#df = pd.DataFrame(np.random.randn(100, 6), columns=['a', 'b', 'c', 'd', 'e', 'f'])
sns.lmplot(data=Rdf_T, x="w_sw_pay_2017", y="w_Earn2017", fit_reg=False, legend=True, legend_out=True)

sns.palplot(sns.color_palette("Blues"))
ax1 = Rdf_T.plot(kind='scatter', x='w_sw_pay_2017', y='w_Earn2017', s=Rdf_T['clust_size'])    
#ax2 = Rdf_C.plot(kind='scatter', x='w_sw_pay_2017', y='w_Earn2017', s=Rdf_C['clust_size'], color='g', ax=ax1)    
xlim(0,14000)
ylim(0,14000)
plt.legend
plt.show()


# #### example below saved in bookmarks

# In[ ]:




from pylab import *

# Generate some random data that looks like yours
x = Rdf['w_sw_pay_2017']
y = Rdf['w_Earn2017']
z = Rdf['clust_size']
 

ERR = X*random(N)

# These are the new arguments that move the error bars behind - lw is setting line width
scatter_kwargs = {"zorder":100}
error_kwargs = {"lw":.5, "zorder":0}

scatter(X,Y,c=Z,**scatter_kwargs)
errorbar(X,Y,yerr=ERR,fmt=None, marker=None, mew=0,**error_kwargs )
xlim(0,1)
show()


# #### Facet

# In[126]:


sns.set(style="ticks", color_codes=True)
g = sns.FacetGrid(Rdf, col="cluster", row="Group name")


g = g.map(plt.hist, "w_WIES2017")


# In[ ]:



g = sns.FacetGrid(tips, col="w_sw_pay_2017",  row="clust_name")
>>> g = g.map(plt.scatter, "total_bill", "tip", edgecolor="w", despine=True)


# In[ ]:


g = sns.relplot(x="w_sw_pay_2017", y="w_Earn2017", hue="Group name", size="clust_size",
                palette=["b", "r"], sizes=(10, 500),
                col="cluster name", data=Rdf, col_wrap=3)


# In[ ]:


scatter_kws={"s": "clust_size"}
sns.lmplot(x="w_sw_pay_2017", y="w_Earn2017", hue="Group name",  col='cluster name', 
           data=Rdf,  col_wrap=3, fit_reg=False, scatter_kws={"s": z*30}, legend=True, legend_out=True)


# In[ ]:


x = Rdf['w_sw_pay_2017']
y = Rdf['w_Earn2017']
z = Rdf['clust_size']


# In[127]:


g = sns.FacetGrid(Rdf, col="cluster",  hue="Group")
g = (g.map(plt.scatter, "w_Earn2017", "w_sw_pay_2017", s="clust_size", edgecolor="w")
.add_legend())


# In[124]:


list(Rdf)


# In[ ]:





# In[ ]:


x = Rdf['w_sw_pay_2017']
y = Rdf['w_Earn2017']
z = Rdf['clust_size']

