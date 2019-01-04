#%% [markdown]
# # Weekly Status

#%%
import pandas as pd              # python package for dataframes
import matplotlib.pyplot as plt  # python package for plotting
from datetime import datetime

#%%
in_df = pd.read_csv('\\\\cskma0294\\F\\Evaluations\\JobPath\\\Quarterly_status\\WeeklyStatus.zip')
in_df = in_df.drop(['Purge_ind'], axis=1)
in_df.head()         

#%%
df = pd.melt(in_df, id_vars=['ppsn'], var_name='date', value_name='status')

#%%
df = df[df['date'] != ''].dropna()
df['date'] = [x.strip('StatusEnd_') for x in df['date']]
#%%
df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True).dt.date

#%%
df = df.set_index(['ppsn', 'date'])

#%%
df_dummies = df['status'].str.get_dummies()
#%%
df.describe()

#%% [markdown]
# ## Split status codes
# Code to split a string containing multiple status codes into a List of single status codes

#%%
# str_split_in_n takes a string x and splits it into a sorted list using a separator (default value '+') of length n. 
# If the length of the list is less than n, them one or two  are more
def str_split_in_n(x, n, sep='+', descend = False):
    s = str.split(str(x),sep)
    s.sort(reverse = descend)
    l = len(s)
    for i in range(len(s),n):
        s.append(float('nan'))             # could use '' instead of float('nan')
    if len(s) > n:
        s[n-1] = '+'.join(s[n-1:])
    return s[:n]

#%% [markdown]
# Examples

#%%
s1 = str_split_in_n('WCP+FIS+UBCO', 3, descend = True)
print(s1)
s2 = str_split_in_n('SPC+SPNC+UA+O 65', 6, descend = True)
print(s2)

#%% [markdown]
# Code to select the element in the position <em>series_index</em> in a series created by splitting string <em>s</em> 
# into a series of length <em>series_length</em> using <em>str_split_in_n</em>, using a separator (default value '+'), in ascending order (default).

#%%
def select_from_str_split(s, series_length, series_index, sep = '+', descend = False):
    return(str_split_in_n(s, series_length, sep, descend)[series_index])

#%% [markdown]
# Split the status codes in 4 in descending order and select the 1st to 4th status codes and put them in new columns.

#%%
num_status_columns = 4     # Number of status columns
loc_status_codes = 2       # Column index of combined status codes
df3_part1['status_1'] = df3_part1.apply(lambda row: 
                                        select_from_str_split(row[loc_status_codes], num_status_columns, 0, descend = True), 
                                        axis=1)


#%%
df3_part1['status_2'] = df3_part1.apply(lambda row: 
                                        select_from_str_split(row[loc_status_codes], num_status_columns, 1, descend = True), axis=1)
df3_part1['status_3'] = df3_part1.apply(lambda row: 
                                        select_from_str_split(row[loc_status_codes], num_status_columns, 2, descend = True), axis=1)
df3_part1['status_4'] = df3_part1.apply(lambda row: 
                                        select_from_str_split(row[loc_status_codes], num_status_columns, 3, descend = True), axis=1)


#%%
df3_part1.head()


#%%
df3_part1['status_1'].value_counts()

#%% [markdown]
# ## Create flags for each status code

#%%
status_list = ['CA', 'C-UA', 'C-UB', 'FASS', 'FISH', 'UA', 'UB', 'INTN', 'SEMP', 'SST', 'STEA', 'DA', 'IP', 'OFP', 
               'OFPJST', 'PRTA', 'SPC', 'WCP', 'O 65', 'BTE', 'BTW', 'FIS', 'PTJI', 'UBCO', 'SPNC', 'SPT', 'LMAF', 
               'WPGO', 'SPFT', 'YDI', 'SMLH'] # add more?

def match_in_range(s, row, start_range, end_range):
    match = 0
    for i in range(start_range, end_range):
        if s == row[i]:
            match = 1
    return match        
    


#%%
for e in status_list:
    df3_part1[e] = df3_part1.apply(lambda row: match_in_range(e, row, 3, 6), axis=1)

#%% [markdown]
# See all the status codes in column 1

#%%
df3_part1['status_1'].value_counts()

#%% [markdown]
# See all the status codes in column 2. 
# A code will appear in this column, for each individual, for each time point where they have more than one status. 

#%%
df3_part1['status_2'].value_counts()

#%% [markdown]
# See all the status codes in column 3.<br>
# A code will appear in this column, for each individual, for each time point where they have more than one status. 

#%%
df3_part1['status_3'].value_counts()

#%% [markdown]
# See all the status codes in column 4.<br>
# A code will appear in this column, for each individual, for each time point where they have more than one status. 

#%%
df3_part1['status_4'].value_counts()

#%% [markdown]
# ## Find all status values
#%% [markdown]
# Find out all the different status codes in the DataFrame. This may be useful when everything is put back into a single DataFrame.

#%%
#all_status = np.unique(df3_part1.iloc[:, 3:7].values)
#all_status


#%%
#np.unique(df.iloc[:, 2:3].values) # This code takes a little while to run

#%% [markdown]
# ## Create summary flags for different status codes
#%% [markdown]
# Initial thoughts on flags:<br> 
# EMPL==1 if values in FIS C-UA C-UB WFP BTW<br>
#     WSWP-LF==1 if values in JA JB JST C-UA C-UB BTW<br>
#     WSWP=1 if values in DA IP OFP CA Mat<br>
#     EducTrain==1 if values in BTEA MOM<br>
#     LR flag==1 if values in C-UA C-UB UBCO UA UB<br>
#     CasualFlag==1 if values in C-UA C-UB<br>
#     CreditsFlag==1 if values in UBCO<br>
#     OverlapFlag==1 if WSWP-LF==1 and WSWP==1<br>
#%% [markdown]
# Implementation of 5 flags (differs a little from the above note)

#%%
df3_part1['LM_WSW'] = df3_part1.apply(lambda row: min(row['C-UA'] + row['C-UB'] + row['FASS'] + row['FISH'] + row['UA'] + 
                                                      row['UB'] + row['INTN'] + row['SEMP'] + row['SST'] + row['YDI'] + 
                                                      row['STEA'] + row['SMLH'], 1), axis=1)


#%%
df3_part1['WSW_Non_LM'] = df3_part1.apply(lambda row: min(row['CA'] + row['DA'] + row['IP'] + row['OFP'] + row['OFPJST'] + 
                                                          row['PRTA'] + row['SPC'] + row['WCP'] + row['O 65'] + 
                                                          row['SPNC'] + row['SPT'] + row['WPGO'], 1), axis=1)

df3_part1['Ed_or_Training'] = df3_part1.apply(lambda row: min(row['BTE'] + row['SPFT'], 1), axis=1)

df3_part1['Empl'] = df3_part1.apply(lambda row: min(row['BTW'] + row['C-UA'] + row['C-UB'] + row['FIS'] + row['INTN'] + 
                                                    row['PTJI'] + row['SEMP'] + row['SST'] + row['STEA'], 1), axis=1)


#%%
df3_part1['LR'] = df3_part1.apply(lambda row: min(row['C-UA'] + row['C-UB'] + row['UA'] + row['UB'] + row['UBCO'], 1), axis=1)


#%%
df3_part1.head()


#%%
df3_part1.describe()


#%%
df3_part1['LM_WSW'].value_counts()


#%%
df3_part1['WSW_Non_LM'].value_counts()


#%%
df3_part1['Ed_or_Training'].value_counts()


#%%
df3_part1['Empl'].value_counts()

#%% [markdown]
# ## Export DataFrame to CSV

#%%
import datetime

'\\\\cskma0294\\F\\Evaluations\\JobPath\\WeeklyStatus_part1_'+str(datetime.datetime.now()).split('.')[0][0:10]+'.csv'


#%%
df3_part1.to_csv('\\\\cskma0294\\F\\Evaluations\\JobPath\\WeeklyStatus_part1_'+str(datetime.datetime.now()).split('.')[0][0:10]+'.csv')


#%%
#df3['LM_WSW'] = df3.apply(lambda row: max(row['CA'] + row['C-UA'] + row['C-UB'] + row['FASS'] + row['FISH'] + 
#                                          row['UA'] + row['UB'] + row['INTN'] + row['SEMP'] + row['SST'] + 
#                                          row['STEA'], 1), axis=1)
#df3['WSW_Non_LM'] = df3.apply(lambda row: max(row['DA'] + row['IP'] + row['OFP'] + row['OFPJST'] + row['PRTA'] + 
#                                              row['SPC'] + row['WCP'] + row['O 65'], 1), axis=1)
#df3['Ed_or_Training'] = df3.apply(lambda row: row['BTE'], axis=1)
#df3['Empl'] = df3.apply(lambda row: max(row['BTW'] + row['C-UA'] + row['C-UB'] + row['FIS'] + row['INTN'] + 
#                                        row['PTJI'] + row['SEMP'] + row['SST'] + row['STEA'], 1), axis=1)

#%% [markdown]
# ## Combine the parts back into a single DataFrame
# 
# In the case where there are 3 parts:

#%%
#df3 = pd.concat([df3_part1, df3_part2, df3_part3])


#%%
#df3.tail()


#%%
#df3.head()

#%% [markdown]
# ## <em>Alternative approach to creating flags across different status codes</em>

#%%
def isLR(x):
    
    lr_types = ['C-UA', 'C-UB', 'UBCO', 'JA', 'JB']
    if type(x) is str:
        # just returns it untouched
        if x in lr_types:
            return 1
        else:
            return 0
    else:
        return x


#%%
#dfLR = df.applymap(isLR)
#dfLR.columns = new_columns
#dfLR.head()


