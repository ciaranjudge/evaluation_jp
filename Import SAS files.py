#!/usr/bin/env python
# coding: utf-8

# ## Import a SAS file and convert to dataframe

# ##### call packages

# In[1]:


from pandas.io.sas.sas7bdat import SAS7BDATReader
import datetime as dt


# ##### define date converter function

# In[2]:


# Converter function for dates in sas7bdat format.
# Automatic conversion via pd.
def sas_dates_to_datetime(in_col):
    out_col = pd.to_timedelta(in_col, unit="D") + pd.Timestamp("1960-01-01")
    return out_col


# In[4]:


# Irritatingly, have to use SAS7BDATReader instead of more natural pd.read_sas()
    # ...because of an issue with converting SAS dates that causes the read to fail.
    # pd.read_sas() is just a thin wrapper that checks SAS datafile type from file extension.
    # For '.sas7bdat' files, it then creates a SAS7BDATReader anyway...
    # ...but pd.read_csv() doesn't expose SAS7BDATReader's "convert_dates=False"
    # ...so only way out is to just directly create the SAS7BDATReader!
in_reader = SAS7BDATReader(row.filepath, convert_dates=False, convert_text=False)

    # This creates a pd.DataFrame from the SAS7BDATReader object.
    # Can specify a number of rows inside read() - empty () means read all the rows!
in_df = in_reader.read()
   


# ##### attempting to import 2015 as test, if successful import other years and rename in_df to reflect year

# In[ ]:


# Irritatingly, have to use SAS7BDATReader instead of more natural pd.read_sas()
    # ...because of an issue with converting SAS dates that causes the read to fail.
    # pd.read_sas() is just a thin wrapper that checks SAS datafile type from file extension.
    # For '.sas7bdat' files, it then creates a SAS7BDATReader anyway...
    # ...but pd.read_csv() doesn't expose SAS7BDATReader's "convert_dates=False"
    # ...so only way out is to just directly create the SAS7BDATReader!
in_reader = SAS7BDATReader('\\\cskma0294\\F\\SH\\cso_payments\\pmts2015.sas7bdat', convert_dates=False, convert_text=False)

    # This creates a pd.DataFrame from the SAS7BDATReader object.
    # Can specify a number of rows inside read() - empty () means read all the rows!
in_df = in_reader.read()
   


# In[ ]:





# In[4]:


# Irritatingly, have to use SAS7BDATReader instead of more natural pd.read_sas()
    # ...because of an issue with converting SAS dates that causes the read to fail.
    # pd.read_sas() is just a thin wrapper that checks SAS datafile type from file extension.
    # For '.sas7bdat' files, it then creates a SAS7BDATReader anyway...
    # ...but pd.read_csv() doesn't expose SAS7BDATReader's "convert_dates=False"
    # ...so only way out is to just directly create the SAS7BDATReader!

my_path='\\\cskma0294\\F\\SH\\cso_payments'
for filename in os.listdir(my_path):
    if filename.startswith("pmts20"): 
        in_reader = SAS7BDATReader(row.filepath, convert_dates=False, convert_text=False)
        continue
    else:
        continue


    # This creates a pd.DataFrame from the SAS7BDATReader object.
    # Can specify a number of rows inside read() - empty () means read all the rows!
in_df = in_reader.read()
   


# In[ ]:


# Now we can fix those dates!
date_cols = [col for col in in_df.columns.to_list() if "date" in col.lower()]
for col in date_cols:
    in_df[col] = sas_dates_to_datetime(in_df[col])


# ##### column selection

# In[ ]:


# Just keep the columns that we actually need
  in_df = in_df[keep_cols]

  # Decode SAS bytestrings (brrrrrr) as normal strings
  for col in byte_cols:
      in_df[col] = in_df[col].str.decode("utf-8")

