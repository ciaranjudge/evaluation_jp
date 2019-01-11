# %%
import pandas as pd  # python package for dataframes
from datetime import datetime

pd.options.display.max_columns = None
pd.options.display.max_rows = None

# %%
status_summary = pd.read_csv("data\status_summary.csv")

# %%
in_csv = "\\\\cskma0294\\F\\Evaluations\\JobPath\\\Quarterly_status\\WeeklyStatus.zip"


# %%
chunksize = 10 ** 4
n_chunks = 1
csv_parser = pd.read_csv(in_csv, chunksize=chunksize, nrows=chunksize * n_chunks)

# %%

for chunk_index, df in enumerate(csv_parser):
    # Remove unneeded column
    df = df.drop(["Purge_ind"], axis=1)

    # Reshape by melting to long format, converting columns to date values
    df = pd.melt(df, id_vars=["ppsn"], var_name="date", value_name="status")
    
    # Tidy status column and remove statuses that are just '.'
    df["status"] = df["status"].str.lstrip(to_strip=".\+")
    df = df[df["status"] != "."]

    # Remove any records with no date and drop nulls (there's lots of nulls!)
    df = df[df["date"] != ""].dropna()

    # Remove 'StatusEnd_' from start of dates
    df["date"] = df["date"].str.lstrip("StatusEnd_")
    # Convert dates to datetime format
    df["date"] = pd.to_datetime(df["date"], infer_datetime_format=True).dt.date

    # Append current chunk to overall output
    df['status'] = df['status'].astype('category')
    print(chunk_index)
    if chunk_index == 0:
        out_df = df
    else:
        out_df = out_df.append(df)

# %%
 # Unpack status column
mask = out_df['status'].str.contains('\+')
process_df = out_df[mask]
result_df = out_df[~mask]

max_sep = process_df['status'].str.count('\+').max() + 1
status_cols = ['s' + str(i) for i in range(max_sep)] 
process_df[status_cols] = process_df['status'].str.split(pat='\+', expand=True)

# process_df = process_df.drop('status', axis='columns')
# for col in status_cols:
#     append_df = process_df[['ppsn', 'date', col]].dropna()
#     result_df = result_df.append(append_df)
# result_df.describe()
process_df.head()


# %%
out_df.info()

# %%
print([f"{c}: {df[c].dtype}" for c in out_df.columns])

# %%
pd.a(df["status"])
# %%
df.info()

# %%
df = df.set_index(["ppsn", "date"])

# %%
# dates = df['date'].unique()
df.index.get_level_values(1)

# %%

# %%
""" df_dummies = (pd.get_dummies(df, prefix='', prefix_sep='')
              .max(level=0, axis=1)
              .astype('bool')
              )

# %%
df_dummies.info() """
""" 
# %%
df_with_dummies = pd.concat([df, df_dummies], axis='columns')

# %%
df_with_dummies.info()

# %%
four_concurrent = df_with_dummies.groupby(status_cols).sum()
print(four_concurrent)
# %%
three_concurrent = df_with_dummies.groupby(status_cols[:3]).sum()
print(three_concurrent)

# %%
two_concurrent = df_with_dummies.groupby(status_cols[:2]).sum()
print(two_concurrent)

# %%
df_dummies.columns
 """

# %%
df_melt = pd.melt(df, id_vars=["ppsn", "date"], var_name="status", value_name="flag")

# %%
df_melt.info()
