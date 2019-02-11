# %%
import numpy as np
import pandas as pd  # python package for dataframes
import matplotlib.pyplot as plt
from pandas.api.types import union_categoricals
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import sqlalchemy as sa

pd.options.display.max_columns = None
pd.options.display.max_rows = None

# %%
status_summary = (
    pd.read_csv("data\status_summary.csv", index_col="Code")
    .drop("Notes", axis="columns")
    .fillna(0)
    .astype("int8")
)

#%%
engine = sa.create_engine("sqlite:///data/jobpath.db")
metadata = sa.MetaData()

# %%
in_csv = "\\\\cskma0294\\F\\Evaluations\\JobPath\\\Quarterly_status\\WeeklyStatus.zip"

# %%
chunksize = 10 ** 5
nrows = None
csv_parser = pd.read_csv(in_csv, chunksize=chunksize, nrows=nrows)

# %%
def concat_categorical(df_a, df_b, ignore_index=True):
    for cat_col in df_a.select_dtypes(["category"]):
        a = df_a[cat_col]
        b = df_b[cat_col]
        a_b = union_categoricals([a, b], ignore_order=True)
        a.cat.set_categories(a_b.categories, inplace=True)
        b.cat.set_categories(a_b.categories, inplace=True)
    return pd.concat([df_a, df_b], ignore_index=ignore_index)

elapsed_time = timedelta(0)
for chunk_index, df in enumerate(csv_parser):
    start_time = datetime.now()
    in_df_shape = df.shape[0]
    # Remove unneeded column
    df = df.drop(["Purge_ind"], axis=1)

    # Reshape by melting to long format, converting columns to date values
    df = pd.melt(df, id_vars=["ppsn"], var_name="date", value_name="status")

    # Tidy status column and remove statuses that are just '.'
    df["status"] = df["status"].str.lstrip(to_strip=".\+")
    df = df[(df["status"] != ".") & (df["status"] != "")]

    # Remove any records with no date and drop nulls (there's lots of nulls!)
    df = df[df["date"] != ""].dropna()

    # Remove 'StatusEnd_' from start of dates
    df["date"] = df["date"].str.lstrip("StatusEnd_")
    # Convert dates to datetime format
    df["date"] = pd.to_datetime(df["date"], infer_datetime_format=True)

    # Convert string variables to categorical
    df[["status", "ppsn"]] = df[["status", "ppsn"]].astype("category")

    # Append current chunk to overall output
    if chunk_index == 0:
        out_df = df
        df.to_sql("weekly_status", con=engine, if_exists="replace")
        in_df_so_far_shape = in_df_shape
    # ...and otherwise add this extract to the end of out_df and the database table
    else:
        out_df = concat_categorical(out_df, df)
        df.to_sql("weekly_status", con=engine, if_exists="append")
        in_df_so_far_shape = in_df_so_far_shape + in_df_shape
    end_time = datetime.now()
    chunk_time = end_time - start_time
    elapsed_time = elapsed_time + chunk_time
    print(
    f"""
    
    ------------------------------------------------------
    
    Chunk {chunk_index} 
    Rows read so far: {in_df_so_far_shape}
    Time taken (this chunk): {chunk_time}
    Elapsed time: {elapsed_time}
    """)
    print("This chunk info:")
    print(df.info())
    print(f"\nOverall info:")
    print(out_df.info())

# %%
cat_df = pd.DataFrame(out_df["status"].cat.categories, columns=["status"])

max_sep = cat_df["status"].str.count("\+").max() + 1
status_cols = ["s" + str(i) for i in range(max_sep)]
cat_df[status_cols] = cat_df["status"].str.split(pat="\+", expand=True)

# cat_df_dummies = (
#     pd.get_dummies(cat_df, prefix="", prefix_sep="", drop_first=True)
#     .max(level=0, axis=1)
#     .astype("bool")
# ).to_sparse(fill_value=False)
# cat_df = pd.concat([cat_df, cat_df_dummies], axis=columns)

# %%
cat_df_long = (
    pd.melt(cat_df, id_vars="status", value_name="code")
    .dropna()
    .drop("variable", axis="columns")
)
cat_df_long['status'] = cat_df_long['status'].astype('category')

# %%
cat_df_status = pd.merge(
    cat_df_long, status_summary, left_on="code", right_index=True
)
cat_df_status.info()

# %%
cat_df_map = (cat_df_status
                .groupby("status")
                .max()
                .sort_index()
                .drop("code", axis=1)
                .fillna(0)
                .astype('int8')
            )
cat_df_map.info()

# %%
out_df = out_df.sort_values("status")

# %%
%time df = pd.merge(out_df, cat_df_map, on="status")
%time df = df.sort_values('date')

#%%
%time df['year'] = df['date'].dt.year

#%%
df.describe()

#%%
df.info(memory_usage='deep')

# %%
pd.to_numeric(df['wsw_labour'])

#%%
df.info(memory_usage='deep')

#%%
df.describe()

#%%
