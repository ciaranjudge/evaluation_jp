#%%
import pandas as pd
from sqlalchemy import create_engine  # database connection
import sqlalchemy as sa
import datetime as dt
from IPython.display import display
import os
from pathlib import Path

pd.options.display.max_columns = None

#%%
f_drive = Path(r"\\cskma0294\F")
ists_extract_folder = f_drive / "ISTS"
ists_extract_filepaths = ists_extract_folder.glob("ists_ext*.sas7bdat")
ists_extract_filenames = [p.name for p in ists_extract_filepaths]

ists_sheep = [
    fname
    for fname in ists_extract_filenames
    if len(fname) == len("ists_ext_26aug2018.sas7bdat")
]

ists_goats = set(ists_extract_filenames) - set(ists_sheep)
# print("Sheep:\n", ists_sheep)
print("Goats:\n", ists_goats)

ists_sheep_dates = pd.to_datetime(ists_sheep, format="ists_ext_%d%b%Y.sas7bdat")
print(ists_sheep_dates)

#%%
engine = sa.create_engine("sqlite:///data/jobpath.db")
metadata = sa.MetaData()
#%%

df_test = pd.DataFrame(
    {
        "name": ["User 1", "User 2", "User 3"],
        "elephant": ["African", "African forest", "Asian"],
    }
)
df_test.to_sql("users", con=engine)
engine.execute("SELECT * FROM users").fetchall()

# # #%%
# # engine.dialect.has_table(engine, 'weekly_statuses')

# #%%
# engine.execute("SELECT * FROM users").fetchall()

#%%
dft = pd.read_sql("users", engine)
dft

#%%
df_long = pd.read_csv("data/weekly_status_long.csv")
df_long.info()


#%%
chunksize = 4 * 10 ** 6

#%%
# 1. Read each chunk from original file
# 2. Does target database table exist?
# Yes -> 2a. Get table columns
#        2b. Add columns from df_chunk if not already in table
#        2c. Append df_chunk rows
# No  -> 2a. Create table with rows, columns from df_chunk
first_chunk = True
for df_chunk in pd.read_csv(
    "data/weekly_status_long.csv", chunksize=chunksize, infer_datetime_format=True
):
    df_wide = df_chunk["status"].str.get_dummies(sep="+").astype("bool")
    df_chunk = pd.concat([df_chunk, df_wide], axis="columns")
    df_chunk = df_chunk.set_index(["ppsn", "date"])
    # # df_chunk.to_sql('weekly_statuses', con=engine, if_exists='append')
    if first_chunk:
        df = df_chunk
        first_chunk = False
        df_chunk.to_csv("data/weekly_status_wide.csv", mode="w")
    else:
        df = df.append(df_chunk, sort=False).fillna(False)
        df_chunk.to_csv("data/weekly_status_wide.csv", mode="a")
    df.info()

#%%
df.info()

#%%
users = sa.Table("users", metadata, autoload=True, autoload_with=engine)

#%%
test_list = ["name", "elephant", "habitat", "food"]
extra_cols = set(test_list) - set(users.columns.keys())


#%%
df = pd.read_csv("data/weekly_status_long.csv", nrows=chunksize)
df = df.set_index(["ppsn", "date"])

#%%
df.head()


#%%
df = df.set_index(["ppsn", "date"])

#%%
df_wide = df["status"].str.get_dummies(sep="+")

#%%
df_wide.describe()

#%%
df = pd.concat([df, df_wide], axis="columns")


#%%
df.head()


#%%
df.loc[df["status"].str.contains("+", regex=False)].groupby("status").sum()


#%%
