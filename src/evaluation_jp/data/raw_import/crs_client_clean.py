# %%
from pathlib import Path
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pandas.api.extensions import register_dataframe_accessor
import sqlalchemy as sa

data_folder = Path("//Cskma0294/f/Evaluations/data")
sqlite_db = data_folder / "wwld.db"
engine = sa.create_engine(f"sqlite:///{sqlite_db}", echo=False)
insp = sa.engine.reflection.Inspector.from_engine(engine)


def get_datetime_cols(table_name):
    column_metadata = insp.get_columns(table_name)
    datetime_cols = [
        col["name"]
        for col in column_metadata
        if type(col["type"]) == sa.sql.sqltypes.DATETIME
    ]
    return datetime_cols


# @pd.api.extensions.register_dataframe_accessor("cleanup")
# @dataclass
# class CleanupAccessor:
#     data: pd.DataFrame

# %%
crs_cols = insp.get_columns("crs_clients")

# %%
usecols = [
    "person_id",
    "sex",
    "date_of_birth",
    "insurance_entry_date",
    "date_of_marriage",
    "marital_status_acode",
    "deceased_ind",
    "date_of_death",
]

query = f"""select {usecols} from crs_clients"""
clients = pd.read_sql(query, con=engine)

# %%
dob__notna = clients["date_of_birth"].notna()
df = clients[dob__notna]

# %%
person_dob_groups = df.groupby(["person_id", "date_of_birth"])
group_df = person_dob_groups.agg({"sex": "count"})
# %%
group_df.describe()

# %%
person_df = (
    group_df.reset_index().groupby(["person_id"]).agg({"date_of_birth": "count"})
)
bad_df = person_df[person_df["date_of_birth"] > 1]

# %%
bad_index = crs_client_df["person_id"].isin(bad_df.index)

# %%
bad_index.value_counts()

# %%
bad = crs_client_df[bad_index].sort("person_id")

# %%
bad.head(100)

# %%
bad.groupby("person_id").agg({"date_of_birth": "count"}).describe()

# %%
