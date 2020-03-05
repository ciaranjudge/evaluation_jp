# %%
from pathlib import Path
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pandas.api.extensions import register_dataframe_accessor
import sqlalchemy as sa


def unpack(listlike):
    return ", ".join(listlike)


data_folder = Path("//Cskma0294/f/Evaluations/data")
crs_client_csv = data_folder / "crs_client.csv"
sqlite_db = data_folder / "wwld.db"
engine = sa.create_engine(f"sqlite:///{sqlite_db}", echo=False)

# %%
def unpack(listlike):
    return ", ".join(listlike)


usecols = ["ppsn"]
query = f"""select {unpack(usecols)} from les"""
clients = pd.read_sql(query, con=engine)
test_clients = clients.head(2000)
test_list = list(test_clients["ppsn"])

# %%
query_text = f"""select * from les where ppsn in :id_list"""
query = sa.sql.text(query_text).bindparams(
    sa.sql.expression.bindparam("id_list", expanding=True)
)
params = {"id_list": test_clients["ppsn"].tolist()}
les_out = pd.read_sql(query, con=engine, params=params)



# %%
