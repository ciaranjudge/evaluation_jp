# %%
import requests
import pandas as pd
from pandas.io.json import json_normalize



# def get_statbank_data(table_name, )

# %%
def cso_statbank_data(table: str, dimensions: list):
    """Given a CSO Statbank table name and dimensions list, return dataframe with all table data.
    !!Assume that all dimensions have to be included in the dimensions list!!
    """
    url = f"https://www.cso.ie/StatbankServices/StatbankServices.svc/jsonservice/responseinstance/{table}"
    json_data = requests.get(url).json()

    dimension_values = [
        value["category"]["label"].values()
        for key, value in json_data["dataset"]["dimension"].items()
        if key in dimensions
    ]
    df_index = pd.MultiIndex.from_product(dimension_values)
    df_index.names = dimensions

    df = pd.DataFrame(json_data["dataset"]["value"], columns=["Value"], index=df_index)
    return df.reset_index()


# %%

# def create_dataframe(data):
#     """ dict -> pd.DataFrame
#     Create a dataframe from the json file data
#     """
#     # 1# Create the MULTIINDEX of the dataframe
#     dimensions = [dimension for dimension in data["dataset"]["dimension"]]
#     for d_key, d_value in dimensions.items():

#     # List of Age Group
#     age = [age for age in dimensions["Age Group"]["category"]["label"].values()]

#     # List of Sex
#     sex_labels = [
#         sex for sex in data["dataset"]["dimension"]["Sex"]["category"]["label"].values()
#     ]

#     # List of Quarter
#     period_labels = [
#         period
#         for period in data["dataset"]["dimension"]["Quarter"]["category"][
#             "label"
#         ].values()
#     ]

#     # List of Statistic
#     stat = []
#     for x in data["dataset"]["dimension"]["Statistic"]["category"]["label"].values():
#         stat.append(x)


#     # 2# Create the DATAFRAME from the json file
#     total_df = pd.DataFrame(data["dataset"]["value"], index=df_index)
#     # Column name
#     total_df.columns = ["Value"]
#     # MultiIndex level names
#     total_df.index.names = ["Age_group", "Sex", "Quarter", "Statistic"]

#     return total_df.reset_index()


# %%
