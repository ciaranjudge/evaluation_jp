# %%
import requests
import pandas as pd

def cso_statbank_data(table: str, dimensions: list):
    """Given a CSO Statbank table name and dimensions list, return dataframe with all table data.
    !!Assume that all dimensions have to be included in the dimensions list!!
    """
    url = f"https://www.cso.ie/StatbankServices/StatbankServices.svc/jsonservice/responseinstance/{table}"
    try: # try first 
        json_data = requests.get(url).json()
    except: # needed if above gives a SSL error
        json_data = requests.get(url, verify=False).json()
    dimension_values = [
        value["category"]["label"].values()
        for key, value in json_data["dataset"]["dimension"].items()
        if key in dimensions
    ]
    df_index = pd.MultiIndex.from_product(dimension_values)
    df_index.names = dimensions

    df = pd.DataFrame(json_data["dataset"]["value"], columns=["Value"], index=df_index)
    return df.reset_index()
