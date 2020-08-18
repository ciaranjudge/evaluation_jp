# %%
import requests
import pandas as pd


def cso_statbank_data(
    table: str, dimensions: list = None, excluded_dimensions: list = ["id", "size", "role"]
):
    """Given a CSO Statbank table name and dimensions list, return dataframe with all table data.
    Will return all dimensions if optional dimensions list is None.
    Some dimensions seem to be for internal use by Statbank and should be excluded.
    """
    url = f"https://www.cso.ie/StatbankServices/StatbankServices.svc/jsonservice/responseinstance/{table}"
    try: 
        json_data = requests.get(url).json()
    except:  # needed if above gives a SSL error
        json_data = requests.get(url, verify=False).json()
    dimension_dict = {
        key: value["category"]["label"].values()
        for key, value in json_data["dataset"]["dimension"].items()
        if key not in excluded_dimensions
    }
    # Use cartesian product of dimension values to make a MultiIndex
    df_index = pd.MultiIndex.from_product(dimension_dict.values())
    df_index.names = dimension_dict.keys()

    df = pd.DataFrame(
        json_data["dataset"]["value"], columns=["value"], index=df_index
    ).reset_index()
    if dimensions is not None:
        df = df[dimensions + ["value"]]
    return df
