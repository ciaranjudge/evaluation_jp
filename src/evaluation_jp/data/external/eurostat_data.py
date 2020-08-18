# %%
import requests
import pandas as pd


def eurostat_data(query: str):
    """Given Eurostat web services query parameters, return a Pandas DataFrame.
    Eurostat API doesn't like returning whole tables so need to specify query in advance.
    Easiest way is to use Eurostat's query builder then copy+paste!
    https://ec.europa.eu/eurostat/web/json-and-unicode-web-services/getting-started/query-builder
    """
    url = f"http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/{query}"
    try:
        json_data = requests.get(url).json()
    except:  # needed if above gives a SSL error
        json_data = requests.get(url, verify=False).json()
    dimension_dict = {
        key: value["category"]["label"].values()
        for key, value in json_data["dimension"].items()
    }
    # Use cartesian product of dimension values to make a MultiIndex
    df_index = pd.MultiIndex.from_product(dimension_dict.values())
    df_index.names = dimension_dict.keys()

    df = pd.DataFrame(
        json_data["value"].values(), columns=["value"], index=df_index
    ).reset_index()
    return df


# %%
