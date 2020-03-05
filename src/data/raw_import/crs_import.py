# %%
from pathlib import Path
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pandas.api.extensions import register_dataframe_accessor
import sqlalchemy as sa

data_folder = Path("//Cskma0294/f/Evaluations/data")
crs_client_csv = data_folder / "crs_client.csv"
sqlite_db = data_folder / "wwld.db"
engine = sa.create_engine(f"sqlite:///{sqlite_db}", echo=False)


usecols = [
    "BEFORE_RSI_NO",
    "BEFORE_SEX",
    "BEFORE_DATE_OF_BIRTH",
    "BEFORE_DOB_VER_IND",
    "BEFORE_INSURANCE_ENTRY_DATE",
    "BEFORE_INSURANCE_DOE_VER_IND",
    "BEFORE_DATE_OF_MARRIAGE",
    "BEFORE_DOM_VER_IND",
    "BEFORE_MARITAL_STATUS_ACODE",
    "BEFORE_DECEASED_IND",
    "BEFORE_DATE_OF_DEATH",
    "BEFORE_EMPLOYER_NO",
    "BEFORE_TOTAL_CONS_PAID",
    "BEFORE_TCP_RECK_IND",
    "BEFORE_TCP_ACTUAL_IND",
    "BEFORE_SI5_YEAR",
    "BEFORE_PRIMARY_REMARKS",
    "BEFORE_GCD",
    "BEFORE_TAX_UNIT",
    "BEFORE_SELF_EMP_IND",
    "BEFORE_HOUSEHOLD_HEAD_IND",
    "BEFORE_CORRESPONDENCE_IND",
    "BEFORE_CLT_INFO_STATUS_NCODE",
    "BEFORE_NAT_COUNTRY_NCODE",
    "BEFORE_HOUSEHOLD_ID_NO",
    "BEFORE_HOUSEHOLD_START_DATE",
    "BEFORE_RECORD_VERS_NO",
]
cleaned_cols = [col_name.replace("BEFORE_", "").lower() for col_name in usecols]
col_dict = dict(zip(usecols, cleaned_cols))
col_dict["BEFORE_RSI_NO"] = "person_id"
cat_cols = [
    "sex",
    "marital_status_acode",
    "gcd",
    "tax_unit",
]
int_cols = ["clt_info_status_ncode", "nat_country_ncode", "si5_year", "record_vers_no"]


def sas_date_to_datetime(
    series, min_date=pd.Timestamp("1900-01-01"), max_date=pd.Timestamp.now()
):
    series = pd.to_numeric(series, errors="coerce")
    min_days = (min_date - pd.Timestamp("1960-01-01")).days
    gt__min_days = series > min_days
    max_days = (max_date - pd.Timestamp("1960-01-01")).days
    le__max_days = series <= max_days
    clean_series = series.where(gt__min_days & le__max_days, np.nan)
    date_series = pd.to_datetime(
        clean_series, unit="D", origin=pd.Timestamp("1960-01-01")
    )
    return date_series


@register_dataframe_accessor("cleanup")
@dataclass
class CleanupAccessor:
    data: pd.DataFrame

    def sas_date_cols_to_datetime(self):
        sas_date_cols = [
            col
            for col in self.data.select_dtypes(exclude=["datetime"]).columns.to_list()
            if "date" in col.lower()
        ]
        print("  Found SAS date columns for conversion:", sas_date_cols)
        for col in sas_date_cols:
            self.data[col] = sas_date_to_datetime(self.data[col])
            print(f"    ...converted {col} to datetime")
        return self.data

    def ind_cols_to_bool(self):
        ind_cols = [
            col
            for col in self.data.select_dtypes(exclude=["datetime"]).columns.to_list()
            if "_ind" in col.lower()
        ]
        print("  Found indicator columns for conversion:", ind_cols)
        for col in ind_cols:
            eq__y_or_n = self.data[col].isin(["y", "n"])
            self.data[col] = (
                self.data[col]
                .where(eq__y_or_n, np.nan)
                .replace({"Y": True, "N": False})
                .astype("boolean")
            )
            print(f"    ...converted {col} to bool")
        return self.data

    def cat_cols_to_categorical(self, cat_cols):
        print("  Categorical columns for conversion:", cat_cols)
        for col in cat_cols:
            self.data[col] = self.data[col].astype("category")
            print(f"    ...converted {col} to categorical")
        return self.data

    def int_cols_to_int(self, int_cols):
        print("  Int columns for conversion:", int_cols)
        for col in int_cols:
            self.data[col] = pd.to_numeric(self.data[col], errors="coerce")
            print(f"    ...converted {col} to int")
        return self.data


# %%
crs_reader = pd.read_csv(
    crs_client_csv,
    chunksize=5_000_000,
    usecols=usecols,
    encoding="ISO-8859–1",
    low_memory=False,
)
print("Here we go!")
for chunk_number, chunk in enumerate(crs_reader):
    print(f"Finished reading chunk #{chunk_number}")
    chunk = (
        chunk.rename(columns=col_dict)
        .cleanup.sas_date_cols_to_datetime()
        .cleanup.ind_cols_to_bool()
        .cleanup.cat_cols_to_categorical(cat_cols)
        .cleanup.int_cols_to_int(int_cols)
    )
    if chunk_number == 0:
        chunk.to_sql("crs_clients", con=engine, if_exists="replace")
    else:
        chunk.to_sql("crs_clients", con=engine, if_exists="append")
    print(f"  Successfully wrote chunk #{chunk_number} to SQL!")


# # %%
# original_cols = pd.read_csv(
#     crs_client_csv, nrows=5, encoding="ISO-8859–1", low_memory=False,
# ).columns
# df = pd.read_csv(
#     crs_client_csv,
#     names=original_cols,
#     skiprows=20_000_000,
#     nrows=500_000,
#     usecols=usecols,
#     encoding="ISO-8859–1",
#     low_memory=False,
# )
# # %%
# data = (
#     df.rename(columns=col_dict)
#     .cleanup.sas_date_cols_to_datetime()
#     .cleanup.ind_cols_to_bool()
#     .cleanup.cat_cols_to_categorical(cat_cols)
#     .cleanup.int_cols_to_int(int_cols)
# )

