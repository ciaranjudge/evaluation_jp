if __name__ == "__main__":
    import evaluation_jp.data.import_helpers as import_helpers

# returned_df = get_ists_claims(
#     pd.Timestamp("2020-01-03"),
#     lr_flag=True,
#     # columns=["lr_code", "clm_comm_date", 'lr_flag'],
#     ids=["0070688N", "0200098K"],
# )
# returned_df.describe()

#%%
# test


# context1 = ssl._create_unverified_context() # add context below
# url_sector = "https://www.cso.ie/StatbankServices/StatbankServices.svc/jsonservice/responseinstance/LRM02"
# try:
#     with urlopen(url_sector) as f:
#         LRM02 = json.load(f)
#         LR_monthly = create_dataframe(LRM02)
# except:
#     with urlopen(url_sector, context=context1) as f:
#         LRM02 = json.load(f)
#         LR_monthly = create_dataframe(LRM02)
