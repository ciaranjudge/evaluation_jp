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