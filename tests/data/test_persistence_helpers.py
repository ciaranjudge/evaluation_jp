
    import evaluation_jp.data.import_helpers
    import evaluation_jp.data.persistence_helpers
    import evaluation_jp.features.build_features
    import evaluation_jp.features.metadata_helpers
    import evaluation_jp.features.selection_helpers
    import evaluation_jp.models.model
    import evaluation_jp.models.slice
    import evaluation_jp.models.period
    

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



# # %%
# from typing import ClassVar, List, Set, Dict, Tuple, Optional
# from dataclasses import dataclass, Field, fields, asdict, astuple, is_dataclass
# import pandas as pd
# from evaluation_jp.data.persistence_helpers import get_name, get_path, populate

# # @dataclass
# # class Metadata:
# #     freq: 
# #     root: str
    
# #     rebuild: bool = False


# # @dataclass
# # class Populator:
    
    
    
# #     ### -- Attributes set in __post_init__ -- ###
# #     name: str = Field(init=False)


# #     @wraps(populator)
# #     # `*args` means all the non-keyword arguments to the function wrapped by this decorator;
# #     # `**kwargs` means all the keyword arguments (arg=something) in the wrapped function.
# #     def populated():

# @dataclass
# class Team:
#     on_first: str = "Who"
#     on_second: str = "What"
#     on_third: str = "I Don't Know"
#     rebuild_all: bool = False
#     data: dict = None
#     logical_root: Tuple[str] = (".",)
#     logical_name: str = "who"

#     def __post_init__(self):
#         self.new_thingy = "new thingy"
#         print(asdict(self))
#         print(self.__dict__)

#     def you_throw_the_ball_to_who(self, who_picks_it_up: bool = False):
#         print("Naturally.")
#         if who_picks_it_up is True:
#             print("Sometimes his wife picks it up.")

#     @populate
#     def get_team_data(self, index="position") -> pd.DataFrame:
#         data = pd.DataFrame(
#             data={"position": ["who", "what"], "first": [1, 2], "second":[3, 4]} 
#         ).set_index("position")
#         return data
# t = Team()
# t.get_team_data(index="position")

# d = pd.DataFrame(
#             data={"position": ["who", "what"], "first": [1, 2], "second":[3, 4]} 
#         ).set_index("position")

# d = pd.read_feather("D:/repos/evaluation_jp/data/processed/who/who_team_data.feather")

