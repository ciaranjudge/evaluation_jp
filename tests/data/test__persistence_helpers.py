import evaluation_jp.data.persistence_helpers
 
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

