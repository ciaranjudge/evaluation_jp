# %%
# Standard library
from typing import List, Set, Dict, Tuple, Optional, Callable, Union
from pathlib import Path
from functools import wraps

# External packages
import pandas as pd


def get_name(
    type_name: str, start_date: pd.Timestamp, freq: str = None, prefix: str = None
) -> str:
    """
    Create logical name as f"[{name_prefix}__]{type_name}_{formatted_date}"
    """
    f_prefix = f"{prefix}__" if prefix is not None else ""
    if freq is not None:
        period = start_date.to_period(freq)
        date_format = (
            "%Y-Q%q" if period.freq == "Q" else "%Y-M%m" if freq == "M" else "%Y-%m-%d"
        )
        f_date = period.strftime(date_format)
    else:
        date_format = "%Y-%m-%d"
        f_date = start_date.strftime(date_format)
    return f"{f_prefix}{type_name}_{f_date}"


def get_path(root, name) -> Path:
    return Path.cwd() / "data" / "processed" / "/".join(r for r in root) / name
    

# Populate decorator
def populate(populator):
    """
    Decorator to control data setup. 
    Loads data["populator"] if file exists, else runs populator() and saves data["populator"]
    
    Parameters
    ----------
    populator(*args, **kwargs)
        Expect a method of an EvaluationXxx class, so that first argument in *args == self...
        ...but can be any function where `args[0]` has attributes 
        -- data: dict
        -- rebuild_all: bool
        -- logical_root: Tuple[str]
        -- logical_name: str
    """

    @wraps(populator)
    # `*args` means all the non-keyword arguments to the function wrapped by this decorator;
    # `**kwargs` means all the keyword arguments (arg=something) in the wrapped function.
    def populated(*args, **kwargs):
        data_name = populator.__name__.replace(
            "get_", ""
        )  # Use populator function name as dataframe name!
        build = args[0].rebuild_all  # `args[0]` is `self` from the wrapped object

        # Try to load data from storage if not explicitly rebuilding
        if not build:
            print("`rebuild_all` flag not set so try to load data")
            try:
                index = kwargs["index"]
                logical_root=args[0].root
                #   logical_name=args[0].name,
                print(index)
                # data = load_data(
                #     data_name=data_name,
                #     logical_root=args[0].root,
                #     logical_name=args[0].name,
                #     index=index
                # )
                # print(f"Successfully loaded data '{data_name}''")
                # return data
            except:
                print(f"Couldn't load data '{data_name}' so try to build and save it")
                build = True

        # Set up and save if build is True (either originally or because loading failed)
        if build:
            print(f"Running get_{data_name}() to set up data '{data_name}'")
            # Now run the original populator() with its original arguments (*args, **kwargs)
            data = populator(*args, **kwargs)
            print(f"Saving data '{data_name}'")
            save_data(
                data=data,
                data_name=data_name,
                logical_root=args[0].logical_root,
                logical_name=args[0].logical_name,
                include_archive=True,
            )
            return data

    return populated


# persist data
def save_data(
    data: pd.DataFrame,
    data_name: str,
    logical_root: Tuple[str],
    logical_name: str,
    include_archive: bool = True,
    specified_file: Optional[Path] = None,
) -> None:
    """
    Save dataframe to persistent storage in standard or specified location

    Standard storage format is feather due to fast read/write speed.
    By default, archive copy also saved to same folder with current timestamp, as gzipped csv.

    Parameters
    ----------
    data: pd.DataFrame
        The actual dataframe to be saved

    data_name: str
        The reference name of this dataframe.
        Used as part of filename and as lookup key in data dictionary

    logical_name: str
        Name of this object. Used to create foldername and as part of filename

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")

    include_archive: bool = True
        If True, create a timestamped gzipped archive file as well as main file

    specified_file: Optional[Path] = None
        Save as specified_file instead of standard. Assumes full path.

    Returns
    -------
    None
        Just saves the dataframe.
    """

    if specified_file is not None:
        # Not checking anything here - just assume specified_file makes sense!
        data.to_feather(specified_file)
    else:
        # Set up folder path: everything to do with this object gets saved here
        folderpath = get_path(logical_root, logical_name)
        if not folderpath.exists():
            folderpath.mkdir(parents=True, exist_ok=True)

        # Set up filename (without any extension)
        filename = f"{logical_name}_{data_name}"

        # Main save file is in .feather format - it's amazingly fast!
        filepath = folderpath / f"{filename}.feather"
        data.reset_index().to_feather(filepath)
        print(f"Saved successfully to feather as {filename}")

        # Save timestamped archive file as (gz-compressed) csv as well.
        if include_archive:
            now_stamp = pd.Timestamp.now().strftime("%Y%m%d%H%M")
            archive_filepath = folderpath / f"{filename}_v{now_stamp}.gz"
            data.to_csv(archive_filepath)


# load data from storage
def load_data(
    data_name: str,
    logical_root: Tuple[str],
    logical_name: str,
    index: Union[list, str],
    specified_file: Optional[Path] = None,
) -> pd.DataFrame:

    """
    Load dataframe from persistent storage in standard or specified location

    Standard storage format is feather due to fast read/write speed.

    Parameters
    ----------
    data_name: str
        The reference name of this dataframe.
        Used as part of filename and as lookup key in data dictionary

    logical_name: str
        Name of this object. Used to create foldername and as part of filename

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")

    specified_file: Optional[Path] = None
        Save as specified_file instead of standard. Assumes full path.

    Returns
    -------
    data: pd.DataFrame
        The dataframe that's just been loaded
    """
    if specified_file is not None:
        # Arg! Need to check what type of file this is...
        # ...for now, make glorious assumption that it's some kind of csv from archive
        data = pd.read_csv(specified_file).set_index(index, verify_integrity=True)
    else:
        folderpath = get_path(logical_root, logical_name)
        filename = f"{logical_name}_{data_name}"
        filepath = folderpath / f"{filename}.feather"
        print(filepath)
        data = pd.read_feather(filepath)
        print(data)
        data = data.set_index(index, verify_integrity=True)

    return data


# # %%
# @dataclass
# class Team:
#     on_first: str = "Who"
#     on_second: str = "What"
#     on_third: str = "I Don't Know"
#     rebuild_all: bool = True
#     data: dict = None
#     logical_root: Tuple[str] = (".",)
#     logical_name: str = "who"

#     def __post_init__(self):
#         self.data = {}
#         self.get_team_data()

#     def you_throw_the_ball_to_who(self, who_picks_it_up: bool = False):
#         print("Naturally.")
#         if who_picks_it_up is True:
#             print("Sometimes his wife picks it up.")

#     @populate
#     def get_team_data(self):
#         print(self.rebuild_all)
#         self.data["team_data"] = pd.DataFrame(
#             data=[[1, 2], [3, 4]], columns=["who", "what"]
#         )
# t = Team()
