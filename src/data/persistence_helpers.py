# %%
# Standard library
from typing import List, Set, Dict, Tuple, Optional
from pathlib import Path
from functools import wraps

# External packages
import pandas as pd

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
        data_name = populator.__name__  # Use populator function name as dataframe name!
        build = args[0].rebuild_all  # `args[0]` is `self` from the wrapped object

        # Try to load data from storage if not explicitly rebuilding
        if not build:
            print("`rebuild_all` flag not set so try to load data")
            try:
                args[0].data[data_name] = load_data(
                    data_name=data_name,
                    logical_root=args[0].logical_root,
                    logical_name=args[0].logical_name,
                )
                print(f"Successfully loaded data['{data_name}']")
            except:
                print(f"Couldn't load data['{data_name}'] so try to build and save it")
                build = True  # Couldn't load so now need to build the dataframe and save it

        # Set up and save if build is True (either originally or because loading failed)
        if build:  
            print(f"Running {data_name}() to set up data['{data_name}']")
            # Now run the original populator() with its original arguments (*args, **kwargs)
            populator(*args, **kwargs)
            print(f"Saving data['{data_name}']")
            save_data(
                data=args[0].data[data_name],
                data_name=data_name,
                logical_root=args[0].logical_root,
                logical_name=args[0].logical_name,
                include_archive=True,
            )
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
        folderpath = (
            Path.cwd()
            / "data"
            / "processed"
            / "/".join(r for r in logical_root)
            / logical_name
        )
        if not folderpath.exists():
            folderpath.mkdir(parents=True, exist_ok=True)

        # Set up filename (without any extension)
        filename = f"{logical_name}_{data_name}"

        # Main save file is in .feather format - it's amazingly fast!
        filepath = folderpath / f"{filename}.feather"
        data.to_feather(filepath)
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
        data = pd.read_csv(specified_file)
    else:
        folderpath = (
            Path.cwd()
            / "data"
            / "processed"
            / "/".join(r for r in logical_root)
            / logical_name
        )
        filename = f"{logical_name}_{data_name}"
        filepath = folderpath / f"{filename}.feather"
        data = pd.read_feather(filepath)

    return data

