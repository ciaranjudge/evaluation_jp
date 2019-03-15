# %%
# Standard library
from typing import List, Set, Dict, Tuple, Optional
from pathlib import Path

# External packages
import pandas as pd


# persist dataframe
def save_dataframe(
    dataframe: pd.DataFrame,
    dataframe_name: str,
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
    dataframe: pd.DataFrame
        The actual dataframe to be saved

    dataframe_name: str
        The reference name of this dataframe.
        Used as part of filename and as lookup key in dataframes dictionary

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
        dataframe.to_feather(specified_file)
    else:
        # Set up folder path: everything to do with this object gets saved here
        folderpath = Path.cwd() / "/".join(r for r in logical_root) / logical_name
        if not folderpath.exists():
            folderpath.mkdir(parents=True, exist_ok=True)

        # Set up filename (without any extension)
        filename = f"{logical_name}_{dataframe_name}"

        # Main save file is in .feather format - it's amazingly fast!!
        filepath = folderpath / f"{filename}.feather"
        dataframe.to_feather(filepath)

        # Save timestamped archive file as (gz-compressed) csv as well.
        if include_archive:
            now_stamp = pd.Timestamp.now().strftime("%Y%m%d%H%M")
            archive_filepath = folderpath / f"{filename}_v{now_stamp}.gz"
            dataframe.to_csv(archive_filepath)


# load dataframe from storage
def load_dataframe(
    dataframe_name: str,
    logical_root: Tuple[str],
    logical_name: str,
    specified_file: Optional[Path] = None,
) -> pd.DataFrame:

    """
    Load dataframe from persistent storage in standard or specified location

    Standard storage format is feather due to fast read/write speed.

    Parameters
    ----------
    dataframe_name: str
        The reference name of this dataframe.
        Used as part of filename and as lookup key in dataframes dictionary

    logical_name: str
        Name of this object. Used to create foldername and as part of filename

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")

    specified_file: Optional[Path] = None
        Save as specified_file instead of standard. Assumes full path.

    Returns
    -------
    dataframe: pd.DataFrame
        The dataframe that's just been loaded
    """
    if specified_file is not None:
        # Arg! Need to check what type of file this is...
        # ...for now, make glorious assumption that it's some kind of csv from archive
        dataframe = pd.read_csv(specified_file)
    else:
        folderpath = Path.cwd() / "/".join(r for r in logical_root) / logical_name
        filename = f"{logical_name}_{dataframe_name}"
        filepath = folderpath / f"{filename}.feather"
        dataframe = pd.read_feather(filepath)

    return dataframe


#%%
