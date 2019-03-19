# %%
# Standard library
from dataclasses import dataclass, field
from typing import ClassVar, List, Set, Dict, Tuple, Optional
from pathlib import Path

# External packages
import pandas as pd

# Local packages
from src.data.persistence_helpers import save_dataframe, load_dataframe


# %%
@dataclass
class EvaluationClass:
    """
    Abstract-ish class to handle some housekeeping stuff for Model, Slice, Period etc
    
    Parameters
    ----------
    start_date: pd.Timestamp
        The start of the period that this object refers to

    end_date: pd.Timestamp
        The end of the period that this object refers to

    logical_root: Tuple[str]
        Ancestors of this object starting from root (".")
    
    seed_dataframe: pd.DataFrame = None
        Dataframe needed to enable object to set itself up correctly

    name_prefix: str = None
        Prefix to be added to logical name of object

    rebuild_all: bool = False
        If True, this will call create_dataframe on this object and all its children
        If False, this object and its children will first try to load existing data

    Attributes
    ----------
    logical_name: str = None
        Logical name of this EvaluationClass instance 
        (NB not guaranteed to be same as object name!) 
        If not specified, created as part of __post_init__ setup

    dataframe_names: Tuple[str]
        Tuple of the names of the dataframes needed by this object.
        Used to set up the dataframes dict.

    dataframes: dict
        Dictionary of the dataframes used by this object.
        Names must be == dataframe_names.

    Methods
    -------
    make_logical_name()
        Abstract - to be implemented in concrete child classes

    setup_dataframe()
        Abstract - to be implemented in concrete child classes

    save_dataframe()

    load_dataframe()

    """

    ### Class variables
    object_type: ClassVar[str] = "abstractclass"  # Specify in child classes!
    dataframe_names: ClassVar[
        Tuple[str]
    ] = ()  # Should be specified directly in child classes!

    ### Parameters to be set when object is created (handled by @dataclass)
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    logical_root: Tuple[str] = (".", )
    seed_population: pd.DataFrame = None
    name_prefix: str = ""
    rebuild_all: bool = False
    eligibility_flags: Tuple[str] = ()

    ### Other attributes
    logical_name: str = field(init=False)
    dataframes: dict = field(default=None, init=False)


    ### Methods
    def __post_init__(self):
        self.start_date = self.start_date.normalize()
        self.end_date = self.end_date.normalize()
        self.make_logical_name()
        if len(self.dataframe_names) > 0:
            self.dataframes = {}

        for dataframe_name in self.dataframe_names:
            print(dataframe_name)
            if self.rebuild_all is True:  # Must create everything from scratch!
                self.setup_dataframe(dataframe_name)
                self.save_dataframe(dataframe_name)
            else:
                try:  # first try to load existing dataframe
                    self.load_dataframe(dataframe_name)
                except:  # ...but if that didn't work...
                    try:  # ...try to create and save one...
                        self.setup_dataframe(dataframe_name)
                        self.save_dataframe(dataframe_name)
                    except:  # ...and if that didn't work, something is wrong!
                        raise Exception(
                            f"Couldn't create {self.logical_name}: {dataframe_name}"
                        )

    def make_logical_name(self):
        """
        Please move along, nothing to see here.

        To be implemented in concrete child classes.
        """
        f_name_prefix = f"{self.name_prefix}_" if len(self.name_prefix) > 0 else ""
        f_start_date = self.start_date.strftime("%Y-%m-%d")
        self.logical_name = f"{f_name_prefix}{self.object_type}_{f_start_date}"
        pass

    def setup_dataframe(self, dataframe_name: str):
        """
        Please move along, nothing to see here.

        To be implemented in concrete child classes.
        """
        pass

    # Common to several child classes
    def setup_eligible(self):
        self.dataframes["eligible"] = (
            self.dataframes["population"]
            .copy()
            .loc[self.dataframes["population"]["eligible"] == True]
            .drop(list(self.eligibility_flags) + ["eligible"], axis="columns")
            .reset_index(drop=True)
        )

    def save_dataframe(
        self,
        dataframe_name: str,
        include_archive: bool = True,
        specified_file: Optional[Path] = None,
    ) -> None:
        """
        Save this object's dataframes to persistent storage.

        Implements src.data.persistence_helpers save_dataframe() function.

        Parameters
        ---------- 
        dataframe_name: str
            The name of the dataframe to be saved.

        include_archive: bool = True
            If True, create a timestamped gzipped archive file as well as main file

        specified_file: Optional[Path] = None
            Save as specified_file instead of standard. Assumes full path.

        """
        save_dataframe(
            dataframe=self.dataframes[dataframe_name],
            dataframe_name=dataframe_name,
            logical_root=self.logical_root,
            logical_name=self.logical_name,
            include_archive=True,
            specified_file=None,
        )

    def load_dataframe(
        self, dataframe_name: str, specified_file: Optional[Path] = None
    ):
        """
        Load this object's dataframe from persistent storage.

        Implements src.data.persistence_helpers load_dataframe() function.

        Parameters
        ---------- 
        dataframe_name: str
            The name of the dataframe to be saved

        specified_file: Optional[Path] = None
            Save as specified_file instead of standard. Assumes full path.

        """
        self.dataframes[dataframe_name] = load_dataframe(
            dataframe_name=dataframe_name,
            logical_root=self.logical_root,
            logical_name=self.logical_name,
            specified_file=None,
        )


#%%
