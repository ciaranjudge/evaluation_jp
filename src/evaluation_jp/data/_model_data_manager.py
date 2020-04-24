# %%
# Standard library
from typing import List, Set, Dict, Tuple, Optional, Callable, Union
from pathlib import Path
from functools import wraps
from dataclasses import dataclass

# External packages
import pandas as pd
import sqlalchemy as sa 
import sqlalchemy_utils

@dataclass
class ModelDataManager:
    """Manages storage and retrieval of model data.
    For now, assume backend is a database with sqlalchemy connection.
    
    """
    data_path: str
    rebuild_all: bool = False
    # name: str  ## Database name (could also be a folder)

    def load(self, table, id):
        """Load dataframe from records in `table` matching `id`
        """
        # 
        pass

    def save(self, table, id, data):
        # Connect to engine
        
        # Database exists? If not, create it!
        
        # Doesn't exist already

        # Exists already
        pass

    # Implement an alternate constructor to copy existing
    @classmethod
    def copy_existing(cls, old_data_path, new_data_path, rebuild_all):
        # Make copy of old database at new_data_path
        pass

    def __call__(self, type, id, ):
        """Given a valid table name (`population_data`, `population_slices`, `treatment_periods`)
    ...does the table exist? If not, create it!
    Given the table exists, can the ID of this item be found?
        """
        pass


    



# %%
