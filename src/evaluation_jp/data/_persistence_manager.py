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
class PersistenceManager:
    """Set up with where, how, and when to store stuff.
    Use as decorator for setup steps (how to pass in?!)
    For now, assume backend is a database with sqlalchemy connection.
    
    """
    persistence_path: str = None ## path/URL for databae engine (not including actual db)
    rebuild_all: bool = False
    name: str  ## Database name (could also be a folder)

    # TODO def load(self, data, )
    # Each dataset written to table with (1) timestamp and (2) 'current' bool

    # Start engine
    # Does database `name` exist? If not, create it!
    # Given a valid table name (`population_data`, `population_slices`, `treatment_periods`)
    # ...does the table exist? If not, create it!
    # Given the table exists, can the ID of this item be found?
    

    pass

