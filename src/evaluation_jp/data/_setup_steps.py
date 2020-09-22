# %%
import abc
from dataclasses import dataclass
from typing import List

import pandas as pd

@dataclass
class SetupStep(abc.ABC):
    # Parameters

    # Setup method
    @abc.abstractmethod
    def run(self, data_id=None, data=None):
        """Do something and return data"""
        pass


# //TODO Check if should inherit from SetupStep
@dataclass
class SetupSteps:
    """Ordered sequence of setup steps, each represented by a dataclass
    Each dataclass should have a run(data) method
    """

    steps: List[SetupStep]

    def run(self, data_id=None, data: pd.DataFrame = None):
        for step in self.steps:
            data = step.run(data_id, data=data)

        return data