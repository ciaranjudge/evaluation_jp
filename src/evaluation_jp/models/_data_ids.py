# Standard library
from dataclasses import dataclass, asdict
from typing import ClassVar
import abc

# External packages
import pandas as pd


# //TODO Refactor PopulatinSliceGenerator, TreatmentPeriodGenerator into subclasses of EvaluationDataGenerator


class DataID(abc.ABC):
    """Abstract base class for DataIDs. 
    Defines one helper method as_flattened_dict()
    """

    # DataID attributes defined in subclasses

    def as_flattened_dict(self, sep="_"):
        """Output dataclass fields as flattened dict with separator `sep`.
        Based on https://gist.github.com/jhsu98/188df03ec6286ad3a0f30b67cc0b8428
        """
        flattened_data_id_dict = {}

        def recurse(t, parent_key=""):
            if isinstance(t, list):
                for i in range(len(t)):
                    recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(t, dict):
                for k, v in t.items():
                    recurse(v, parent_key + sep + k if parent_key else k)
            else:
                flattened_data_id_dict[parent_key] = t

        recurse(asdict(self))

        return flattened_data_id_dict


@dataclass(order=True, frozen=True)
class PopulationSliceID(DataID):
    date: pd.Timestamp


@dataclass(order=True, frozen=True)
class TreatmentPeriodID(DataID):
    population_slice: PopulationSliceID
    time_period: pd.Period


# //TODO Add base class for DataIDGenerator


@dataclass
class PopulationSliceIDGenerator:

    start: pd.Timestamp
    end: pd.Timestamp
    freq: str = "QS"

    def __call__(self):
        for date in pd.date_range(start=self.start, end=self.end, freq=self.freq):
            yield PopulationSliceID(date=date)


@dataclass
class TreatmentPeriodIDGenerator:

    end: pd.Timestamp
    freq: str = "M"

    def __call__(self, population_slice_id):
        for time_period in pd.period_range(
            start=population_slice_id.date, end=self.end, freq=self.freq
        ):
            yield TreatmentPeriodID(
                population_slice=population_slice_id, time_period=time_period
            )

