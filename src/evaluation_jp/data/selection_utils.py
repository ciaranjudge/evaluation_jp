import collections


import pandas as pd


class NearestKeyDict(collections.UserDict):
    """Dictionary for looking up nearest key to a given key.
    Useful for 'nearest date' lookups but can work for any keys implementing <= >=

    Based on https://stackoverflow.com/a/3387975/13088500
    """

    def __init__(self, data={}, how="find_last_before"):
        self.data = data
        self.how = how

    def __getitem__(self, key):
        return self.data[self._keytransform(key)]

    def __setitem__(self, key, value):
        self.data[self._keytransform(key)] = value

    def __delitem__(self, key):
        del self.data[self._keytransform(key)]

    def _keytransform(self, key):
        if self.how == "find_last_before":
            if len(candidate_keys := [k for k in sorted(self.data) if k <= key]):
                return max(candidate_keys)
            else:
                raise KeyError(f"No data key found before {key}")
        elif self.how == "find_first_after":
            if len(candidate_keys := [k for k in sorted(self.data) if k >= key]):
                return min(candidate_keys)
            else:
                raise KeyError(f"No data key found after {key}")


def dates_between_durations(
    dates: pd.Series,
    ref_date: pd.Timestamp,
    min_duration: pd.DateOffset = None,
    max_duration: pd.DateOffset = None,
) -> pd.Series:
    """Return boolean series for records with dates between specified limits.
    True if `min_duration` < (`ref_date` - `date`) < `max_duration`, False otherwise.
    Assume all dates are in the past.
    """
    if min_duration:
        latest_possible_date = ref_date - min_duration
        # If duration is longer, date must be earlier!
        ge_min_duration = dates <= latest_possible_date
    else:
        ge_min_duration = pd.Series(data=True, index=dates.index)

    if max_duration:
        earliest_possible_date = ref_date - max_duration
        # If duration is longer, date must be earlier!
        lt_max_duration = earliest_possible_date < dates
    else:
        lt_max_duration = pd.Series(data=True, index=dates.index)

    return ge_min_duration & lt_max_duration
