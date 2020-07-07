import collections


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

