#%% [markdown]
# ## Generate *all* possible PPS numbers, and assign random number to each.
# PPS numbers have the form nnnnnnnCa...
#
# ...where 'nnnnnnn' means 7 digits
#
# ...'a' means a letter (for new PPS numbers) or blank (for older numbers)
#
# ...and 'C' is a check letter whose value is based on weighted sum of annnnnnn.
# 
# Strategy is:
# 
# 1. Generate the required number of pseudorandom IDs
# 2. Produce 10**7 digit components of PPSNs: string, partial checksum
# 3. Generate PPSNs ending with each final char - blank (older) or A-Z (newer)
# 4. Append each batch of PPSNs to the ppsn_map output dataframe
# 5. Save to parquet!

#%% [markdown]
# ## 0. Import packages, define helper functions, set up some parameters
# 
# Import packages...
#
# ...define helper functions ```get_digit_range``` and ```get_digit```...
# 
# ...and initiate ```digits```, ```CHECK_ALPHABET```, ```A2_ALPHABET```, ```ppsn_range```
# 

#%%
# Standard libraries
import string
import itertools
from pathlib import Path

# Third party packages
import pandas as pd
import numpy as np

# Constants
DIGITS_COUNT = 7
CHECK_ALPHABET = "WABCDEFGHIJKLMNOPQRSTUV"
A2_ALPHABET = [""] + list(string.ascii_uppercase)

#%%
def get_digit_range(
    series: pd.Series, start_exp: int = None, stop_exp: int = None
) -> pd.Series:
    """Given a numeric pd.Series and optional start_exp and stop_exp,
    return integer value of the digits from start_exp to stop_exp
    """
    if stop_exp is not None:
        series = series % 10 ** (stop_exp + 1)
    if start_exp is not None:
        series = series // 10 ** start_exp
    return series


def get_digit(series: pd.Series, exp: int) -> pd.Series:
    """Given a numeric pd.Series and exp value,
    return integer value of the digits at that exp (10**exp)
    """
    return get_digit_range(series, start_exp=exp, stop_exp=exp)

#%%
def generate_pseudo_ids(chunk_size, num_chunks, random_scaler=4):
    """Return pd.Series with chunk_size * num_chunks rows,
    and 1 column pseudo_id, containing non-duplicated pseudo-random IDs.
    """
    # Using random seed for reproducibility - it's the year PPS numbers were introduced ;)
    np.random.seed(1998)
    # Set up random range that's bigger than the needed PPS count   
    required_count = chunk_size * num_chunks
    random_count = required_count * random_scaler

    return pd.Series(
        np.random.choice(random_count, required_count, replace=False), 
        name="pseudo_id"
        )


#%%
def create_partial_ppsns() -> pd.DataFrame:
    """Given DIGITS_COUNT, return dataframe with two columns:
    'digits_string': zero-padded string representation of each number
    'digit_sum': weighted sum of digits for including in checksum

    Produce 10**7 digit components of PPSNs: string, partial checksum

    1. Generate weighted sum of digits for modulo 23 check digit calculation.
    2. Add weighted multiple of each numeric digit:

    -- Rightmost digit (10**0) => weighting is 1
    -- Weighting is 2 for 2nd-rightmost (10**1), 3 for 3rd-rightmost (10**2)
    -- etc.

    3. Convert 7-digit numbers to 0-filled strings, and drop number.
    """
    digits_range = range(10**DIGITS_COUNT)
    # First, get the 7 digits...
    partial_ppsn = pd.DataFrame(digits_range, columns=["numeric_partial_ppsn"])
    # Initiate 0-val series
    partial_ppsn["digit_sum"] = 0
    # Add weighted multiple of each digit
    for i in range(7):
        partial_ppsn["digit_sum"] += get_digit(partial_ppsn["numeric_partial_ppsn"], i) * (i + 2)
    # Produce string representation, with leading zeroes where needed
    partial_ppsn["digits_string"] = partial_ppsn["numeric_partial_ppsn"].astype(str).str.zfill(7)
    # And clean up unneeded column.
    partial_ppsn.drop(columns=["numeric_partial_ppsn"], inplace=True)
    return partial_ppsn

#%%
def create_ppsn_chunk(partial_ppsns, char_val, char):
    check_map = {number: letter for number, letter in enumerate(CHECK_ALPHABET)}
    # Get checksum component for char_val
    char_val_sum = char_val * (DIGITS_COUNT + 2)
    checksum = partial_ppsns["digit_sum"] + char_val_sum
    numeric_check_char = checksum % 23
    check_char = numeric_check_char.map(check_map)
    ppsn = partial_ppsns["digits_string"] + check_char + char
    ppsn.name = "ppsn"
    return ppsn

#%%
def generate_ppsn_map_chunks():
    """Yield a pd.DataFrame with 10**DIGITS_COUNT rows, abd two columns:
    ppsn: every PPS number ending with current A2_ALPHABET char
    pseudo_id: a pseudorandom number for each ppsn
    """
    chunk_size = 10 ** DIGITS_COUNT
    num_chunks = len(A2_ALPHABET)

    partial_ppsns = create_partial_ppsns()
    pseudo_ids = generate_pseudo_ids(chunk_size, num_chunks, random_scaler=4)

    # Now set up loop to yield each ppsn_chunk
    for char_val, char in enumerate(A2_ALPHABET):
        ppsn_chunk = create_ppsn_chunk(partial_ppsns, char_val, char)

        start = char_val * chunk_size
        stop = start + chunk_size
        chunk_index = pd.RangeIndex(start=start, stop=stop)

        ppsn_chunk.index = chunk_index
        pseudo_id_chunk = pseudo_ids[chunk_index]

        ppsn_map_chunk = pd.concat(
            [ppsn_chunk, pseudo_id_chunk], 
            axis="columns"
            )
        yield ppsn_map_chunk



#%%
# Replace with relative path!
# Add check for folder existence!
save_folder = Path("E:\\repos\\jobpath_evaluation\\data\\ppsn_map")

ppsn_map_chunks = generate_ppsn_map_chunks()

for char in A2_ALPHABET:
    # Save to feather
    save_path = save_folder / f"nnnnnnnx{char}"
    print(char, save_path)
    chunk = next(ppsn_map_chunks)
    # Have to save with default index due to feather limitation
    chunk.index = pd.RangeIndex(10**DIGITS_COUNT)
    chunk.to_feather(save_path)

#%%
"""
In:
pd.Series of ppsns

Out:
pseudo_id series, unmatched series

How:
1. Clean the PPSNS
- Strip whitespace
- Capitalise letters
2. Split by length


"""
load_path = save_folder / "nnnnnnnx"
%time p0 = pd.read_feather(load_path)

#%%
p0.head()

#%%
me = p0["ppsn"] = "6754687B"
p0.loc[me]

#%%
p0.iloc[6433435, :]

#%%
p0.head()

#%%
# feather vs parquet vs sqlite
# for reading and writing!

