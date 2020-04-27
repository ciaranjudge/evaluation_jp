from pathlib import Path

import sqlalchemy as sa

from evaluation_jp.data import ModelDataHandler

def test__ModelDataHandler__init(tmpdir):
    """Simple test to make sure everything gets initiated correctly
    """
    data_path = f"sqlite:///{tmpdir}"
    results = ModelDataHandler(data_path)
    assert results.engine.name == sa.create_engine(data_path).name


# def test__ModelDataHandler__save__population_slice(fixture__population_slice):
#     """Given a population_slice instance that's not saved, save it correctly
#     """
#     f = fixture__population_slice
#     assert True


# def test__ModelDataHandler__load__population_slice(fixture__population_slice):
#     """Given a population_slice instance that's saved already, load it correctly
#     """
#     pass


# def test__ModelDataHandler__call__population_slice__new(fixture__population_slice):
#     """Given a constructor for a new population_slice, run constructor and save it
#     -- Correctly determine that population_slice doesn't exist.
#     -- Delegate to constructor...
#     -- ... and save created dataframe in right place.
#     """
#     pass


# def test__ModelDataHandler__call__population_slice__existing_rebuild(
#     fixture__population_slice,
# ):
#     """Given a constructor for an *existing* population_slice, run constructor and save it
#     -- Correctly determine that population_slice exists
#     -- Delegate to constructor...
#     -- ... and save created dataframe in right place.
#     """
#     pass


# def test__ModelDataHandler__call__population_slice__existing_norebuild(
#     fixture__population_slice,
# ):
#     """
#     """
#     pass
