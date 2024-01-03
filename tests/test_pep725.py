from contextlib import contextmanager
from textwrap import dedent

import pandas as pd
import geopandas as gpd
from springtime.datasets import PEP725Phenor, load_dataset

import pytest

from springtime.config import CONFIG


# Sample data for tests is shipped with the package
TEST_CACHE = "tests/reference_data/"
CONFIG.cache_dir = TEST_CACHE

REFERENCE_RECIPE = dedent(
    """\
        dataset: PEP725Phenor
        years:
        - 2000
        - 2002
        species: Syringa vulgaris
        include_cols:
        - year
        - geometry
        - day
        """
)


def update_reference_data(redownload=False):
    """Update the reference data for these tests."""
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    if redownload:
        dataset._location.unlink()
    loaded_data = dataset.load()
    loaded_data.to_file(TEST_CACHE + "pep725_load_reference.geojson")


@pytest.fixture
def area():
    return {
        "name": "Germany",
        "bbox": [
            5.98865807458,
            47.3024876979,
            15.0169958839,
            54.983104153,
        ],
    }


@contextmanager
def temporary_cache_dir(directory):
    """Temporarily change the cache dir in config."""
    global CONFIG_DIR
    old_cache_dir = CONFIG.cache_dir
    CONFIG.cache_dir = directory
    yield
    CONFIG.cache_dir = old_cache_dir



def test_instantiate_class():
    PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])


def test_instantiate_with_area(area):
    PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002], area=area)


def test_to_recipe():
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe():
    original = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload():
    original = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.download
def test_download(tmp_path):
    """Check download hasn't changed; also uses raw_load"""
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.raw_load()
    with temporary_cache_dir(tmp_path):
        dataset.download()
        new_data = dataset.raw_load()

    pd.testing.assert_frame_equal(new_data, reference)


def test_load():
    """Compare loaded (i.e. processed) data with stored reference."""
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    loaded_data = dataset.load()
    reference = gpd.read_file(TEST_CACHE + "pep725_load_reference.geojson")
    assert set(loaded_data.columns) == set(reference.columns), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values])
