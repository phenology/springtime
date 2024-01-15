from textwrap import dedent

import pandas as pd
import geopandas as gpd
from springtime.datasets import PEP725Phenor, load_dataset

import pytest


from springtime.config import CONFIG


"""
To update reference data, run one of the following:

    pytest tests/datasets/test_pep725.py --update-reference
    pytest tests/datasets/test_pep725.py --update-reference --redownload


To include download, run:

    pytest tests/datasets/test_pep725.py --include-downloads
"""

REFERENCE_DATA = CONFIG.cache_dir / "pep725_load_reference.geojson"
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


def test_instantiate_class():
    PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])


def test_instantiate_with_area(germany):
    PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002], area=germany)


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
def test_download(temporary_cache_dir):
    """Check download hasn't changed; also uses raw_load"""
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.raw_load()

    with temporary_cache_dir():
        dataset.download()
        new_data = dataset.raw_load()

    pd.testing.assert_frame_equal(new_data, reference)


def test_load():
    """Compare loaded (i.e. processed) data with stored reference."""
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    loaded_data = dataset.load()
    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values])


@pytest.mark.update
def test_update_reference_data(redownload):
    """Update the reference data for these tests."""
    dataset = PEP725Phenor(species="Syringa vulgaris", years=[2000, 2002])
    if redownload:
        dataset._location.unlink()
    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)
