import shutil
from textwrap import dedent

import geopandas as gpd
import pandas as pd
import pytest

from springtime.config import CONFIG
from springtime.datasets import RNPN, load_dataset

"""
To update reference data, run one of the following:

    pytest tests/datasets/test_rnpn.py --update-reference
    pytest tests/datasets/test_rnpn.py --update-reference --redownload


To include download, run:

    pytest tests/datasets/test_rnpn.py --include-downloads
"""

REFERENCE_DATA = CONFIG.cache_dir / "rnpn_load_reference.geojson"
REFERENCE_RECIPE = dedent(
    """\
        dataset: RNPN
        years:
        - 2010
        - 2011
        species_ids:
          name: Syringa
          items:
          - 36
        phenophase_ids:
          name: leaves
          items:
          - 483
        use_first: true
        aggregation_operator: median
        """
)


@pytest.fixture
def dataset():
    return RNPN(
        species_ids={"name": "Syringa", "items": [36]},
        phenophase_ids={"name": "leaves", "items": [483]},
        years=[2010, 2011],
    )


def test_load(dataset):
    """Compare loaded (i.e. processed) data with stored reference."""
    loaded_data = dataset.load()
    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values])


def test_to_recipe(dataset):
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe(dataset):
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert dataset == reloaded


def test_export_reload(dataset):
    recipe = dataset.to_recipe()
    reloaded = load_dataset(recipe)
    assert dataset == reloaded


@pytest.mark.download
def test_download(dataset, temporary_cache_dir):
    """Check download hasn't changed; also uses raw_load"""

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.raw_load()

    with temporary_cache_dir():
        dataset.download()
        new_data = dataset.raw_load()

    pd.testing.assert_frame_equal(new_data, reference)


@pytest.mark.update
def test_update_reference_data(dataset, redownload):
    """Update the reference data for these tests."""
    if redownload:
        from springtime.datasets.rnpn import cache_dir

        shutil.rmtree(cache_dir)

    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)
