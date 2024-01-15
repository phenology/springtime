import shutil
from textwrap import dedent
import pandas as pd

import pytest
from springtime.datasets import load_dataset
from springtime.config import CONFIG
from springtime.datasets import Phenocam
import geopandas as gpd

"""
To update reference data, run one of the following:

    pytest tests/datasets/test_Phenocam.py --update-reference
    pytest tests/datasets/test_Phenocam.py --update-reference --redownload


To include download, run:

    pytest tests/datasets/test_Phenocam.py --include-downloads
"""

REFERENCE_DATA = CONFIG.cache_dir / "phenocam_load_reference.geojson"
REFERENCE_RECIPE = dedent(
    """\
        dataset: phenocam
        years:
        - 2010
        - 2015
        frequency: '3'
        variables: []
        site: harvard$
        """
)

@pytest.fixture
def harvard_bbox():
    return {"name": "harvard", "bbox": [-73, 42, -72, 43]}

@pytest.fixture
def reference_args():
    return dict(site="harvard$", years=(2010, 2015))


def test_load_exact_match(reference_args):
    dataset = Phenocam(**reference_args)
    loaded_data = dataset.load()

    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values], check_dtype=False)


def test_instantiate_approximate_match(reference_args):
    reference_args.update(site="harvard")
    Phenocam(**reference_args)


@pytest.mark.download
def test_load_approx(reference_args):
    reference_args.update(site="harvard")
    dataset = Phenocam(**reference_args)
    dataset.load()

@pytest.mark.download
def test_load_area(reference_args, harvard_bbox):
    reference_args.update(site=None, area=harvard_bbox)
    dataset = Phenocam(**reference_args)
    dataset.load()


def test_instantiate_area(reference_args, harvard_bbox):
    reference_args.update(site=None, area=harvard_bbox)
    Phenocam(**reference_args)


def test_to_recipe(reference_args):
    dataset = Phenocam(**reference_args)
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe(reference_args):
    original = Phenocam(**reference_args)
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload(reference_args):
    """Assure to and from yaml are consistent"""
    original = Phenocam(**reference_args)
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.download
def test_download(temporary_cache_dir, reference_args):
    """Check download hasn't changed; also uses raw_load"""
    dataset = Phenocam(**reference_args)

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.load()

    with temporary_cache_dir():
        new_data = dataset.load()

    pd.testing.assert_frame_equal(new_data, reference)


@pytest.mark.update
def test_update_reference_data(reference_args, redownload):
    """Update the reference data for these tests."""
    dataset = Phenocam(**reference_args)

    if redownload:
        shutil.rmtree(dataset._root_dir)

    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)
