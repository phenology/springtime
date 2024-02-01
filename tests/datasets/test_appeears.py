from textwrap import dedent

import geopandas as gpd
import pandas as pd
import pytest

from springtime.config import CONFIG
from springtime.datasets import Appeears, load_dataset

"""
To update reference data, run one of the following:

    pytest tests/datasets/test_appeears.py --update-reference
    pytest tests/datasets/test_appeears.py --update-reference --redownload


To include download, run:

    pytest tests/datasets/test_appeears.py --include-downloads
"""

REFERENCE_DATA = CONFIG.cache_dir / "appeears_load_reference.geojson"
REFERENCE_RECIPE = dedent(
    """\
        dataset: appears
        years:
        - 2009
        - 2011
        product: MCD12Q2
        version: '061'
        layers:
        - Greenup
        - Dormancy
        area:
          name: eastfrankfurt
          bbox:
          - 9.0
          - 49.0
          - 10.0
          - 50.0
        points:
        - - 9.1
          - 49.1
        - - 9.6
          - 49.6
        - - 9.9
          - 49.9
        infer_date_offset: true
        """
)


@pytest.fixture
def reference_args():
    return dict(
        years=[2009, 2011],
        product="MCD12Q2",
        version="061",
        layers=("Greenup", "Dormancy"),
        points=[(9.1, 49.1), (9.6, 49.6), (9.9, 49.9)],
        area={"name": "eastfrankfurt", "bbox": [9.0, 49.0, 10.0, 50.0]},
    )


def test_load_points(reference_args):
    reference_args.update(area=None)
    dataset = Appeears(**reference_args)
    dataset.load()


def test_load_area(reference_args):
    reference_args.update(points=None)
    dataset = Appeears(**reference_args)
    dataset.load()


def test_load_points_and_area(reference_args):
    dataset = Appeears(**reference_args)
    loaded_data = dataset.load()

    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(
        loaded_data, reference[loaded_data.columns.values], check_dtype=False
    )


def test_to_recipe(reference_args):
    dataset = Appeears(**reference_args)
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe(reference_args):
    original = Appeears(**reference_args)
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload(reference_args):
    """Assure to and from yaml are consistent"""
    original = Appeears(**reference_args)
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.update
def test_update_reference_data(reference_args, redownload):
    """Update the reference data for these tests."""
    dataset = Appeears(**reference_args)

    if redownload:
        fn = dataset._area_dir / dataset._area_path
        fn.unlink()

    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)


@pytest.mark.update
def test_update_point_data(reference_args, redownload):
    """Update the point reference data."""
    if redownload:
        reference_args.update(area=None)
        dataset = Appeears(**reference_args)
        fn = dataset._root_dir / dataset._point_path(dataset.points)
        fn.unlink()
        dataset.download()


@pytest.mark.download
def test_download(temporary_cache_dir, reference_args):
    """Check download hasn't changed; also uses raw_load"""
    dataset = Appeears(**reference_args)

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.load()

    with temporary_cache_dir():
        new_data = dataset.load()

    pd.testing.assert_frame_equal(new_data, reference)
