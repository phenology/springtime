import shutil
from textwrap import dedent

import geopandas as gpd
import pandas as pd
import pytest

from springtime.config import CONFIG
from springtime.datasets import Daymet, load_dataset

"""
To update reference data, run one of the following:

    pytest tests/datasets/test_daymet.py --update-reference
    pytest tests/datasets/test_daymet.py --update-reference --redownload


To include download, run:

    pytest tests/datasets/test_daymet.py --include-downloads
"""

REFERENCE_DATA = CONFIG.cache_dir / "daymet_load_reference.geojson"
REFERENCE_RECIPE = dedent(
    """\
      dataset: daymet
      years:
      - 2000
      - 2002
      points:
      - - -84.2625
        - 36.0133
      - - -86.0
        - 39.6
      - - -85.0
        - 40.0
      area:
        name: indianapolis
        bbox:
        - -86.5
        - 39.5
        - -86.0
        - 40.1
      variables:
      - tmin
      - tmax
      mosaic: na
      frequency: monthly
        """
)


@pytest.fixture
def indianapolis():
    return {"name": "indianapolis", "bbox": [-86.5, 39.5, -86, 40.1]}


@pytest.fixture
def points():
    return [
        [-84.2625, 36.0133],
        [-86, 39.6],
        [-85, 40],
    ]


@pytest.fixture
def reference_args(points, indianapolis):
    return dict(
        variables=["tmin", "tmax"],
        points=points,
        area=indianapolis,
        years=[2000, 2002],
        frequency="monthly",
    )


def test_load_points(reference_args):
    reference_args.update(area=None, frequency="daily")
    dataset = Daymet(**reference_args)
    dataset.load()


def test_load_area(reference_args):
    reference_args.update(points=None)
    dataset = Daymet(**reference_args)
    dataset.load()


def test_load_points_and_area(reference_args):
    dataset = Daymet(**reference_args)
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
    dataset = Daymet(**reference_args)
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe(reference_args):
    original = Daymet(**reference_args)
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload(reference_args):
    """Assure to and from yaml are consistent"""
    original = Daymet(**reference_args)
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.update
def test_update_reference_data(reference_args, redownload):
    """Update the reference data for these tests."""
    dataset = Daymet(**reference_args)

    if redownload:
        shutil.rmtree(dataset._box_dir)

    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)


@pytest.mark.download
def test_download_points(temporary_cache_dir, reference_args):
    """Check that the downloaded CSV file is properly parsed"""
    reference_args.update(area=None, frequency="daily")
    dataset = Daymet(**reference_args)

    with temporary_cache_dir():
        data = dataset.raw_load()

    # check number of entries
    num_points = len(reference_args["points"])
    year_range = reference_args["years"]
    num_years = year_range[1] - year_range[0] + 1
    assert len(data) == num_points*num_years*365

    # check column headers
    assert "year" in data.columns
    assert "yday" in data.columns
    for variable in reference_args["variables"]:
        assert data.columns.str.contains(variable).any()


@pytest.mark.download
def test_download(temporary_cache_dir, reference_args):
    """Check download hasn't changed; also uses raw_load"""
    dataset = Daymet(**reference_args)

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.load()

    with temporary_cache_dir():
        new_data = dataset.load()

    pd.testing.assert_frame_equal(new_data, reference)
