import shutil
from textwrap import dedent

import pandas as pd
import geopandas as gpd
from springtime.datasets import load_dataset
import xarray as xr
import pytest

from springtime.config import CONFIG
from springtime.datasets.meteo.eobs import EOBS, extract_points

REFERENCE_DATA = CONFIG.cache_dir / "eobs_load_reference.geojson"
REFERENCE_RECIPE = dedent(
    """\
        dataset: E-OBS
        years:
        - 2000
        - 2002
        product_type: ensemble_mean
        variables:
        - mean_temperature
        - minimum_temperature
        grid_resolution: 0.25deg
        version: 26.0e
        points:
        - - 5.0
          - 10.0
        - - 10.0
          - 12.0
        keep_grid_location: false
        area:
          name: Germany
          bbox:
          - 5.98865807458
          - 47.3024876979
          - 15.0169958839
          - 54.983104153
        minimize_cache: true
        """
)

@pytest.fixture
def reference_args(germany):
    return dict(
        grid_resolution = "0.25deg",
        years=["2000", "2002"],
        points = [(5, 10), (10, 12)],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
        area = germany,
        minimize_cache = True
    )


def update_reference_data(redownload=False):
    """Update the reference data for these tests."""
    dataset = EOBS(**reference_args)
    if redownload:
        shutil.rmtree(dataset._root_dir)
    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)


def test_load_full_grid(reference_args):
    args = {**reference_args, "points": None}
    ds = EOBS(**args).load()
    assert isinstance(ds, xr.Dataset)


def test_load_single_point(reference_args):
    args = {**reference_args, "points": (10, 10)}
    df = EOBS(**args).load()
    assert isinstance(df, pd.DataFrame)


def test_load_multiple_points(reference_args):
    args = {**reference_args, "points": [(10, 10), (20, 20)]}
    df = EOBS(**args).load()
    assert isinstance(df, pd.DataFrame)


def test_to_recipe(reference_args):
    dataset = EOBS(**reference_args)
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe(reference_args):
    original = EOBS(**reference_args)
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload(reference_args):
    original = EOBS(**reference_args)
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.download
def test_download(temporary_cache_dir, reference_args):
    """Check download hasn't changed; also uses raw_load"""
    dataset = EOBS(**reference_args)

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.raw_load()

    with temporary_cache_dir():
        new_data = dataset.raw_load()

    pd.testing.assert_frame_equal(new_data, reference)


def test_load(reference_args):
    """Compare loaded (i.e. processed) data with stored reference."""
    dataset = EOBS(**reference_args)
    loaded_data = dataset.load()
    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values], check_dtype=False)


def test_extract_points(reference_args):
    ds = EOBS(**reference_args).raw_load()
    points = gpd.GeoSeries(gpd.points_from_xy(x=[0, 5, 7], y=[5, 10, 12]), name="geometry")

    # Extract multipe points
    extract_points(ds, points)

    # Extract single point
    extract_points(ds, points[0:1])
