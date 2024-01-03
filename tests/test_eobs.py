import shutil
from textwrap import dedent

import pandas as pd
import geopandas as gpd
from springtime.datasets import load_dataset

import pytest

from springtime.config import CONFIG
from springtime.datasets.meteo.eobs import EOBS


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
        """
)


def update_reference_data(redownload=False):
    """Update the reference data for these tests."""
    dataset = EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
        area = germany  # TODO make this available to non-test function
        minimize_cache = True  # TODO implement
    )
    if redownload:
        shutil.rmtree(dataset._root_dir)
    loaded_data = dataset.load()
    loaded_data.to_file(REFERENCE_DATA)


def test_instantiate_class():
    EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
    )


def test_instantiate_single_point():
    EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
        point=(10, 10),
    )


def test_instantiate_multiple_points():
    EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
        points=[(10, 10), (20, 20)],
    )


def test_instantiate_area(germany):
    EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
        area=germany,
    )


def test_instantiate_area_and_points():
    EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
        points=[(10, 10), (20, 20)],
        area=germany,
    )


def test_to_recipe():
    dataset = EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
    )
    recipe = dataset.to_recipe()
    assert recipe == REFERENCE_RECIPE


def test_from_recipe():
    original = EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
    )
    reloaded = load_dataset(REFERENCE_RECIPE)
    assert original == reloaded


def test_export_reload():
    original = EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
    )
    recipe = original.to_recipe()
    reloaded = load_dataset(recipe)
    assert original == reloaded


@pytest.mark.download
def test_download(temporary_cache_dir):
    """Check download hasn't changed; also uses raw_load"""
    dataset = EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
    )

    # The reference data is shipped with the test suite, loaded from TEST_CACHE
    reference = dataset.raw_load()

    with temporary_cache_dir():
        dataset._maybe_download()
        new_data = dataset.raw_load()

    pd.testing.assert_frame_equal(new_data, reference)


def test_load():
    """Compare loaded (i.e. processed) data with stored reference."""
    dataset = EOBS(
      grid_resolution = "0.25deg",
        years=["2000", "2002"],
        variables=[
            "mean_temperature",
            "minimum_temperature",
        ],
    )
    loaded_data = dataset.load()
    reference = gpd.read_file(REFERENCE_DATA)
    assert set(loaded_data.columns) == set(
        reference.columns
    ), f"""
        Columns differ. New columns are {loaded_data.columns},
        vs reference {reference.columns}."""

    pd.testing.assert_frame_equal(loaded_data, reference[loaded_data.columns.values])



# load multiple points
# import geopandas as gpd
# from springtime.datasets.meteo.eobs import extract_points

# points = gpd.GeoSeries(gpd.points_from_xy(x=[0, 5, 7], y=[5, 10, 12]), name="geometry")
# extract_points(eobs_ds, points)

# load single point
# extract_points(eobs_ds, points[0:1])
