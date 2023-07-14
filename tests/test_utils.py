# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from geopandas.testing import assert_geodataframe_equal
from numpy.testing import assert_array_equal
from shapely.geometry import Point

from springtime.utils import points_from_cube, resample, rolling_mean, transponse_df


def test_join_spatiotemporal_same_geometry():
    input = gpd.GeoDataFrame(
        {
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "geometry": gpd.GeoSeries(
                [
                    Point(1, 1),
                    Point(1, 1),
                    Point(2, 2),
                    Point(2, 2),
                    Point(1, 1),
                    Point(1, 1),
                    Point(2, 2),
                    Point(2, 2),
                ]
            ),
            "doy": [1, 2, 1, 2, 1, 2, 1, 2],
            "measurementx": [1, 2, 3, 4, 5, 6, 7, 8],
        }
    )
    predictor = transponse_df(input)
    target = gpd.GeoDataFrame(
        {
            "year": [1999, 2000, 2000, 2001, 2001, 2002],
            "geometry": gpd.GeoSeries(
                # [Point(1.01, 1.01), Point(1.01, 1.01), Point(2.1, 2.1),
                # Point(1.01, 1.01), Point(2.1, 2.1), Point(2.1, 2.1)] Only do
                # geometry exact match
                [
                    Point(1.0, 1.0),
                    Point(1.0, 1.0),
                    Point(2, 2),
                    Point(1.0, 1.0),
                    Point(2, 2),
                    Point(2, 2),
                ]
                # [Point(1.01, 1.01), Point(1.01, 1.01), Point(2, 2),
                # Point(1.01, 1.01), Point(2, 2), Point(2, 2)]
            ),
            "spring-onset": [
                0.5,
                1,
                3,
                4,
                6,
                7.5,
            ],
        }
    )

    result = predictor.merge(
        target,
        how="outer",
        on=["year", "geometry"],
    )

    expected = gpd.GeoDataFrame(
        data=[
            [2000, Point(1, 1), 1.0, 2.0, 1.0],
            [2000, Point(2, 2), 3.0, 4.0, 3.0],
            [2001, Point(1, 1), 5.0, 6.0, 4.0],
            [2001, Point(2, 2), 7.0, 8.0, 6.0],
            [1999, Point(1, 1), np.nan, np.nan, 0.5],
            [2002, Point(2, 2), np.nan, np.nan, 7.5],
        ],
        columns=[
            "year",
            "geometry",
            "measurementx_1",
            "measurementx_2",
            "spring-onset",
        ],
    )

    assert_geodataframe_equal(result, expected)


def test_transpose_geometry_doy_as_column():
    input = pd.DataFrame(
        {
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "geometry": gpd.GeoSeries(
                [
                    Point(1, 1),
                    Point(1, 1),
                    Point(2, 2),
                    Point(2, 2),
                    Point(1, 1),
                    Point(1, 1),
                    Point(2, 2),
                    Point(2, 2),
                ]
            ),
            "doy": [1, 2, 1, 2, 1, 2, 1, 2],
            "measurementx": [1, 2, 3, 4, 5, 6, 7, 8],
        }
    )

    result = transponse_df(input)

    expected = gpd.GeoDataFrame(
        {
            "year": [2000, 2000, 2001, 2001],
            "geometry": gpd.GeoSeries(
                [Point(1, 1), Point(2, 2), Point(1, 1), Point(2, 2)]
            ),
            "measurementx_1": [
                1,
                3,
                5,
                7,
            ],
            "measurementx_2": [
                2,
                4,
                6,
                8,
            ],
        }
    )
    print(input)
    print(result)
    print(expected)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.skip(reason="not yet implemented")  # noqa: F821
def test_rolling_average():
    input = pd.DataFrame(
        {
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "geometry": gpd.GeoSeries(
                [
                    Point(1, 1),
                    Point(1, 1),
                    Point(2, 2),
                    Point(2, 2),
                    Point(1, 1),
                    Point(1, 1),
                    Point(2, 2),
                    Point(2, 2),
                ]
            ),
            "doy": [1, 2, 1, 2, 1, 2, 1, 2],
            "measurementx": [1, 2, 3, 4, 5, 6, 7, 8],
        }
    )

    result = rolling_mean(
        input, over=["measurementx"], groupby=["year", "geometry"], window_sizes=[2]
    )

    expected = pd.DataFrame(
        {
            "year": [2000, 2000, 2001, 2001],
            "geometry": gpd.GeoSeries(
                [
                    Point(1, 1),
                    Point(2, 2),
                    Point(1, 1),
                    Point(2, 2),
                ]
            ),
            # 2 = size
            # 0 = window index, here mean where doy in {1,2}
            "measurementx_2_0": [1.5, 3.5, 5.5, 7.5],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.fixture
def sample_df():
    index = pd.date_range("20100101", "20111231", freq="h")
    data = np.random.randn(len(index))
    geometry = gpd.points_from_xy(np.ones(len(index)), np.ones(len(index)))
    df = gpd.GeoDataFrame({"values": data, "datetime": index}, geometry=geometry)
    return df


def test_resample_monthly(sample_df):
    resampled = resample(sample_df, freq="month")

    assert len(resampled) == 24
    assert_array_equal(resampled.month.unique(), np.arange(12) + 1)
    assert_array_equal(resampled.year.unique(), np.array([2010, 2011]))


def test_points_from_cube():
    # create a test dataset
    lons = np.arange(-180, 180, 20)
    lats = np.arange(-90, 90, 20)
    time = pd.date_range("2000-01-01", periods=2)
    shape = (len(time), len(lats), len(lons))
    data1 = np.arange(np.prod(shape)).reshape(shape)
    data2 = np.arange(np.prod(shape), 2 * np.prod(shape)).reshape(shape) + 0.5
    ds = xr.Dataset(
        data_vars={
            "var1": (["time", "latitude", "longitude"], data1),
            "var2": (["time", "latitude", "longitude"], data2),
        },
        coords={"longitude": lons, "latitude": lats, "time": time},
    )

    # create some test points
    points = [(0, 0), (10, 10), (-20, 30), (100, -50)]

    result = points_from_cube(ds, points)

    expected = gpd.GeoDataFrame(
        {
            "time": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-02")] * 4,
            "var1": [99, 261, 100, 262, 116, 278, 50, 212],
            "var2": [423.5, 585.5, 424.5, 586.5, 440.5, 602.5, 374.5, 536.5],
            "geometry": gpd.GeoSeries(
                [
                    Point(0, 0),
                    Point(0, 0),
                    Point(10, 10),
                    Point(10, 10),
                    Point(-20, 30),
                    Point(-20, 30),
                    Point(100, -50),
                    Point(100, -50),
                ]
            ),
        }
    )
    pd.testing.assert_frame_equal(result, expected)
