import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import Point

from springtime.utils import rolling_mean, transponse_df


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
                # [Point(1.01, 1.01), Point(1.01, 1.01), Point(2.1, 2.1), Point(1.01, 1.01), Point(2.1, 2.1), Point(2.1, 2.1)]
                # Only do geometry exact match
                [
                    Point(1.0, 1.0),
                    Point(1.0, 1.0),
                    Point(2, 2),
                    Point(1.0, 1.0),
                    Point(2, 2),
                    Point(2, 2),
                ]
                # [Point(1.01, 1.01), Point(1.01, 1.01), Point(2, 2), Point(1.01, 1.01), Point(2, 2), Point(2, 2)]
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
       data=[[2000, Point(1,1), 1.0, 2.0, 1.0],
       [2000, Point(2,2), 3.0, 4.0, 3.0],
       [2001, Point(1,1), 5.0, 6.0, 4.0],
       [2001, Point(2,2), 7.0, 8.0, 6.0],
       [1999, Point(1,1), np.nan, np.nan, 0.5],
       [2002, Point(2,2), np.nan, np.nan, 7.5]],
       columns=['year', 'geometry', 'measurementx_1', 'measurementx_2', 'spring-onset']
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

    expected = pd.DataFrame(
        {
            "year": [2000, 2000, 2001, 2001],
            "geometry": gpd.GeoSeries(
                [Point(1, 1), Point(2, 2), Point(1, 1), Point(2, 2)]
            ),
            "doy1-measurementx": [
                1,
                3,
                4,
                6,
            ],
            "doy2-measurementx": [
                2,
                7,
                5,
                8,
            ],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


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
