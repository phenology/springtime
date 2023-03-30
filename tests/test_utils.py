import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from springtime.utils import rolling_mean, transponse_df


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
            "doy": [1, 2, 1, 2 ,1, 2, 1, 2],
            "measurementx": [1, 2, 3, 4, 5, 6, 7,8],
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
            "doy": [1, 2, 1, 2 ,1, 2, 1, 2],
            "measurementx": [1, 2, 3, 4, 5, 6, 7,8],
        }
    )

    result = rolling_mean(input, over=['measurementx'], groupby=['year', 'geometry'], window_sizes=[2])
                             
    expected = pd.DataFrame(
        {
            "year": [2000, 2000,  2001, 2001],
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