# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

import geopandas as gpd
from shapely import Point
from pandas.testing import assert_frame_equal

from springtime.datasets.insitu.rnpn import RNPN, _reformat


def test_reformat_given_phenophase_ids_and_side_first_and_single_row():
    dataset = RNPN(
        phenophase_ids={"name": "Leaves", "items": [483]}, years=(2010, 2010)
    )

    df = gpd.GeoDataFrame(
        {
            "geometry": [Point(4, 5)],
            "phenophase_id": [483],
            "first_yes_year": [2010],
            "first_yes_month": [1],
            "first_yes_day": [10],
            "first_yes_doy": [10],
        }
    )

    result = _reformat(dataset, df)

    expected = gpd.GeoDataFrame(
        {
            "geometry": [Point(4, 5)],
            "datetime": [datetime(2010, 1, 10)],
            "Leaves_doy": [10],
        }
    )
    assert_frame_equal(result, expected)


def test_reformat_given_phenophase_ids_and_side_first_and_double_row_per_year():
    dataset = RNPN(
        phenophase_ids={"name": "Leaves", "items": [483, 484]}, years=(2010, 2010)
    )

    df = gpd.GeoDataFrame(
        {
            "geometry": [Point(4, 5), Point(4, 5), Point(4, 5)],
            "phenophase_id": [483, 484, 483],
            "first_yes_year": [2010, 2010, 2011],
            "first_yes_month": [1, 1, 1],
            "first_yes_day": [10, 20, 11],
            "first_yes_doy": [10, 20, 11],
        }
    )

    result = _reformat(dataset, df)

    expected = gpd.GeoDataFrame(
        {
            "geometry": [Point(4, 5), Point(4, 5)],
            "datetime": [datetime(2010, 1, 10), datetime(2011, 1, 11)],
            "Leaves_doy": [10, 11],
        }
    )
    assert_frame_equal(result, expected)


def test_reformat_given_phenophase_ids_and_side_last_and_single_row():
    dataset = RNPN(
        phenophase_ids={"name": "Leaves", "items": [483]},
        years=(2010, 2010),
        use_first=False,
    )

    df = gpd.GeoDataFrame(
        {
            "geometry": [Point(4, 5)],
            "phenophase_id": [483],
            "last_yes_year": [2010],
            "last_yes_month": [1],
            "last_yes_day": [10],
            "last_yes_doy": [10],
        }
    )

    result = _reformat(dataset, df)

    expected = gpd.GeoDataFrame(
        {
            "geometry": [Point(4, 5)],
            "datetime": [datetime(2010, 1, 10)],
            "Leaves_doy": [10],
        }
    )
    assert_frame_equal(result, expected)
