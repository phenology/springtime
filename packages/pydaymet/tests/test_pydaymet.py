import pytest
from unittest.mock import patch
import numpy as np
import xarray as xr
from pydaymet import download, get_dataset


def test_get_one_dataset():
    longitudes = [-80, -79.95]
    latitudes = [35, 35.05]
    var_names = ["prcp"]
    years = [2019]

    data_arrays = get_dataset(longitudes, latitudes, var_names, years)
    assert len(data_arrays) == 1

    data_array = data_arrays[0]
    assert "prcp" in data_array
    assert data_array.attrs["start_year"] == 2019

    np.testing.assert_almost_equal(
            data_array.lon.min().values, min(longitudes), decimal=2
            )
    np.testing.assert_almost_equal(
            data_array.lon.max().values, max(longitudes), decimal=2
            )
    np.testing.assert_almost_equal(
            data_array.lat.min().values, min(latitudes), decimal=2
            )
    np.testing.assert_almost_equal(
            data_array.lat.max().values, max(latitudes), decimal=2
            )


def test_get_multiple_dataset():
    longitudes = [-80, -79.95]
    latitudes = [35, 35.05]
    var_names = ["prcp", "tmax"]
    years = [2019, 2020]

    data_arrays = get_dataset(longitudes, latitudes, var_names, years)
    assert len(data_arrays) == 4

    assert "prcp" in data_arrays[0]
    assert "tmax" in data_arrays[2]
    assert data_arrays[1].attrs["start_year"] == 2020


def test_download_multiple_dataset(tmp_path):
    longitudes = [-80, -79.95]
    latitudes = [35, 35.05]
    var_names = ["prcp", "tmax"]
    years = [2019]

    with patch("time.strftime") as mocked_time:
        mocked_time.return_value = "2022_11_29_1200"
        data_file_name = download(
                longitudes, latitudes, var_names, years, download_dir=tmp_path
                )
    assert data_file_name == f"{tmp_path}/daymet_v4_daily_2022_11_29_1200.nc"
    
    dataset = xr.open_dataset(data_file_name)
    assert "tmax" in dataset
    assert "prcp" in dataset
