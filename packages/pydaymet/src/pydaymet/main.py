""""Download Daymet data from
Daily Surface Weather Data on a 1-km Grid for North America, Version 4 R1
https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=2129

temporal coverage: 1950-01-01 to 2021-12-31
spatial coverage:
    N:        S:       E:      W:
hi 23.52	17.95	-154.77	-160.31
na 82.91	14.07	-53.06	-178.13
pr 19.94	16.84	-64.12	-67.99


dayl	Duration of the daylight period (seconds/day)
prcp	Daily total precipitation (mm/day)
srad	Incident shortwave radiation flux density(W/m2)
swe	Snow water equivalent (kg/m2)
tmax	Daily maximum 2-meter air temperature (°C)
tmin	Daily minimum 2-meter air temperature (°C)
vp	Water vapor pressure (Pa)

requires:
xarray + pydap
pyproj
"""

import datetime
import xarray as xr
import pyproj
from typing import Tuple, Iterable


def _build_url(file_name:str) -> str:
    """Build dataset url.

    Args:
        file_name(str): example "na_tmin_2015.nc"

    Returns:
        string, dataset url

    """
    base_url = "https://thredds.daac.ornl.gov/thredds/dodsC/ornldaac/2129/daymet_v4_daily_"
    return f"{base_url}{file_name}"


def _build_file_name(region:str, var_name:str, year:str) -> str:
    """"Build the file name of the dataset to be retrieved.

    Args:
        region(str): the name of the region where daymet is available, one of
        the "hi", "na", "pr".
        var_name(str): variable name of daymet, one of the "dayl", "prcp",
        "srad", "swe", "tmax", "tmin", "vp".
        year(str): the year which daymet is available, from 1950-01-01 to
        2021-12-31

    Returns:
        str: example "na_tmin_2015.nc"
    """
    return f"{region}_{var_name}_{year}.nc"


def _project_bbox(
        user_lon:Iterable[float] , user_lat:Iterable[float]
        ) -> Tuple(Iterable[float], Iterable[float]):
    """Convert lon/lat (in degrees) to x/y native Daymet projection coordinates
    (in meters).

    Args:
        user_lon(list): a list containing min and max values of longitudes in degree.
        user_lat(list): a list containing min and max values of latitudes in degree.

    Returns:
        Tuple(list, list): converted coordinates in daymet projection.
    """
    daymet_proj =(
        "+proj=lcc +lat_1=25 +lat_2=60 +lat_0=42.5 +lon_0=-100 "
        "+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
        )

    return pyproj.Proj(daymet_proj)(user_lon, user_lat)


def _find_region(user_lon:Iterable[float] , user_lat:Iterable[float]) -> Iterable[str]:
    """Find Daymet regions where user bbox falls into.

    Args:
        user_lon(list): a list containing min and max values of longitudes in degree.
        user_lat(list): a list containing min and max values of latitudes in degree.

    Returns:
        A list of regions.
    """
    if len(user_lon)!=2 or len(user_lat)!=2:
        raise ValueError("Provide only min and max of longitudes/latitudes.")

    regions = {
        "hi": {"lon": [-160.31, -154.77], "lat": [17.95, 23.52]},
        "na": {"lon": [-178.13, -53.06], "lat": [14.07, 82.91]},
        "pr": {"lon": [-67.99, -64.12], "lat": [16.84, 19.94]},
    }
    found_regions = []
    for region, coords in regions.items():
        region_lon = coords["lon"]
        region_lat = coords["lat"]
        if _has_overlap(user_lon, user_lat, region_lon, region_lat):
            found_regions.append(region)
    if not found_regions:
        raise ValueError("Change longitude/latitude values.")
    return found_regions


def _has_overlap(
        user_lon:Iterable[float] , user_lat:Iterable[float],
        region_lon:Iterable[float], region_lat:Iterable[float],
        ) -> bool:
    """Check if user-defined coordinates fall in one of the daymet regions.

    Args:
        user_lon(list): a list containing min and max values of user-defined
        longitudes in degree.
        user_lat(list): a list containing min and max values of user-defined
        latitudes in degree.
        region_lon(list): a list containing min and max values of daymet
        longitudes in degree.
        region_lat(list): a list containing min and max values of daymet
        latitudes in degree.

    Returns:
        True if user-defined coordinates fall in one of the daymet regions.
    """
    x1, x2 = user_lon
    y1, y2 = user_lat

    X1, X2 = region_lon
    Y1, Y2 = region_lat

    # it works only if user bbox is smaller than region bbox
    return (x1>=X1) and (x2<=X2) and (y1>=Y1) and (y2<=Y2)


def _clip_dataset(
        longitudes:Iterable[float], latitudes:Iterable[float], data:xr.DataArray
        ) -> xr.DataArray:
    """Clip a data array using coordinates.

    Args:
        longitudes(list): a list containing min and max values of
        longitudes in degree.
        latitudes(list): a list containing min and max values of
        latitudes in degree.
        data(xr.DataArray): data to be resized.

    Returns:
        xr.DataArray
    """
    daymet_longitudes,  daymet_latitudes = _project_bbox(longitudes, latitudes)

    x1, x2 = daymet_longitudes
    y1, y2 = daymet_latitudes

    return data.sel(x=slice(x1, x2), y=slice(y2, y1))


def _open_dataset(region:str, var_name:str, year:str) -> xr.DataArray:
    """Open daymet datasets using OPeNDAP and xarray.

    Args:
        region(str): the name of the region where daymet is available, one of
        the "hi", "na", "pr".
        var_name(str): variable name of daymet, one of the "dayl", "prcp",
        "srad", "swe", "tmax", "tmin", "vp".
        year(str): the year which daymet is available, from 1950-01-01 to
        2021-12-31

    Returns:
        xr.DataArray
    """
    file_name = _build_file_name(region, var_name, year)
    data_url = _build_url(file_name)

    # not downloded
    return xr.open_dataset(
        xr.backends.PydapDataStore.open(data_url, timeout=500),
        decode_coords="all"
        )

def _check_lon(longitudes:Iterable[float]):
    """Check if longitude falls in [-180, 180]."""
    if min(longitudes) < -180 or max(longitudes) > 180:
        raise ValueError("Longitudes should be in [-180, 180].")


def _check_lat(latitudes:Iterable[float]):
    """Check if latitudes falls in [0, 90]."""
    if min(latitudes) < 0 or max(latitudes) > 90:
        raise ValueError("Latitudes should be in [0, 90].")


def _check_var_name(var_names:Iterable[str]):
    daymet_var_names = ["dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp"]
    for name in var_names:
        if name not in daymet_var_names:
            raise ValueError(f"{name} not in {daymet_var_names}.")


def _check_years(years:Iterable[str]):
    now = datetime.datetime.now()
    max_year = str(now.year - 1)
    #1980 is the begining of the Daymet time series
    if min(years) < "1980" or max(years) > max_year:
        raise ValueError(f"years should be in [1980 to {max_year}].")


def get_dataset(
        longitudes:Iterable[float], latitudes:Iterable[float],
        var_names:Iterable[str], years:Iterable[str]
        ) -> Iterable[xr.DataArray]:
    """Return a list of data arrays that contains requested data.

    Args:
        longitudes(list): an array containing min and max values of
        longitudes in degree.
        latitudes(list): an array containing min and max values of
        latitudes in degree.
        var_names(list): variable names of daymet, a list of the "dayl", "prcp",
        "srad", "swe", "tmax", "tmin", "vp".
        years(list): the year which daymet is available, from 1950-01-01 to
        2021-12-31

    Retruns:
        a lsit of xr.DataArray
    """
    _check_lon(longitudes)
    _check_lat(latitudes)
    _check_var_name(var_names)
    _check_years(years)

    start_year, end_year = years
    year_range = [str(year) for year in range(start_year, end_year + 1)]

    regions = _find_region(longitudes, latitudes)

    data_arrays = []
    for region in regions:
        for var_name in var_names:
            for year in year_range:
                remote_data = _open_dataset(region, var_name, year)
                data_array = _clip_dataset(longitudes, latitudes, remote_data)
                data_arrays.append(data_array)
    # no download
    return data_arrays


def download(
        longitudes:Iterable[float], latitudes:Iterable[float],
        var_names:Iterable[str], years:Iterable[str],
        download_dir:str = "."
        ) -> str:
    """Download a list of data arrays that contains requested data.

    Args:
        longitudes(list): an array containing min and max values of
        longitudes in degree.
        latitudes(list): an array containing min and max values of
        latitudes in degree.
        var_names(list): variable names of daymet, a list of the "dayl", "prcp",
        "srad", "swe", "tmax", "tmin", "vp".
        years(list): the year which daymet is available, from 1950-01-01 to
        2021-12-31
        download_dir(str): pth where data should be stored.

    Retruns:
        filename of a netcdf file that contains requested data.
    """

    data_arrays = get_dataset(longitudes, latitudes, var_names, years)

    # download starts, memory usage
    data_set = xr.merge(data_arrays)
    now = datetime.datetime.now()
    timestamp = now.strftime("%m_%d_%Y_%H_%M")
    data_file_name = f"{download_dir}/daymet_v4_daily_{timestamp}.nc"
    data_set.to_netcdf(data_file_name)
    return data_file_name
