"""Utilities to prepare/load data for use cases."""

import numpy as np
import pyproj
import xarray as xr
import pyPhenology

from py_ppo import download
from pydaymet import get_dataset


def prepare_observations(dataset_name, options):
    """"""
    if dataset_name == "ppo":
        return _prepare_ppo(options)
    raise NotImplementedError


def _prepare_ppo(options):
    """"""
    df = download(
        genus=options["genus"],
        source=options["source"],
        year=options["year"],
        latitude=options["latitude"],
        longitude=options["longitude"],
        termID=options["termID"],
        explode=False,
        limit=1535,
        timeout=10)
    # these two lines should be moved to py_ppo.download
    df.index.name = "location"
    df.drop(
        ["genus","specificEpithet","eventRemarks","termID","source","eventId"],
        axis=1,
        inplace=True,
    )
    return df


def _mask_obs(dataset, x_coord, y_coord):
    x_obs_array = xr.DataArray(x_coord, dims=['location'])
    y_obs_array = xr.DataArray(y_coord, dims=['location'])
    return dataset.sel(x=x_obs_array, y=y_obs_array, method='nearest')


def season_mean(ds):
    """Calculate annual monlthly average"""
    # Make a DataArray with the number of days in each month
    month_length = ds.time.dt.days_in_month

    # Calculate the weights by grouping by 'time.season'
    weights = (
        month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    )

    # Calculate the weighted average
    weighted_average = (ds * weights).groupby("time.season").sum(dim="time")

    # Add year and return
    return weighted_average.assign_coords(year=np.unique(ds.time.dt.year.values))


def _daymet_project(lon, lat):
    """Convert lon/lat (in degrees) to x/y native Daymet projection coordinates
        (in meters).
    """
    daymet_proj =(
        "+proj=lcc +lat_1=25 +lat_2=60 +lat_0=42.5 +lon_0=-100 "
        "+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
        )

    return pyproj.Proj(daymet_proj)(lon, lat)


def prepare_eo(dataset, options):
    if dataset == "Daymet":
        return _prepare_daymet(options)
    raise NotImplementedError


def _prepare_daymet(options):
    """"""
    ## one array per year per var
    daymet_arrays = get_dataset(
        options["longitudes"],
        options["latitudes"],
        options["var_names"],
        options["years"],
        )

    # calculate statistics
    if options["statistics"] == "annual monlthly average":
        daymet_stat_arrays = [season_mean(data_array) for data_array in daymet_arrays]

    # merge data arrays
    daymet_dataset = xr.merge(daymet_stat_arrays, compat='override')
    daymet_dataset.attrs = {"dataset": "Daymet"}
    return daymet_dataset


def merge_eo_obs(eo_dataset, obs_dataframe):
    """"""

    dataset_name = eo_dataset.attrs["dataset"]
    if dataset_name == "Daymet":
        x_obs, y_obs = _daymet_project(
            obs_dataframe.longitude,
            obs_dataframe.latitude
            )
    else:
        raise NotImplementedError

    ### obs coords don't match exactly ratser coords
    data_subset = _mask_obs(eo_dataset, x_obs, y_obs)

    # to a dataframe
    eo_dataframe = data_subset.to_dataframe().reset_index()

    # merge into obs
    eo_obs_dataframe = obs_dataframe.merge(eo_dataframe, on=["location", "year"], how="right")
    return eo_obs_dataframe.dropna(inplace=True)


# TODO refactor as observations and predictors!
def load_daymet_ppo(options):
    obs_options = options["obs_options"]
    eo_options = options["eo_options"]
    obs_df = prepare_observations("ppo", obs_options)
    eo_ds =  prepare_eo("Daymet", eo_options)
    eo_obs_df = merge_eo_obs(eo_ds, obs_df)
    return eo_ds, eo_obs_df


USECASE_ID = {
    "1": "pyPhenology",
    "2": "daymet_ppo",
}


def load_data(options):
    usecase_id = options["usecase_id"]
    dataset_name = USECASE_ID[usecase_id]
    if dataset_name == "pyPhenology":
        return pyPhenology.utils.load_test_data(
            name=options["dataset"],
            phenophase=options["phenophase"],
            )
    if dataset_name == "daymet_ppo":
        return load_daymet_ppo(options)
    raise NotImplementedError
