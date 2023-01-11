"""Utilities to prepare/load data for use cases.

TODO:
- write tests
- save/load workflow
- plot results
- see other todo below
- add documentations

"""

import numpy as np
import pyPhenology
import pyproj
import xarray as xr
from py_ppo import download
from pydaymet import get_dataset
from pyPhenology.models import utils as models_utils
from sklearn.model_selection import ShuffleSplit


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

    # this is needed for merging later
    df.index.name = "location"

    # clean the dataframe
    df.drop(
        ["specificEpithet","eventRemarks","termID","source","eventId"],
        axis=1,
        inplace=True,
    )
    print("PPO data are retrieved.")
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
    print("Daymet data are retrieved.")

    # calculate statistics
    print("Calculating statistics (loading eo data in memory) ...")
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
            obs_dataframe.latitude,
            )
    else:
        raise NotImplementedError

    ### obs coords don't match exactly ratser coords
    data_subset = _mask_obs(eo_dataset, x_obs, y_obs)

    # to a dataframe
    eo_dataframe = data_subset.to_dataframe().reset_index()

    # merge into obs
    eo_obs_dataframe = obs_dataframe.merge(eo_dataframe, on=["location", "year"], how="right")
    return eo_obs_dataframe.dropna()


def split_data(data, method_name):
    """Split arrays into random train and test subsets.

    Methods are available with sklearn package.
    Args:
        dataset : array
            data to be splitted.

        method_name : str
            one of the sklearn train test split functions.

    Returns:
        test, train : tuple
            arrays for testing and training.
    """
    if method_name=="ShuffleSplit":
        # TODO pass arguments to function
        rs = ShuffleSplit(n_splits=2, test_size=.25, random_state=2)
        for train_index, test_index in rs.split(data):
            return data.iloc[test_index], data.iloc[train_index]

    raise NotImplementedError(f"Unsupported train_test strategy {method_name}")


def prepare_daymet_ppo_data(options):
    """"""
    obs_options = options["obs_options"]
    obs_df = prepare_observations("ppo", obs_options)

    eo_options = options["eo_options"]
    eo_ds = prepare_eo("Daymet", eo_options)

    # TODO check if merging data in this way makes sense!
    eo_obs_df = merge_eo_obs(eo_ds, obs_df)

    # organize data in dataframes
    # TODO check if organizng data in this way makes sense!
    data_frames = {}
    data_frames["targets"], data_frames["predictors"] = obs_df, eo_ds.to_dataframe().reset_index()

    test_df, train_df = split_data(eo_obs_df, options["train_test_strategy"])
    data_frames["targets_test"] = test_df[["dayOfYear"]]
    data_frames["targets_train"] = train_df[["dayOfYear"]]

    data_frames["predictors_test"] = test_df[eo_options["var_names"]]
    data_frames["predictors_train"] = train_df[eo_options["var_names"]]

    # organize data for fitting and prediction in sklearn models in data arrays
    data_arrays = {}
    data_arrays["targets_test"] = data_frames["targets_test"].dayOfYear.values
    data_arrays["targets_train"] = data_frames["targets_train"].dayOfYear.values

    data_arrays["predictors_test"] = data_frames["predictors_test"].to_numpy()
    data_arrays["predictors_train"] = data_frames["predictors_train"].to_numpy()

    return {"data_frames": data_frames, "data_arrays": data_arrays}


def prepare_pyPhenology_data(options):
    """Load and prepare data from pyPhenology package.

    Datasets are available with pyPhenology package.
    Args:

    Returns:
    """

    # dataframes
    data_frames = {}
    data_frames["targets"], data_frames["predictors"] = pyPhenology.utils.load_test_data(
        name=options["dataset"],phenophase=options["phenophase"]
        )

    data_frames["targets_test"], data_frames["targets_train"] = split_data(
        data_frames["targets"], options["train_test_strategy"]
        )

    # organize data for fitting in sklearn models in data arrays
    data_arrays = {}
    data_arrays["targets_train"], data_arrays["predictors_train"], _ = models_utils.misc.temperature_only_data_prep(
        data_frames["targets_train"], data_frames["predictors"], for_prediction=False
        )
    data_arrays["predictors_train"] = data_arrays["predictors_train"].T

    # organize data for prediction in sklearn models
    data_arrays["predictors_test"], _ = models_utils.misc.temperature_only_data_prep(
        data_frames["targets_test"], data_frames["predictors"], for_prediction=True
        )
    data_arrays["predictors_test"] = data_arrays["predictors_test"].T

    data_arrays["targets_test"] = data_frames["targets_test"].doy.values
    return {"data_frames": data_frames, "data_arrays": data_arrays}



USECASE = {
    "0": {"dataset_name": "pyPhenology"},
    "1": {"dataset_name": "daymet_ppo"},
}


def load_data(options):
    usecase_id = options["usecase_id"]
    dataset_name = USECASE[usecase_id]["dataset_name"]
    if dataset_name == "pyPhenology":
        return prepare_pyPhenology_data(options)
    if dataset_name == "daymet_ppo":
        return prepare_daymet_ppo_data(options)
    raise NotImplementedError