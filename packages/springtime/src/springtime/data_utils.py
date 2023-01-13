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


USECASE = {
    "0": {"data_name": "pyPhenology"},
    "1": {"data_name": "daymet_ppo"},
}


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
        ["specificEpithet", "eventRemarks", "termID", "source", "eventId"],
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
    """Calculate annual seasonal average"""
    # Make a DataArray with the number of days in each month
    month_length = ds.time.dt.days_in_month

    # Calculate the weights by grouping by 'time.season'
    weights = (
        month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    )

    # Calculate the weighted average
    weighted_average = (ds * weights).groupby("time.season").sum(dim="time")

    # Add year and return
    return weighted_average.expand_dims("year").assign_coords(
        year=np.unique(ds.time.dt.year.values)
        )


def _daymet_project(lon, lat):
    """Convert lon/lat (in degrees) to x/y native Daymet projection coordinates
        (in meters).
    """
    daymet_proj = (
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
    # one array per year per var
    daymet_arrays = get_dataset(
        options["longitudes"],
        options["latitudes"],
        options["var_names"],
        options["years"],
        )
    print("Daymet data are retrieved.")

    # calculate statistics
    print("Calculating statistics (loading data into memory) ...")
    if options["statistics"] == "annual seasonal average":
        daymet_stat_arrays = [season_mean(data_array) for data_array in daymet_arrays]
    else:
        raise NotImplementedError

    # combine data arrays
    daymet_dataset = xr.combine_by_coords(daymet_stat_arrays, combine_attrs="drop_conflicts")
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

    # obs coords don't match exactly ratser coords
    data_subset = _mask_obs(eo_dataset, x_obs, y_obs)

    # to a dataframe
    eo_dataframe = data_subset.to_dataframe().reset_index()

    # merge into obs
    eo_obs_dataframe = eo_dataframe.merge(obs_dataframe, on=["location", "year"], how="left")
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
    if method_name == "ShuffleSplit":
        # TODO pass arguments to function
        rs = ShuffleSplit(n_splits=2, test_size=.25, random_state=2)
        for train_index, test_index in rs.split(data):
            return data.iloc[test_index], data.iloc[train_index]

    raise NotImplementedError(f"Unsupported train_test strategy {method_name}")


def prepare_daymet_ppo_data(options):
    """"""
    obs_varname = "dayOfYear"
    obs_options = options["obs_options"]
    obs_df = prepare_observations("ppo", obs_options)

    eo_options = options["eo_options"]
    eo_ds = prepare_eo("Daymet", eo_options)

    # mask obs from eo data and merg einto one
    eo_obs_df = merge_eo_obs(eo_ds, obs_df)

    # reshape data frame
    if eo_options["statistics"] == "annual seasonal average":
        features = "season"
    else:
        raise NotImplementedError
    eo_obs_df = eo_obs_df.pivot(
        index=["location", "year", obs_varname],
        columns=[features],
        values=eo_options["var_names"]
        )
    eo_obs_df = eo_obs_df.reset_index(level=obs_varname)

    # organize data in dict
    train_test = {}
    train_test["targets"] = obs_df

    eo_df = eo_ds.to_dataframe().reset_index()
    train_test["predictors"] = eo_df.pivot(
        index=["x", "y", "year"],
        columns=[features],
        values=eo_options["var_names"]
        )

    test_df, train_df = split_data(eo_obs_df, options["train_test_strategy"])
    train_test["targets_test"] = test_df[[obs_varname]]
    train_test["targets_train"] = train_df[[obs_varname]]

    train_test["predictors_test"] = test_df[eo_options["var_names"]]
    train_test["predictors_train"] = train_df[eo_options["var_names"]]

    return {"train_test": train_test, "eo_data": eo_ds, "obs_data": obs_df}


def prepare_pyphenology_data(options):
    """Load and prepare data from pyPhenology package.

    Datasets are available with pyPhenology package.
    Args:

    Returns:
    """
    obs_varname = "doy"

    # dataframes for pyPhenology
    pyphenology_data = {}
    pyphenology_data["targets"], pyphenology_data["predictors"] = pyPhenology.utils.load_test_data(
        name=options["dataset"], phenophase=options["phenophase"]
        )
    pyphenology_data["targets_test"], pyphenology_data["targets_train"] = split_data(
        pyphenology_data["targets"], options["train_test_strategy"]
        )

    # dataframes for sklearn
    train_test = {}
    _, predictors_train, _ = models_utils.misc.temperature_only_data_prep(
       pyphenology_data["targets_train"],
       pyphenology_data["predictors"],
       for_prediction=False,
       )
    train_test["predictors_train"] = predictors_train.T

    predictors_test, _ = models_utils.misc.temperature_only_data_prep(
        pyphenology_data["targets_test"],
        pyphenology_data["predictors"],
        for_prediction=True,
        )
    train_test["predictors_test"] = predictors_test.T

    train_test["targets_train"] = pyphenology_data["targets_train"][[obs_varname]]
    train_test["targets_test"] = pyphenology_data["targets_test"][[obs_varname]]
    return {"train_test": train_test, "pyPhenology": pyphenology_data}


def load_data(options):
    usecase_id = options["usecase_id"]
    dataset_name = USECASE[usecase_id]["data_name"]
    if dataset_name == "pyPhenology":
        return prepare_pyphenology_data(options)
    if dataset_name == "daymet_ppo":
        return prepare_daymet_ppo_data(options)
    raise NotImplementedError
