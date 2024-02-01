"""
This module contains functionality to download and load E-OBS data.

Fetches complete grid from
<https://surfobs.climate.copernicus.eu/dataaccess/access_eobs.php>.

Example: Example: Get elevation of whole E-OBS grid

    ```python
    from springtime.datasets import EOBS
    datasource = EOBS(product_type='elevation',
                      variables=['land_surface_elevation'],
                      years=[2000, 2002]
                      )
    ds = datasource.load()
    ```

Example: Example: Get all variables for a single point

    ```python
    from springtime.datasets import EOBS
    datasource = EOBS(points=[5, 50],
                      product_type='ensemble_mean',
                      grid_resolution='0.25deg',
                      years=[2000,2002])
    df = datasource.load()
    ```

Example: Example: Get elevation

    ```python
    from springtime.datasets import EOBS
    datasource = EOBS(points=[5, 50],
                      product_type='elevation',
                      variables=['land_surface_elevation'],
                      years=[2000, 2002]
                      )
    df = datasource.load()
    ```

Examples: Example: Load all variables for a selection of points.

    ```python
    from springtime.datasets import EOBS
    datasource = EOBS(points=[[5, 50], [5, 55]],
                      product_type='ensemble_mean',
                      grid_resolution='0.25deg',
                      years=[2000,2002])
    df = datasource.load()
    ```

Example: Example: Load coarse mean temperature around amsterdam from 2002 till 2002

    ```python
    from springtime.datasets import EOBS
    dataset = EOBS(years=[2000,2002],
                   area={
                       'name': 'amsterdam',
                       'bbox': [4, 50, 5, 55]
                   },
                   grid_resolution='0.25deg')
    df = dataset.load()
    ```

"""

import logging
from datetime import datetime
from typing import Any, Literal, Optional, Sequence
from urllib.request import urlretrieve

import geopandas as gpd
import numpy as np
import xarray as xr
from pydantic import field_validator
from xarray import open_mfdataset

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import (
    NamedArea,
    Point,
    Points,
    get_points_from_raster,
    split_time,
)

logger = logging.getLogger(__name__)

Variable = Literal[
    "maximum_temperature",
    "mean_temperature",
    "minimum_temperature",
    "precipitation_amount",
    "relative_humidity",
    "sea_level_pressure",
    "surface_shortwave_downwelling_radiation",
    "wind_speed",
    "land_surface_elevation",
]

short_vars = {
    "maximum_temperature": "tx",
    "mean_temperature": "tg",
    "minimum_temperature": "tn",
    "precipitation_amount": "rr",
    "relative_humidity": "hu",
    "sea_level_pressure": "pp",
    "surface_shortwave_downwelling_radiation": "qq",
    "wind_speed": "fg",
    "land_surface_elevation": "elevation",
}

operators = {
    "mean": np.mean,
    "min": np.min,
    "max": np.max,
    "sum": np.sum,
    "median": np.median,
}


class EOBS(Dataset):
    """E-OBS dataset.

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        variables: Some variables are specific for a certain product type.
        points: (Sequence of) point(s) as (longitude, latitude) in WGS84 projection.
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.
            If both area and points are defined, will first crop the area before
            extracting points, so points outside area will be lost. If points is
            None, will return the full dataset as xarray object; this cannot be
            joined with other datasets.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling. Else, should be a dictonary of the form {frequency:
            'M', operator: 'mean', **other_options}. Currently supported
            operators are 'mean', 'min', 'max', 'sum', 'median'. For valid
            frequencies see [1]. Other options will be passed directly to
            xr.resample [2]
        product_type: one of "ensemble_mean", "ensemble_spread", "elevation".
        grid_resolution: either "0.25deg" or "0.1deg"
        version: currently only possible value is "26.0e"
        keep_grid_location: If True, keep the eobs_longitude and eobs_latitude
            columns. If False, keep input points instead.
        minimize_cache: if True, only store the data for the selected years and
            optionally area. This saves disk space, but it requires
            re-downloading for new years/areas. Default is False, i.e. keep the full
            EOBS grid.

        [1]
        https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        [2]
        https://docs.xarray.dev/en/stable/generated/xarray.Dataset.resample.html
    """

    dataset: Literal["E-OBS"] = "E-OBS"
    product_type: Literal[
        "ensemble_mean", "ensemble_spread", "elevation"
    ] = "ensemble_mean"
    variables: Sequence[Variable] = ("mean_temperature",)
    grid_resolution: Literal["0.25deg", "0.1deg"] = "0.1deg"
    version: Literal["26.0e"] = "26.0e"
    points: Point | Points | None = None
    keep_grid_location: bool = False  # TODO implement
    area: NamedArea | None = None
    minimize_cache: bool = False
    resample: Optional[dict[str, Any]] = None

    # TODO add root validator that use same valid combinations as on
    # https://cds.climate.copernicus.eu/cdsapp#!/dataset/insitu-gridded-observations-europe?tab=form
    @field_validator("years")
    def _valid_years(cls, years):
        assert (
            years.start >= 1950
        ), f"Asked for year {years.start}, but no data before 1950"
        assert years.end <= 2022, f"Asked for year {years.end}, but no data after 2022"
        # TODO Max is whatever the chosen version has
        return years

    @property
    def _periods(self):
        # TODO these ranges are not inclusive, so edge years are excluded now.
        periods = [
            range(1950, 1964),
            range(1965, 1979),
            range(1980, 1994),
            range(1995, 2010),
            range(2011, 2022),  # version 26.0e has till end of 2022
        ]
        matched_periods = set()
        for period in periods:
            if self.years.start in period or self.years.end in period:
                matched_periods.add(f"{period.start}-{period.stop}")
        return matched_periods

    @property
    def _root_dir(self):
        return CONFIG.cache_dir / "e-obs"

    def _server_filename(self, variable: Variable, period: str):
        short_var = short_vars[variable]
        if self.product_type == "ensemble_mean":
            rpv = f"{self.grid_resolution}_reg_{period}_v{self.version}"
            return f"{short_var}_ens_mean_{rpv}.nc"
        elif self.product_type == "ensemble_spread":
            rpv = f"{self.grid_resolution}_reg_{period}_v{self.version}"
            return f"{short_var}_ens_spread_{rpv}.nc"
        return f"elev_ens_{self.grid_resolution}_reg_v{self.version}.nc"

    def _server_url(self, variable: Variable, period: str):
        root = "https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/"
        base = f"{root}Grid_{self.grid_resolution}_reg_ensemble/"
        return base + self._server_filename(variable, period)

    def _default_path(self, variable: Variable, period: str):
        """Return default path with filename as downloaded from server."""
        default_filename = self._server_filename(variable, period)
        return self._root_dir / default_filename

    def _minimized_path(self, variable: Variable):
        """Return path with modified filename when minimize_cache is True."""
        # Same pattern, custom period/region:
        assert self.years, "self.years should be defined"  # type narrowing
        period = f"{self.years.start}-{self.years.end}"
        minimized_filename = self._server_filename(variable, period)

        if self.area is not None:
            minimized_filename = minimized_filename.replace("reg", self.area.name)

        return self._root_dir / minimized_filename

    def download(self):
        """Download the data if necessary and return the file paths."""
        logger.info("Locating data")
        self._root_dir.mkdir(exist_ok=True)
        full_period = f"{self.years.start}-{self.years.end}"

        paths = []
        for variable in self.variables:
            logger.info(f"Looking for variable {variable} in period {full_period}...")

            # Check if we can skip because minized path exists
            minimized_path = self._minimized_path(variable)
            if (
                self.minimize_cache
                and minimized_path.exists()
                and not CONFIG.force_override
            ):
                logger.info(f"Found {minimized_path}")
                paths.append(minimized_path)
                continue

            # Check default paths
            sub_paths = []
            for sub_period in self._periods:
                default_path = self._default_path(variable, sub_period)

                if default_path.exists() and not CONFIG.force_override:
                    logger.info(f"Found {default_path}")
                else:
                    logger.info(f"Downloading {default_path}.")
                    url = self._server_url(variable, sub_period)
                    urlretrieve(url, default_path)

                sub_paths.append(default_path)

            if self.minimize_cache:
                arrays = [xr.open_dataarray(p) for p in sub_paths]
                da = arrays[0] if len(arrays) == 1 else xr.concat(arrays, dim="time")

                da = extract_time(da, self.years.start, self.years.end)

                if self.area is not None:
                    da = extract_area(da, self.area.bbox)

                da.to_netcdf(minimized_path)
                [p.unlink() for p in sub_paths]

                paths.append(minimized_path)
            else:
                paths.extend(sub_paths)

        return paths

    def raw_load(self):
        """Load the dataset from disk into memory without further processing."""
        paths = self.download()

        ds = open_mfdataset(
            paths,
            chunks="auto",
            # For 0.1deg grid we saw the lat/longs are not exactly the same
            # the difference is very small (1e-10), but it causes problems when joining
            join="override",
        )

        # Rename vars
        # TODO: discard this step, stick with defaults. NC files have good attrs.
        short2long = {v: k for k, v in short_vars.items() if k in self.variables}
        ds = ds.rename_vars(short2long)
        return ds

    def load(self):
        """Load and pre-process the data."""
        ds = self.raw_load()

        # Select time
        ds = extract_time(ds, self.years.start, self.years.end)

        # Select area
        if self.area is not None:
            ds = extract_area(ds, self.area.bbox)

        # Resample
        if self.resample is not None:
            resample_kwargs = self.resample
            frequency = resample_kwargs.pop("frequency", "M")
            operator_key = resample_kwargs.pop("operator", "mean")
            operator = operators[operator_key]
            ds = ds.resample(time=frequency, **resample_kwargs).reduce(operator)
        else:
            frequency = "daily"

        # Extract year and DOY columns
        if "time" in ds.dims:
            ds = split_time(ds, freq=frequency)

        # Early return if no points are extracted
        if self.points is None:
            logger.warning(
                """
                No points selected; returning gridded data as xarray object.
                Useful for prediction, but cannot be used in a recipe.
                """
            )
            return ds

        # Extract points/records
        ds = get_points_from_raster(self.points, ds)

        return self._to_dataframe(ds)

    def _to_dataframe(self, ds: xr.Dataset):
        """Transform to dataframe and process to final format."""
        # TODO simplify; maybe add other option, to include data as list?
        if "index" in ds.coords:
            df = ds.to_dataframe()
            df = df.set_index(["year", "geometry"], append=True).unstack("timeinyear")
            df.columns = df.columns.map("{0[0]}|{0[1]}".format)
            df = df.reset_index("index", drop=True).reset_index()
            df = gpd.GeoDataFrame(df)
        else:
            df = ds.to_dataframe().reset_index()
            df = df.set_index(["year", "geometry", "timeinyear"]).unstack("timeinyear")
            df.columns = df.columns.map("{0[0]}|{0[1]}".format)
            df = df.reset_index()
            df = gpd.GeoDataFrame(df)
        return df


def extract_time(ds, start, end):
    """Extract time range from xarray dataset."""
    if "time" not in ds.dims:
        # elevation has no time dimension, so make it. TODO (why??)
        return ds.expand_dims({"time": [datetime(start, 1, 1)]}, axis=0)
    return ds.sel(time=slice(f"{start}-01-01", f"{end}-12-31"))


def extract_area(ds, bbox):
    """Extract bounding box from xarray dataset."""
    return ds.sel(
        longitude=slice(bbox[0], bbox[2]),
        latitude=slice(bbox[1], bbox[3]),
    )


def monthly_agg(ds, operator=np.mean):
    """Return monthly aggregates based on DOY dimension."""
    bins = np.linspace(0, 366, 13, dtype=int)
    grouped = ds.groupby_bins(group="doy", bins=bins, labels=bins[1:], precision=0)
    aggregated = grouped.reduce(operator).rename(doy_bins="doy")
    aggregated["doy"] = aggregated["doy"].values.astype(int)
    return aggregated


def monthly_gdd(ds, t_base=5):
    """Return monthly growing degree days based on DOY dimension."""
    # TODO rename variable to GDD?
    # only operate on data-arrays, not datasets?
    bins = np.linspace(0, 366, 13, dtype=int)
    gdd = (ds - t_base).cumsum("doy")
    grouped = gdd.groupby_bins(
        group="doy", bins=bins, labels=bins[1:].astype(int), precision=0
    )
    aggregated = grouped.max().rename(doy_bins="doy")
    aggregated["doy"] = aggregated["doy"].values.astype(int)
    return aggregated


# TODO
# - What to do with keep_grid_location?
# - Fix elevation
# - Different kind of transpose ds to df (keep time as series?)
