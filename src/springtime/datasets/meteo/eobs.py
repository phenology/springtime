# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
"""
This module contains functionality to download and load E-OBS data.

Fetches complete grid from
<https://surfobs.climate.copernicus.eu/dataaccess/access_eobs.php>.

Example: Example: Get elevation of whole E-OBS grid

    ```python
    from springtime.datasets.meteo.eobs import EOBS
    datasource = EOBS(product_type='elevation',
                    variables=['land_surface_elevation'],
                    years=[2000, 2002]
                    )
    datasource.download()
    ds = datasource.load()
    ```


Example: Example: Get all variables for a single point

    ```python
    from springtime.datasets.meteo.eobs import EOBSSinglePoint
    datasource = EOBSSinglePoint(point=[5, 50],
                                product_type='ensemble_mean',
                                grid_resolution='0.25deg',
                                years=[2000,2002])
    datasource.download()
    df = datasource.load()
    ```

Example: Example: Get elevation

    ```python
    from springtime.datasets.meteo.eobs import EOBSSinglePoint
    datasource = EOBSSinglePoint(point=[5, 50],
                                product_type='elevation',
                                variables=['land_surface_elevation'],
                                years=[2000, 2002]
                                )
    datasource.download()
    df = datasource.load()
    ```

Examples: Example: Load all variables for a selection of points.

    ```python
    from springtime.datasets.meteo.eobs import EOBSMultiplePoints
    datasource = EOBSMultiplePoints(points=[
                                        [5, 50],
                                        [5, 55],
                                    ],
                                    product_type='ensemble_mean',
                                    grid_resolution='0.25deg',
                                    years=[2000,2002])
    datasource.download()
    df = datasource.load()
    ```

Example: Example: Load coarse mean temperature around amsterdam from 2002 till 2002

    ```python
    from springtime.datasets.meteo.eobs import EOBSBoundingBox

    dataset = EOBSBoundingBox(
        years=[2000,2002],
        area={
            'name': 'amsterdam',
            'bbox': [4, 50, 5, 55]
        },
        grid_resolution='0.25deg'
    )
    dataset.download()
    df = dataset.load()
    ```

"""


from datetime import datetime
import time
import logging
from itertools import product
from typing import Literal, Sequence, Tuple
from urllib.request import urlretrieve
from springtime.datasets.abstract import Dataset

import geopandas as gpd
import pandas as pd
import xarray as xr
from pydantic import field_validator
from xarray import open_mfdataset

from springtime.config import CONFIG
from springtime.utils import NamedArea, Points

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


# TODO merge all EOBS classes into a single one with a different load method for
# different geometries?
# def load(self):
#     if isinstance(self.geometry, Point):
#         return self._load_single_point()
#     if isinstance(self.geometry, Points):
#         return self._load_multiple_points()
#     if isinstance(self.geometry, NamedArea):
#         return self._load_area()


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
            no resampling.
        product_type: one of "ensemble_mean", "ensemble_spread", "elevation".
        grid_resolution: either "0.25deg" or "0.1deg"
        version: currently only possible value is "26.0e"
        keep_grid_location: If True, keep the eobs_longitude and eobs_latitude
            columns. If False, keep input points instead.
        use_area_for_cache: if True, the cached data is replaced with the
            cropped area. This saves disk space, but it requires re-downloading
            for new areas. Default is False, i.e. keep the full EOBS grid.
    """

    dataset: Literal["E-OBS"] = "E-OBS"
    product_type: Literal[
        "ensemble_mean", "ensemble_spread", "elevation"
    ] = "ensemble_mean"
    variables: Sequence[Variable] = ("mean_temperature",)
    grid_resolution: Literal["0.25deg", "0.1deg"] = "0.1deg"
    version: Literal["26.0e"] = "26.0e"
    points: Tuple[float, float] | Points | None = None
    keep_grid_location = False
    area: NamedArea
    use_area_for_cache = False
    # resample TODO implement

    # TODO add root validator that use same valid combinations as on
    # https://cds.climate.copernicus.eu/cdsapp#!/dataset/insitu-gridded-observations-europe?tab=form

    def _url(self, variable: Variable, period: str):
        root = "https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/"
        base = f"{root}Grid_{self.grid_resolution}_reg_ensemble/"
        return base + self._filename(variable, period)

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

    def _filename(self, variable: Variable, period: str):
        short_var = short_vars[variable]
        if self.product_type == "ensemble_mean":
            rpv = f"{self.grid_resolution}_reg_{period}_v{self.version}"
            return f"{short_var}_ens_mean_{rpv}.nc"
        elif self.product_type == "ensemble_spread":
            rpv = f"{self.grid_resolution}_reg_{period}_v{self.version}"
            return f"{short_var}_ens_spread_{rpv}.nc"
        return f"elev_ens_{self.grid_resolution}_reg_v{self.version}.nc"

    @property
    def _root_dir(self):
        return CONFIG.cache_dir / "e-obs"

    def _path(self, variable: Variable, period: str):
        return self._root_dir / self._filename(variable, period)

    def _to_dataframe(self, ds: xr.Dataset):
        df = ds.to_dataframe()
        df = df.reset_index().rename(columns={"time": "datetime"})
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf[["datetime", "geometry"] + list(self.variables)]

    def _maybe_download(self, variable, period):
        """Download the data and return the file path."""
        self._root_dir.mkdir(exist_ok=True)
        url = self._url(variable, period)
        path = self._path(variable, period)
        if not path.exists() or CONFIG.force_override:
            msg = f"Downloading E-OBS variable {variable} "
            msg = msg + f"for {period} period from {url} to {path}"
            logger.warning(msg)
            urlretrieve(url, path)

            # TODO process use_area_for_cache
        else:
            msg = f"{path} already exists, skipping"
            logger.warning(msg)
        return path

    def download(self):
        """Download all files in one go"""
        for variable, period in product(self.variables, self._periods):
            self._maybe_download(variable, period)

    def raw_load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        paths = []
        for variable, period in product(self.variables, self._periods):
            path = self._maybe_download(variable, period)
            paths.append(path)

        ds = open_mfdataset(
            paths,
            chunks='auto',
            # For 0.1deg grid we saw the lat/longs are not exactly the same
            # the difference is very small (1e-10), but it causes problems when joining
            join="override",
        )
        return ds

    def load(self):
        ds = self.raw_load()

        # Rename vars
        short2long = {v: k for k, v in short_vars.items() if k in self.variables}
        ds = ds.rename_vars(short2long)

        # Select time
        ds = extract_time(ds, self.years.start, self.years.end)

        # Select area
        if self.area is not None:
            ds = extract_area(ds, self.area.bbox)

        # Extract points
        if isinstance(self.points, tuple):
            ds = extract_single_point(ds, self.points)
            return self._to_dataframe(ds)
        elif isinstance(self.points, Points):
            # Multiple points
            lons = xr.DataArray([p[0] for p in self.points], dims="points_index")
            lats = xr.DataArray([p[1] for p in self.points], dims="points_index")
            points_df = gpd.GeoDataFrame(
                geometry=gpd.points_from_xy(lons, lats)
            ).reset_index(names="points_index")
            logger.warning(f"Loading E-OBS for {len(self.points)} points for {self.years}")
            period_dfs = []
            for period in self._periods:
                period_df = None
                for variable in self.variables:
                    # TODO this is basically the same as super.load()
                    logger.warning(f"Loading {variable} for {period}")
                    start = time.time()
                    path = self._path(variable, period)
                    var_ds = xr.open_dataset(path)
                    var_ds = var_ds.rename_vars({short_vars[variable]: variable})
                    if self.product_type == "elevation":
                        dt = datetime(self.years.start, 1, 1)
                        var_ds = var_ds.expand_dims({"time": [dt]}, axis=0)
                    else:
                        var_ds = var_ds.sel(
                            time=slice(
                                f"{self.years.start}-01-01", f"{self.years.end}-12-31"
                            )
                        )
                    var_ds = var_ds.sel(
                        longitude=lons,
                        latitude=lats,
                        method="nearest",
                    )
                    var_df = var_ds.to_dataframe()
                    var_df.reset_index(inplace=True)
                    if self.keep_grid_location:
                        var_df.rename(
                            columns={
                                "longitude": "eobs_longitude",
                                "latitude": "eobs_latitude",
                            },
                            inplace=True,
                        )
                    else:
                        var_df.drop(columns=["longitude", "latitude"], inplace=True)
                    var_df = pd.merge(points_df, var_df, on="points_index")
                    var_df = var_df.drop(columns=["points_index"]).rename(
                        columns={"time": "datetime"}
                    )
                    if period_df is None:
                        period_df = var_df
                    else:
                        if self.keep_grid_location:
                            # Having eobs_* one time is enough
                            var_df = var_df.drop(
                                columns=["eobs_longitude", "eobs_latitude"]
                            )
                        period_df = pd.merge(
                            period_df, var_df, on=["datetime", "geometry"], how="outer"
                        )
                    end = time.time()
                    logger.warning(f"Loaded {variable} for {period} in {end-start} seconds")
                period_dfs.append(period_df)
            df = pd.concat(period_dfs)
            return gpd.GeoDataFrame(df).sort_values(by=["datetime", "geometry"])
        else:
            # TODO don't convert giant ds to dataframe?
            return self._to_dataframe(ds)


def extract_time(ds, start, end):
    """Extract time range from xarray dataset."""
    if 'time' not in ds.dims:
        # elevation has no time dimension, so make it. (why??)
        return ds.expand_dims({"time": [datetime(start, 1, 1)]}, axis=0)
    return ds.sel(time=slice(f"{start}-01-01", f"{end}-12-31"))


def extract_area(ds, bbox):
    """Extract bounding box from xarray dataset."""
    return ds.sel(
        longitude=slice(bbox[0], bbox[2]),
        latitude=slice(bbox[1], bbox[3]),
    )

def extract_single_point(ds, point, method='nearest'):
    """Extract single point from xarray dataset."""
    return ds.sel(longitude=point[0], latitude=point[1], method="method")

