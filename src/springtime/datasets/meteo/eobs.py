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
import logging
from itertools import product
from typing import Literal, Sequence
from urllib.request import urlretrieve
from springtime.datasets.abstract import Dataset

import geopandas as gpd
import pandas as pd
import xarray as xr
from pydantic import field_validator
from xarray import open_mfdataset

from springtime.config import CONFIG
from springtime.utils import NamedArea, Point, Points

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
        minimize_cache: if True, only store the data for the selected years and
            optionally area. This saves disk space, but it requires
            re-downloading for new years/areas. Default is False, i.e. keep the full
            EOBS grid.
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
    resample: bool = False  # TODO implement

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

    def download(self):
        """Download the data if necessary and return the file paths."""
        logger.info("Locating data")
        self._root_dir.mkdir(exist_ok=True)

        paths = []
        for variable, period in product(self.variables, self._periods):
            path = self._path(variable, period)
            if path.exists() and not CONFIG.force_override:
                logger.info(f"Found {path}, skipping download")
            else:
                url = self._url(variable, period)
                msg = f"""Downloading E-OBS variable {variable} for {period} period
                    from {url} to {path}."""
                logger.info(msg)
                urlretrieve(url, path)
            paths.append(path)

        # TODO process minimize_cache

        return paths

    def raw_load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
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
        ds = self.raw_load()

        # Select time
        ds = extract_time(ds, self.years.start, self.years.end)

        # Select area
        if self.area is not None:
            ds = extract_area(ds, self.area.bbox)

        # Extract points/records
        if self.points is None:
            logger.warning(
                """No points selected; returning gridded data as
                           xarray object. Useful for prediction, but cannot be
                           used in a recipe."""
            )
            return ds

        if isinstance(self.points, Point):
            x = [self.points.x]
            y = [self.points.y]
            geometry = gpd.GeoSeries(gpd.points_from_xy(x, y), name="geometry")
            ds = extract_records(ds, geometry)
        elif isinstance(self.points, Sequence):
            x = [p.x for p in self.points]
            y = [p.y for p in self.points]
            geometry = gpd.GeoSeries(gpd.points_from_xy(x, y), name="geometry")
            ds = extract_records(ds, geometry)
        else:
            # only remaining option is PointsFromOther
            records = self.points._records
            ds = extract_records(ds, records)

        return self._to_dataframe(ds)

    def _to_dataframe(self, ds: xr.Dataset):
        """Transform to dataframe and process to final format."""
        df = ds.to_dataframe()
        df = df.set_index(["year", "geometry"], append=True).unstack("doy")
        df.columns = df.columns.map("{0[0]}|{0[1]}".format)
        df = df.reset_index("index", drop=True).reset_index()
        df = gpd.GeoDataFrame(df)
        return df

def extract_time(ds, start, end):
    """Extract time range from xarray dataset."""
    if "time" not in ds.dims:
        # elevation has no time dimension, so make it. (why??)
        return ds.expand_dims({"time": [datetime(start, 1, 1)]}, axis=0)
    return ds.sel(time=slice(f"{start}-01-01", f"{end}-12-31"))


def extract_area(ds, bbox):
    """Extract bounding box from xarray dataset."""
    return ds.sel(
        longitude=slice(bbox[0], bbox[2]),
        latitude=slice(bbox[1], bbox[3]),
    )


def split_time(ds):
    """Split datetime coordinate into year and dayofyear."""
    year = ds.time.dt.year.values
    doy = ds.time.dt.dayofyear.values
    split_time = pd.MultiIndex.from_arrays(
        [year, doy],
        names=["year", "doy"],
    )
    return ds.assign_coords(time=split_time).unstack("time")


def extract_points(ds, points: gpd.geoseries.GeoSeries, method="nearest"):
    """Extract list of points from gridded dataset."""
    x = xr.DataArray(points.unique().x, dims=["geometry"])
    y = xr.DataArray(points.unique().y, dims=["geometry"])
    geometry = xr.DataArray(points.unique(), dims=["geometry"])
    return (
        ds.sel(longitude=x, latitude=y, method=method)
        .drop(["latitude", "longitude"])
        .assign_coords(geometry=geometry)
    )


def extract_records(ds, records: gpd.geodataframe.GeoDataFrame):
    """Extract list of year/geometry records from gridded dataset."""
    x = records.geometry.x.to_xarray()
    y = records.geometry.y.to_xarray()
    year = records.year.to_xarray()
    geometry = records.geometry.to_xarray()
    # TODO ensure all years present before allowing 'nearest' on year
    return (
        ds.sel(longitude=x, latitude=y, year=year, method="nearest")
        .drop(["latitude", "longitude"])
        .assign_coords(year=year, geometry=geometry)
    )
