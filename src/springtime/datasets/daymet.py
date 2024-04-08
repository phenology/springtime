"""
Functionality to retrieve Daymet Daily Surface Weather Data for North
America, Version 4 R1.

Source: <https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=2129>

Fetches data from https://daymet.ornl.gov/.

Requires daymetr. Install with
```R
install.packages("daymetr")
```

* Temporal coverage: 1950-01-01 to 2021-12-31
* Spatial coverage (1 km grid):

|                    | North | South | East    | West   |
| -------------------|-------|-------|---------|--------|
| Hawaii (hi)        | 23.52 | 17.95 | -154.77 | -160.31|
| North America (na) | 82.91 | 14.07 | -53.06  | -178.13|
| Puearto Rico (pr)  | 19.94 | 16.84 | -64.12  | -67.99 |

Example: Example: Download data for 3 points

    ```pycon
    from springtime.datasets import Daymet

    source = Daymet(
        points=[
            [-84.2625, 36.0133],
            [-86, 39.6],
            [-85, 40],
        ],
        years=[2000,2002]
    )
    source.download()
    df = source.load()
    ```

Example: Example: Download daily data

    ```pycon
    from springtime.datasets import Daymet

    source = Daymet(
        variables = ["tmin", "tmax"],
        area = {
            "name": "indianapolis",
            "bbox": [-86.5, 39.5, -86, 40.1]
            },
        years=[2000, 2002]
    )
    source.download()
    df = source.load()
    ```

Example: Example: download monthly data

    ```pycon
    from springtime.datasets import Daymet

    source = Daymet(
        variables = ["tmin", "tmax"],
        area = {
            "name": "indianapolis",
            "bbox": [-86.5, 39.5, -86, 40.1]
            },
        years=[2000, 2002],
        frequency = "monthly",
    )
    source.download()
    df = source.load()
    ```


"""

import logging
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Literal, Optional, Sequence, get_args

import geopandas as gpd
import pandas as pd
import xarray as xr
from pydantic import field_validator, model_validator

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import (
    NamedArea,
    Point,
    Points,
    ResampleConfig,
    YearRange,
    resample,
    run_r_script,
)

logger = logging.getLogger(__name__)

DaymetVariables = Literal["dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp"]
"""Daymet variables."""

DaymetFrequencies = Literal["daily", "monthly", "annual"]
"""Daymet frequencies"""


class Daymet(Dataset):
    """Base class for common Daymet attributes.

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        variables: List of variable you want to download. Options:

            * dayl: Duration of the daylight period (seconds/day)
            * prcp: Daily total precipitation (mm/day)
            * srad: Incident shortwave radiation flux density(W/m2)
            * swe:  Snow water equivalent (kg/m2)
            * tmax: Daily maximum 2-meter air temperature (°C)
            * tmin: Daily minimum 2-meter air temperature (°C)
            * vp:   Water vapor pressure (Pa)

            When empty will download all the previously mentioned climate
            variables.
        points: (Sequence of) point(s) as (longitude, latitude) in WGS84 projection.
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.
            If both area and points are defined, will first crop the area before
            extracting points, so points outside area will be lost. If points is
            None, will return the full dataset as xarray object; this cannot be
            joined with other datasets.
        mosaic: Daymet tile mosaic; required when using `area`. Defaults to “na”
            for North America. Use “pr” for Puerto Rico and “hi” for Hawaii.
        frequency: required when using `area`. Choose from "daily", "monthly",
            or "annual"
        resample: Resample the dataset to a different time resolution. If None,
            no resampling. Else, should be a dictonary of the form {frequency:
            'M', operator: 'mean', **other_options}. Currently supported
            operators are 'mean', 'min', 'max', 'sum', 'median'. For valid
            frequencies see [1]. Other options will be passed directly to
            xr.resample [2]


    """

    dataset: Literal["daymet"] = "daymet"
    years: YearRange
    points: Point | Points | None = None
    area: NamedArea | None = None
    variables: Sequence[DaymetVariables] = get_args(DaymetVariables)
    resample: Optional[ResampleConfig] = None
    mosaic: Literal["na", "hi", "pr"] = "na"
    frequency: DaymetFrequencies = "daily"

    # TODO validator for valid regions?
    # regions = {
    #     "hi": {"lon": [-160.31, -154.77], "lat": [17.95, 23.52]},
    #     "na": {"lon": [-178.13, -53.06], "lat": [14.07, 82.91]},
    #     "pr": {"lon": [-67.99, -64.12], "lat": [16.84, 19.94]},
    # }

    @model_validator(mode="before")
    def _expand_variables(cls, data):
        if data.get("variables") is None:
            if data.get("frequency", "daily") == "daily":
                v = ("dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp")
            else:
                v = ("prcp", "tmax", "tmin", "vp")
            data["variables"] = v

        return data

    @model_validator(mode="after")
    def check_points_or_area(self):
        if not self.points and not self.area:
            raise ValueError("Either points or area (or both) is required")

        return self

    @model_validator(mode="after")
    def check_frequency(self):
        freq = self.frequency
        if self.resample and not freq == "daily":
            raise ValueError("Can only resample on daily data.")

        if not self.area and not freq == "daily":
            raise ValueError(f"Frequency set to {freq} but point data is always daily.")

        return self

    @field_validator("years")
    @classmethod
    def _valid_years(cls, v):
        assert v.start >= 1980, f"Asked for year {v.start}, but no data before 1980"
        last_year = datetime.now().year - 1
        msg = f"Asked for year {v.end}, but no data till/after {last_year}"
        assert v.end < last_year, msg
        return v

    @property
    def _root_dir(self):
        return CONFIG.cache_dir / "daymet"

    @property
    def _box_dir(self):
        """Directory in which download_daymet_ncss writes nc file."""
        return self._root_dir / f"bbox_{self.area.name}_{self.frequency}"

    def _box_path(self, variable: str, year: int) -> Path:
        """Get path for netcdf subsets.

        Monthly and annual data are accumulated.
        Precipitation is totalled, others are averaged.

        Filenames are abbreviated as:

        monthly total -> monttl
        annual average --> annavg

        """
        frequency = self.frequency

        if frequency == "daily":
            path = f"{variable}_daily_{year}_ncss.nc"
        elif variable == "prcp":
            path = f"{variable}_{frequency[:3]}ttl_{year}_ncss.nc"
        else:
            path = f"{variable}_{frequency[:3]}avg_{year}_ncss.nc"

        return self._box_dir / path

    def _point_path(self, point: Point) -> Path:
        """Path to downloaded file for single point."""
        location_name = f"{point.x}_{point.y}"
        time_stamp = f"{self.years.start}_{self.years.end}"
        filename = f"daymet_{location_name}_{time_stamp}.csv"
        return self._root_dir / filename

    def download(self):
        """Download data for dataset."""
        if self.area is not None:
            return self.download_bbox()

        # Conditional type checking is tricky
        assert self.points, "self.area and self.points not set, this shouldn't happen."

        if isinstance(self.points, Point):
            return [self.download_point(self.points)]

        return [self.download_point(point) for point in self.points]

    def download_point(self, point: Point):
        """Download data for a single point."""
        path = self._point_path(point)
        path.parent.mkdir(exist_ok=True, parents=True)

        if path.exists() and not CONFIG.force_override:
            logger.info(f"Found {path}")
        else:
            logger.info(f"Downloading data to {self._point_path(point)}.")
            run_r_script(self._r_download_point(point))
            logger.info("Please notice the metadata at the top of the file.")
        return path

    def download_bbox(self):
        """Download data for area in bbox as netcdf subset."""
        dir = self._box_dir
        dir.mkdir(exist_ok=True, parents=True)

        paths = []
        for variable, year in product(self.variables, self.years.range):
            path = self._box_path(variable, year)

            if path.exists() and not CONFIG.force_override:
                logger.info(f"Found {path}")
            else:
                logger.info(f"Downloading variable {variable} for year {year}")
                # Download tests/recipes/daymet.yaml:daymet_bounding_box_all_variables
                # took more than 30s so upped timeout
                run_r_script(self._r_download_ncss(variable, year), timeout=120)

            paths.append(path)

        return paths

    def raw_load(self) -> xr.Dataset | gpd.GeoDataFrame:
        """Load raw data."""
        paths = self.download()

        if self.area is not None:
            return xr.open_mfdataset(paths)

        # Conditional type checking is tricky
        assert self.points, "self.area and self.points not set, this shouldn't happen."

        if isinstance(self.points, Point):
            return self.raw_load_point(self.points)

        df = pd.concat([self.raw_load_point(point) for point in self.points])
        return gpd.GeoDataFrame(df)

    def raw_load_point(self, point) -> gpd.GeoDataFrame:
        """Read csv file for a single daymet data point.

        Lat/lon is in csv headers. Add geometry column instead.
        """
        file = self._point_path(point)
        df = pd.read_csv(file, skiprows=6)

        # Add geometry since we want to batch read dataframes with different coords
        geometry = gpd.points_from_xy([point.x] * len(df), [point.y] * len(df))
        gdf = gpd.GeoDataFrame(df).set_geometry(geometry)

        # type checker is iffy
        assert isinstance(gdf, gpd.GeoDataFrame), "Something unexpected happened"

        return gdf  # .set_index([ 'geometry', 'year', 'yday'])

    def load(self) -> gpd.GeoDataFrame:
        if self.area is None:
            # Load csv data into (geo)dataframe
            gdf = self.raw_load()

            # Remove unit from column names
            gdf.attrs["units"] = gdf.columns.values
            gdf.columns = [col.split(" (")[0] for col in gdf.columns]

            # Filter columns of interest
            columns = ["year", "yday", "geometry"] + list(self.variables)
            gdf = gdf[columns]

        else:
            # Load netcdf data into (geo)dataframe
            ds = self.raw_load()

            gdf = self._to_dataframe(ds)

            if self.points is not None:
                gdf = self._extract_points(gdf, self.points)

            gdf = self._split_time(gdf)

        # if isinstance(self.points, PointsFromOther):
        #     other = self.points._records
        #     gdf = join_dataframes([gdf, other]).reset_index()  # TODO dropna??

        if self.resample is not None:
            gdf = self._resample_yday(
                gdf, self.resample.frequency, self.resample.operator
            )

        # type checker is iffy
        assert isinstance(gdf, gpd.GeoDataFrame), "Something unexpected happened"

        # Pivot dataframe such that yday/month is on the columns
        if self.resample is not None:
            freq = self.resample.frequency
            gdf = gdf.set_index(["year", "geometry", freq]).unstack(freq)
        elif self.frequency == "daily":
            gdf = gdf.set_index(["year", "geometry", "yday"]).unstack("yday")
            gdf.columns = gdf.columns.map("{0[0]}|{0[1]}".format)
        elif self.frequency == "monthly":
            gdf = gdf.set_index(["year", "geometry", "month"]).unstack("month")
            gdf.columns = gdf.columns.map("{0[0]}|{0[1]}".format)

        # Return flat geodataframe & ensure geometry column recognized as such
        return gpd.GeoDataFrame(gdf).reset_index().set_geometry("geometry")

    def _split_time(self, gdf) -> gpd.GeoDataFrame:
        """Replace datetime with year (+ month/yday)"""
        gdf["year"] = gdf["time"].dt.year

        if self.frequency == "daily":
            gdf["yday"] = gdf["time"].dt.dayofyear
        elif self.frequency == "monthly":
            gdf["month"] = gdf["time"].dt.month

        gdf = gdf.drop(columns="time")
        return gdf

    def _extract_points(self, gdf, points):
        """Find nearest points from dataframe.

        The same point is returned for all times.
        """
        subset = gpd.points_from_xy(*map(list, zip(*points)))
        subset_gdf = gpd.GeoDataFrame().set_geometry(subset)
        gdf = gpd.sjoin_nearest(subset_gdf, gdf.set_index(["time"])).rename(
            columns={"index_right": "time"}
        )

        return gdf

    def _to_dataframe(self, ds):
        """Convert ds to gdf, set geometry and drop trash."""
        df = ds.to_dataframe().reset_index()

        # Infer geometry
        geometry = gpd.points_from_xy(df.pop("lon"), df.pop("lat"))
        gdf = gpd.GeoDataFrame(df).set_geometry(geometry)
        gdf = gdf.drop(columns=["x", "y", "lambert_conformal_conic"])
        return gdf

    # TODO harmonize with eobs resampling and with utils.resample
    def _resample_yday(self, gdf, frequency, operator) -> gpd.GeoDataFrame:
        """Resample a dataframe that has year and yday columns."""

        gdf["datetime"] = pd.to_datetime(
            gdf["year"] * 1000 + gdf["yday"], format="%Y%j"
        )
        gdf = gdf.drop(columns=["year", "yday"])

        return resample(gdf, freq=frequency, operator=operator, column="datetime")

    def _r_download_point(self, point):
        """Download single point using daymetR."""
        return f"""\
        library(daymetr)
        daymetr::download_daymet(
            site = "daymet_{point.x}_{point.y}",
            lat = {point.y},
            lon = {point.x},
            start = {self.years.start},
            end =  {self.years.end},
            path="{self._root_dir}",
            internal = FALSE)
        """

    def _r_download_ncss(self, variable, year):
        """Download netcdf subset using daymetR."""
        # daymet wants bbox as top left / bottom right pair (lat,lon,lat,lon).
        # Aka north,west,south,east in WGS84 projection.
        # while self.area.bbox is xmin, ymin, xmax, ymax
        # so do some reshuffling 3,0,1,2
        assert self.area, "_r_download_ncss called but self.area is not set"
        box = self.area.bbox
        return f"""\
        library(daymetr)
        daymetr::download_daymet_ncss(
            location = c({box[3]},{box[0]},{box[1]},{box[2]}),
            start = {year},
            end =  {year},
            frequency = "{self.frequency}",
            param = "{variable}",
            mosaic = "{self.mosaic}",
            path = "{self._box_dir}")
        """
