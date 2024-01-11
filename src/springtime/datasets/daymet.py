# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of daymetR

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
    from springtime.datasets.meteo.daymet import DaymetBoundingBox

    source = DaymetBoundingBox(
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
    from springtime.datasets.meteo.daymet import DaymetBoundingBox

    source = DaymetBoundingBox(
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

from datetime import datetime
from itertools import product
from typing import Literal, Sequence, Tuple, Union
import logging
import geopandas as gpd
import pandas as pd
import xarray as xr
from pydantic import model_validator, field_validator

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import Point, Points, PointsFromOther, YearRange
from springtime.utils import NamedArea, run_r_script

logger = logging.getLogger(__name__)

DaymetVariables = Literal["dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp"]
"""Daymet variables."""


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


    """
    dataset: Literal["daymet"] = "daymet"
    years: YearRange
    points: Point | Points | None = None
    area: NamedArea | None = None
    variables: Sequence[DaymetVariables] = tuple()
    # resample: str  # TODO implement
    mosaic: Literal["na", "hi", "pr"] = "na"
    frequency: Literal["daily", "monthly", "annual"] = "daily"
    # TODO monthly saves as *daily*.nc

    @model_validator(mode='before')
    def _expand_variables(cls, data):
        if data.get("variables") is None:
            if data.get("frequency", "daily") == "daily":
                v = ("dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp")
            else:
                v = ("prcp", "tmax", "tmin", "vp")
            data["variables"] = v

        return data

    @field_validator("years")
    @classmethod
    def _valid_years(cls, v):
        assert (
            v.start >= 1980
        ), f"Asked for year {v.start}, but no data before 1980"
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

    def _box_paths(self, variable, year):
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

    def _point_path(self, point):
        """Path to downloaded file for single point."""
        location_name = f"{point.x}_{point.y}"
        time_stamp = f"{self.years.start}_{self.years.end}"
        filename = f"daymet_single_point_{location_name}_{time_stamp}.csv"
        return self._root_dir / filename

    def download(self):
        """Download data for dataset."""
        if self.area is not None:
            return self.download_bbox()

        if isinstance(self.points, Point):
            return [self.download_point(self.points)]

        return [self.download_point(point) for point in self.points]

    def download_point(self, point):
        """Download data for a single point."""
        path = self._point_path(point)

        if path.exists() and not CONFIG.force_override:
            logger.info(f"Found {path}")
        else:
            logger.info(f"Downloading data to {self._point_path}")
            run_r_script(self._r_download_point(point))
        return path

    def download_bbox(self):
        """Download data for area in bbox as netcdf subset."""
        dir = self._box_dir
        dir.mkdir(exist_ok=True, parents=True)

        paths = []
        for variable, year in product(self.variables, self.years.range):
            path = self._box_paths(variable, year)

            if path.exists() and not CONFIG.force_override:
                logger.info("Found {path}")
            else:
                logger.info(f"Downloading variable {variable} for year {year}")
                # Downloading tests/recipes/daymet.yaml:daymet_bounding_box_all_variables
                # took more than 30s so upped timeout
                run_r_script(self._r_download_ncss(variable, year), timeout=120)

            paths.append(path)

        return paths

    def raw_load(self):
        paths = self.download()

        if self.area is not None:
            return self.load_bbox()

        return self.load_points()

    def load(self):
        df = self.raw_load()

        # Do stuff

        return df

    def load_points(self):
        if isinstance(self.points, Point):
            return self.load_point(self.points)

        dataframes = []
        headers = {}
        for point in self.points:
            gdf = self.load_point(point)
            dataframes.append(gdf)
            headers[f"headers_{point.x}_{point.y}"] = gdf.attrs["headers"]

        all = pd.concat(dataframes)
        all.attrs = headers
        return all

    def load_point(self, point):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        with open(self._point_path(point)) as file:
            nr_of_metadata_lines = 7
            headers = [file.readline() for _ in range(nr_of_metadata_lines)]
            df = pd.read_csv(file)

        geometry = gpd.points_from_xy(
            [point.x] * len(df), [point.y] * len(df)
        )
        gdf = gpd.GeoDataFrame(df, geometry=geometry)

        gdf.attrs["headers"] = "\n".join(headers)

        # Remove unit from column names
        gdf.attrs["units"] = gdf.columns.values
        gdf.columns = [col.split(" (")[0] for col in gdf.columns]

        # Convert year and yday to datetime
        gdf["datetime"] = pd.to_datetime(gdf["year"], format="%Y") + pd.to_timedelta(
            gdf["yday"] - 1, unit="D"
        )
        var_columns = list(gdf.columns[2:-2])
        if self.variables:
            # Drop columns that are not in self.variables
            var_columns = list(self.variables)
        return gdf[["datetime", "geometry"] + var_columns]

    def load_bbox(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        files = self.download()
        df = xr.open_mfdataset(files).to_dataframe().reset_index()
        df.rename(columns={"time": "datetime"}, inplace=True)
        geometry = gpd.points_from_xy(df.pop("lon"), df.pop("lat"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf[["datetime", "geometry"] + list(self.variables)]

    def _r_download_point(self, point):
        """Download single point using daymetR."""
        return f"""\
        library(daymetr)
        daymetr::download_daymet(
            site = "daymet_single_point_{point.x}_{point.y}",
            lat = {point.y},
            lon = {point.x},
            start = {self.years.start},
            end =  {self.years.end},
            path="{CONFIG.cache_dir}",
            internal = FALSE)
        """

    def _r_download_ncss(self, variable, year):
        """Download netcdf subset using daymetR."""
        # daymet wants bbox as top left / bottom right pair (lat,lon,lat,lon).
        # Aka north,west,south,east in WGS84 projection.
        # while self.area.bbox is xmin, ymin, xmax, ymax
        # so do some reshuffling 3,0,1,2
        box = self.area.bbox
        return f"""\
        library(daymetr)
        daymetr::download_daymet_ncss(
            location = c({box[3]},{box[0]},{box[1]},{box[2]}),
            start = {year},
            end =  {year},
            frequency = "{self.frequency}",
            param = {variable},
            mosaic = "{self.mosaic}",
            path = "{self._box_dir}")
        """

# regions = {
#     "hi": {"lon": [-160.31, -154.77], "lat": [17.95, 23.52]},
#     "na": {"lon": [-178.13, -53.06], "lat": [14.07, 82.91]},
#     "pr": {"lon": [-67.99, -64.12], "lat": [16.84, 19.94]},
# }
