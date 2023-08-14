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
    source = DaymetMultiplePoints(
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
from typing import Literal, Sequence, Tuple, Union
import logging
import geopandas
import pandas as pd
import xarray as xr
from pydantic import root_validator, validator

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import PointsFromOther
from springtime.utils import NamedArea, run_r_script

logger = logging.getLogger(__name__)

DaymetVariables = Literal["dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp"]
"""Daymet variables."""


class Daymet(Dataset):
    """Base class for common Daymet attributes.

    Attributes:
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
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.


    """

    variables: Sequence[DaymetVariables] = tuple()

    @validator("years")
    def _valid_years(cls, years):
        assert (
            years.start >= 1980
        ), f"Asked for year {years.start}, but no data before 1980"
        last_year = datetime.now().year - 1
        msg = f"Asked for year {years.end}, but no data till/after {last_year}"
        assert years.end < last_year, msg
        return years


class DaymetSinglePoint(Daymet):
    """Daymet data for single point.

    Attributes:
        variables: List of variable you want to download. See
            [Daymet][springtime.datasets.meteorology.daymet.Daymet]
        point: Point as longitude, latitude in WGS84 projection.
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.

    """

    dataset: Literal["daymet_single_point"] = "daymet_single_point"
    point: Tuple[float, float]

    @property
    def _path(self):
        """Path to downloaded file."""
        location_name = f"{self.point[0]}_{self.point[1]}"
        time_stamp = f"{self.years.start}_{self.years.end}"
        return (
            CONFIG.cache_dir / f"daymet_single_point_{location_name}_{time_stamp}.csv"
        )

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or CONFIG.force_override
        is TRUE.
        """
        if not self._path.exists() or CONFIG.force_override:
            run_r_script(self._r_download())

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        with open(self._path) as file:
            nr_of_metadata_lines = 7
            headers = [file.readline() for _ in range(nr_of_metadata_lines)]
            df = pd.read_csv(file)

        geometry = geopandas.points_from_xy(
            [self.point[0]] * len(df), [self.point[1]] * len(df)
        )
        gdf = geopandas.GeoDataFrame(df, geometry=geometry)
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

    def _r_download(self):
        logger.info(f"Downloading data to {self._path}")
        return f"""\
        library(daymetr)
        daymetr::download_daymet(
            site = "daymet_single_point_{self.point[0]}_{self.point[1]}",
            lat = {self.point[1]},
            lon = {self.point[0]},
            start = {self.years.start},
            end =  {self.years.end},
            path="{CONFIG.cache_dir}",
            internal = FALSE)
        """


class DaymetMultiplePoints(Daymet):
    """Daymet data for multiple points.

    Attributes:
        variables: List of variable you want to download. See
            [Daymet][springtime.datasets.meteorology.daymet.Daymet]
        points: List of points as [[longitude, latitude], ...], in WGS84
            projection
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.


    """

    dataset: Literal["daymet_multiple_points"] = "daymet_multiple_points"
    points: Union[Sequence[Tuple[float, float]], PointsFromOther]

    @property
    def _handlers(self):
        return [
            DaymetSinglePoint(years=self.years, point=point, variables=self.variables)
            for point in self.points
        ]

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or CONFIG.force_override
        is TRUE.
        """
        for handler in self._handlers:
            handler.download()

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        dataframes = []
        headers = {}
        for point, handler in zip(self.points, self._handlers):
            df = handler.load()
            geometry = geopandas.points_from_xy(
                [point[0]] * len(df), [point[1]] * len(df)
            )
            geo_df = geopandas.GeoDataFrame(df, geometry=geometry)
            dataframes.append(geo_df)
            headers[f"headers_{point[0]}_{point[1]}"] = df.attrs["headers"]
        all = pd.concat(dataframes)
        all.attrs = headers
        return all


class DaymetBoundingBox(Daymet):
    """Daymet data for a bounding box.

    Attributes:
        variables: List of variable you want to download. See
            [Daymet][springtime.datasets.meteorology.daymet.Daymet]
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`. Do not make
            bounding box too large as there is a 6Gb maximum download size.
        mosaic: Daymet tile mosaic. Defaults to “na” for North America. Use
            “pr” for Puerto Rico and “hi” for Hawaii.
        frequency: Choose from "daily", "monthly", or "annual"
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.

    """

    dataset: Literal["daymet_bounding_box"] = "daymet_bounding_box"
    area: NamedArea
    mosaic: Literal["na", "hi", "pr"] = "na"
    frequency: Literal["daily", "monthly", "annual"] = "daily"
    # TODO monthly saves as *daily*.nc

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or CONFIG.force_override
        is TRUE.
        """
        box_dir_exists = self._box_dir.exists()
        if not box_dir_exists or CONFIG.force_override or self._missing_files():
            self._box_dir.mkdir(exist_ok=True, parents=True)
            # Downloading tests/recipes/daymet.yaml:daymet_bounding_box_all_variables
            # took more than 30s so upped timeout
            run_r_script(self._r_download(), timeout=120)

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        files = list(self._box_dir.glob("*.nc"))
        df = xr.open_mfdataset(files).to_dataframe().reset_index()
        df.rename(columns={"time": "datetime"}, inplace=True)
        geometry = geopandas.points_from_xy(df.pop("lon"), df.pop("lat"))
        gdf = geopandas.GeoDataFrame(df, geometry=geometry)
        return gdf[["datetime", "geometry"] + list(self.variables)]

    @root_validator()
    def _expand_variables(cls, values):
        v = values["variables"]
        if len(v) == 0:
            if values.get("frequency", "daily") == "daily":
                v = ("dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp")
            else:
                v = ("prcp", "tmax", "tmin", "vp")
        values["variables"] = v
        return values

    def _r_download(self):
        param_list = ",".join([f"'{p}'" for p in self.variables])
        params = f"c({param_list})"
        # daymet wants bbox as top left / bottom right pair (lat,lon,lat,lon).
        # Aka north,west,south,east in WGS84 projection.
        # while self.area.bbox is xmin, ymin, xmax, ymax
        # so do some reshuffling 3,0,1,2
        box = self.area.bbox
        return f"""\
        library(daymetr)
        daymetr::download_daymet_ncss(
            location = c({box[3]},{box[0]},{box[1]},{box[2]}),
            start = {self.years.start},
            end =  {self.years.end},
            frequency = "{self.frequency}",
            param = {params},
            mosaic = "{self.mosaic}",
            path = "{self._box_dir}")
        """

    def _missing_files(self):
        n_years = self.years.end - self.years.start + 1
        n_files = len(list(self._box_dir.glob("*.nc")))
        # TODO make smarter as box_dir can have files
        # which are not part of this instance
        return n_files != n_years * len(self.variables)

    @property
    def _box_dir(self):
        """Directory in which download_daymet_ncss writes nc file.

        For each variable/year combination."""
        return (
            CONFIG.cache_dir / f"daymet_bounding_box_{self.area.name}_{self.frequency}"
        )


# regions = {
#     "hi": {"lon": [-160.31, -154.77], "lat": [17.95, 23.52]},
#     "na": {"lon": [-178.13, -53.06], "lat": [14.07, 82.91]},
#     "pr": {"lon": [-67.99, -64.12], "lat": [16.84, 19.94]},
# }
