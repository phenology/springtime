# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of daymetR

""""Daymet data from
Daily Surface Weather Data on a 1-km Grid for North America, Version 4 R1
https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=2129

temporal coverage: 1950-01-01 to 2021-12-31
spatial coverage:
    N:        S:       E:      W:
hi 23.52	17.95	-154.77	-160.31
na 82.91	14.07	-53.06	-178.13
pr 19.94	16.84	-64.12	-67.99

regions = {
    "hi": {"lon": [-160.31, -154.77], "lat": [17.95, 23.52]},
    "na": {"lon": [-178.13, -53.06], "lat": [14.07, 82.91]},
    "pr": {"lon": [-67.99, -64.12], "lat": [16.84, 19.94]},
}

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

from datetime import datetime
from typing import Literal, Sequence, Tuple

import geopandas as gpd
import pandas as pd
import xarray as xr
from pydantic import BaseModel, root_validator, validator
from shapely import Point

from springtime.config import CONFIG
from springtime.utils import NamedArea, run_r_script, transponse_df

DaymetVariables = Literal["dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp"]


class DaymetSinglePoint(BaseModel):
    """Daymet data for single point using daymetr.

    Fetches data from https://daymet.ornl.gov/.

    Requires daymetr. Install with
    ```R
    install.packages("daymetr")
    ```

    """

    dataset: Literal["daymet_single_point"] = "daymet_single_point"
    point: Tuple[float, float]
    """Point as longitude, latitude in WGS84 projection."""
    years: Tuple[int, int]
    """ years is passed as range for example years=[2000, 2002] downloads data
    for three years."""

    @property
    def _path(self):
        """Path to downloaded file."""
        location_name = f"{self.point[0]}_{self.point[1]}"
        time_stamp = f"{self.years[0]}_{self.years[1]}"
        return CONFIG.data_dir / f"daymet_single_point_{location_name}_{time_stamp}.csv"

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        if not self._path.exists() or CONFIG.force_override:
            run_r_script(self._r_download())

    def load(self):
        raw_df = self.load_raw()
        return transponse_df(raw_df, columns=['yday'])

    def load_raw(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        with open(self._path) as file:
            nr_of_metadata_lines = 7
            headers = [file.readline() for _ in range(nr_of_metadata_lines)]
            df = pd.read_csv(file)

        df.attrs["headers"] = "\n".join(headers)

        return gpd.GeoDataFrame(df, geometry=[Point(self.point)]*len(df))

    def _r_download(self):
        return f"""\
        library(daymetr)
        daymetr::download_daymet(
            site = "daymet_single_point_{self.point[0]}_{self.point[1]}",
            lat = {self.point[1]},
            lon = {self.point[0]},
            start = {self.years[0]},
            end =  {self.years[1]},
            path="{CONFIG.data_dir}",
            internal = FALSE)
        """


def resample(raw_df, frequency='month', operator='mean'):
    date = pd.to_datetime(raw_df.year.astype(str) + raw_df.yday.astype(str), format="%Y%j")
    if frequency == 'month':
        timegrouper = date.dt.month
    elif frequency == 'week':
        timegrouper = date.dt.week
    else:
        raise ValueError("Frequency {frequency} not supported. Choose `week` or `month`.")

    return raw_df.assign({frequency: timegrouper}).groupby(['year', 'geometry', timegrouper]).agg(operator).reset_index()


class DaymetMultiplePoints(BaseModel):
    """Daymet data for multiple points using daymetr.

    Fetches data from https://daymet.ornl.gov/.

    Requires daymetr. Install with
    ```R
    install.packages("daymetr")
    ```

    Example:

        To download data for 3 points::

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

    """

    dataset: Literal["daymet_multiple_points"] = "daymet_multiple_points"
    points: Sequence[Tuple[float, float]]
    """Points as longitude, latitude in WGS84 projection."""
    years: Tuple[int, int]
    """ years is passed as range for example years=[2000, 2002] downloads data
    for three years."""

    @property
    def _handlers(self):
        return [
            DaymetSinglePoint(years=self.years, point=point) for point in self.points
        ]

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        for handler in self._handlers:
            handler.download()

    def load_raw(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        dataframes = []
        headers = {}
        for point, handler in zip(self.points, self._handlers):
            df = handler.load()
            df["x"] = point[0]
            df["y"] = point[1]
            geo_df = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df.pop('x'), df.pop('y'))
            )
            dataframes.append(geo_df)
            headers[f"headers_{point[0]}_{point[1]}"] = df.attrs["headers"]
        all = pd.concat(dataframes)
        all.attrs = headers
        return all


class DaymetBoundingBox(BaseModel):
    """Daymet data for a bounding box using daymetr.

    Fetches data from https://daymet.ornl.gov/.

    Requires daymetr. Install with
    ```R
    install.packages("daymetr")
    ```

    Do not make bounding box too large as there is a 6Gb maximum download size.
    """

    dataset: Literal["daymet_bounding_box"] = "daymet_bounding_box"
    area: NamedArea
    years: Tuple[int, int]
    """ years is passed as range for example years=[2000, 2002] downloads data
    for three years."""
    mosaic: Literal["na", "hi", "pr"] = "na"
    """tile mosaic to use.

    Defaults to “na” for North America (use “pr” for Puerto Rico and “hi” for Hawaii).
    """
    variables: Sequence[DaymetVariables] = tuple()
    """climate variable you want to download vapour pressure (vp),
    minimum and maximum temperature (tmin,tmax), snow water equivalent (swe),
    solar radiation (srad), precipitation (prcp) , day length (dayl).
    When empty will download all the previously mentioned climate variables.
    """
    frequency: Literal["daily", "monthly", "annual"] = "daily"
    # TODO monthly saves as *daily*.nc

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        box_dir_exists = self._box_dir.exists()
        if not box_dir_exists or CONFIG.force_override or self._missing_files():
            self._box_dir.mkdir(exist_ok=True, parents=True)
            # Downloading tests/recipes/daymet.yaml:daymet_bounding_box_all_variables
            # took more than 30s so upped timeout
            run_r_script(self._r_download(), timeout=120)

    def load_raw(self):
        files = list(self._box_dir.glob("*.nc"))
        return xr.open_mfdataset(files)
        # TODO skip files not asked for by
        # self.years + self.variables + self.frequency combinations
        # TODO: add pre-processing to convert to dataframe.

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
            start = {self.years[0]},
            end =  {self.years[1]},
            param = {params},
            mosaic = "{self.mosaic}",
            path = "{self._box_dir}")
        """

    def _missing_files(self):
        n_years = self.years[1] - self.years[0] + 1
        n_files = len(list(self._box_dir.glob("*.nc")))
        # TODO make smarter as box_dir can have files
        # which are not part of this instance
        return n_files != n_years * len(self.variables)

    @property
    def _box_dir(self):
        """Directory in which download_daymet_ncss writes nc file.

        For each variable/year combination."""
        return (
            CONFIG.data_dir / f"daymet_bounding_box_{self.area.name}_{self.frequency}"
        )

    @validator("years")
    def _valid_years(cls, years):
        assert years[0] >= 1980, "No data before 1980"
        assert years[1] > years[0], "Start year must be smaller than end year"
        assert years[1] < datetime.now().year - 1, "Recent data not available"
        return years
