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

import datetime
import tempfile
import subprocess
from rpy2.robjects.packages import importr
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri

from typing import Iterable, Tuple, Literal, Union

import pyproj
import xarray as xr

from springtime import CONFIG
from pydantic import validator, BaseModel


DaymetVariables = Literal["dayl", "prcp", "srad", "swe", "tmax", "tmin", "vp"]

from shapely.geometry import Polygon


class NamedArea(BaseModel):
    # TODO generalize
    # perhaps use https://github.com/developmentseed/geojson-pydantic
    name: str
    bbox: Tuple[float, float, float, float]

    @validator('bbox')
    def _parse_bbox(cls, values):
        xmin, ymin, xmax, ymax = values
        assert xmax > xmin, "xmax should be larger than xmin"
        assert ymax > ymin, "ymax should be larger than ymin"
        assert ymax < 90 and ymin > 0, "Latitudes should be in [0, 90]"
        assert xmin > -180 and xmax < 180, "Longitudes should be in [-180, 180]."
        return values

    @property
    def polygon(self):
        return Polygon.from_bounds(*self.bbox)


class DaymetThredds(BaseModel):
    """Daymet data for bounding box using thredds server."""

    dataset: Literal["daymet_thredds"] = "daymet_thredds"
    region: Literal["na", "hi", "pr"]
    area: NamedArea  # TODO also verify that region and namedArea consistent
    variables: Iterable[DaymetVariables]
    years: Iterable[int]

    @validator("years")
    def _valid_years(cls, years):
        assert years[0] > 1980, "No data before 1980"
        assert years[1] > years[0], "Start year must be smaller than end year"
        assert years[1] < datetime.now.year - 1, "Recent data not available"
        return years

    def _remote_url(self, region, variable, year):
        base_url = "https://thredds.daac.ornl.gov/thredds/dodsC/ornldaac/2129"
        return f"{base_url}/daymet_v4_daily_{region}_{variable}_{year}.nc"

    def _local_file(self, variable, year):
        base_url = CONFIG.datadir / f"daymet_v4_daily_{self.area.name}_{variable}_{year}.nc"

    # TODO: perhaps a single location property doesn't make sense after all
    @property
    def location(self) -> Path:
        """Show filename(s) that this dataset would have on disk."""
        # TODO one variable per file, or only allow all?
        return [_local_file(variable, year)
            for variable in self.variables
            for year in range(self.years[0], self.years[1])]

    def download(self):
        for variable in self.variables:
            for year in range(self.years[0], self.years[1]):
                if not self.local_file(variable, year).exists() or CONFIG.force_override:
                    remote_url = self._remote_url(self.region, variable, year)
                    remote_data = xr.open_dataset(
                        xr.backends.PydapDataStore.open(remote_url, timeout=500),
                        decode_coords="all"
                    )
                    data_array = _clip_dataset(self.area, remote_data)
                    data_array.to_netcdf(file)

    def load(self):
        # TODO: add pre-processing to convert to dataframe.
        return xr.merge([xr.open_dataset(file) for file in self.location], compat='override')


def _clip_dataset(
        area: NamedArea, data:xr.DataArray
        ) -> xr.DataArray:
    """Clip a data array using coordinates."""
    lon_range = [area.bbox[0], area.bbox[2]],
    lat_range = [area.bbox[1], area.bbox[3]]

    daymet_proj = pyproj.Proj(
        "+proj=lcc +lat_1=25 +lat_2=60 +lat_0=42.5 +lon_0=-100 "
        "+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
    )

    [x1, x2], [y1, y2] = daymet_proj(lon_range, lat_range)

    return data.sel(x=slice(x1, x2), y=slice(y2, y1))


class DaymetSinglePoint(BaseModel):
    """Daymet data for (multiple) single pixel using
    daymetr.
    Requires daymetr. Install with
    ```R
    devtools::install_github("bluegreen-labs/daymetr@v1.4")
    ```

    ."""

    dataset: Literal["daymet_thredds"] = "daymet_thredds"
    area: NamedArea  # TODO also verify that region and namedArea consistent
    coordinates: Union[tuple(float, float), Iterable[tuple(float, float)]]
    variables: Iterable[DaymetVariables]
    years: Iterable[int]

    def _concat_args(self):
        # TODO use self.area to create file names
        lines = [f"Variables:{','.join(self.variables)}\n",
        f"years:{', '.join(self.years)}\n"
        ]
        lines.extend(','.join(coords) for coords in self.coordinates)
        return lines

    # TODO fix it
    def download(self):
        subprocess.run(["R", "--no-save"], input=self._r_download().encode())
        def _r_download(self):
            return f"""\
            library(daymetr)
            species_id <- phenor::check_pep725_species(species = "{self.species}")
            daymetr::download_daymet(
                site = "Oak Ridge National Laboratories",
                lat = 36.0133,
                lon = -84.2625,
                start = 1980,
                end = 2010,
                internal = TRUE)
            """
