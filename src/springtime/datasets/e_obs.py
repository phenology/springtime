# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

import logging
from itertools import product
from typing import Literal, Sequence, Tuple
from urllib.request import urlretrieve

from pydantic import BaseModel
from xarray import open_mfdataset

from springtime.config import CONFIG
from springtime.utils import NamedArea

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


class EOBS(BaseModel):
    """E-OBS dataset.

    Fetches complete grid from
    https://surfobs.climate.copernicus.eu/dataaccess/access_eobs.php .
    """

    dataset: Literal[
        "E-OBS", "EOBSSinglePoint", "EOBSMultiplePoints", "EOBSBoundingBox"
    ] = "E-OBS"
    product_type: Literal[
        "ensemble_mean", "ensemble_spread", "elevation"
    ] = "ensemble_mean"
    variables: Sequence[Variable] = ("mean_temperature",)
    """Some variables are specific for a certain product type."""
    grid_resolution: Literal["0.25deg", "0.1deg"] = "0.1deg"
    years: Tuple[int, int]
    """years is passed as range for example years=[2000, 2002] downloads data
    for three years. Max is whatever the chosen version has."""
    version: Literal["26.0e"] = "26.0e"

    # TODO add root validator that use same valid combinations as on
    # https://cds.climate.copernicus.eu/cdsapp#!/dataset/insitu-gridded-observations-europe?tab=form

    def _url(self, variable: Variable, period: str):
        root = "https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/"
        base = f"{root}Grid_{self.grid_resolution}_reg_ensemble/"
        return base + self._filename(variable, period)

    @property
    def _periods(self):
        periods = [
            range(1950, 1964),
            range(1965, 1979),
            range(1980, 1994),
            range(1995, 2010),
            range(2011, 2022),  # version 26.0e has till end of 2022
        ]
        matched_periods = set()
        for period in periods:
            if self.years[0] in period or self.years[1] in period:
                matched_periods.add(f"{period.start}-{period.stop}")
        return matched_periods

    def _filename(self, variable: Variable, period: str):
        short_vars = {
            "maximum_temperature": "tx",
            "mean_temperature": "tg",
            "minimum_temperature": "tn",
            "precipitation_amount": "rr",
            "relative_humidity": "hu",
            "sea_level_pressure": "pp",
            "surface_shortwave_downwelling_radiation": "qq",
            "wind_speed": "fg",
            "land_surface_elevation": "",
        }
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
        return CONFIG.data_dir / "e-obs"

    def _path(self, variable: Variable, period: str):
        return self._root_dir / self._filename(variable, period)

    def download(self):
        self._root_dir.mkdir(exist_ok=True)
        for variable, period in product(self.variables, self._periods):
            url = self._url(variable, period)
            path = self._path(variable, period)
            if not path.exists() or CONFIG.force_override:
                msg = f"Downloading E-OBS variable {variable} "
                msg = msg + f"for {period} period from {url} to {path}"
                logger.warning(msg)
                urlretrieve(url, path)

    def load(self):
        paths = [
            self._path(variable, period)
            for variable, period in product(self.variables, self._periods)
        ]
        ds = open_mfdataset(paths)
        if self.product_type == "elevation":
            return ds.to_dataframe()
        return ds.sel(time=slice(f"{self.years[0]}-01-01", f"{self.years[1]}-12-31"))


class EOBSSinglePoint(EOBS):
    """E-OBS dataset for a single point.

    Fetches complete grid from
    https://surfobs.climate.copernicus.eu/dataaccess/access_eobs.php .

    Example:

    ```python
    from springtime.datasets.e_obs import EOBSPoint
    datasource = EOBSPoint(point=[5, 50],
                           product_type='ensemble_mean',
                           years=[2000,2002])
    datasource.download()
    ds = datasource.load()
    ```

    """

    dataset: Literal["EOBSSinglePoint"] = "EOBSSinglePoint"
    point: Tuple[float, float]
    """Point as longitude, latitude in WGS84 projection."""

    def load(self):
        ds = super().load()
        return ds.sel(
            longitude=self.point[0], latitude=self.point[1], method="nearest"
        ).to_dataframe()


class EOBSMultiplePoints(EOBS):
    """E-OBS dataset for a multiple points.

    Fetches complete grid from
    https://surfobs.climate.copernicus.eu/dataaccess/access_eobs.php .

    """

    dataset: Literal["EOBSMultiplePoints"] = "EOBSMultiplePoints"
    points: Sequence[Tuple[float, float]]
    """Points as longitude, latitude in WGS84 projection."""

    def load(self):
        ds = super().load()
        return ds.sel(
            longitude=[p[0] for p in self.points],
            latitude=[p[1] for p in self.points],
            method="nearest",
        ).to_dataframe()


class EOBSBoundingBox(EOBS):
    """E-OBS dataset for a multiple points.

    Fetches complete grid from
    https://surfobs.climate.copernicus.eu/dataaccess/access_eobs.php .

    Example:

        To load coarse mean temperature around amsterdam from 2002 till 2002::

            from springtime.datasets.e_obs import EOBSBoundingBox

            dataset = EOBSBoundingBox(
                years=[2000,2002],
                area={
                    'name': 'amsterdam',
                    'bbox': [4, 50, 5, 55]
                },
                grid_resolution='0.25deg'
            )
            dataset.download()
            ds = dataset.load()

    """

    dataset: Literal["EOBSBoundingBox"] = "EOBSBoundingBox"
    area: NamedArea

    def load(self):
        ds = super().load()
        box = self.area.bbox
        return ds.sel(
            longitude=slice(box[0], box[2]),
            latitude=slice(box[1], box[3]),
        )
