from itertools import product
from typing import Literal, Sequence, Tuple
from pydantic import BaseModel
from urllib.request import urlretrieve
import logging

from xarray import open_mfdataset

from springtime.config import CONFIG

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
    dataset: Literal["E-OBS"] = "E-OBS"
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

    def _url(self, variable: Variable, period: str):
        # https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble/elev_ens_0.1deg_reg_v26.0e.nc
        # https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble/tg_ens_mean_0.1deg_reg_1950-1964_v26.0e.nc

        # https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble/tg_ens_mean_0.1deg_reg_1980-2010_v26.0e.nc

        # https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble/tg_ens_spread_0.1deg_reg_1950-1964_v26.0e.nc
        # https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_0.1deg_reg_ensemble/tg_ens_mean_0.1deg_reg_v26.0e.nc
        base = f"https://knmi-ecad-assets-prd.s3.amazonaws.com/ensembles/data/Grid_{self.grid_resolution}_reg_ensemble/"
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
        }
        short_var = short_vars[variable]
        if self.product_type == "ensemble_mean":
            return f"{short_var}_ens_mean_{self.grid_resolution}_reg_{period}_v{self.version}.nc"
        elif self.product_type == "ensemble_spread":
            return f""
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
                logger.warning(
                    f"Downloading E-OBS variable {variable} for {period} period from {url} to {path}"
                )
                urlretrieve(url, path)

    def load(self):
        paths = [self._path(variable, period) for variable, period in product(self.variables, self._periods)]
        ds = open_mfdataset(paths)
        return ds.sel(time=slice(f"{self.years[0]}-01-01", f"{self.years[1]}-12-31"))


class EOBSPoint(EOBS):
    point: Tuple[float, float]
    """Point as longitude, latitude in WGS84 projection."""

    def load():
        ds = super().load()
        # TODO spatial select
        return ds