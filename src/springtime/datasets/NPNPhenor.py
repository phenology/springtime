from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from pydantic import BaseModel
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.daymet import NamedArea


class NPNPhenor(BaseModel):
    """Download and load data from NPN.

    Uses phenor (https://bluegreen-labs.github.io/phenor/) as client.

    Could use https://data.usanpn.org/observations/get-started to figure out
    which species/phenophases combis are available.

    Example:

        ```python
        from springtime.datasets.NPNPhenor import NPNPhenor
        dataset = NPNPhenor(species=36, phenophase=483, years=[2010, 2011])
        dataset.download()
        gdf = dataset.load()

        # or with area bounds
        dataset = NPNPhenor(species=3, phenophase=371, years=[2010, 2011], area={'name':'some', 'bbox':(4, 45, 8, 50)})
        ```

    Requires phenor R package. Install with

    ```R
    devtools::install_github("bluegreen-labs/phenor@v1.3.1")
    ```

    """
    dataset: Literal["NPNPhenor"] = "NPNPhenor"

    species: int
    phenophase: int
    years: tuple[int, int]
    area: Optional[NamedArea] = None

    @property
    def directory(self):
        return CONFIG.data_dir / "NPN"

    def _filename(self, year):
        """Path where files will be downloaded to and loaded from.

        In phenor you can only specify dir, filename is chosen for you. So we
        reproduce the filename that phenor creates.
        """
        phenor_filename = (
            f"phenor_npn_data_{self.species}_{self.phenophase}"
            f"_{year}-01-01_{year}-12-31.rds"
        )
        return self.directory / phenor_filename

    def download(self):
        """Download the data."""
        self.directory.mkdir(parents=True, exist_ok=True)

        for year in range(*self.years):
            filename = self._filename(year)

            if filename.exists():
                print(f"{filename} already exists, skipping")
            else:
                print(f"downloading {filename}")
                self._r_download(year)


    def load(self):
        """Load the dataset into memory."""
        df = pd.concat([self._r_load(year) for year in range(*self.years)])
        geometry = gpd.points_from_xy(df.pop('longitude'), df.pop('latitude'))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf

    def _r_download(self, year):
        """Download data using phenor's download function.

        This executes R code in python using the rpy2 package.
        """
        phenor = importr("phenor")

        extent = ro.NULL
        if self.area is not None:
            extent = list(self.area.bbox)

        phenor.pr_dl_npn(
            species=self.species,
            phenophase=self.phenophase,
            start=f"{year}-01-01",
            end=f"{year}-12-31",
            extent=extent,
            internal=False,
            path=str(self.directory),
        )

    def _r_load(self, year) -> pd.DataFrame:
        """Read data with r and return as (python) pandas dataframe."""
        filename = str(self._filename(year))
        readRDS = ro.r['readRDS']
        data = readRDS(filename)
        df = ro.pandas2ri.rpy2py_dataframe(data)
        return df


def list_species():
    return _r_list_species()

def list_phenophases():
    return _r_list_phenophases()

def _r_list_species():
    phenor = importr("phenor")
    phenor.check_npn_species(list=True)
    # TODO: forward log messages

def _r_list_phenophases():
    phenor = importr("phenor")
    phenor.check_npn_phenophases()
    # TODO: forward log messages