# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of rnpn

from typing import Literal, Optional, Tuple

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from pydantic import BaseModel
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.daymet import NamedArea


class RNPN(BaseModel):
    """Download and load data from NPN.

    Uses rnpn (https://rdrr.io/cran/rnpn/) as client.

    Could use https://data.usanpn.org/observations/get-started to figure out
    which species/phenophases combis are available.

    Example:

        ```python
        from springtime.datasets.rnpn import (
            RNPN,
            npn_species,
            npn_phenophases
        )

        # List IDs and names for available species, phenophases
        species = npn_species()
        phenophases = npn_phenophases()

        # Load dataset
        dataset = RNPN(species=36, phenophase=483, years=[2010, 2011])
        dataset.download()
        gdf = dataset.load()

        # or with area bounds
        dataset = RNPN(
            species = 3,
            phenophase = 371,
            years = [2010, 2011],
            area = {'name':'some', 'bbox':(4, 45, 8, 50)}
        )
        ```

    Requires rnpn R package. Install with

    ```R
    install.packages("rnpn")
    ```

    """

    dataset: Literal["RNPN"] = "RNPN"

    species: int
    phenophase: int
    years: Tuple[int, int]
    area: Optional[NamedArea] = None

    @property
    def directory(self):
        return CONFIG.data_dir / "NPN"

    def _filename(self, year):
        """Path where files will be downloaded to and loaded from.

        In rnpn you can only specify dir, filename is chosen for you. So we
        reproduce the filename that rnpn creates.
        """
        rnpn_filename = (
            f"rnpn_npn_data_{self.species}_{self.phenophase}"
            f"_{year}-01-01_{year}-12-31.rds"
        )
        return self.directory / rnpn_filename

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
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf

    def _r_download(self, year):
        """Download data using rnpn's download function.

        This executes R code in python using the rpy2 package.
        """
        rnpn = importr("rnpn")

        extent = ro.NULL
        if self.area is not None:
            extent = list(self.area.bbox)

        rnpn.pr_dl_npn(
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
        readRDS = ro.r["readRDS"]
        data = readRDS(filename)
        df = ro.pandas2ri.rpy2py_dataframe(data)
        return df


def npn_species():
    rnpn = importr("rnpn")
    r_subset = ro.r['subset']
    r_as = ro.r['as.data.frame']
    r_df = r_as(r_subset(rnpn.npn_species(), select='-species_type'))
    return ro.pandas2ri.rpy2py_dataframe(r_df)


def npn_phenophases(phenophase=ro.NULL, list=True):
    rnpn = importr("rnpn")
    r_df = rnpn.check_npn_phenophases(phenophase=phenophase, list=list)
    return ro.pandas2ri.rpy2py_dataframe(r_df)
