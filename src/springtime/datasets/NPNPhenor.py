# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of phenor

from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea


class NPNPhenor(Dataset):
    """Download and load data from NPN.

    Uses phenor (https://bluegreen-labs.github.io/phenor/) as client.

    Could use https://data.usanpn.org/observations/get-started to figure out
    which species/phenophases combis are available.

    Attributes:
        dataset: lorem ipsum
        species: lorem ipsum
        phenophase: lorem ipsum
        area: lorem ipsum

    Example:

        ```python
        from springtime.datasets.NPNPhenor import (
            NPNPhenor,
            npn_species,
            npn_phenophases
        )

        # List IDs and names for available species, phenophases
        species = npn_species()
        phenophases = npn_phenophases()

        # Load dataset
        dataset = NPNPhenor(species=36, phenophase=483, years=[2010, 2011])
        dataset.download()
        gdf = dataset.load()

        # or with area bounds
        dataset = NPNPhenor(
            species = 3,
            phenophase = 371,
            years = [2010, 2011],
            area = {'name':'some', 'bbox':(4, 45, 8, 50)}
        )
        ```

    Requires phenor R package. Install with

    ```R
    devtools::install_github("bluegreen-labs/phenor@v1.3.1")
    ```

    """
    dataset: Literal["NPNPhenor"] = "NPNPhenor"
    species: int
    phenophase: int
    area: Optional[NamedArea] = None

    @property
    def directory(self):
        """Return the directory where data is stored."""
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

        for year in self.years.range:
            filename = self._filename(year)

            if filename.exists():
                print(f"{filename} already exists, skipping")
            else:
                print(f"downloading {filename}")
                self._r_download(year)

    def load(self):
        """Load the dataset into memory."""
        df = pd.concat([self._r_load(year) for year in self.years.range])
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
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
        readRDS = ro.r["readRDS"]
        data = readRDS(filename)
        df = ro.pandas2ri.rpy2py_dataframe(data)
        return df


def npn_species(species=ro.NULL, list=True):
    """List the available species.

    Examples:

        # >>> pd.set_option('display.width', None)
        # >>> pd.set_option("display.max_columns", 7)
        # >>> pd.set_option("display.max_colwidth", 20)
        >>> df = npn_species()
        >>> df.head()
           species_id         common_name         genus  ...  family_id  family_name  family_common_name
        1         120        'ohi'a lehua  Metrosideros  ...        301    Myrtaceae       Myrtle Family
        2        1436          absinthium     Artemisia  ...        242   Asteraceae        Aster Family
        3        1227  Acadian flycatcher     Empidonax  ...        154   Tyrannidae  Tyrant Flycatchers
        4        1229    acorn woodpecker    Melanerpes  ...        158      Picidae         Woodpeckers
        5        2110        Adam and Eve     Aplectrum  ...        307  Orchidaceae       Orchid Family
        <BLANKLINE>
        [5 rows x 18 columns]
    """
    phenor = importr("phenor")
    r_df = phenor.check_npn_species(species=species, list=list)
    return ro.pandas2ri.rpy2py_dataframe(r_df)


def npn_phenophases(phenophase=ro.NULL, list=True):
    """List the available phenophases."""
    phenor = importr("phenor")
    r_df = phenor.check_npn_phenophases(phenophase=phenophase, list=list)
    return ro.pandas2ri.rpy2py_dataframe(r_df)
