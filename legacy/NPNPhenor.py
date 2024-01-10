# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of phenor
"""
The NPNPhenor module contains functionality to download and load data from NPN,
using [phenor](https://bluegreen-labs.github.io/phenor/) as client.

Requires phenor R package. Install with

```R
devtools::install_github("bluegreen-labs/phenor@v1.3.1")
```

It can be tricky to figure out which combinations of species/phenophases are
available. This link may serve as a starting point:
<https://data.usanpn.org/observations/get-started>.

Example: Example: List IDs and names for available species

    ```pycon
    >>> from springtime.datasets.insitu.npn.NPNPhenor import npn_species
    >>> df = npn_species()
    >>> df.head()
       species_id         common_name  ...  family_name  family_common_name
    1         120        'ohi'a lehua  ...    Myrtaceae       Myrtle Family
    2        1436          absinthium  ...   Asteraceae        Aster Family
    3        1227  Acadian flycatcher  ...   Tyrannidae  Tyrant Flycatchers
    4        1229    acorn woodpecker  ...      Picidae         Woodpeckers
    5        2110        Adam and Eve  ...  Orchidaceae       Orchid Family
    <BLANKLINE>
    [5 rows x 18 columns]

    ```

Example: Example: List IDs and names for available phenophases

    ```pycon
    >>> from springtime.datasets.insitu.npn.NPNPhenor import npn_phenophases
    >>> npn_phenophases()  # prints a long list
        phenophase_id                  phenophase_name phenophase_category  color
    1              56                      First leaf               Leaves   <NA>
    2              57             75% leaf elongation               Leaves   <NA>
    3              58                    First flower              Flowers   <NA>
    4              59                      Last flower             Flowers   <NA>
    ...

    ```

Example: Example: Load dataset

    ```pycon
    >>> from springtime.datasets.insitu.npn.NPNPhenor import NPNPhenor
    >>> dataset = NPNPhenor(species=36, phenophase=483, years=[2010, 2011])
    >>> dataset.download()
    >>> gdf = dataset.load()
    >>> gdf.head()
       site_id  ...                    geometry
    1    17967  ...  POINT (-91.37602 38.38862)
    2    17994  ...  POINT (-79.97169 39.53892)
    3    17999  ...  POINT (-85.60993 39.79147)
    4    18032  ...  POINT (-76.62881 40.94780)
    5    18051  ...  POINT (-91.69318 41.29201)
    <BLANKLINE>
    [5 rows x 24 columns]

    ```

Example: Example: Or with area bounds:

    ```pycon
    >>> from springtime.datasets.insitu.npn.NPNPhenor import NPNPhenor
    >>> dataset = NPNPhenor(
    ...     species = 3,
    ...     phenophase = 371,
    ...     years = [2010, 2011],
    ...     area = {'name':'some', 'bbox':(4, 45, 8, 50)}
    ... )
    >>> dataset.download()
    >>> gdf = dataset.load()
    >>> gdf.head()
       site_id  ...                    geometry
    1        2  ...  POINT (-70.69133 43.08535)
    2      459  ...  POINT (-92.75200 36.52450)
    3      374  ...  POINT (-87.64120 38.05990)
    4      950  ...  POINT (-81.75751 30.17840)
    5     1068  ...  POINT (-75.15203 38.77611)
    <BLANKLINE>
    [5 rows x 24 columns]
    ```
"""
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea

import logging

logger = logging.getLogger(__name__)


class NPNPhenor(Dataset):
    """Download and load data from NPN.

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.
        species: npn species id
        phenophase: npn phenophase id
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.
    """

    dataset: Literal["NPNPhenor"] = "NPNPhenor"
    species: int
    phenophase: int
    area: Optional[NamedArea] = None

    @property
    def directory(self):
        """Return the directory where data is stored."""
        return CONFIG.cache_dir / "NPN"

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
                logger.info(f"{filename} already exists, skipping")
            else:
                logger.info(f"downloading {filename}")
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
    """List the available species."""
    phenor = importr("phenor")
    r_df = phenor.check_npn_species(species=species, list=list)
    return ro.pandas2ri.rpy2py_dataframe(r_df)


def npn_phenophases(phenophase=ro.NULL, list=True):
    """List the available phenophases."""
    phenor = importr("phenor")
    phenor.check_npn_phenophases(phenophase=phenophase, list=list)
