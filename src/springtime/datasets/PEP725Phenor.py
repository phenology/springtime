# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of phenor

from typing import Literal, Optional

import geopandas
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea, run_r_script


class PEP725Phenor(Dataset):
    """Download and load data from https://pep725.eu .

    Uses phenor (https://bluegreen-labs.github.io/phenor/) as client.

    Example:

        ```python
        from springtime.datasets.PEP725Phenor import PEP725Phenor
        dataset = PEP725Phenor(species='Syringa vulgaris')
        dataset.download()
        df = dataset.load()

        # or with filters
        dataset = PEP725Phenor(
            species='Syringa vulgaris',
            years=[2000, 2000],
            area={'name':'some', 'bbox':(4, 45, 8, 50)}
        )
        ```

    Requires phenor R package. Install with

    ```R
    devtools::install_github("bluegreen-labs/phenor@v1.3.1")
    ```

    Requires `~/.config/pep725_credentials.txt` file with your PEP725 credentials.
    Email adress on first line, password on second line.
    """

    dataset: Literal["PEP725Phenor"] = "PEP725Phenor"
    species: str
    area: Optional[NamedArea]
    bbch: int = 60
    """60 === Beginning of flowering"""
    credential_file: str = "~/.config/pep725_credentials.txt"

    @property
    def location(self):
        """Path where files will be downloaded to and loaded from."""
        species_dir = CONFIG.data_dir / "PEP725" / self.species
        return species_dir

    def exists_locally(self) -> bool:
        """Tell if the data is already present on disk."""
        return self.location.exists()

    def download(self):
        """Download the data."""
        if self.location.exists():
            print("File already exists:", self.location)
        else:
            self.location.mkdir(parents=True)
            run_r_script(self._r_download())

    def load(self):
        """Load the dataset from disk into memory."""
        phenor = importr("phenor")
        r_df = phenor.pr_merge_pep725(str(self.location))
        with ro.default_converter + pandas2ri.converter:
            df = ro.conversion.get_conversion().rpy2py(r_df)
        years_set = set(self.years.range)
        df["species"] = df["species"].astype("category")
        df["country"] = df["country"].astype("category")

        df = geopandas.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df.lon, df.lat)
        )

        # Filter on years
        df = df[(df["year"].isin(years_set))]

        df = df[(df["bbch"] == self.bbch)]
        if self.area is None:
            return df
        return df.cx[
            self.area.bbox[0] : self.area.bbox[2], self.area.bbox[1] : self.area.bbox[3]
        ]

    def _r_download(self):
        return f"""\
        library(phenor)
        species_id <- phenor::check_pep725_species(species = "{self.species}")
        phenor::pr_dl_pep725(
            credentials = "~/.config/pep725_credentials.txt",
            species = species_id,
            path = "{self.location}",
            internal = FALSE
        )
        """

    # TODO get list of bbch and their descriptions
    # each tarball has PEP725_BBCH.csv file with bbch;description columns

    # TODO get list of species identifiers and their scientific name
