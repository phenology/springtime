import subprocess
from typing import Iterable, Literal, Optional

import geopandas
import rpy2.robjects as ro
from pydantic import BaseModel
from rpy2.robjects import pandas2ri, NULL
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.daymet import NamedArea



class NPNPhenor(BaseModel):
    """Download and load data from https://pep725.eu .

    Uses phenor (https://bluegreen-labs.github.io/phenor/) as client.

    Example:

        ```python
        from springtime.datasets.NPNPhenor import NPNPhenor
        dataset = NPNPhenor(species=36, phenophase=371, years=[2010, 2011])
        dataset.download()
        df = dataset.load()  # TODO: implement

        # or with filters
        dataset = NPNPhenor(species=36, phenophase=371, years=[2010, 2011], area={'name':'some', 'bbox':(4, 45, 8, 50)})
        ```

    Requires phenor R package. Install with

    ```R
    devtools::install_github("bluegreen-labs/phenor@v1.3.1")
    ```

    in R, these queries work:

    ```R
    library(phenor)
    pr_dl_npn(species=3, phenophase=371, start="2010-01-01", end="2011-01-01", extent=NULL)  # red maple: breaking leaf buds
    pr_dl_npn(species=36, phenophase=483, start="2010-01-01", end="2011-01-01", extent=NULL)  # common lilac: leaves
    ```

    """
    dataset: Literal["NPNPhenor"] = "NPNPhenor"

    species: int
    phenophase: int
    years: tuple[int, int]
    area: Optional[NamedArea] = None

    @property
    def directory(self):
        return CONFIG.data_dir / "PEP725"

    def _filename(self, year):
        """Path where files will be downloaded to and loaded from."""
        return self.directory / f"{self.species}_{self.phenophase}_{year}.csv"

    def download(self):
        """Download the data."""
        self.directory.mkdir(parents=True, exist_ok=True)
        for year in range(*self.years):
            if not self._filename(year).exists():
                self._r_download(year)

    def load(self):
        """Load the dataset from disk into memory."""
        raise NotImplementedError("To do")

    def _r_download(self, year):
        """Download data using phenor's download function.

        This executes R code in python using the rpy2 package.
        """
        extent = NULL
        if self.area is not None:
            extent = self.area.bbox

        phenor = importr("phenor")
        phenor.pr_dl_npn(
            species=self.species,
            phenophase=self.phenophase,
            start=f"{self.years[0]}-01-01",
            end=f"{self.years[1]}-12-31",
            extent=extent,
            internal=False,
            path=self._filename(year),
        )


# dataset = NPNPhenor(species=36, phenophase=371, years=[2010, 2011])
# dataset.download()
