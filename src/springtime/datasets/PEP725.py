import subprocess
from typing import Iterable, Literal, Optional
from bs4 import BeautifulSoup
from pydantic import BaseModel
import requests
import geopandas
from rpy2.robjects.packages import importr
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri

from springtime.datasets.daymet import NamedArea

class PhenorPEP725(BaseModel):
    """Download and load data from https://pep725.eu .

    Uses phenor (https://bluegreen-labs.github.io/phenor/) as client.

    Example:

        ```python
        from springtime.datasets.PEP725 import PhenorPEP725
        dataset = PhenorPEP725(species='Syringa vulgaris')
        dataset.download()
        df = dataset.load()

        # or with filters
        dataset = PhenorPEP725(species='Syringa vulgaris', years=[2000], area={'name':'some', 'bbox':(4, 45, 8, 50)})
        ```

    Requires phenor R package. Install with

    ```R
    devtools::install_github("bluegreen-labs/phenor@v1.3.1")
    ```
    """

    dataset: Literal["PEP725"] = "PEP725"
    species: str
    years: Iterable[int] = tuple()
    """Empty list means all years."""
    area: Optional[NamedArea]
    bbch: int = 60
    """60 === Beginning of flowering"""
    credential_file: str = "~/.config/pep725_credentials.txt"

    def download(self):
        """

        Requires `~/.config/pep725_credentials.txt` file with your PEP725 credentials.
        Email adress on first line, password on second line.

        """
        subprocess.run(["R", "--no-save"], input=self._r_download().encode())

    def load(self):
        phenor = importr("phenor")
        r_df = phenor.pr_merge_pep725(".")
        with ro.default_converter + pandas2ri.converter:
            df = ro.conversion.get_conversion().rpy2py(r_df)
        years_set = set(self.years)
        df['species'] = df['species'].astype("category")
        df['country'] = df['country'].astype("category")
        
        df = geopandas.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df.lon, df.lat))
        
        if self.years:
            df = df[(df['year'].isin(years_set))]

        df = df[(df['bbch'] == self.bbch)]
        if self.area is None:
            return df
        return df.cx[self.area.bbox[0]:self.area.bbox[2], self.area.bbox[1]:self.area.bbox[3]]

    def _r_download(self):
        return f"""\
        library(phenor)
        species_id <- phenor::check_pep725_species(species = "{self.species}")
        phenor::pr_dl_pep725(
            credentials = "~/.config/pep725_credentials.txt",
            species = species_id,
            path = ".",
            internal = FALSE
        )
        """

    # TODO get list of bbch and their descriptions

    # TODO get list of species identifiers and their scientific name