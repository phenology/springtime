"""
This module contains functionality to download data from the Plant Phenology
Ontology data portal.

Uses [rppo](https://docs.ropensci.org/rppo/) to get data from
<http://plantphenology.org/>

Example:

    ```python
    from springtime.datasets.ppo import RPPO
    dataset = RPPO(
        genus="Quercus Pinus",
        termID="obo:PPO_0002313",
        limit=10,
        years=[2019, 2020],
        area=dict(name="somewhere", bbox=[-83, 27,-82, 28])
    )
    dataset.download()
    df = dataset.load()
    df
    ```
"""
import logging
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
from pydantic.types import PositiveInt

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea, run_r_script

logger = logging.getLogger(__name__)


class RPPO(Dataset):
    """Download and load PPO data.

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        genus: plant genus, e.g. "Quercus". See [PPO
            documentation](https://github.com/PlantPhenoOntology/ppo/blob/master/documentation/ppo.pdf).
        termID: plant development stage, e.g. "obo:PPO_0002313" means "true
            leaves present". See [PPO
            documentation](https://github.com/PlantPhenoOntology/ppo/blob/master/documentation/ppo.pdf).
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.
        limit: Maximum number of records to retreive
        timeLimit: Number of seconds to wait for the server to respond
        exclude_terms: termIDs to exclude from the results
        infer_event: whether to keep all (state-based) observations or infer the
            phenological event by setting it to "first_yes_day" or "last_yes_day".

    """

    # TODO years not required?
    # TODO rppo support more keywords, like scientific name. Also support those?
    dataset: Literal["rppo"] = "rppo"
    genus: str  # TODO support list of geni?
    termID: str = "obo:PPO_0002313"
    area: Optional[NamedArea] = None
    limit: Optional[PositiveInt] = None
    timeLimit: PositiveInt = 60
    exclude_terms: list[str] = []
    infer_event: None | Literal["first_yes_day", "last_yes_day"] = None

    def _path(self, suffix="csv"):
        fn = self._build_filename(suffix=suffix)
        return CONFIG.cache_dir / "PPO" / fn

    def _build_filename(self, suffix="csv"):
        parts = [self.genus, self.termID]

        if self.years is not None:
            parts.append(f"{self.years.start}-{self.years.end}")

        if self.area is not None:
            parts.append(self.area.name)

        if self.limit is not None:
            parts.append(f"0-{self.limit}")

        parts.append(suffix)
        return ".".join(parts)

    def download(self):
        """Download data."""
        logger.info(f"Downloading data to {self._path()}")

        self._path().parent.mkdir(parents=True, exist_ok=True)
        run_r_script(self._r_download(), timeout=300)
        logger.info(
            f"""Download successful. Please see the readme
                    and citation files in {self._path().parent}"""
        )

    def raw_load(self):
        logger.info("Locating data...")
        if self._path().exists():
            print(f"Found {self._path()}")
        else:
            self.download()

        df = pd.read_csv(self._path())
        return df

    def load(self):
        """Load data from disk."""
        df = self.raw_load()

        # Filter terms
        exclude_rows = df.termID.map(
            lambda x: any(term in x for term in self.exclude_terms)
        )
        df = df[~exclude_rows]

        # Transform to events?
        if self.infer_event == "first_yes_day":
            df = (
                df.groupby(["year", "latitude", "longitude"])["dayOfYear"]
                .agg("min")
                .reset_index()
            )
        elif self.infer_event == "last_yes_day":
            df = (
                df.groupby(["year", "latitude", "longitude"])["dayOfYear"]
                .agg("max")
                .reset_index()
            )
        else:
            df = df[["year", "latitude", "longitude", "dayOfYear"]]

        # Standardize
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)

        return gdf

    def _r_download(self):
        if self.area is None:
            box = ""
        else:
            abox = self.area.bbox
            box = f"bbox='{abox[1]},{abox[0]},{abox[3]},{abox[2]}',"

        years = f"fromYear={self.years.start}, toYear={self.years.end},"
        limit = f"limit={self.limit}L," if self.limit else ""

        return f"""\
        library(rppo)
        response <- ppo_data(
            genus = c('{self.genus}'),
            termID='{self.termID}',
            {box}
            {years}
            {limit}
            timeLimit = {self.timeLimit})

        write.csv(response$data, "{self._path()}", row.names=FALSE)
        write(response$readme, file="{self._path(suffix="readme")}")
        write(response$citation, file="{self._path(suffix="citation")}")
        """


def ppo_get_terms():
    """Call rppo:ppo_get_terms, save and load result."""
    filename = CONFIG.cache_dir / "PPO" / "ppo_terms.csv"

    script = f"""\
        library(rppo)
        terms = ppo_get_terms(present=TRUE, absent=TRUE)
        write.csv(terms, "{filename}", row.names=FALSE)
        """

    logger.info("Downloading terms")

    run_r_script(script, timeout=300)

    return pd.read_csv(filename)
