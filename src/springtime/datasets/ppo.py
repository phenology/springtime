# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: GPL-2.0-only
# due to import of rppo

import subprocess
from typing import Literal, Optional, Tuple

import geopandas as gpd
import rpy2.robjects as ro
from pydantic import BaseModel
from pydantic.types import PositiveInt

from springtime.config import CONFIG
from springtime.utils import NamedArea

class RPPO(BaseModel):
    """Data from the Plant Phenology Ontology data portal.

    Uses rppo (https://docs.ropensci.org/rppo/) to get data from
    http://plantphenology.org/


    Example:

    ```python
    dataset = RPPO(
        genus="Quercus Pinus",
        termID="obo:PPO_0002313",
        limit=10,
        years=[2019, 2020]
        # area=dict(name="somewhere", bbox=[-83, 27,-82, 28])
    )
    dataset.download()
    gdf = dataset.load()
    ```
    """

    dataset: Literal["rppo"] = "rppo"
    genus: str
    termID: str = "obo:PPO_0002313"
    """true leaves present == obo:PPO_0002313"""
    years: Optional[Tuple[int, int]]
    """For example (2000,2021)

    First tuple entry is the start year.
    Second tuple entry is the end year.
    """
    area: Optional[NamedArea]
    limit: PositiveInt = 100000
    """Maximum number of records to retreive"""
    timeLimit: PositiveInt = 60
    """Number of seconds to wait for the server to respond"""

    @property
    def path(self):
        fn = self._build_filename()
        return CONFIG.data_dir / "PPO" / fn

    def download(self):
        if self.path.exists():
            print("File already exists:", self.path)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(["R", "--no-save"], input=self._r_download().encode())

    def _r_download(self):
        genus = ", ".join([f'"{p}"' for p in self.genus.split(" ")])
        if self.area is None:
            box = ""
        else:
            abox = self.area.bbox
            box = f"bbox='{abox[1]},{abox[0]},{abox[3]},{abox[2]}',"
        if self.years is None:
            years = ""
        else:
            years = f"fromYear={self.years[0]}, toYear={self.years[1]},"
        return f"""\
        library(rppo)
        response <- ppo_data(genus = c({genus}), termID='{self.termID}',
            {box}
            {years}
            limit={self.limit}, timeLimit = {self.timeLimit})
        saveRDS(response, file="{self.path}")
        """

    def load(self):
        """Load data from disk."""
        readRDS = ro.r["readRDS"]
        rdata = readRDS(str(self.path))

        data = dict(zip(rdata.names, list(rdata)))
        df = ro.pandas2ri.rpy2py_dataframe(data["data"])
        df.attrs["readme"] = data["readme"][0]
        df.attrs["citation"] = data["citation"][0]

        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf

    def _build_filename(self):
        parts = [self.genus.replace(" ", "_"), self.termID]
        if self.years is not None:
            parts.append(f"{self.years[0]}-{self.years[1]}")
        else:
            parts.append("na")
        if self.area is not None:
            parts.append(self.area.name)
        else:
            parts.append("na")
        parts.append("rds")
        return ".".join(parts)
