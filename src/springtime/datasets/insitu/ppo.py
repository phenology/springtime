# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: GPL-2.0-only
# due to import of rppo

from typing import Literal, Optional, Sequence

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from pydantic.types import PositiveInt

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea, run_r_script

# non-numerice variables are removed from the list below.
PPOVariables = Literal["dayOfYear"]


class RPPO(Dataset):
    """Data from the Plant Phenology Ontology data portal.

    Uses rppo (https://docs.ropensci.org/rppo/) to get data from
    http://plantphenology.org/

    Example:

        ```python
        from springtime.datasets.ppo import RPPO
        dataset = RPPO(
            genus="Quercus Pinus",
            termID="obo:PPO_0002313",
            limit=10,
            years=[2019, 2020]
            # area=dict(name="somewhere", bbox=[-83, 27,-82, 28])
        )
        dataset.download()
        df = dataset.load()
        df
        ```
    """

    dataset: Literal["rppo"] = "rppo"
    genus: str
    termID: str = "obo:PPO_0002313"
    """true leaves present == obo:PPO_0002313"""
    area: Optional[NamedArea]
    limit: PositiveInt = 100000
    """Maximum number of records to retreive"""
    timeLimit: PositiveInt = 60
    """Number of seconds to wait for the server to respond"""
    variables: Sequence[PPOVariables] = tuple()
    """variables you want to load. When empty will load all the
    variables.
    """

    @property
    def path(self):
        fn = self._build_filename()
        return CONFIG.cache_dir / "PPO" / fn

    def download(self):
        if self.path.exists():
            print("File already exists:", self.path)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            run_r_script(self._r_download(), timeout=300)

    def _r_download(self):
        genus = ", ".join([f'"{p}"' for p in self.genus.split(" ")])
        if self.area is None:
            box = ""
        else:
            abox = self.area.bbox
            box = f"bbox='{abox[1]},{abox[0]},{abox[3]},{abox[2]}',"
        years = f"fromYear={self.years.start}, toYear={self.years.end},"
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

        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        df = gpd.GeoDataFrame(df, geometry=geometry)
        df["datetime"] = pd.to_datetime(df["year"], format="%Y") + pd.to_timedelta(
            df["dayOfYear"] - 1, unit="D"
        )

        non_variables = {"geometry", "datetime", "year"}
        variables = [v for v in df.columns if v not in non_variables]
        if self.variables:
            variables = list(self.variables)
        df = df[["datetime", "geometry"] + variables]

        df.attrs["readme"] = data["readme"][0]
        df.attrs["citation"] = data["citation"][0]
        df.attrs["number_possible"] = data["number_possible"][0]
        return df

    def _build_filename(self):
        parts = [self.genus.replace(" ", "_"), self.termID]
        if self.years is not None:
            parts.append(f"{self.years.start}-{self.years.end}")
        else:
            parts.append("na")
        if self.area is not None:
            parts.append(self.area.name)
        else:
            parts.append("na")
        parts.append("rds")
        return ".".join(parts)