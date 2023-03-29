# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import subprocess
from typing import Literal, Optional, Tuple

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from pydantic import BaseModel
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.utils import NamedArea, NamedIdentifiers, retry

request_source = "Springtime user https://github.com/springtime/springtime"
data_dir = CONFIG.data_dir / "rnpn"
species_file = data_dir / "species.csv"
phenophases_file = data_dir / "phenophases.csv"
stations_file = data_dir / "stations.csv"


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
        dataset = RNPN(
            species_ids=[36],  # Syringa vulgaris / common lilac
            phenophase_ids=[483],  # Leaves
            years=[2010, 2012],
        )
        dataset.download()
        gdf = dataset.load()

        # or with area bounds
        dataset = RNPN(
            years = [2010, 2011],
            area = {'name':'some', 'bbox':[-112, 30, -108, 35.0]}
        )
        ```

    Requires rnpn R package. Install with

    ```R
    install.packages("rnpn")
    ```

    """

    dataset: Literal["RNPN"] = "RNPN"
    years: Tuple[int, int]
    species_ids: Optional[NamedIdentifiers]
    phenophase_ids: Optional[NamedIdentifiers]
    area: Optional[NamedArea] = None

    def _filename(self, year):
        """Path where files will be downloaded to and loaded from.

        In rnpn you can only specify dir, filename is chosen for you. So we
        reproduce the filename that rnpn creates.
        """
        parts = [
            "rnpn_npn_data",
            "y",
            year,
        ]
        if self.species_ids is not None:
            parts.append(self.species_ids.name)
        if self.phenophase_ids is not None:
            parts.append(self.phenophase_ids.name)
        if self.area is not None:
            parts.append(self.area.name)
        rnpn_filename = "_".join([str(p) for p in parts]) + ".csv"
        return data_dir / rnpn_filename

    def load(self):
        """Load the dataset into memory."""
        df = pd.concat(
            [
                pd.read_csv(self._filename(year))
                for year in range(self.years[0], self.years[1] + 1)
            ]
        )
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf

    def download(self, timeout=30):
        """Download the data.

        Args:
            timeout: time in seconds to wait for a response of the npn server.

        Raises:
            TimeoutError: If requests still fails after 3 attempts.

        """
        data_dir.mkdir(parents=True, exist_ok=True)

        for year in range(self.years[0], self.years[1] + 1):
            filename = self._filename(year)

            if filename.exists() and not CONFIG.force_override:
                print(f"{filename} already exists, skipping")
            else:
                print(f"downloading {filename}")
                retry(timeout=timeout)(self._download_year)(filename, year)

    def _download_year(self, filename: Path, year):
        subprocess.run(
            ["R", "--vanilla", "--no-echo"],
            input=self._r_download(filename, year).encode(),
            stderr=subprocess.PIPE,
        )

    def _r_download(self, filename: Path, year):
        opt_args = []
        if self.area is not None:
            coords = [str(self.area.bbox[i]) for i in [1, 0, 3, 2]]
            opt_args.append(f"coords = c({','.join(coords)})")
        if self.species_ids is not None:
            species_ids = map(str, self.species_ids.items)
            opt_args.append(f"species_id = c({','.join(species_ids)})")
        if self.phenophase_ids is not None:
            phenophase_ids = map(str, self.phenophase_ids.items)
            opt_args.append(f"phenophase_id = c({','.join(phenophase_ids)})")

        return f"""\
            library(rnpn)
            npn_download_individual_phenometrics(
                request_source = "{request_source}",
                years = c({year}),
                download_path = "{str(filename)}",
                {','.join(opt_args)}
            )
            """


@retry()
def npn_species():
    """Get available species on npn.

    Returns:
        Pandas dataframe with species_id and other species related fields.
    """
    if not species_file.exists() or CONFIG.force_override:
        data_dir.mkdir(parents=True, exist_ok=True)
        rnpn = importr("rnpn")
        r_subset = ro.r["subset"]
        r_as = ro.r["as.data.frame"]
        # species_type column has nested df, which can not be converted, so drop it
        nested_column_index = -19
        r_df = r_as(r_subset(rnpn.npn_species(), select=nested_column_index))
        df = ro.pandas2ri.rpy2py_dataframe(r_df)
        df.to_csv(species_file, index=False)
    return pd.read_csv(species_file)


@retry()
def npn_phenophases():
    """Get available phenophases on npn.

    Returns:
        Pandas dataframe with phenophase_id and other phenophase related fields.
    """
    if not phenophases_file.exists() or CONFIG.force_override:
        data_dir.mkdir(parents=True, exist_ok=True)
        rnpn = importr("rnpn")
        r_df = rnpn.npn_phenophases()
        df = ro.pandas2ri.rpy2py_dataframe(r_df)
        df.to_csv(phenophases_file, index=False)
    return pd.read_csv(phenophases_file)


@retry()
def npn_stations():
    """Get available stations on npn.

    Returns:
        Pandas dataframe with station_id and other station related fields.
    """
    if not stations_file.exists() or CONFIG.force_override:
        data_dir.mkdir(parents=True, exist_ok=True)
        rnpn = importr("rnpn")
        r_df = rnpn.npn_stations()
        df = ro.pandas2ri.rpy2py_dataframe(r_df)
        df.to_csv(stations_file, index=False)
    else:
        df = pd.read_csv(stations_file)
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
