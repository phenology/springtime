# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Dict, Literal, Optional, Sequence, Tuple

import geopandas as gpd
import pandas as pd
import rpy2.robjects as ro
from pydantic import BaseModel
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.utils import NamedArea, NamedIdentifiers

request_source = "Springtime user https://github.com/springtime/springtime"


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

    @property
    def directory(self):
        return CONFIG.data_dir / "rnpn"

    def _filename(self):
        """Path where files will be downloaded to and loaded from.

        In rnpn you can only specify dir, filename is chosen for you. So we
        reproduce the filename that rnpn creates.
        """
        parts = [
            "rnpn_npn_data",
            "y",
            self.years[0],
            self.years[1],
        ]
        if self.species_ids is not None:
            parts.append(self.species_ids.name)
        if self.phenophase_ids is not None:
            parts.append(self.phenophase_ids.name)
        if self.area is not None:
            parts.append(self.area.name)
        rnpn_filename = "_".join([str(p) for p in parts]) + ".csv"
        return self.directory / rnpn_filename

    def download(self):
        """Download the data."""
        self.directory.mkdir(parents=True, exist_ok=True)

        filename = self._filename()

        if filename.exists():
            print(f"{filename} already exists, skipping")
        else:
            print(f"downloading {filename}")
            self._r_download(filename)

    def load(self):
        """Load the dataset into memory."""
        filename = self._filename()
        df = pd.read_csv(filename)
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf

    def _r_download(self, filename: Path):
        """Download data using rnpn's download function.

        This executes R code in python using the rpy2 package.
        """

        opt_args: Dict[str, Sequence] = {}
        if self.area is not None:
            opt_args["coords"] = [str(self.area.bbox[i]) for i in [1, 0, 3, 2]]
        if self.species_ids is not None:
            opt_args["species_id"] = self.species_ids.items
        if self.phenophase_ids is not None:
            opt_args["phenophase_id"] = self.phenophase_ids.items

        rnpn = importr("rnpn")
        rnpn.npn_download_individual_phenometrics(
            request_source=request_source,
            years=list(range(self.years[0], self.years[1] + 1)),
            download_path=str(filename),
            **opt_args,
        )


def npn_species():
    """Get available species on npn.

    Returns:
        Pandas dataframe with species_id and other species related fields.
    """
    rnpn = importr("rnpn")
    r_subset = ro.r["subset"]
    r_as = ro.r["as.data.frame"]
    # species_type column has nested df, which can not be converted, so drop it
    nested_column_index = -19
    r_df = r_as(r_subset(rnpn.npn_species(), select=nested_column_index))
    return ro.pandas2ri.rpy2py_dataframe(r_df)


def npn_phenophases():
    """Get available phenophases on npn.

    Returns:
        Pandas dataframe with phenophase_id and other phenophase related fields.
    """
    rnpn = importr("rnpn")
    r_df = rnpn.npn_phenophases()
    return ro.pandas2ri.rpy2py_dataframe(r_df)
