# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea, NamedIdentifiers, run_r_script

request_source = "Springtime user https://github.com/springtime/springtime"
data_dir = CONFIG.data_dir / "rnpn"
species_file = data_dir / "species.csv"
phenophases_file = data_dir / "phenophases.csv"
stations_file = data_dir / "stations.csv"


class RNPN(Dataset):
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
            # Syringa vulgaris / common lilac
            species_ids={'name': 'lilac', 'items': [36]},
            phenophase_ids={'name': 'Leaves', 'items': [483]},
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
    species_ids: Optional[NamedIdentifiers]
    phenophase_ids: NamedIdentifiers
    area: Optional[NamedArea] = None
    use_first: bool = True
    """When true uses first_yes columns as value, otherwise the last_yes columns."""
    aggregation_operator: Literal["min", "max", "mean", "median"] = "min"

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
                for year in self.years.range
                if self._filename(year).exists()
            ]
        )
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return _reformat(self, gdf)

    def download(self, timeout=30):
        """Download the data.

        Args:
            timeout: time in seconds to wait for a response of the npn server.

        Raises:
            TimeoutError: If requests still fails after 3 attempts.

        """
        data_dir.mkdir(parents=True, exist_ok=True)

        for year in self.years.range:
            filename = self._filename(year)

            if filename.exists() and not CONFIG.force_override:
                print(f"{filename} already exists, skipping")
            else:
                print(f"downloading {filename}")
                run_r_script(self._r_download(filename, year), timeout=timeout)

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


def npn_species():
    """Get available species on npn.

    Returns:
        Pandas dataframe with species_id and other species related fields.
    """
    if not species_file.exists() or CONFIG.force_override:
        data_dir.mkdir(parents=True, exist_ok=True)
        script = f"""\
            library(rnpn)
            # species_type column has nested df, which can not be converted, so drop it
            df = subset(npn_species(), select=-species_type)
            write.table(df, file="{species_file}", sep=",", 
                        eol="\n", row.names=FALSE, col.names=TRUE)
        """
        run_r_script(script)
    return pd.read_csv(species_file)


def npn_phenophases():
    """Get available phenophases on npn.

    Returns:
        Pandas dataframe with phenophase_id and other phenophase related fields.
    """
    if not phenophases_file.exists() or CONFIG.force_override:
        data_dir.mkdir(parents=True, exist_ok=True)
        script = f"""\
            library(rnpn)
            df = npn_phenophases()
            write.table(df, file="{phenophases_file}", sep=",", 
                        eol="\\n", row.names=FALSE, col.names=TRUE)
        """
        run_r_script(script)
    return pd.read_csv(phenophases_file)


def npn_stations():
    """Get available stations on npn.

    Returns:
        Pandas dataframe with station_id and other station related fields.
    """
    if not stations_file.exists() or CONFIG.force_override:
        data_dir.mkdir(parents=True, exist_ok=True)
        script = f"""\
            library(rnpn)
            df = npn_stations()
            write.table(df, file="{stations_file}", sep=",", 
                        eol="\n", row.names=FALSE, col.names=TRUE)
        """
        run_r_script(script)
    df = pd.read_csv(stations_file)
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))


def npn_species_ids_by_functional_type(functional_type):
    species = _lookup(npn_species(), "functional_type", functional_type)
    return NamedIdentifiers(name=functional_type, items=species.species_id.to_list())


def npn_phenophase_ids_by_name(phenophase_name):
    phenophases = _lookup(npn_phenophases(), "phenophase_name", phenophase_name)
    return NamedIdentifiers(
        name=phenophase_name, items=phenophases.phenophase_id.to_list()
    )


def _lookup(df, column, expression):
    """Return rows where column matches expression."""
    return df[df[column].str.lower().str.contains(expression.lower())]


def _reformat(self: RNPN, df):
    var_name = self.phenophase_ids.name + "_doy"
    if self.use_first:
        df["datetime"] = pd.to_datetime(
            {
                "year": df.first_yes_year,
                "month": df.first_yes_month,
                "day": df.first_yes_day,
            }
        )
        df.rename(columns={"first_yes_doy": var_name}, inplace=True)
    else:
        df["datetime"] = pd.to_datetime(
            {
                "year": df.last_yes_year,
                "month": df.last_yes_month,
                "day": df.last_yes_day,
            }
        )
        df.rename(columns={"last_yes_doy": var_name}, inplace=True)

    df = (
        df[["datetime", "geometry", var_name]]
        .groupby(by=[df.datetime.dt.year, "geometry"], as_index=False, sort=False)
        .aggregate(self.aggregation_operator)
    )
    return gpd.GeoDataFrame(df)
