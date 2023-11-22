# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of phenor
"""
This module contains functionality to download PEP725 data using
[phenor](https://bluegreen-labs.github.io/phenor/) as client.

PEP725 is the Pan-European Phenology network (<http://www.pep725.eu/>).

Requires phenor R package. Install with

```R
devtools::install_github("bluegreen-labs/phenor@v1.3.1")
```

Requires `~/.config/pep725_credentials.txt` file with your PEP725 credentials.
Email adress on first line, password on second line.

Example:

    ```python
    from springtime.datasets.insitu.pep725 import PEP725Phenor
    dataset = PEP725Phenor(species='Syringa vulgaris', years=[2000, 2000])
    dataset.download()
    df = dataset.load()

    # or with filters
    dataset = PEP725Phenor(
        species='Syringa vulgaris',
        years=[2000, 2000],
        area={'name':'some', 'bbox':(4, 45, 8, 50)}
    )
    ```

"""

from pathlib import Path
from typing import List, Literal, Optional
import geopandas
import pandas as pd

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea, YearRange, run_r_script


class PEP725Phenor(Dataset):
    """Download and load data from PEP725.

    Attributes:
        species: Full species name, see <http://www.pep725.eu/pep725_gss.php>
            for options.
        credential_file: Path to PEP725 credentials file. Email adress on first
            line, password on second line.

    """
    # Generic settings
    dataset: Literal["PEP725Phenor"] = "PEP725Phenor"
    credential_file: Path = CONFIG.pep725_credentials_file

    # Download arguments
    species: str

    # Load arguments
    select_phenophase: int | None = 60
    extract_area: NamedArea | None = None
    extract_years: list[int] | None = [2000, 2002]  # TODO: use YearRange
    set_index: List[str] | None = ['year', 'geometry']
    include_cols: List[str] | Literal['all'] = ["day"]


    @property
    def _location(self):
        """Path where files will be downloaded to and loaded from."""
        species_file = CONFIG.cache_dir / "PEP725" / self.species
        return species_file.with_suffix('.csv')

    def _exists_locally(self) -> bool:
        """Tell if the data is already present on disk."""
        return self._location.exists()

    def download(self):
        """Download the data."""
        if self._location.exists():
            print("File already exists:", self._location)
        else:
            print("Downloading data: ", self._location)
            run_r_script(self._r_download())

    def load(self):
        """Load the dataset from disk into memory.

        select_phenophase: Phenological development stage according to BBCH scale. See
            <http://www.pep725.eu/pep725_phase.php> for options. Default is 60:
            'Beginning of flowering'.
        """
        if not self._location.exists():
            self.download()

        df = pd.read_csv(self._location)

        if self.select_phenophase:
            df = df[(df["bbch"] == self.select_phenophase)]
            df.rename(columns={'day': f'DOY {self.select_phenophase}'})

        if self.extract_years:
            years_set = set(range(*self.extract_years))
            df = df[df["year"].isin(years_set)]

        # Convert to geodataframe, lat/lon to geometry
        df = geopandas.GeoDataFrame(
                        df, geometry=geopandas.points_from_xy(df.pop("lon"), df.pop("lat"))
                    )

        if self.extract_area:
            bbox = self.extract_area.bbox
            df = df.cx[bbox[0] : bbox[2], bbox[1] : bbox[3]]

        if self.set_index:
            df.set_index(self.set_index, inplace=True)

        if self.include_cols != 'all':
            return df[self.include_cols]
        return df

    def _r_download(self):
        # Note: unpacking during download (internal=True) is more interoperable
        # but at the expense of disk space (844K vs 14M for syringa vulgaris).
        # TODO Recompress processed csv file?
        return f"""\
        library(phenor)
        species_id <- phenor::check_pep725_species(species = "{self.species}")
        tidy_pep_data <- phenor::pr_dl_pep725(
            credentials = "{self.credential_file}",
            species = species_id,
            internal = TRUE
        )
        write.csv(tidy_pep_data, "{self._location}", row.names=FALSE)
        """
