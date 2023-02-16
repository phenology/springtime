"""

"""

import subprocess
from typing import Literal, Tuple
import pandas as pd
from pydantic import BaseModel, conset
from springtime.config import CONFIG

class Extent(BaseModel):
    horizontal: float = 0.0
    vertical: float = 0.0


class ModisSinglePoint(BaseModel):
    """MODIS land products subsets for single point using MODISTools.

    Fetches data from https://modis.ornl.gov/data/modis_webservice.html.

    Requires MODISTools. Install with
    ```R
    install.packages("MODISTools")
    ```
    """
    dataset: Literal["modis_single_point"] = "modis_single_point"
    point: Tuple[float, float]
    """Point as longitude, latitude in WGS84 projection."""
    years: Tuple[int, int]
    """years is passed as range for example years=[2000, 2002] downloads data
    for three years."""
    product: str
    """a MODIS product."""
    bands: conset(str, min_items=1)
    """MODIS product bands"""

    extent: Extent = Extent()

    @property
    def _paths(self):
        """Path to downloaded file."""
        location_name = f"{self.point[0]}_{self.point[1]}"
        time_stamp = f"{self.years[0]}-01-01{self.years[1]}-12-31"
        paths = []
        for band in self.bands:
            product_band = f'{self.product}_{band}'
            path = CONFIG.data_dir / f"modis_{location_name}_{product_band}_{time_stamp}.csv"
            paths.append(path)
        return paths

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        some_paths_missing = any(not p.exists() for p in self._paths)
        if some_paths_missing or CONFIG.force_override:
            subprocess.run(["R", "--no-save"], input=self._r_download().encode())

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        dataframes = [pd.read_csv(path) for path in self._paths]
        return pd.concat(dataframes)

    def _r_download(self):
        if len(self.bands) == 1:
            bands = f'"{list(self.bands)[0]}"'
        else:
            band_vector = ','.join([f'"{b}"' for b in self.bands])
            bands = f'c({band_vector})'
        return f"""\
            library(MODISTools)
            mt_subset(product = "{self.product}",
                lat = {self.point[1]},
                lon = {self.point[0]},
                band = {bands},
                start = "{self.years[0]}-01-01",
                end = "{self.years[1]}-12-31",
                km_lr = {self.extent.horizontal},
                km_ab = {self.extent.vertical},
                site_name = "modis_{self.point[0]}_{self.point[1]}",
                out_dir="{CONFIG.data_dir }",
                internal = FALSE,
                progress = FALSE)
        """
