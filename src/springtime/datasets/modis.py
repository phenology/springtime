"""Datasets for downloading MODIS land products subsets.

Fetches data from https://modis.ornl.gov/data/modis_webservice.html.
"""

import subprocess
from typing import Literal, Sequence, Tuple

import geopandas
import pandas as pd
import rpy2.robjects as ro
from pydantic import BaseModel, conset
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr

from springtime.config import CONFIG


class Extent(BaseModel):
    """Extent to sample."""

    horizontal: float = 0.0
    """Left right extent to sample in kilometers."""
    vertical: float = 0.0
    """Above below extent to sample in kilometers."""


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
    bands: conset(str, min_items=1)  # type: ignore
    """MODIS product bands"""
    extent: Extent = Extent()
    """By default a single pixel returned.
    Give custom extend to get more pixels around point.
    """

    @property
    def _paths(self):
        """Path to downloaded file."""
        location_name = f"{self.point[0]}_{self.point[1]}"
        time_stamp = f"{self.years[0]}-01-01{self.years[1]}-12-31"
        paths = []
        for band in self.bands:
            product_band = f"{self.product}_{band}"
            path = (
                CONFIG.data_dir
                / f"modis_{location_name}_{product_band}_{time_stamp}.csv"
            )
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
        # TODO make geopandas dataframe
        # TODO replace band+value column with columns named after band
        return pd.concat(dataframes)

    def _r_download(self):
        if len(self.bands) == 1:
            bands = f'"{list(self.bands)[0]}"'
        else:
            band_vector = ",".join([f'"{b}"' for b in self.bands])
            bands = f"c({band_vector})"
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


class ModisMultiplePoints(BaseModel):
    """MODIS land products subsets for multiple points using MODISTools.

    Fetches data from https://modis.ornl.gov/data/modis_webservice.html.

    Requires MODISTools. Install with
    ```R
    install.packages("MODISTools")
    ```
    """

    dataset: Literal["modis_multiple_points"] = "modis_multiple_points"
    points: Sequence[Tuple[float, float]]
    """Points as longitude, latitude in WGS84 projection."""
    years: Tuple[int, int]
    """years is passed as range.

    For example years=[2000, 2002] downloads data for three years. Use
    `modis_dates(product, lon, lat)` to get list of available dates.
    """
    product: str
    """a MODIS product. Use `modis_products()` to get list of available products."""
    bands: conset(str, min_items=1)  # type: ignore
    """MODIS product bands.

    Use `modis_bands(product)` to get list of available bands for a product.
    """
    # TODO when no bands are given return all bands of product

    @property
    def _handlers(self):
        return [
            ModisSinglePoint(
                point=point, years=self.years, product=self.product, bands=self.bands
            )
            for point in self.points
        ]

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        for handler in self._handlers:
            handler.download()

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        dataframes = [handler.load() for handler in self._handlers]
        df = pd.concat(dataframes)
        return geopandas.GeoDataFrame(
            df, geometry=geopandas.points_from_xy(df.longitude, df.latitude)
        )


def modis_products():
    """List all available products."""
    modistools = importr("MODISTools")
    rdata = modistools.mt_products()
    with ro.default_converter + pandas2ri.converter:
        return ro.conversion.get_conversion().rpy2py(rdata)


def modis_bands(product: str):
    """List all available bands for a given product."""
    modistools = importr("MODISTools")
    rdata = modistools.mt_bands(product)
    with ro.default_converter + pandas2ri.converter:
        return ro.conversion.get_conversion().rpy2py(rdata)


def modis_dates(product: str, lon: float, lat: float):
    """List all available dates (temporal coverage) for a given product and
    location.
    """
    modistools = importr("MODISTools")
    rdata = modistools.mt_dates(product, lat, lon)
    with ro.default_converter + pandas2ri.converter:
        return ro.conversion.get_conversion().rpy2py(rdata)
