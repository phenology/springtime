# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-only
# due to import of phenocamr
"""
This module contains functionality to download and load data from phenocam
observations
(<https://phenocam.nau.edu/webcam/>) using
[phenocamr](https://cran.r-project.org/web/packages/phenocamr/index.html) as
client.

Requires phenocamr R package.
Install with

```R
install.packages("phenocamr")
```


Example:

    ```python
    from springtime.datasets import PhenocamrSite

    dataset = PhenocamrSite(
        site="harvard$",
        years=(2019, 2020),
    )
    dataset.download()
    df = dataset.load()
    ```

Example:

    ```python
    from springtime.datasets import PhenocamrBoundingBox

    dataset = PhenocamrBoundingBox(
        area={
            "name": "harvard",
            "bbox": [-73, 42, -72, 43],
        },
        years=(2019, 2020),
    )
    dataset.download()
    df = dataset.load()
    ```

"""
import logging
from pathlib import Path
from typing import List, Literal, Optional, Sequence

import geopandas
import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea

logger = logging.getLogger(__file__)

phenocam_cache_dir = CONFIG.cache_dir / "phenocam"

# variables with "flag" in their names are removed from the list below because
# their values might be NaN.
PhenocamVariables = Literal[
    "midday_r",
    "midday_g",
    "midday_b",
    "midday_gcc",
    "midday_rcc",
    "r_mean",
    "r_std",
    "g_mean",
    "g_std",
    "b_mean",
    "b_std",
    "gcc_mean",
    "gcc_std",
    "gcc_50",
    "gcc_75",
    "gcc_90",
    "rcc_mean",
    "rcc_std",
    "rcc_50",
    "rcc_75",
    "rcc_90",
    "max_solar_elev",
    "smooth_gcc_mean",
    "smooth_gcc_50",
    "smooth_gcc_75",
    "smooth_gcc_90",
    "smooth_rcc_mean",
    "smooth_rcc_50",
    "smooth_rcc_75",
    "smooth_rcc_90",
    "smooth_ci_gcc_mean",
    "smooth_ci_gcc_50",
    "smooth_ci_gcc_75",
    "smooth_ci_gcc_90",
    "smooth_ci_rcc_mean",
    "smooth_ci_rcc_50",
    "smooth_ci_rcc_75",
    "smooth_ci_rcc_90",
]
"""Variables available in phenocamr."""


class Phenocam(Dataset):
    """Download and load data from phenocam

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.
        veg_type: Vegetation type (DB, EN). Default is "all".
        frequency: Frequency of the time series product.
        variables: Variables you want to download. When empty will download all
            the variables.

    """

    veg_type: Optional[str]
    frequency: Literal["1", "3", "roistats"] = "3"
    variables: Sequence[PhenocamVariables] = tuple()

    def _location(self, row: pd.Series) -> Path:
        freq = "roistats"
        if self.frequency != "roistats":
            freq = f"{self.frequency}day"
        return (
            phenocam_cache_dir
            / f"{row.site}_{row.veg_type}_{row.roi_id_number:04}_{freq}.csv"
        )

    def _exists_locally(self, locations) -> bool:
        return all([location.exists() for location in locations])

    def _to_geopandas(self, df):
        sites = list_sites()
        site_locations = sites[["site", "geometry"]]
        df = df.merge(site_locations, on="site")
        df = geopandas.GeoDataFrame(df, geometry=df.geometry)
        df.rename(columns={"date": "datetime"}, inplace=True)
        # Do not return variables that are not derived from image data
        # to get raw file see phenocam_cache_dir directory.
        non_derived_variables = {
            # Columns from https://phenocam.nau.edu/data/archive/harvard/ROI/harvard_DB_1000_3day.csv
            "date",
            "year",
            "doy",
            "image_count",
            "midday_filename",
            # Columns added by phenocamr
            "site",
            # Columns added by us
            "datetime",
            "geometry",
        }
        variables = [var for var in df.columns if var not in non_derived_variables]
        if self.variables:
            # Drop columns that are not in self.variables
            variables = list(self.variables)
        return df[["datetime", "geometry"] + variables]


class PhenocamrSite(Phenocam):
    """PhenoCam time series for site.

    Attributes:
        veg_type: Vegetation type (DB, EN). Default is "all".
        frequency: Frequency of the time series product.
        variables: Variables you want to download. When empty will download all
            the variables.
        site: Name of site. Append `$` to get exact match.
        rois: The id of the ROI to download. Default is all ROIs at site.
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.

    """

    dataset: Literal["phenocam"] = "phenocam"
    site: str
    rois: Optional[List[int]]

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or CONFIG.force_override
        is TRUE.
        """
        phenocam_cache_dir.mkdir(parents=True, exist_ok=True)
        optional_args = dict()
        if self.veg_type is not None:
            optional_args["veg_type"] = self.veg_type
        if self.rois is not None:
            optional_args["roi_id"] = self.rois
        phenocamr = importr("phenocamr")
        if self._exists_locally(self._locations()) or CONFIG.force_override:
            logger.info(f"Phenocam files already downloaded {self._locations()}")
        else:
            phenocamr.download_phenocam(
                site=self.site,
                frequency=self.frequency,
                internal=False,
                out_dir=str(phenocam_cache_dir),
                **optional_args,
            )

    def _locations(self) -> List[Path]:
        rois_df = list_rois()
        rois_df = rois_df.loc[rois_df.site.str.contains(self.site)]
        if self.veg_type is not None:
            rois_df = rois_df.loc[rois_df.veg_type == self.veg_type]
        if self.rois is not None:
            rois_df = rois_df.loc[rois_df.roi_id_number.isin(self.rois)]

        return rois_df.apply(self._location, axis="columns")

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        df = pd.concat([_load_location(location) for location in self._locations()])
        df = df.loc[(self.years.start <= df.year) & (df.year <= self.years.end)]
        return self._to_geopandas(df)


class PhenocamrBoundingBox(Phenocam):
    """PhenoCam time series for sites in a bounding box.

    Attributes:
        years: timerange. For example years=[2000, 2002] downloads data for three years.
        resample: Resample the dataset to a different time resolution. If None,
            no resampling.
        veg_type: Vegetation type (DB, EN). Default is "all".
        frequency: Frequency of the time series product.
        variables: Variables you want to download. When empty will download all
            the variables.
        area: A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.

    """

    dataset: Literal["phenocambbox"] = "phenocambbox"
    area: NamedArea

    def _selection(self):
        rois_df = list_rois()
        if self.veg_type is not None:
            rois_df = rois_df.loc[rois_df.veg_type == self.veg_type]
        box = self.area.bbox
        rois_df = rois_df.cx[
            # xmin:xmax, ymin:ymax
            box[0] : box[2],
            box[1] : box[3],
        ]
        return rois_df

    def _locations(self):
        return self._selection().apply(self._location, axis="columns")

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or CONFIG.force_override
        is TRUE.
        """
        if self._exists_locally(self._locations()) or CONFIG.force_override:
            logger.info(f"Phenocam files already downloaded {self._locations()}")
        for site in self._selection().site.unique():
            fetcher = PhenocamrSite(
                site=f"{site}$",
                veg_type=self.veg_type,
                frequency=self.frequency,
                years=self.years,
            )
            fetcher.download()

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        df = pd.concat([_load_location(location) for location in self._locations()])
        df = df.loc[(self.years.start <= df.year) & (df.year <= self.years.end)]
        return self._to_geopandas(df)


def _load_location(location: Path) -> pd.DataFrame:
    # TODO store header of csv in df.attr
    df = pd.read_csv(location, skiprows=24, parse_dates=["date"])
    (site, veg_type, roi_id_number, _freq) = location.stem.split("_")
    df.insert(0, "veg_type", veg_type)
    df.insert(0, "roi_id_number", roi_id_number)
    df.insert(0, "site", site)
    return df


def _rdf2pandasdf(r_df) -> pd.DataFrame:
    with localconverter(ro.default_converter + pandas2ri.converter):
        return ro.conversion.get_conversion().rpy2py(r_df)


sites_file = phenocam_cache_dir / "site_meta_data.csv"


def list_sites() -> geopandas.GeoDataFrame:
    """List of phenocam sites.

    Returns:
        Data frame containing phenocam sites
    """
    if not sites_file.exists():
        logger.warning(f"Downloading phenocam sites to {sites_file}")
        _download_sites()
    df = pd.read_csv(sites_file)
    return geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.pop("lon"), df.pop("lat"))
    )


def _download_sites():
    phenocamr = importr("phenocamr")
    r_sites = phenocamr.list_sites(internal=True)
    sites = _rdf2pandasdf(r_sites)
    sites_file.parent.mkdir(parents=True, exist_ok=True)
    sites.to_csv(sites_file, index=False)


rois_file = phenocam_cache_dir / "roi_data.csv"


def list_rois() -> geopandas.GeoDataFrame:
    """List of phenocam regions of interest (ROI).

    Returns:
        Data frame containing phenocam Regions of Interest.
    """
    if not rois_file.exists():
        logger.warning(f"Downloading phenocam rois to {rois_file}")
        _download_rois()
    df = pd.read_csv(rois_file)
    return geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.pop("lon"), df.pop("lat"))
    )


def _download_rois():
    phenocamr = importr("phenocamr")
    r_rois = phenocamr.list_rois(internal=True)
    rois = _rdf2pandasdf(r_rois)
    rois_file.parent.mkdir(parents=True, exist_ok=True)
    rois.to_csv(rois_file, index=False)
