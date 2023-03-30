# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: GPL-2.0-only
# due to import of phenocamr

import logging
from pathlib import Path
from typing import List, Literal, Optional, Tuple
import pandas as pd
import geopandas
import rpy2.robjects as ro
from pydantic import BaseModel, PositiveInt
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr
from rpy2.robjects.conversion import localconverter

from springtime.config import CONFIG
from springtime.utils import NamedArea

logger = logging.getLogger(__file__)

phenocam_data_dir = CONFIG.data_dir / "phenocam"


class PhenocamrSite(BaseModel):
    """PhenoCam time series for site.

    Fetch data from https://phenocam.nau.edu/webcam/

    Requires phenocamr R package.
    Install with
    ```R
    install.packages("phenocamr")
    ```
    """

    dataset: Literal["phenocam"] = "phenocam"
    site: str
    """Name of site. Append `$` to get exact match."""
    veg_type: Optional[str]
    """Vegetation type (DB, EN). Default is all."""
    frequency: Literal["1", "3", "roistats"] = "3"
    """Frequency of the time series product."""
    rois: Optional[List[int]]
    """The id of the ROI to download. Default is all ROIs at site."""
    years: Optional[Tuple[PositiveInt, PositiveInt]]
    """years is passed as range for example years=[2000, 2002] downloads data
    for three years."""

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        phenocam_data_dir.mkdir(parents=True, exist_ok=True)
        optional_args = dict()
        if self.veg_type is not None:
            optional_args["veg_type"] = self.veg_type
        if self.rois is not None:
            optional_args["roi_id"] = self.rois
        phenocamr = importr("phenocamr")
        if self._exists_locally() or CONFIG.force_override:
            logger.info(f"Phenocam files already downloaded {self._locations()}")
        else:
            phenocamr.download_phenocam(
                site=self.site,
                frequency=self.frequency,
                internal=False,
                out_dir=str(phenocam_data_dir),
                **optional_args,
            )

    def _exists_locally(self) -> bool:
        return all([location.exists() for location in self._locations()])

    def _location(self, row: pd.Series) -> Path:
        freq = "roistats"
        if self.frequency != "roistats":
            freq = f"{self.frequency}day"
        return (
            phenocam_data_dir
            / f"{row.site}_{row.veg_type}_{row.roi_id_number:04}_{freq}.csv"
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
        if self.years is not None:
            df = df.loc[(self.years[0] <= df.year) & (df.year <= self.years[1])]
        return df


class PhenocamrBoundingBox(BaseModel):
    """PhenoCam time series for sites in a bounding box.

    Fetch data from https://phenocam.nau.edu/webcam/

    Requires phenocamr R package.
    Install with
    ```R
    install.packages("phenocamr")
    ```
    """

    dataset: Literal["phenocambbox"] = "phenocambbox"
    area: NamedArea
    veg_type: Optional[str]
    """Vegetation type (DB, EN). Default is all."""
    frequency: Literal["1", "3", "roistats"] = "3"
    """Frequency of the time series product."""
    years: Optional[Tuple[PositiveInt, PositiveInt]]
    """years is passed as range for example years=[2000, 2002] downloads data
    for three years."""

    def _location(self, row: pd.Series) -> Path:
        # TODO dont duplicate code
        freq = "roistats"
        if self.frequency != "roistats":
            freq = f"{self.frequency}day"
        return (
            phenocam_data_dir
            / f"{row.site}_{row.veg_type}_{row.roi_id_number:04}_{freq}.csv"
        )

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

    def _exists_locally(self) -> bool:
        return all([location.exists() for location in self._locations()])

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """
        if self._exists_locally() or CONFIG.force_override:
            logger.info(f"Phenocam files already downloaded {self._locations()}")
        for site in self._selection().site.unique():
            fetcher = PhenocamrSite(
                site=f"{site}$", veg_type=self.veg_type, frequency=self.frequency
            )
            fetcher.download()

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        df = pd.concat([_load_location(location) for location in self._locations()])
        if self.years is not None:
            df = df.loc[(self.years[0] <= df.year) & (df.year <= self.years[1])]
        return df


def _load_location(location: Path) -> pd.DataFrame:
    # TODO store header of csv in df.attr
    df = pd.read_csv(location, skiprows=24)
    (site, veg_type, roi_id_number, _freq) = location.stem.split("_")
    df.insert(0, "veg_type", veg_type)
    df.insert(0, "roi_id_number", roi_id_number)
    df.insert(0, "site", site)
    return df


def _rdf2pandasdf(r_df) -> pd.DataFrame:
    with localconverter(ro.default_converter + pandas2ri.converter):
        return ro.conversion.get_conversion().rpy2py(r_df)


sites_file = phenocam_data_dir / "site_meta_data.csv"


def list_sites() -> geopandas.GeoDataFrame:
    """List of phenocam sites.

    Returns:
        Data frame.
    """
    if not sites_file.exists():
        logger.warning(f"Downloading phenocam sites to {sites_file}")
        _download_sites()
    df = pd.read_csv(sites_file)
    return geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.lon, df.lat))


def _download_sites():
    phenocamr = importr("phenocamr")
    r_sites = phenocamr.list_sites(internal=True)
    sites = _rdf2pandasdf(r_sites)
    sites_file.parent.mkdir(parents=True, exist_ok=True)
    sites.to_csv(sites_file, index=False)


rois_file = phenocam_data_dir / "roi_data.csv"


def list_rois():
    """List of phenocam regions of interest (ROI).

    Returns:
        Data frame.
    """
    if not rois_file.exists():
        logger.warning(f"Downloading phenocam rois to {rois_file}")
        _download_rois()
    df = pd.read_csv(rois_file)
    return geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.lon, df.lat))


def _download_rois():
    phenocamr = importr("phenocamr")
    r_rois = phenocamr.list_rois(internal=True)
    rois = _rdf2pandasdf(r_rois)
    rois_file.parent.mkdir(parents=True, exist_ok=True)
    rois.to_csv(rois_file, index=False)
