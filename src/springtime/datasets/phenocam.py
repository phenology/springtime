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
    from springtime.datasets import Phenocam

    dataset = Phenocamr(
        site="harvard$",
        years=(2019, 2020),
    )
    dataset.download()
    df = dataset.load()
    ```

Example:

    ```python
    from springtime.datasets import Phenocam

    dataset = Phenocamr(
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
from typing import Literal, Optional, Sequence

import geopandas as gpd
import pandas as pd

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset
from springtime.utils import NamedArea, run_r_script

logger = logging.getLogger(__file__)

rois_file = CONFIG.cache_dir / "phenocam" / "roi_data.csv"
sites_file = CONFIG.cache_dir / "phenocam" / "site_meta_data.csv"

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
        veg_type: Vegetation type (DB, EN). Default is "all".
        frequency: Frequency of the time series product.
        variables: Variables you want to download. When empty will download all
            the variables.
        site: Download a single site, specified by the name of site. Append `$`
            to get exact match.
        rois: The id of the ROI to download. Default is all ROIs at site.
        area: Download all sites in an area. A dictionary of the form
            `{"name": "yourname", "bbox": [xmin, ymin, xmax, ymax]}`.

    """

    # TODO: resample?
    dataset: Literal["phenocam"] = "phenocam"
    veg_type: Optional[str] = None
    frequency: Literal["1", "3", "roistats"] = "3"
    variables: Sequence[PhenocamVariables] = []
    site: Optional[str] = None
    roi_id: Optional[str] = None
    area: Optional[NamedArea] = None
    # TODO specify either a site (and rois) or area, not both

    @property
    def _root_dir(self):
        return CONFIG.cache_dir / "phenocam"

    def _filter_rois(self):
        """Reduce the full ROI listing to the applicable records."""
        rois_df = list_rois()

        # First narrow down on area
        if self.area is not None:
            box = self.area.bbox
            rois_df = rois_df.cx[
                # xmin:xmax, ymin:ymax
                box[0] : box[2],
                box[1] : box[3],
            ]

        # Narrow to site?
        if self.site is not None:
            if self.site.endswith("$"):
                rois_df = rois_df.loc[rois_df.site == self.site[:-1]]
            else:
                rois_df = rois_df.loc[rois_df.site.str.contains(self.site)]

        # Narrow to single ROI?
        if self.roi_id is not None:
            rois_df = rois_df.loc[rois_df.roi_id_number == self.roi_id]

        # Narrow to vegetation type?
        if self.veg_type is not None:
            rois_df = rois_df.loc[rois_df.veg_type == self.veg_type]

        return rois_df

    def _derive_path(self, roi: pd.Series) -> Path:
        freq = "roistats" if self.frequency == "roistats" else f"{self.frequency}day"
        return (
            self._root_dir
            / f"{roi.site}_{roi.veg_type}_{roi.roi_id_number:04}_{freq}.csv"
        )

    def _locations(self):
        unique_rois = self._filter_rois()
        return unique_rois.apply(self._derive_path, axis="columns")

    def _exists_locally(self, locations) -> bool:
        return all([location.exists() for location in locations])

    def _to_geopandas(self, df):
        sites = list_sites()
        site_locations = sites[["site", "geometry"]]
        df = df.merge(site_locations, on="site")
        df = gpd.GeoDataFrame(df).set_geometry("geometry")
        df.rename(columns={"date": "datetime"}, inplace=True)
        # Do not return variables that are not derived from image data
        # to get raw file see self._root_dir directory.
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

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or CONFIG.force_override
        is TRUE.
        """
        logger.info("Looking for data")
        self._root_dir.mkdir(parents=True, exist_ok=True)
        paths = []

        # Default roi_id in phenocamr is "ALL", then it loops over all ROIs in R.
        # Might as well loop here --> more readable code.
        for _, row in self._filter_rois().iterrows():
            path = self._derive_path(roi=row)
            if path.exists() and not CONFIG.force_override:
                logger.info(f"Found {path}")
            else:
                _r_download_data(
                    site=row.site,
                    frequency=self.frequency,
                    output_dir=self._root_dir,
                    veg_type=row["veg_type"],
                    roi_id=row["roi_id_number"],
                )

            paths.append(path)

        return paths

    def raw_load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        paths = self.download()
        df = pd.concat([_load_location(path) for path in paths])
        return df

    def load(self):
        raw_df = self.raw_load()

        # Select years
        df = raw_df.loc[
            (self.years.start <= raw_df.year) & (raw_df.year <= self.years.end)
        ]

        # Convert to gpd
        gdf = self._to_geopandas(df)

        # TODO: infer event?

        return gdf


def _load_location(location: Path) -> pd.DataFrame:
    # TODO store header of csv in df.attr
    df = pd.read_csv(location, skiprows=24, parse_dates=["date"])
    (site, veg_type, roi_id_number, _freq) = location.stem.split("_")
    df.insert(0, "veg_type", veg_type)
    df.insert(0, "roi_id_number", roi_id_number)
    df.insert(0, "site", site)
    return df


def list_sites() -> gpd.GeoDataFrame:
    """List of phenocam sites.

    Returns:
        Data frame containing phenocam sites
    """
    if not sites_file.exists():
        _r_download_sites()

    df = pd.read_csv(sites_file)

    lon = df.pop("lon")
    lat = df.pop("lat")
    geometry = gpd.points_from_xy(lon, lat)
    return gpd.GeoDataFrame(df).set_geometry(geometry)


def list_rois() -> gpd.GeoDataFrame:
    """List of phenocam regions of interest (ROI).

    Returns:
        Data frame containing phenocam Regions of Interest.
    """
    if not rois_file.exists():
        _r_download_rois()

    df = pd.read_csv(rois_file)

    lon = df.pop("lon")
    lat = df.pop("lat")
    geometry = gpd.points_from_xy(lon, lat)
    return gpd.GeoDataFrame(df).set_geometry(geometry)


def _r_download_sites():
    script = f"""\
        library(phenocamr)
        df = list_sites(internal=TRUE)
        write.csv(df, "{sites_file}", row.names=FALSE)
        """

    logger.info("Downloading sites")

    run_r_script(script, timeout=300)


def _r_download_rois():
    script = f"""\
        library(phenocamr)
        df = list_rois(internal=TRUE)
        write.csv(df, "{rois_file}", row.names=FALSE)
        """

    logger.info("Downloading rois")

    run_r_script(script, timeout=300)


def _r_download_data(site, frequency, output_dir, veg_type, roi_id):
    """Download data for an exact site match.

    $ is appended to site to get an exact match.
    """
    veg_type = "NULL" if veg_type is None else f"'{veg_type}'"
    roi_id = "NULL" if roi_id is None else f"'{roi_id}'"
    script = f"""\
        library(phenocamr)
        download_phenocam(
            site="{site}$",
            frequency={frequency},
            internal=FALSE,
            out_dir='{output_dir}',
            veg_type={veg_type},
            roi_id={roi_id}
        )
        """

    logger.info(f"Downloading data for {site}, roi_id {roi_id}, veg_type {veg_type}")

    run_r_script(script, timeout=300)
