# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: GPL-2.0-only
# due to import of phenocamr

import logging
from typing import List, Literal, Optional, Tuple
import pandas as pd
import geopandas
import rpy2.robjects as ro
from pydantic import BaseModel, PositiveInt
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr

from springtime.config import CONFIG
from springtime.datasets.daymet import NamedArea

logger = logging.getLogger(__file__)

phenocam_data_dir = CONFIG.data_dir / "phenocam"

class PhenocamrSite(BaseModel):
    dataset: Literal["phenocam"] = "phenocam"
    site: str
    veg_type: Optional[str]
    frequency: Literal["1", "3", 'roistats'] = "3"
    rois: Optional[List[str]]
    years: Optional[Tuple[PositiveInt, PositiveInt]]

    def download(self):
        phenocamr = importr("phenocamr")
        phenocam_data_dir.mkdir(parents=True, exist_ok=True)
        optional_args = dict()
        if self.veg_type is not None:
            optional_args['veg_type'] = self.veg_type
        if self.rois is not None:
            optional_args['roi_id'] = self.rois
        phenocamr.download_phenocam(site=self.site, 
                                    frequency=self.frequency,
                                    internal=False,
                                    out_dir=str(phenocam_data_dir),
                                    **optional_args,
                                    )

    def _roi_filename(self, roi: str):
        return phenocam_data_dir / f'{self.site}_{self.veg_type}_{roi}_{self.frequency}'

    def load(self):
        if self.rois == None:
            rois = list_rois().loc[rois.site == self.site]
        else:
            rois = self.rois
        filenames = [self._roi_filename(roi) for roi in self.rois]


class PhenocamrBoundingBox(BaseModel):
    dataset: Literal["phenocambbox"] = "phenocambbox"
    area: NamedArea
    veg_type: Optional[str]
    frequency: Literal["1", "3", 'roistats'] = "3"
    years: Optional[Tuple[PositiveInt, PositiveInt]]



def _rdf2pandasdf(r_df) -> pd.DataFrame:
    with ro.default_converter + pandas2ri.converter:
        return ro.conversion.get_conversion().rpy2py(r_df)


sites_file = phenocam_data_dir / "site_meta_data.csv"

def list_sites():
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

rois_file = phenocam_data_dir / 'roi_data.csv'

def list_rois():
    if not rois_file.exists():
        logger.warning(f'Downloading phenocam rois to {rois_file}')
        _download_rois()
    df = pd.read_csv(rois_file)
    return df

def _download_rois():
    phenocamr = importr("phenocamr")
    r_rois = phenocamr.list_rois(internal=True)
    rois = _rdf2pandasdf(r_rois)
    rois_file.parent.mkdir(parents=True, exist_ok=True)
    rois.to_csv(rois_file, index=False)

def list_veg_types():
    return list_rois().veg_type.unique()
