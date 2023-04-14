# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

"""Example datasets from pyPhenology package

In the process of "downloading" the data, we also do some
conversions to make it suitable for ML workflows.
"""
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
from pyPhenology.models.utils.misc import temperature_only_data_prep
from pyPhenology.utils import load_test_data

from springtime.config import CONFIG
from springtime.datasets.abstract import Dataset


class PyPhenologyDataset(Dataset):
    """Example datasets from pyphenology package.

    Available combinations:
    - vaccinium, budburst
    - vaccinium, flowers
    - aspen, budburst

    Example:

        from springtime.datasets.pyphenology import PyPhenologyDataset
        ds = PyPhenologyDataset(name='aspen', phenophase='budburst')
        ds.location
        ds.exists_locally()  # -> False
        ds.download()
        ds.exists_locally()  # -> True
        df = ds.load()

    """

    dataset: Literal["pyphenology"] = "pyphenology"
    name: Literal["vaccinium", "aspen"]
    phenophase: Literal["budburst", "flowers"]
    # TODO: forbid combination of aspen and flowers

    @property
    def location(self) -> Path:
        """Show filename(s) that this dataset would have on disk.

        Should use a generic data reference sytax combined with a local
        filesystem configuration.
        """
        return CONFIG.data_dir / f"pyphenology_{self.name}_{self.phenophase}.csv"

    def exists_locally(self) -> bool:
        """Tell if the data is already present on disk."""
        return self.location.exists()

    def download(self):
        """Download the data."""
        # Pretend loading from source is equivalent to downloading
        # In the process we do some cleaning
        obs, pred = load_test_data(name=self.name, phenophase=self.phenophase)
        obs = obs.reset_index(drop=True).drop("phenophase", axis=1)

        # Align the weather data with the observations.
        combined_data = _align_data(obs, pred)

        # Save to standardized location
        combined_data.to_csv(self.location)

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
        df = pd.read_csv(self.location, index_col=0)
        df = df.loc[(self.years.start <= df.year) & (df.year <= self.years.end)]
        geometry = gpd.points_from_xy(df.pop("longitude"), df.pop("latitude"))
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf


def _align_data(obs, pred):
    """Align the weather data with the observations."""
    # Apply PyPhenology's built-in preprocessing
    index, data, columns = temperature_only_data_prep(obs, pred)
    df = pd.DataFrame(data.T, index=index, columns=columns)

    # The returned data supposedly has the DOY column of obs as its index
    assert all(df.index == obs.doy)

    # To join dataframes, we want to make sure both indexes are aligned ()
    df = df.reset_index(drop=True)
    assert all(df.index == obs.index)

    # Now we can safely combine data
    transformed_df = pd.concat([obs, df], axis=1)

    return transformed_df
