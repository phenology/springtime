# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Optional
from pydantic import BaseModel, validator

from springtime.utils import ResampleConfig, YearRange


class Dataset(BaseModel, ABC):
    dataset: str
    """The name of the dataset."""
    years: YearRange
    """ years is passed as range for example years=[2000, 2002] downloads data
    for three years."""
    resample: Optional[ResampleConfig] = None
    # TODO run multiple resamplings like weekly, monthly with min and max?

    @validator("years")
    def _validate_year_range(cls, values: YearRange):
        assert (
            values.start <= values.end
        ), f"start year ({values.start}) should be smaller than end year ({values.end})"
        return values

    @abstractmethod
    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """

    @abstractmethod
    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """
