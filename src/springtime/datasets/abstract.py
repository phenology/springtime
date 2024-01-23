"""
Standard interface for springtime datasets.

All springtime datasets should inherit from the abstract Dataset class and
implement the basic functionality described here.
"""

from abc import ABC, abstractmethod

import yaml
from pydantic import BaseModel

from springtime.utils import YearRange


class Dataset(BaseModel, ABC, validate_default=True, validate_assignment=True):
    """Base class for springtime datasets.

    Attributes:
        dataset: The name of the dataset.
        years: timerange. For example years=[2000, 2002] downloads data for
            three years.
    """

    dataset: str
    years: YearRange | None = None  # TODO not optional or not in abstract

    @abstractmethod
    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.cache_dir or
        CONFIG.force_override
        is TRUE.
        """

    def raw_load(self):
        """Loads from disk with minimal modification.

        Mostly intended to provide insight into the modifications made in the
        load method.
        """
        raise NotImplementedError("raw_load not implemented for this dataset.")

    @abstractmethod
    def load(self):
        """Load, harmonize, and optionally pre-process the data.

        Default output of load should be compatible with recipe execution.

        kwargs can be used to control behaviour that is useful in API, but
        breaks the recipe. For example, don't convert to geopandas.
        """

    def to_recipe(self):
        """Print out a recipe to reproduce this dataset."""
        recipe = self.model_dump(
            mode="json", exclude_none=True, exclude=["credential_file"]
        )
        return yaml.dump(recipe, sort_keys=False)
