# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from typing import Protocol


class Dataset(Protocol):
    """Interface for working with phenology datasets."""

    dataset: str
    """The name of the dataset.

    Used for type inference when using nested pydantic models:
    https://docs.pydantic.dev/usage/types/#discriminated-unions-aka-tagged-unions

    Also see: https://stackoverflow.com/q/69322097
    TODO: checkout https://github.com/pydantic/pydantic/issues/503
    """

    def download(self):
        """Download the data.

        Only downloads if data is not in CONFIG.data_dir or CONFIG.force_override
        is TRUE.
        """

    def load(self):
        """Load the dataset from disk into memory.

        This may include pre-processing operations as specified by the context, e.g.
        filter certain variables, remove data points with too many NaNs, reshape data.
        """


class Model(Protocol):
    """Interface for working with various ML models."""

    def fit(self, data):
        """Fit model to data."""

    def predict(self, new_x):
        """Make a prediction for new data."""


class CV(Protocol):
    """Interface for cross-validation strategy."""

    def split(self, data):
        """Split the data into train/test sets."""

    def search(self):
        """Do grid search or something like that..."""
