# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from typing import Protocol


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
