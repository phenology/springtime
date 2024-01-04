# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of r dependencies
"""Functionality related to working with data sources."""

from typing import Union

from pydantic import Field, TypeAdapter
from typing_extensions import Annotated

import yaml
from springtime.datasets.abstract import Dataset

from springtime.datasets.meteo.daymet import (
    DaymetBoundingBox,
    DaymetMultiplePoints,
    DaymetSinglePoint,
)
from springtime.datasets.meteo.eobs import (
    EOBS
)
from springtime.datasets.satellite.modis.appeears import (
    AppeearsPoints,
    AppeearsPointsFromArea,
    AppeearsArea,
)
from springtime.datasets.satellite.modis.modistools import (
    ModisMultiplePoints,
    ModisSinglePoint,
)
from springtime.datasets.insitu.npn.NPNPhenor import NPNPhenor
from springtime.datasets.insitu.pep725 import PEP725Phenor
from springtime.datasets.insitu.phenocam import PhenocamrBoundingBox, PhenocamrSite
from springtime.datasets.insitu.ppo import RPPO
from springtime.datasets.insitu.npn.rnpn import RNPN

Datasets = Annotated[
    Union[
        RPPO,
        PEP725Phenor,
        DaymetSinglePoint,
        DaymetMultiplePoints,
        DaymetBoundingBox,
        NPNPhenor,
        ModisSinglePoint,
        ModisMultiplePoints,
        AppeearsPoints,
        AppeearsArea,
        AppeearsPointsFromArea,
        EOBS,
        RNPN,
        PhenocamrSite,
        PhenocamrBoundingBox,
    ],
    Field(discriminator="dataset"),
]


def load_dataset(recipe: str) -> Dataset:
    """Load a dataset formatted as (yaml) recipe.

    Args:
        recipe: the yaml representation of the dataset.

    """
    model_dict = yaml.safe_load(recipe)

    dataset_loader = TypeAdapter(Datasets)
    dataset = dataset_loader.validate_python(model_dict)
    return dataset
