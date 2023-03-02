# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of r dependencies

from typing import Union

from pydantic import Field
from typing_extensions import Annotated

from springtime.datasets.daymet import (
    DaymetBoundingBox,
    DaymetMultiplePoints,
    DaymetSinglePoint,
)
from springtime.datasets.e_obs import (
    EOBS,
    EOBSBoundingBox,
    EOBSMultiplePoints,
    EOBSSinglePoint,
)
from springtime.datasets.modis import ModisMultiplePoints, ModisSinglePoint
from springtime.datasets.NPNPhenor import NPNPhenor
from springtime.datasets.PEP725Phenor import PEP725Phenor
from springtime.datasets.ppo import RPPO
from springtime.datasets.pyphenology import PyPhenologyDataset

Datasets = Annotated[
    Union[
        RPPO,
        PyPhenologyDataset,
        PEP725Phenor,
        DaymetSinglePoint,
        DaymetMultiplePoints,
        DaymetBoundingBox,
        NPNPhenor,
        ModisSinglePoint,
        ModisMultiplePoints,
        EOBS,
        EOBSSinglePoint,
        EOBSMultiplePoints,
        EOBSBoundingBox,
    ],
    Field(discriminator="dataset"),
]
