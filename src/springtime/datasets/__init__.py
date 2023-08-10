# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of r dependencies
"""Functionality related to working with data sources."""

from typing import Union

from pydantic import Field
from typing_extensions import Annotated
from springtime.datasets.appeears import AppeearsPoints, AppeearsPointsFromArea

from springtime.datasets.meteorology.daymet import (
    DaymetBoundingBox,
    DaymetMultiplePoints,
    DaymetSinglePoint,
)
from springtime.datasets.meteorology.e_obs import (
    EOBS,
    EOBSBoundingBox,
    EOBSMultiplePoints,
    EOBSSinglePoint,
)
from springtime.datasets.modis import ModisMultiplePoints, ModisSinglePoint
from springtime.datasets.meteorology.insitu.NPNPhenor import NPNPhenor
from springtime.datasets.PEP725Phenor import PEP725Phenor
from springtime.datasets.phenocam import PhenocamrBoundingBox, PhenocamrSite
from springtime.datasets.ppo import RPPO
from springtime.datasets.rnpn import RNPN

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
        AppeearsPointsFromArea,
        EOBS,
        EOBSSinglePoint,
        EOBSMultiplePoints,
        EOBSBoundingBox,
        RNPN,
        PhenocamrSite,
        PhenocamrBoundingBox,
    ],
    Field(discriminator="dataset"),
]
