# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
# due to import of r dependencies
"""Functionality related to working with data sources."""

from typing import Union

from pydantic import Field
from typing_extensions import Annotated

from springtime.datasets.meteo.daymet import (
    DaymetBoundingBox,
    DaymetMultiplePoints,
    DaymetSinglePoint,
)
from springtime.datasets.meteo.eobs import (
    EOBS,
    EOBSBoundingBox,
    EOBSMultiplePoints,
    EOBSSinglePoint,
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
        EOBSSinglePoint,
        EOBSMultiplePoints,
        EOBSBoundingBox,
        RNPN,
        PhenocamrSite,
        PhenocamrBoundingBox,
    ],
    Field(discriminator="dataset"),
]
