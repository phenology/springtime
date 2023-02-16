from typing import Union

from pydantic import Field
from typing_extensions import Annotated

from springtime.datasets.daymet import (DaymetBoundingBox,
                                        DaymetMultiplePoints,
                                        DaymetSinglePoint)
from springtime.datasets.NPNPhenor import NPNPhenor
from springtime.datasets.PEP725Phenor import PEP725Phenor
from springtime.datasets.modis import ModisSinglePoint
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
    ],
    Field(discriminator="dataset"),
]
