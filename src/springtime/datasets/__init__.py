from typing import Union

from pydantic import Field
from typing_extensions import Annotated
from springtime.datasets.PEP725Phenor import PEP725Phenor
from springtime.datasets.NPNPhenor import NPNPhenor

from springtime.datasets.pyphenology import PyPhenologyDataset
from springtime.datasets.daymet import DaymetBoundingBox, DaymetMultiplePoints, DaymetSinglePoint

Datasets = Annotated[Union[PyPhenologyDataset, PEP725Phenor, NPNPhenor, DaymetSinglePoint, DaymetMultiplePoints, DaymetBoundingBox], Field(discriminator="dataset")]
