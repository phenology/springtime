from typing import Literal, Union

from pydantic import Field
from typing_extensions import Annotated

from springtime.datasets.pyphenology import PyPhenologyDataset


class Dummy(PyPhenologyDataset):
    """Dummy dataset type to test discriminatory union."""

    dataset: Literal["dummy"] = "dummy"


Datasets = Annotated[Union[PyPhenologyDataset, Dummy], Field(discriminator="dataset")]
