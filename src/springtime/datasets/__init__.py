"""Functionality related to working with data sources."""

from typing import Union

import yaml
from pydantic import Field, TypeAdapter
from typing_extensions import Annotated

from springtime.datasets.appeears import Appeears
from springtime.datasets.daymet import Daymet
from springtime.datasets.eobs import EOBS
from springtime.datasets.pep725 import PEP725Phenor
from springtime.datasets.phenocam import Phenocam
from springtime.datasets.ppo import RPPO
from springtime.datasets.rnpn import RNPN

Datasets = Annotated[
    Union[
        RPPO,
        PEP725Phenor,
        Daymet,
        Appeears,
        EOBS,
        RNPN,
        Phenocam,
    ],
    Field(discriminator="dataset"),
]


def load_dataset(recipe: str) -> Datasets:
    """Load a dataset formatted as (yaml) recipe.

    Args:
        recipe: the yaml representation of the dataset.

    """
    model_dict = yaml.safe_load(recipe)

    dataset_loader = TypeAdapter(Datasets)
    dataset = dataset_loader.validate_python(model_dict)
    return dataset  # type: ignore  # https://github.com/pydantic/pydantic/discussions/7094
