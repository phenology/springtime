from ast import Tuple
from typing import Literal, Optional
from pydantic import BaseModel
from pydantic.types import PositiveInt
from springtime.datasets.daymet import NamedArea

class PyPPO(BaseModel):
    dataset: Literal["pyppo"] = "pyppo"
    genus: str
    source: Optional[str]
    """For example: PEP725"""
    year: Optional[Tuple[int, int]]
    """For example (2000,2021)

    First tuple entry is the start year.
    Second tuple entry is the end year.
    """
    area: Optional[NamedArea]
    termID: str ="obo:PPO_0002313",
    """true leaves present == obo:PPO_0002313"""
    limit: PositiveInt = 1000,
    """Maximum number of records to retreive"""
    timeout: float = 3.05
    """Number of seconds to wait for the server to respond"""
    # load options
    explode=False,
    """If True, each termID will get its own row."""

    def download(self):
        ...

    def load(self):
        ...
