from typing import Literal, Optional
from pydantic import BaseModel

class PyPPO(BaseModel):
    dataset: Literal["pyppo"] = "pyppo"
    genus: str
    source: Optional[str]
    """For example: PEP725"""
    year: Optional[str]
    """For example [2000 TO 2021]"""
    latitude="[40 TO 70]",
    longitude="[-10 TO 40]",
    termID="obo:PPO_0002313",
    explode=False,
    limit=1535,

    def download(self):
        ...

    def load(self):
        ...
