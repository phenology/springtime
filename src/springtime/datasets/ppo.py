import subprocess
from typing import Literal, Optional, Tuple
from pydantic import BaseModel
from pydantic.types import PositiveInt
import pandas as pd
from springtime.config import CONFIG
from springtime.datasets.daymet import NamedArea


class RPPO(BaseModel):
    dataset: Literal["rppo"] = "rppo"
    genus: str
    termID: str = "obo:PPO_0002313"
    """true leaves present == obo:PPO_0002313"""
    source: Optional[str]
    """For example: PEP725"""
    years: Optional[Tuple[int, int]]
    """For example (2000,2021)

    First tuple entry is the start year.
    Second tuple entry is the end year.
    """
    area: Optional[NamedArea]
    limit: PositiveInt = (100000,)
    """Maximum number of records to retreive"""
    timeLimit: PositiveInt = 60
    """Number of seconds to wait for the server to respond"""

    @property
    def path(self):
        fn = self._build_filename()
        return CONFIG.data_dir / fn

    def download(self):
        if self.path.exists():
            print("File already exists:", self.path)
        else:
            subprocess.run(["R", "--no-save"], input=self._r_download().encode())

    def _r_download(self):
        genus = ", ".join([f'"{p}"' for p in self.genus.split(" ")])
        if self.area is None:
            box = ""
        else:
            abox = self.area.bbox
            box = f"bbox='{abox[1]},{abox[0]},{abox[3]},{abox[2]}',"
        if self.years is None:
            years = ""
        else:
            years = f"fromYear={self.years[0]}, toYear={self.years[1]},"
        return f"""\
        library(rppo)
        library(readr)
        response <- ppo_data(genus = c({genus}), termID='{self.termID}',
            {box}
            {years}
            limit={self.limit}, timeLimit = {self.timeLimit})
        write.csv(response$data, "{self.path}")
        write_file(response$readme, "{self.path.with_suffix('.readme')}")
        write_file(response$citation, "{self.path.with_suffix('.citation')}")
        """

    def load(self):
        

    def _build_filename(self):
        parts = ['ppo', self.genus.replace(' ', '_'), self.termID]
        if self.source is not None:
            parts.append(self.source)
        else:
            parts.append("na")
        if self.years is not None:
            parts.append(f"{self.years[0]}-{self.years[1]}")
        else:
            parts.append("na")
        if self.area is not None:
            parts.append(self.area.name)
        else:
            parts.append("na")
        parts.append('csv')
        return ".".join(parts)
