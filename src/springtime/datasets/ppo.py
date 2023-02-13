from ast import Tuple
from typing import Literal, Optional
from pydantic import BaseModel
from pydantic.types import PositiveInt
import requests
from springtime.datasets.daymet import NamedArea

class PyPPO(BaseModel):
    dataset: Literal["pyppo"] = "pyppo"
    genus: str
    termID: str ="obo:PPO_0002313",
    """true leaves present == obo:PPO_0002313"""
    source: Optional[str]
    """For example: PEP725"""
    year: Optional[Tuple[int, int]]
    """For example (2000,2021)

    First tuple entry is the start year.
    Second tuple entry is the end year.
    """
    area: Optional[NamedArea]
    limit: PositiveInt = 1000,
    """Maximum number of records to retreive"""
    timeout: float = 3.05
    """Number of seconds to wait for the server to respond"""
    # load options
    explode=False,
    """If True, each termID will get its own row."""

    def download(self):
        url = self._build_url()

        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()
        filename = self._build_filename()
        with open(filename, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=4096):
                fd.write(chunk)

    def load(self):
        ...

    def _build_url(self):
        """Parse options to build query string."""
        base_url = f"https://biscicol.org/api/v3/download/_search?limit={self.limit}"

        options = {
            'genus': self.genus,
            'termID': self.termID
        }
        if self.source is not None:
            options['source'] = self.source
        if self.year is not None:
            options['year'] = f'[{self.year[0]} TO {self.year[1]}]'
        if self.area is not None:
            options['latitude'] = f'[{self.area.bbox[1]} TO {self.area.bbox[3]}]'
            options['longitude'] = f'[{self.area.bbox[0]} TO {self.area.bbox[2]}]'

        query = "&q=" + "+AND+".join([f"{k}:{v}" for k, v in options.items()])

        return base_url + query

    def _build_filename(self):
        parts = [
            self.genus,
            self.termID
        ]
        if self.source is not None:
            parts.append(self.source)
        else:
            parts.append('na')
        if self.year is not None:
            parts.append(f'{self.year[0]}-{self.year[1]}')
        else:
            parts.append('na')
        if self.area is not None:
            parts.append(self.area.name)
        else:
            parts.append('na')
        return '.'.join(parts)
