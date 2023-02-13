import subprocess
from typing import Iterable, Literal, Optional
from bs4 import BeautifulSoup
from pydantic import BaseModel
import requests

# Code borrowed from https://github.com/stuckyb/pheno_paper/blob/master/pep725/pepProcessFiles.py

class PyPEP725(BaseModel):
    dataset: Literal["PEP725"] = "PEP725"
    species: str
    country: Optional[str]
    years: Iterable[int]
    phenophase: int
    username: str
    password: str

    def download(self):
        ...

    def load(self):
        ...

    def species():
        url = 'http://www.pep725.eu/data_download/data_selection.php'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

class PhenorPEP725(BaseModel):
    """
    
    Example

    ```python
    from springtime.datasets.PEP725 import PhenorPEP725
    dataset = PhenorPEP725(species='Syringa vulgaris', years=[])
    dataset.download()
    ```

    Requires phenor R package. Install with 
    
    ```R
    devtools::install_github("bluegreen-labs/phenor@v1.3.1")
    ```
    """
    dataset: Literal["PEP725"] = "PEP725"
    species: str
    country: Optional[str]
    years: Iterable[int]
    phenophase: int = 60
    """60 === Beginning of flowering"""
    credential_file: str = '~/.config/pep725_credentials.txt'

    def download(self):
        """
        
        Requires `~/.config/pep725_credentials.txt` file with your PEP725 credentials.
        Email adress on first line, password on second line.
        
        """
        subprocess.run(["R", '--no-save'], input=self._r_download().encode())

    def load(self):
        ...

    def _r_download(self):
        return f"""\
        library(phenor)
        species_id <- phenor::check_pep725_species(species = "{self.species}")
        phenor::pr_dl_pep725(
            credentials = "~/.config/pep725_credentials.txt",
            species = species_id,
            path = ".",
            internal = FALSE
        )
        """