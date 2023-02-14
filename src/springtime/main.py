from pathlib import Path
from tempfile import gettempdir
from typing import Dict, List, Optional
import logging

import click
import pandas as pd
import yaml
from pydantic import BaseModel, validator

from springtime import CONFIG
from springtime.datasets import Datasets

logger = logging.getLogger(__name__)


class Session(BaseModel):
    """Session for executing a workflow."""

    output_dir: Path = Path(gettempdir()) / "output"

    @validator("output_dir")
    def _make_dir(cls, path):
        """Create dirs if they don't exist yet."""
        if not path.exists():
            print(f"Creating folder {path}")
            path.mkdir(parents=True)
        return path

    class Config:
        validate_all = True


class Workflow(BaseModel):
    datasets: Dict[str, Datasets]
    cross_validation: List = []
    models: List = []
    recipe: Optional[Path] = None
    session: Optional[Session] = None

    @classmethod
    def from_recipe(cls, recipe: Path):
        with open(recipe, "r") as raw_recipe:
            options = yaml.safe_load(raw_recipe)

        return cls(**options)

    def execute(self):
        """(Down)load data, pre-process, run models, evaluate."""
        self.create_session()
        # self.autocomplete()
        self.download_data()
        df = self.load_data()
        self.run_experiments(df)

    def create_session(self):
        """Create a context for executing the experiment."""
        self.session = Session()
        if self.recipe is not None:
            self.recipe.copy(self.session.output_dir / "data.csv")

    def autocomplete(self):
        """Substitute time and area in datasets and model function mappings."""

    def download_data(self):
        """Download the data."""
        for name, dataset in self.datasets.items():
            print("Downloading dataset: ", name)
            dataset.download()

    def load_data(self):
        """Load and merge input datasets."""
        data = []
        for dataset_name, dataset in self.datasets.items():
            df = dataset.load()
            logger.warn(f'Dataset {dataset_name} loaded with {len(df)} rows')
            data.append(df)

        df = pd.concat(data)
        df.to_csv(self.session.output_dir / "data.csv")

        return df

    def run_experiments(self, df):
        """Train and evaluate ML models."""
        scores = {}
        for model in self.models:
            with self.cross_validation as cv:
                model.fit(df)
                score = model.score()
                scores[model] = score

        with open(self.session.output_dir / "data.csv", "w") as output_file:
            yaml.dump(scores, output_file)


def main(recipe):
    Workflow.from_recipe(recipe).execute()


@click.command()
@click.argument("recipe")
def cli(recipe):
    main(recipe)


if __name__ == "__main__":
    cli()
