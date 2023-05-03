# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from tempfile import gettempdir
from typing import Dict, List, Optional

import click
import pandas as pd
import yaml
from pydantic import BaseModel, validator

from springtime.datasets import Datasets
from springtime.utils import PointsFromOther, resample

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

        dataframes = {}
        # TODO check for dependencies to infer order
        for dataset_name, dataset in self.datasets.items():
            if hasattr(dataset, "points") and isinstance(
                dataset.points, PointsFromOther
            ):
                dataset.points.get_points(dataframes[dataset.points.source])
            print("Downloading dataset: ", dataset_name)
            dataset.download()
            ds = dataset.load()
            logger.warning(f"Dataset {dataset_name} loaded with {len(ds)} rows")
            if dataset.resample:
                if issubclass(ds.__class__, pd.DataFrame):
                    ds = resample(
                        ds,
                        freq=dataset.resample.frequency,
                        operator=dataset.resample.operator,
                    )
                else:
                    # TODO resample xarray dataset
                    raise NotImplementedError()
            logger.warning(f"Dataset {dataset_name} resampled to {len(ds)} rows")
            dataframes[dataset_name] = ds

        # TODO resample and transpose
        # df = pd.concat(dataframes)
        # df.to_csv(self.session.output_dir / "data.csv")

        # TODO do something with datacubes
        # self.run_experiments(df)

    def create_session(self):
        """Create a context for executing the experiment."""
        self.session = Session()
        if self.recipe is not None:
            self.recipe.copy(self.session.output_dir / "data.csv")

    def run_experiments(self, df):
        """Train and evaluate ML models."""
        scores = {}
        for model in self.models:
            with self.cross_validation:
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
