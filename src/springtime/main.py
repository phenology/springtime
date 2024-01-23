import logging
from datetime import datetime
from pathlib import Path
from tempfile import gettempdir
from typing import Dict, Optional

import click
import pandas as pd
import yaml
from pydantic import BaseModel, field_validator

from springtime.config import CONFIG
from springtime.config import Config as SpringtimeConfig
from springtime.datasets import Datasets
from springtime.utils import PointsFromOther, join_dataframes

logger = logging.getLogger(__name__)


class Session(BaseModel):
    """Session for executing a workflow."""

    output_dir: Path = Path(gettempdir()) / "output"

    @field_validator("output_dir")
    def _make_dir(cls, path):
        """Create dirs if they don't exist yet."""
        if not path.exists():
            print(f"Creating folder {path}")
            path.mkdir(parents=True)
        return path

    @classmethod
    def for_recipe(
        cls,
        recipe: Path,
        output_dir: Path | None = None,
        config: SpringtimeConfig = CONFIG,
    ) -> "Session":
        if output_dir is None:
            now = datetime.now().strftime("%Y%m%d-%H%M%S")
            output_dir = config.output_root_dir / f"springtime-{recipe.stem}-{now}"
        return cls(output_dir=output_dir)

    class Config:
        validate_default = True


class Workflow(BaseModel):
    datasets: Dict[str, Datasets] = {}
    # preparation: Preparation = Preparation()

    @classmethod
    def from_recipe(cls, recipe: Path):
        with open(recipe, "r") as raw_recipe:
            options = yaml.safe_load(raw_recipe)

        return cls(**options)

    def to_recipe(self):
        """Return thew workflow as a recipe string."""
        return yaml.dump(self.model_dump(mode="json"), sort_keys=False)

    def save_recipe(self, path: Path):
        """Save the workflow as a recipe file."""

        with open(path, "w") as f:
            yaml.dump(self.model_dump(mode="json"), f, sort_keys=False)

    def execute(self, session: Session):
        """(Down)load data, pre-process, run models, evaluate."""
        self.save_recipe(session.output_dir / "recipe.yaml")

        dataframes: dict[str, pd.DataFrame] = {}
        # TODO check for dependencies to infer order
        for dataset_name, dataset in self.datasets.items():
            if hasattr(dataset, "points") and isinstance(
                dataset.points, PointsFromOther
            ):
                dataset.points.get_points(dataframes[dataset.points.source])

            df = dataset.load()
            logger.info(f"Dataset {dataset_name} loaded with {len(df)} rows")

            # TODO add a check whether the combination of (year and geometry) is unique.
            dataframes[dataset_name] = df

        # TODO refactor to generic "join" utility
        df = join_dataframes(dataframes.values())

        # # TODO: refactor to generic "prepare" utility
        # df = self.preparation.prepare(df)

        logger.info(f"Datasets joined to shape: {df.shape}")
        data_fn = session.output_dir / "data.csv"
        df.to_csv(data_fn)
        logger.info(f"Data saved to: {data_fn}")


def main(recipe, output_dir: Optional[Path]):
    session = Session.for_recipe(recipe, output_dir)

    Workflow.from_recipe(recipe).execute(session)


@click.command
@click.argument("recipe", type=click.Path(exists=True, path_type=Path))
@click.option("--cache-dir", default=CONFIG.cache_dir, type=click.Path(path_type=Path))
@click.option("--output-dir", default=None, type=click.Path(path_type=Path))
@click.option(
    "--output-root-dir", default=CONFIG.output_root_dir, type=click.Path(path_type=Path)
)
@click.option(
    "--pep725-credentials-file",
    default=CONFIG.pep725_credentials_file,
    type=click.Path(path_type=Path),
)
def cli(
    recipe: Path,
    cache_dir: Path,
    output_dir: Optional[Path],
    output_root_dir: Path,
    pep725_credentials_file: Path,
):
    logging.basicConfig(level=logging.INFO)
    CONFIG.cache_dir = cache_dir
    CONFIG.output_root_dir = output_root_dir
    CONFIG.pep725_credentials_file = pep725_credentials_file
    main(recipe, output_dir)


if __name__ == "__main__":
    cli()
