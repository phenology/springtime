# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from tempfile import gettempdir
from typing import Dict, Optional

import click
import pandas as pd
import yaml
from pydantic import BaseModel, Field, validator
from pycaret import regression

from springtime.datasets import Datasets
from springtime.experiment import (
    ClassificationExperiment,
    RegressionExperiment,
    materialize_estimator,
    materialize_estimators,
)
from springtime.utils import PointsFromOther, resample, transponse_df

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
    dropna: bool = True
    experiment: RegressionExperiment | ClassificationExperiment | None = Field(
        discriminator="experiment_type"
    )
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
                    # Transpose
                    ds = transponse_df(
                        ds,
                        index=("year", "geometry"),
                        columns=(dataset.resample.frequency,),
                    )
                else:
                    # TODO resample xarray dataset
                    raise NotImplementedError()
            else:
                # make sure "year" column exist.
                ds["year"] = ds.datetime.dt.year
                ds.drop("datetime", axis="columns", inplace=True)
            logger.warning(f"Dataset {dataset_name} resampled to {len(ds)} rows")
            # TODO add a check whether the combination of (year and geometry) is unique.
            dataframes[dataset_name] = ds

        # do join
        others = [
            ds.set_index(["year", "geometry"])
            for ds in dataframes.values()
            if issubclass(ds.__class__, pd.DataFrame)
        ]
        main_df = others.pop(0)
        df = main_df.join(others, how="outer")
        if self.dropna:
            df.dropna(inplace=True)
        logger.warning(f"Datesets joined to shape: {df.shape}")
        data_fn = self.session.output_dir / "data.csv"
        df.to_csv(data_fn)
        logger.warning(f"Data saved to: {data_fn}")

        # TODO do something with datacubes
        self.run_experiments(df)

    def create_session(self):
        """Create a context for executing the experiment."""
        # TDDO make session dir unique on each run
        self.session = Session()
        if self.recipe is not None:
            self.recipe.copy(self.session.output_dir / "data.csv")

    def run_experiments(self, df):
        """Train and evaluate ML models."""
        if self.experiment is None:
            return

        if self.experiment.experiment_type == "regression":
            s = regression.RegressionExperiment()
        else:
            raise ValueError("Unknown experiment type")

        s.setup(df, **self.experiment.setup)
        if self.experiment.create_model:
            cm = self.experiment.create_model
            raw_estimator = cm["estimator"]
            cm["estimator"] = materialize_estimator(
                name=raw_estimator,
                buildin_models=s.models().index,
                init_kwargs=self.experiment.init_kwargs,
            )
            model = s.create_model(**cm)
            self.save_model(s, model, raw_estimator)
            self.plots_model(s, model, raw_estimator)

            if self.experiment.create_model["cross_validation"]:
                self.save_leaderboard(s)

        if self.experiment.compare_models:
            cm = self.experiment.compare_models
            cm["include"] = materialize_estimators(
                names=cm.get("include"),
                buildin_models=s.models().index,
                init_kwargs=self.experiment.init_kwargs,
            )
            if self.experiment.compare_models["n_select"]:
                best_models = s.compare_models(**cm)
                for i, model in enumerate(best_models):
                    name = f"best#{i}"
                    self.save_model(s, model, name=name)
                    self.plots_model(s, model, name)
            else:
                best_model = s.compare_models(**cm)
                name = "best"
                self.save_model(s, best_model, name)
                self.plots_model(s, best_model, name)

            if self.experiment.compare_models["cross_validation"]:
                self.save_leaderboard(s)

    def plots_model(self, experiment, model, model_name):
        if self.experiment.plots is None:
            return

        for plot_name in self.experiment.plots:
            self.plot_model(experiment, model, model_name, plot_name)

    def plot_model(self, experiment, model, model_name, plot_name):
        plot_fn_in_cwd = Path(experiment.plot_model(model, plot=plot_name, save=True))
        plot_fn = self.session.output_dir / f"{model_name} {plot_fn_in_cwd.name}"
        plot_fn_in_cwd.rename(plot_fn)
        logger.warning(f"Saving {plot_name} plot to {plot_fn}")

    def save_model(self, s, model, name):
        model_fn = self.session.output_dir / name
        logger.warning(f"Saving model to {model_fn}")
        s.save_model(model, model_fn)

    def save_leaderboard(self, s):
        df = s.get_leaderboard()
        leaderboard_fn = self.session.output_dir / "leaderboard.csv"
        logger.warning(f"Saving leaderboard to {leaderboard_fn}")
        df.drop("Model", axis="columns").to_csv(leaderboard_fn)


def main(recipe):
    Workflow.from_recipe(recipe).execute()


@click.command()
@click.argument("recipe")
def cli(recipe):
    main(recipe)


if __name__ == "__main__":
    cli()
