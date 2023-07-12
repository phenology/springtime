# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from tempfile import gettempdir
from typing import Dict, Optional

import click
import pandas as pd
import geopandas as gpd
import yaml
from pydantic import BaseModel, Field, validator

from springtime.datasets import Datasets
from springtime.experiment import (
    RegressionExperiment,
    TSForecastingExperiment,
    compare_models,
    create_model,
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


class DerivedFeatures(BaseModel):
    """Derived features to add to the data for experiment."""

    longitude: bool = False
    """Add longitude as a feature if True."""
    latitude: bool = False
    """Add latitude as a feature if True."""


class Preparation(BaseModel):
    """Data preparation.

    Data preparation done
    after data from all the datasets has been joined together
    and before the data is passed to the experiment and save as data.csv.
    """

    dropna: bool = True
    """Drop rows with missing values if True."""
    derived: DerivedFeatures = DerivedFeatures()

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.dropna:
            df.dropna(inplace=True)
        if self.derived.longitude:
            df2 = df.reset_index()
            longitudes = gpd.GeoSeries(df2.geometry).x
            df2["longitude"] = longitudes
            df = df2.set_index(["year", "geometry"])
        if self.derived.latitude:
            df2 = df.reset_index()
            latitudes = gpd.GeoSeries(df2.geometry).y
            df2["latitude"] = latitudes
            df = df2.set_index(["year", "geometry"])
        return df


class Workflow(BaseModel):
    datasets: Dict[str, Datasets] = {}
    preparation: Preparation = Preparation()
    experiment: RegressionExperiment | TSForecastingExperiment | None = Field(
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
        if len(others) == 1:
            df = others[0]
        else:
            main_df = others.pop(0)
            df = main_df.join(others, how="outer")
        df = self.preparation.prepare(df)

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

        s = self.experiment.run()
        # TODO check rest of code also works for time series, not just regression

        s.setup(df, **self.experiment.setup)

        output_dir = self.session.output_dir
        if self.experiment.create_model:
            create_model(
                s,
                output_dir,
                self.experiment.create_model,
                self.experiment.init_kwargs,
                self.experiment.plots,
            )

        if self.experiment.compare_models:
            compare_models(
                s,
                output_dir,
                self.experiment.compare_models,
                self.experiment.init_kwargs,
                self.experiment.plots,
            )


def main(recipe):
    Workflow.from_recipe(recipe).execute()


@click.command
@click.argument("recipe")
def cli(recipe):
    main(recipe)


if __name__ == "__main__":
    cli()
