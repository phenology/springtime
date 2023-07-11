# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from importlib import import_module
import logging
from pathlib import Path
from typing import Any, Literal, Sequence

from pydantic import BaseModel
from pycaret import time_series

from springtime.NestedFitRegressionExperiment import NestedFitRegressionExperiment

logger = logging.getLogger(__name__)


class CaretExperiment(BaseModel):
    """Base class for pycaret experiments."""

    experiment_type: str
    init_kwargs: dict[str, dict[str, Any]] | None
    """Init kwargs for models that are not buildin pycaret models."""


class TSForecastingExperiment(CaretExperiment):
    experiment_type: Literal["time_series"]
    setup: dict[str, Any]
    """See
    https://pycaret.readthedocs.io/en/latest/api/time_series.html#pycaret.time_series.setup
    for available arguments.
    """
    create_model: dict[str, Any] | None
    """See
    https://pycaret.readthedocs.io/en/latest/api/time_series.html#pycaret.time_series.create_model
    for available arguments.
    """
    compare_models: dict[str, Any] | None
    """See
    https://pycaret.readthedocs.io/en/latest/api/time_series.html#pycaret.time_series.compare_models
    for available arguments.
    """
    plots: Sequence[str] | None
    """See
    https://pycaret.readthedocs.io/en/latest/api/time_series.html#pycaret.time_series.plot_model
    for available plot names."""

    def run(self):
        return time_series.TSForecastingExperiment()


# TODO: try to use pydantic validatedfunction to dynamically construct pydantic
# models based on pycaret function signatures
RegressionSetup = dict[str, Any]
RegressionCreateModel = dict[str, Any]
RegressionCompareModels = dict[str, Any]


class RegressionExperiment(CaretExperiment):
    experiment_type: Literal["regression"]

    setup: RegressionSetup
    """See
    https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.setup
    for available arguments.
    """
    create_model: RegressionCreateModel | None
    """See
    https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.create_model
    for available arguments.
    """
    compare_models: RegressionCompareModels | None
    """See
    https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.compare_models
    for available arguments.
    """
    plots: Sequence[str] | None
    """See
    https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.plot_model
    for available plot names."""

    def run(self):
        return NestedFitRegressionExperiment()


def materialize_estimators(
    names: Sequence[str] | None,
    buildin_models: Sequence[str],
    init_kwargs: dict[str, dict[str, Any]] | None,
):
    """Materialize pycaret models from model names.

    Args:
        model_names (list[str]): list of model names
        buildin_models (list[str]): list of buildin pycaret models
        init_kwargs (dict[str,dict[str,Any]]): init kwargs for models

    Returns:
        list[str]: list of model names that can be materialized
    """
    if names is None:
        return None
    return [
        materialize_estimator(model_name, buildin_models, init_kwargs)
        for model_name in names
    ]


def materialize_estimator(name, buildin_models, init_kwargs):
    """Materialize model from model name.

    Pycaret understands some strings, but not all. This function materializes
    a model based on the string in the `<module>.<class>` format.
    """
    if name in buildin_models:
        return name
    elif "." not in name:
        raise ValueError(
            f"{name} is not a valid model name! \
            Choose one of {list(buildin_models)} \
            or '<module>.<class>' like 'sklearn.svm.SVR'"
        )
    else:
        module_name, class_name = name.rsplit(".", 1)
        module = import_module(module_name)
        model_class = getattr(module, class_name)
        if init_kwargs and name in init_kwargs:
            kwargs = init_kwargs[name]
            model = model_class(**kwargs)
        else:
            model = model_class()
        return model


def create_model(s, output_dir, cm, init_kwargs, plots):
    raw_estimator = cm["estimator"]
    cm["estimator"] = materialize_estimator(
        name=raw_estimator,
        buildin_models=s.models().index,
        init_kwargs=init_kwargs,
    )

    model = s.create_model(**cm)
    save_model(s, model, output_dir, raw_estimator)
    plots_model(plots, s, model, output_dir, raw_estimator)

    if "cross_validation" in cm and cm["cross_validation"]:
        save_leaderboard(s, output_dir)


def compare_models(s, output_dir, cm, init_kwargs, plots):
    cm["include"] = materialize_estimators(
        names=cm.get("include"),
        buildin_models=s.models().index,
        init_kwargs=init_kwargs,
    )

    if cm["n_select"]:
        best_models = s.compare_models(**cm)
        # if only single model succeeeded then it is returned not as a list but itself
        if not isinstance(best_models, list):
            best_models = [best_models]
        for i, model in enumerate(best_models):
            name = f"best#{i}"
            save_model(s, model, output_dir, name=name)
            plots_model(plots, s, model, output_dir, name)
    else:
        best_model = s.compare_models(**cm)
        name = "best"
        save_model(s, best_model, output_dir, name)
        plots_model(plots, s, best_model, output_dir, name)

    if cm["cross_validation"]:
        save_leaderboard(s, output_dir)


def plot_model(experiment, model, model_name, plot_name, output_dir):
    plot_fn_in_cwd = Path(experiment.plot_model(model, plot=plot_name, save=True))
    plot_fn = output_dir / f"{model_name} {plot_fn_in_cwd.name}"
    plot_fn_in_cwd.rename(plot_fn)
    logger.warning(f"Saving {plot_name} plot to {plot_fn}")


def plots_model(plots, experiment, model, output_dir, model_name):
    if plots is None:
        return

    for plot_name in plots:
        plot_model(experiment, model, model_name, plot_name, output_dir)


def save_leaderboard(s, output_dir):
    df = s.get_leaderboard()
    leaderboard_fn = output_dir / "leaderboard.csv"
    logger.warning(f"Saving leaderboard to {leaderboard_fn}")
    df.drop("Model", axis="columns").to_csv(leaderboard_fn)


def save_model(s, model, output_dir, name):
    model_fn = output_dir / name
    logger.warning(f"Saving model to {model_fn}")
    s.save_model(model, model_fn)
