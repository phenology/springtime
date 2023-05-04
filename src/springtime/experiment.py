# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from importlib import import_module
from typing import Any, Literal, Sequence
from pydantic import BaseModel


RegressionSetup = dict[str, Any]

RegressionCreateModel = dict[str, Any]

RegressionCompareModels = dict[str, Any]


class ClassificationExperiment(BaseModel):
    experiment_type: Literal["classification"]
    # TODO implement


class RegressionExperiment(BaseModel):
    experiment_type: Literal["regression"]

    setup: RegressionSetup
    """See
    https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.setup
    for available arguments. 
    """
    init_kwargs: dict[str, dict[str, Any]] | None
    """Init kwargs for models that are not buildin pycaret models."""
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


# TODO: try to use pydantic validatedfunction to dynamically construct pydantic
# models based on pycaret function signatures


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
    if name in buildin_models:
        return name
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
