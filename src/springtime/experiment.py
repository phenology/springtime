# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

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
