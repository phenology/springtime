# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Literal
from pydantic import BaseModel
from pycaret.utils.generic import MLUsecase
import pycaret.regression


RegressionSetup = dict[str, Any]

RegressionCreateModel = dict[str, Any]

RegressionCompareModels = dict[str, Any]

class ClassificationExperiment(BaseModel):
    experiment_type: Literal['classification']
    # TODO implement

class RegressionExperiment(BaseModel):
    experiment_type: Literal['regression']

    setup: RegressionSetup
    create_model: RegressionCreateModel | None
    compare_models: RegressionCompareModels | None

# TODO: try to use pydantic validatedfunction to dynamically construct pydantic
# models based on pycaret function signatures
