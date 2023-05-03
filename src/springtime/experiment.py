# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel
from pydantic.decorator import ValidatedFunction
from pycaret.utils.generic import MLUsecase
import pycaret
import pycaret.regression


RegressionSetup = ValidatedFunction(
    pycaret.regression.RegressionExperiment.setup, None
).model

RegressionCreateModel = ValidatedFunction(
    pycaret.regression.RegressionExperiment.create_model,
    config={"arbitrary_types_allowed": True},
).model

RegressionCompareModels = ValidatedFunction(
    pycaret.regression.RegressionExperiment.compare_models,
    config={"arbitrary_types_allowed": True},
).model


class RegressionExperiment(BaseModel):
    type: MLUsecase.REGRESSION

    setup: RegressionSetup
    create_model: RegressionCreateModel | None
    compare_models: RegressionCompareModels | None


# TODO: try to use pydantic validatedfunction to dynamically construct pydantic
# models based on pycaret function signatures
