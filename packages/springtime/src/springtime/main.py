"""Run a basic workflow.

This script contains function to run a basic workflow with steps below:
- load (preprocessed) data
- split data train/test
- select a model
- fit model
- predict

A data can be loaded from pyPhenology package:
https://pyphenology.readthedocs.io/en/master/utils.html#test-data


TODO:
- write tests
- save/load workflow
- plot results
- see other todo below
- add documentations
"""
import numpy as np
import pyPhenology
from sklearn import linear_model

from . import data_utils


# metric utilities
def rmse(obs, pred):
    """Root mean square error.
    Args:
        obs : array
            array of observations.

        pred : array
            array of predictions.

    Returns:
       rmse : float
            rmse value.
    """
    return np.sqrt(((pred - obs)**2).mean())


# model utilities
def get_full_model_name(model_name):
    models_source = {
        "ThermalTime": "pyPhenology.primary_model",
        "Linear": "pyPhenology.primary_model",
        "LinearRegression": "sklearn.linear_model",
    }
    if models_source.get(model_name):
        return models_source.get(model_name)
    raise ValueError(f"Unsupported model {model_name}.")


def select_model(model_name):
    if get_full_model_name(model_name) == "pyPhenology.primary_model":
        model = pyPhenology.utils.load_model(model_name)
    elif get_full_model_name(model_name) == "sklearn.linear_model":
        model = getattr(linear_model, model_name)
    return model()


# workflow utilities
def define_metrics(metric_name):
    if metric_name == "RMSE":
        return rmse
    print(f"Unsupported metric method {metric_name}")
    return None


def build_workflow(options, name):
    # TODO validate_options(options)
    workflow = {"data": data_utils.load_data(options)}

    workflow["metric_func"] = define_metrics(options["metric_name"])
    workflow["model"] = select_model(options["model_name"])

    workflow["name"] = name
    workflow["options"] = options
    # TODO save_workflow(workflow)
    return workflow


def run_pyphenology_workflow(workflow):
    model = workflow["model"]
    y = workflow["data"]["pyPhenology"]["targets_train"]
    x = workflow["data"]["pyPhenology"]["predictors"]

    # TODO pass extra arguments to fit()
    model.fit(y, x, optimizer_params='practical')

    y = workflow["data"]["pyPhenology"]["targets_test"]
    y_predict = model.predict(y, x)

    y = workflow["data"]["train_test"]["targets_test"].to_numpy()
    if metric_func:= workflow["metric_func"]:
        metric_val = metric_func(y, y_predict)
    return model, y_predict, metric_val


def run_sklearn_workflow(workflow):
    model = workflow["model"]

    y = workflow["data"]["train_test"]["targets_train"]
    x = workflow["data"]["train_test"]["predictors_train"]
    # TODO pass extra arguments to fit()
    model.fit(x, y)

    # organize data for prediction
    x = workflow["data"]["train_test"]["predictors_test"]
    y_predict = model.predict(x)  # this is a numpy array

    y = workflow["data"]["train_test"]["targets_test"].to_numpy()
    if metric_func:= workflow["metric_func"]:
        metric_val = metric_func(y, y_predict)
    return model, y_predict, metric_val


def run_workflow(workflow):
    # TODO validate_workflow(workflow)

    model_name = workflow["options"]["model_name"]

    if get_full_model_name(model_name) == "pyPhenology.primary_model":
        fitted_model, predictions, metric_value = run_pyphenology_workflow(workflow)

    elif get_full_model_name(model_name) == "sklearn.linear_model":
        fitted_model, predictions, metric_value = run_sklearn_workflow(workflow)

    workflow.update({
        "fitted_model": fitted_model,
        "predictions": predictions,
        "metric_value": metric_value
    })

    return workflow
