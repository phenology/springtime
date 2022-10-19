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
import pyPhenology
import numpy as np
from pyPhenology.models import utils as models_utils
from sklearn import linear_model
from sklearn.model_selection import ShuffleSplit


# data utilities
def load_data(dataset, phenophase):
    """Load test data from pyPhenology package.

    Datasets are available with pyPhenology package.
    Args:
        dataset : str
            Name of the test dataset

        phenophase : str
            Name of the phenophase. Either 'budburst','flowers', 'colored_leaves',
            or 'all'.

    Returns:
        obs, predictors : tuple
            Pandas dataframes of phenology observations
            and associated temperatures.
    """
    return pyPhenology.utils.load_test_data(name=dataset, phenophase=phenophase)


def split_data(data, method_name):
    """Split arrays into random train and test subsets.

    Methods are available with sklearn package.
    Args:
        dataset : array
            data to be splitted.

        method_name : str
            one of the sklearn train test split functions.

    Returns:
        test, train : tuple
            arrays for testing and training.
    """
    if method_name=="ShuffleSplit":
        # TODO pass arguments to function
        rs = ShuffleSplit(n_splits=2, test_size=.25, random_state=2)
        for train_index, test_index in rs.split(data):
            return data.iloc[test_index], data.iloc[train_index]

    print(f"Unsupported train_test strategy {method_name}")
    return None
            

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
def check_model_name(model_name):
    models_source = {
        "ThermalTime": "pyPhenology.primary_model",
        "Linear": "pyPhenology.primary_model",
        "LinearRegression": "sklearn.linear_model",
    }
    if models_source.get(model_name):
        return models_source.get(model_name)
    raise ValueError(f"Unsupported model {model_name}.")

    
def select_model(model_name):
    if check_model_name(model_name) == "pyPhenology.primary_model":
        Model = pyPhenology.utils.load_model(model_name)
    elif check_model_name(model_name) == "sklearn.linear_model":
        Model = getattr(linear_model, model_name)
    return Model()
    

# workflow utilities
def define_metrics(metric_name):
    if metric_name=="RMSE":
        return rmse
    print(f"Unsupported metric method {metric_name}")
    return None


def build_workflow(options, name):
    # TODO validate_options(options)
    workflow = {}
    workflow["observations"], workflow["predictors"] = load_data(
        options["dataset"], options["phenophase"]
        )

    workflow["observations_test"], workflow["observations_train"] = split_data(
        workflow["observations"], options["train_test_strategy"]
        )
    
    workflow["metric_func"] = define_metrics(options["metric_name"])
    workflow["model"] = select_model(options["model_name"])

    workflow["name"] = name
    workflow["options"] = options
    # TODO save_workflow(workflow)
    return workflow


def run_pyPhenology_workflow(workflow):
    model = workflow["model"]
    Y = workflow["observations_train"]
    X = workflow["predictors"]

    # TODO pass extra arguments to fit()
    model.fit(Y, X, optimizer_params='practical')

    Y = workflow["observations_test"]
    Y_predict = model.predict(Y, X)

    Y = workflow["observations_test"].doy.values
    if metric_func:= workflow["metric_func"]:
        metric_val = metric_func(Y, Y_predict)
    return model, Y_predict, metric_val


def run_sklearn_workflow(workflow):
    model = workflow["model"]
    
    # organize data for fitting
    Y, X, _ = models_utils.misc.temperature_only_data_prep(
        workflow["observations_train"], workflow["predictors"], for_prediction=False
        )

    # TODO pass extra arguments to fit()
    model.fit(X.T, Y)

    # organize data for prediction
    X, _ = models_utils.misc.temperature_only_data_prep(
        workflow["observations_test"], workflow["predictors"], for_prediction=True)
    Y_predict = model.predict(X.T)

    Y = workflow["observations_test"].doy.values
    if metric_func:= workflow["metric_func"]:
        metric_val = metric_func(Y, Y_predict)
    return model, Y_predict, metric_val


def run_workflow(workflow):
    # TODO validate_workflow(workflow)

    model_name = workflow["options"]["model_name"]

    if check_model_name(model_name) == "pyPhenology.primary_model":
        fitted_model, predictions, metric_value = run_pyPhenology_workflow(workflow)
       
    elif check_model_name(model_name) == "sklearn.linear_model":
        fitted_model, predictions, metric_value= run_sklearn_workflow(workflow)
    
    workflow.update({
        "fitted_model": fitted_model,
        "predictions": predictions,
        "metric_value": metric_value
    })

    return workflow
    