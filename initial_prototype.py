""" Model selection via RMSE
This scripts runs one of the examples of pyPhenology.
https://pyphenology.readthedocs.io/en/master/examples.html#model-selection-via-aic

following the workflow defined in https://github.com/phenology/phenologyX/issues/4

Required dependencies:
Python
scipy
numpy
pandas
joblib
pyPhenology
scikit-learn # for ML methods
plotnine # for visualization

usage:
python initial_prototype.py

output:
model ThermalTime got an rmse of 5.513619500836088
model Alternating got an rmse of 4.1952353926806065
model Linear got an rmse of 9.754014106073047
model LinearRegression got an rmse of 6.797252144708919
Best model: ThermalTime

"""
from pyPhenology import utils
import numpy as np
import pandas as pd
from pyPhenology.models import utils as models_utils
from sklearn import linear_model

# 1 and 2 load observations and predictors
dataset='vaccinium'
phenophase='flowers'
observations, predictors = utils.load_test_data(name=dataset, phenophase=phenophase)

# 3 Preprocesses the data

# 4 Define train/test strategy
from sklearn.model_selection import ShuffleSplit

rs = ShuffleSplit(n_splits=2, test_size=.25, random_state=2)
for train_index, test_index in rs.split(observations):
    observations_test = observations.iloc[test_index]
    observations_train = observations.iloc[train_index]


# 5 basic/benchmark/reference models
models_to_test = ["ThermalTime", "Linear"]

# 6 ML models
models_to_test.append("LinearRegression")

# 7 Model selection strategy
# RMSE
def rmse(obs, pred):
    return np.sqrt(((pred - obs)**2).mean())

# Run the workflow
MODELS_SOURCE = {"ThermalTime": "pyPhenology.primary_model",
                 "Linear": "pyPhenology.primary_model",
                 "LinearRegression": "sklearn.linear_model",
             }
    
def fit_model(model_name, X, Y):
    model_source = MODELS_SOURCE.get(model_name, False)
    if model_source == "pyPhenology.primary_model":
        Model = utils.load_model(model_name)
        model = Model()
        model.fit(Y, X, optimizer_params='practical')
    elif model_source == "sklearn.linear_model":
        # select the model
        Model = getattr(linear_model, model_name)
        model = Model()
        # prepare data
        Y, X, _ = models_utils.misc.temperature_only_data_prep(Y, X, for_prediction=False)
        model.fit(X.T, Y)
    else:
        print(f"Unsupported model {model_name}.")
        model = None
    return model
        

def predict(model, X, Y):
    model_source = MODELS_SOURCE.get(model_name, False)
    if model_source == "pyPhenology.primary_model":
        return model.predict(Y, X)
    
    elif model_source == "sklearn.linear_model":
        # prepare data
        X, _ = models_utils.misc.temperature_only_data_prep(Y, X, for_prediction=True)
        return model.predict(X.T)
    
    else:
        print(f"Unsupported model {model_name}.")
        return None
    
best_rmse = np.Inf
best_base_model = None
best_base_model_name = None

results = {
    "dataset": [],
    "phenophase": [],
    "model": [],
    "rmse": [],
}

for model_name in models_to_test:
    if fitted_model:= fit_model(model_name, predictors, observations_train):
        predictions = predict(fitted_model, predictors, observations_test)
    
    model_rmse = rmse(observations_test.doy.values, predictions)
    
    if model_rmse < best_rmse:
        best_model_name = model_name
        best_rmse = model_rmse
        
    print(f'model {model_name} got an rmse of {model_rmse}')
    
    results['rmse'].append(model_rmse)
    results['dataset'].append(dataset)
    results['phenophase'].append(phenophase)
    results['model'].append(model_name)
    
print(f'Best model: {best_model_name}')