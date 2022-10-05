""" Model selection via AIC
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

usage:
python initial_prototype.py

output:
model ThermalTime got an aic of 55.51000634199631
model Alternating got an aic of 60.45760650906022
Best model: ThermalTime

"""
from pyPhenology import utils
import numpy as np

# 1 and 2 load observations and predictors
observations, predictors = utils.load_test_data(name='vaccinium', phenophase='budburst')

# 3 Preprocesses the data

# 4 Define train/test strategy
observations_test = observations[:10]
observations_train = observations[10:]

# 5 basic/benchmark/reference models
models_to_test = ['ThermalTime', 'Alternating']

# 6 ML models

# 7 Model selection strategy
# AIC based off mean sum of squares
def aic(obs, pred, n_param):
    return len(obs) * np.log(np.mean((obs - pred)**2)) + 2*(n_param + 1)

# Run the workflow
best_aic=np.inf
best_base_model = None
best_base_model_name = None

for model_name in models_to_test:
    Model = utils.load_model(model_name)
    model = Model()
    model.fit(observations_train, predictors, optimizer_params='practical')
    
    model_aic = aic(obs = observations_test.doy.values,
                    pred = model.predict(observations_test,predictors),
                    n_param = len(model.get_params()))
    
    if model_aic < best_aic:
        best_model = model
        best_model_name = model_name
        best_aic = model_aic
        
    print('model {m} got an aic of {a}'.format(m=model_name,a=model_aic))

print('Best model: {m}'.format(m=best_model_name))