import numpy as np


def rmse_loss(params, f, X, y):
    """RMSE loss between thermaltime predictions and observations.

    f is a prediction model of the form f(X, *params)
    params is a tuple with the parameters to f
    """
    y_pred = f(X, *params)
    sq_err = (y_pred - y) ** 2
    return np.mean(sq_err) ** 0.5


LOSS_FUNCTIONS = {
    "RMSE": rmse_loss,
}
