import numpy as np
from sklearn.base import (
    BaseEstimator,
    RegressorMixin,
    check_is_fitted,
)
from numpy.typing import ArrayLike
from scipy.optimize import basinhopping


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

class ThermalTime(RegressorMixin, BaseEstimator):
    """Thermal Time Model

    The classic growing degree day model using a fixed temperature threshold
    above which forcing accumulates.

    This implementation uses scipy's basinhopper optimization algorithm for
    fitting the model.
    """
    _core_params_names = ('t1', 'T', 'F')
    _core_params_defaults = (1, 5, 500)
    _core_params_bounds = ((-67, 298), (-25, 25), (0, 1000))

    def __init__(self, niter= 50000, T= 0.5, stepsize= 0.5, disp=False, minimizer_method="L-BFGS-B", loss_function='RMSE'):
        self.niter = niter
        self.T = T
        self.stepsize = stepsize
        self.disp = disp
        self.minimizer_method = minimizer_method
        self.loss_function = loss_function

    @staticmethod
    def _core_model(X, t1: int = 1, T: int = 5, F: int = 500):
        """Make prediction with the thermaltime model.

        Args:
            X: array-like, shape (n_samples, n_features).
                Daily mean temperatures for each unique site/year (n_samples) and for
                each DOY (n_features). The first feature should correspond to
                the first DOY, and so forth up to (max) 366.
            t1: The DOY at which forcing accumulating beings (should be within [-67,298])
            T: The threshold above which forcing accumulates (should be within [-25,25])
            F: The total forcing units required (should be within [0,1000])
        """
        # This allows us to pass both 1D and 2D arrays of temperature
        # Copying X to safely modify it later on (may not be necessary, but readable)
        X_2d = np.atleast_2d(np.copy(X))

        # DOY starts at 1, where python array index start at 0
        # TODO: Make this an optional argument?
        doy = np.arange(X_2d.shape[1]) + 1

        # Exclude days before the start of the growing season
        X_2d[:, doy < t1] = 0

        # Exclude days with temperature below threshold
        X_2d[X_2d < T] = 0

        # Accumulate remaining data
        S = np.cumsum(X_2d, axis=-1)

        # Find first entry that exceeds the total forcing units required.
        doy = np.argmax(S > F, axis=-1)

        return doy

    # TODO: consider adding DOY index series to fit/predict as optional argument
    def fit(self, X: ArrayLike, y: ArrayLike):
        """Fit the model to the available observations.

        Parameters:
            X: 2D Array of shape (n_samples, n_features).
                Daily mean temperatures for each unique site/year (n_samples) and
                for each DOY (n_features). The first feature should correspond to
                the first DOY, and so forth up to (max) 366.
            y: 1D Array of length n_samples
                Observed DOY of the spring onset for each unique site/year.

        Returns:
            Fitted model
        """
        X, y = self._validate_data(X, y)
        # TODO: check additional assumptions about input

        loss_function = LOSS_FUNCTIONS[self.loss_function]

        # Perform the fit
        bh = basinhopping(
            loss_function,
            x0=self._core_params_defaults,
            niter = self.niter,
            T = self.T,
            stepsize = self.stepsize,
            disp = self.disp,
            minimizer_kwargs={
                "method": self.minimizer_method,
                "args": (self._core_model, X, y),
                "bounds": self._core_params_bounds,
            },
        )

        # Store the fitted parameters
        self.core_params_ = bh.x

        return self

    def predict(self, X: ArrayLike):
        """Predict values of y given new predictors

        Parameters:
            X: array-like, shape (n_samples, n_features).
               Daily mean temperatures for each unique site/year (n_samples) and
               for each DOY (n_features). The first feature should correspond to
               the first DOY, and so forth up to (max) 366.

        Returns:
            y: array-like, shape (n_samples,)
               Predicted DOY of the spring onset for each sample in X.
        """
        X = self._validate_data(X)
        check_is_fitted(self, 'core_params_')

        return self._core_model(X, *self.core_params_)





