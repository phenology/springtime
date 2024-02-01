from sklearn.base import (
    BaseEstimator,
    RegressorMixin,
    check_is_fitted,
)

from numpy.typing import ArrayLike
from scipy.optimize import basinhopping

from .phenology_models import PHENOLOGY_MODELS
from .loss_functions import LOSS_FUNCTIONS


class BasinHopper(RegressorMixin, BaseEstimator):
    """SKlearn wrapper around the scipy basinhopper algorithm.

    Can fit a model of the form f(X, *params) given the parameters ranges and
    default values.
    """

    def __init__(
        self,
        niter=50000,
        T=0.5,
        stepsize=0.5,
        disp=False,
        minimizer_method="L-BFGS-B",
        core_model="thermaltime",
        loss_function="RMSE",
    ):
        self.niter = niter
        self.T = T
        self.stepsize = stepsize
        self.disp = disp
        self.minimizer_method = minimizer_method
        self.loss_function = loss_function
        self.core_model = core_model

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

        core_model = PHENOLOGY_MODELS[self.core_model]
        loss_function = LOSS_FUNCTIONS[self.loss_function]

        # Perform the fit
        bh = basinhopping(
            loss_function,
            x0=core_model.params_defaults,
            niter=self.niter,
            T=self.T,
            stepsize=self.stepsize,
            disp=self.disp,
            minimizer_kwargs={
                "method": self.minimizer_method,
                "args": (core_model.predict, X, y),
                "bounds": core_model.params_bounds,
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
        check_is_fitted(self, "core_params_")

        core_model = PHENOLOGY_MODELS[self.core_model]
        return core_model.predict(X, *self.core_params_)

    def _more_tags(self):
        # Pass checks related to performance of model as the thermaltime model
        # cannot be expected to perform well for random data.
        # https://scikit-learn.org/stable/developers/develop.html#estimator-tags
        return {"poor_score": True}
