import numpy as np

from .phenology_model import PhenologyModel


def predict_thermaltime(X, t1: int = 1, T: int = 5, F: int = 500):
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


thermaltime = PhenologyModel(
    predict=predict_thermaltime,
    params_names=("t1", "T", "F"),
    params_defaults=(1, 5, 500),
    params_bounds=((-67, 298), (-25, 25), (0, 1000)),
)
