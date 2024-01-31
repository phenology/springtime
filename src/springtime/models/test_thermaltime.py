from thermaltime import ThermalTime

from sklearn.utils.estimator_checks import parametrize_with_checks
import numpy as np

thermaltime = ThermalTime._core_model


@parametrize_with_checks([ThermalTime(niter=100),])
def test_sklearn_compatible_estimator(estimator, check):
    check(estimator)


def test_1d_base_case():
    # 10 degrees every day:
    X_test = np.ones(365) * 10
    assert thermaltime(X_test) == 50


def test_late_growing_season():
    # If the growing season starts later, the spring onset is later as well.
    X_test = np.ones(365) * 10
    assert thermaltime(X_test, t1=11) == 60


def test_higher_threshold():
    # If the total accumulated forcing required is higher, spring onset is later.
    X_test = np.ones(365) * 10
    assert thermaltime(X_test, F=600) == 60


def test_exclude_cold_days():
    # If some days are below the minimum growing T, spring onset is later.
    X_test = np.ones(365) * 10
    X_test[[1, 4, 8, 12, 17, 24, 29, 33, 38, 42]] = 3
    assert thermaltime(X_test) == 60


def test_lower_temperature_threshold():
    # If the minimum growing T is lower, fewer days are exluded. However, the
    # accumulated temperature rises more slowly.
    X_test = np.ones(365) * 10

    X_test[[1, 4, 8, 12, 17, 24, 29, 33, 38, 42]] = 5
    assert thermaltime(X_test, T=2) == 55


def test_2d():
    # Should be able to predict for multiple samples at once
    X_test = np.ones((10, 365)) * 10
    expected = np.ones(10) * 50
    result = thermaltime(X_test)
    assert np.all(result == expected)
