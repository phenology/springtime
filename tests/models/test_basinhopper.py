from sklearn.utils.estimator_checks import parametrize_with_checks

from springtime.models.basinhopper import BasinHopper


@parametrize_with_checks(
    [
        BasinHopper(niter=100),
    ]
)
def test_sklearn_compatible_estimator(estimator, check):
    check(estimator)
