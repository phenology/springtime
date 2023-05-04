# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0
from sklearn.svm import SVR
from springtime.experiment import materialize_estimator


def test_materialize_estimator():
    result = materialize_estimator(
        "sklearn.svm.SVR", ["lr"], {"sklearn.svm.SVR": {"kernel": "linear"}}
    )

    expected = SVR(kernel="linear")

    # sklearn does not have __eq__ so use str() to compare
    assert str(result) == str(expected)
