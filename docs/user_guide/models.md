<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

## High-level ML with springtime & pycaret

Springtime executes machine learning algorithms that are supported by the
package [PyCaret](https://pycaret.readthedocs.io/). PyCaret is a Python wrapper
around several machine learning libraries and frameworks such as
[scikit-learn](https://scikit-learn.org/stable/), and
[XGBoost](https://xgboost.readthedocs.io/en/latest/).

For a ML model to work with pycaret, it needs to adhere to the sklearn reference
format. Since not all models adhere to the same data structure, we have made
modifications to MERF [mixed effects random forest
(MERF)](https://manifoldai.github.io/merf/) and
[PyPhenology](https://github.com/sdtaylor/pyPhenology). In the following
chapters we'll explain the modifications and how they enable the use of these
packages in a coherent framework.
