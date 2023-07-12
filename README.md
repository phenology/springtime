<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

[![Documentation Status](https://readthedocs.org/projects/springtime/badge/?version=latest)](https://springtime.readthedocs.io/en/latest/?badge=latest)
[![RSD](https://img.shields.io/badge/RSD-springtime-blue)](https://research-software-directory.org/software/springtime)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7835299.svg)](https://doi.org/10.5281/zenodo.7835299)


<!--intro-start-->
# Springtime

The Springtime Python package helps to streamline workflows for doing machine
learning with phenological datasets.

Phenology is the scientific discipline in which we study the lifecycle of plants
and animals. A common objective is to develop (Machine Learning) models that can
explain or predict the occurrence of phenological events, such as the blooming
of plants. Since there is a variety of data sources and existing tools to
retrieve and analyse phenology data, it is easy to get lost and disorganized.

With Springtime, we aim to provide a more streamlined workflow for working with
a variety of datasets and (ML) models. You can run Springtime as a command line
tool in a terminal or use it as a Python library e.g. in a Jupyter notebook.

<!--intro-end-->

For more information and instruction, please refer to the
[documentation](https://springtime.readthedocs.io/)


<!--illustration-start-->
## Example task

Predict the day of first bloom of the common lilac given indirect observations
(e.g. satellite data) and/or other indicators (e.g. sunshine and temperature).

![illustration_example_use_case](docs/illustration.png)
<!--illustration-end-->

## Usage
<!--usage-start-->
You can run `springtime` as a command-line tool in a terminal or use it as a python library e.g. in a Jupyter notebook. Below, we explain both CLI and API.
<!--usage-end-->

<!--recipe-start-->
### CLI to run recipes

The main component of `springtime` command-line is the recipe (scientific
workflow). A recipe is a file with `yaml` extension that includes a set of
instructions to reproduce a certain result. Recipes are written in a nice and
readable format,
e.g.

```yaml
datasets:
  npn_obs:
    dataset: RNPN
    species_ids:
      functional_type: "Deciduous broadleaf" # multiple species
    phenophase_ids:
        name: breaking leaf buds
    years: [2015, 2020]
    area:
      name: Washington
      bbox:
        [
          -124.08406940413612,
          45.50277198520317,
          -117.39620059586387,
          49.99938001479683,
        ]
  daymet:
    dataset: daymet_multiple_points
    points:
      source: npn_obs
    years: [2015, 2020]
    variables:
      - tmin
      - tmax
    resample:
      frequency: month
      operator: median
dropna: True
experiment:
  experiment_type: regression  # --> pycaret.regression.RegressionExperiment
  setup:
    ... # setup of the experiment
  init_kwargs:
    ... # intial arguments for models
  compare_models:
    include:
      - 'lr'  # linear regression
      - 'rf'  # random forest regressor
      - 'sklearn.svm.SVR'
      - 'interpret.glassbox.ExplainableBoostingRegressor'
    cross_validation: true
    n_select: 2
  plots:
    - error
    - residuals
```

Such a recipe can then be executed with `springtime` command in a terminal:

```bash
springtime model_comparison_usecase.yaml
```

We provide several "recipes" for running
[experiments](https://springtime.readthedocs.io/en/latest/experiments/).
<!--recipe-end-->

<!--api-start-->
### Python API

Springtime is written in Python (with parts in R) and can also be used in an
interactive (IPython/Jupyter) session. For example:

#### Downloading data

```Python
from springtime.datasets.PEP725Phenor import PEP725Phenor
dataset = PEP725Phenor(species='Syringa vulgaris')
dataset.download()
df = dataset.load()
```

We provide several notebooks for downloading data from various sources.
See "Datasets"
[documentation](https://springtime.readthedocs.io/en/latest/datasets/).
<!--api-end-->
