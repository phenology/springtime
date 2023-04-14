<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

[![Documentation Status](https://readthedocs.org/projects/springtime/badge/?version=latest)](https://springtime.readthedocs.io/en/latest/?badge=latest)
[![RSD](https://img.shields.io/badge/RSD-springtime-blue)](https://research-software-directory.org/software/springtime)

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

[Documentation](https://springtime.readthedocs.io/)

<!--installation-start-->
## Requirements

This project requires python and R. To simplify installation of the (indirect) R
depencencies you can create a Anaconda environment using [Mamba
forge](https://github.com/conda-forge/miniforge#mambaforge) from our environment file:

```shell
curl -o environment.yml https://raw.githubusercontent.com/phenology/springtime/main/environment.yml
mamba env create --file environment.yml
conda activate springtime
```

## Install Python package with dependencies

Once you have python and R, to install springtime in your current environment,
type

```bash
pip install git+https://github.com/phenology/springtime.git
```

## Install R dependencies

Some datasets use [R](https://www.r-project.org/) libraries. The R dependencies
can be installed with

```bash
Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
Rscript -e 'install.packages(c("daymetr", "MODISTools", "phenocamr", "rnpn"), repos = "http://cran.us.r-project.org")'
```


## Verify installation

```bash
python -m rpy2.situation
> ...
> Calling `R RHOME`: /home/peter/miniconda3/envs/springtime/lib/R
> Environment variable R_LIBS_USER: None
> ...
```
<!--installation-end-->

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
  syringa_filtered:
    dataset: PEP725Phenor
    species: Syringa vulgaris
    years: [2000, 2001]
    area:
      name: somewhere
      bbox: [4, 45, 8, 50]
models:
  target: day_of_first_bloom
  sklearn:
    model: sklearn.linear_model
    options: ...
```

Such a recipe can then be executed with `springtime` command in a terminal:

```bash
springtime run recipe_syringa.yaml
```

We provide several "recipes" for downloading data from various sources.
See "Datasets"
[documentation](https://springtime.readthedocs.io/en/latest/datasets/).

<!--recipe-end-->

<!--api-start-->
### Python API

Springtime is written in Python (with parts in R) and can also be used in an
interactive (IPython/Jupyter) session. For example:

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
