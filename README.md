# Springtime
Spatiotemporal phenology research with interpretable models.

Phenology is a scientific discipline in which we study the lifecycle of plants
and animals. In the Springtime project, we aim to develop (Machine Learning)
models for the occurence of phenological events, such as the blooming of plants.
Since there is a variety of data sources, a substantial part of this project
focuses on data retrieval and pre-processing as well.

We try to use existing tools as much as possible. At the same time, working with
many different datasets and a variety of tools to download them, project folders
and code organization can quickly get messy. That's why we focus heavily on
streamline our workflows, such that you can always execute them with a single
command. We use standardized locations for storing raw and intermediate data,
and a standardized "recipe" format to define the steps in our workflows.


## Installation

To install springtime in your current environment, type

```bash
pip install git+https://github.com/phenology/springtime.git
```

## R dependencies

Some datasets use [R](https://www.r-project.org/) libraries.

The R dependencies can be installed from R shell with
```R
if(!require(devtools)){install.packages(devtools)}
devtools::install_github("bluegreen-labs/phenor@v1.3.1")
```

To complete installation you might need to install some OS dependencies.

## Detailed info

## Example task:

Predict the day of first bloom of the common lilac given indirect observations
(e.g. satellite data) and/or other indicators (e.g. sunshine and temperature).

![illustration_example_use_case](illustration.png)

## Scientific recipes (workflows)

Workflows can be written in a nice and readable format, e.g.

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

Such a recipe can then be executed with a single command line call:

```bash
springtime run recipe_syringa.yaml
```

## Python API

Springtime is written in Python (with parts in R) and can also be used in an
interactive (IPython/Jupyter) session. For example:

```Python
from springtime.datasets.PEP725Phenor import PEP725Phenor
dataset = PEP725Phenor(species='Syringa vulgaris')
dataset.download()
df = dataset.load()
```

## Developers

This package is built with [hatch](https://hatch.pypa.io/latest/).

```bash
# Clone the repo
git clone git@github.com:phenology/springtime
cd springtime

# Create development environment for springtime
hatch env create

# Enter/activate development environment
hatch shell
springtime --help
exit  # to get out/deactivate

# Alternatively, use hatch run to execute command in default env
hatch run springtime --help

# Testing etc
hatch run pytest
hatch run isort src tests
hatch run black src tests
```

## Install on CRIB or other managed JupyterHub service

If you want to run or develop springtime using JupyterHub on a machine that you
don't manage, you can add your own environment by making a new (conda or
virtualenv) environment, and adding it to the Jupyter kernelspec list. To this end:

```bash
# 1. Make sure you have mamba
which mamba  # should return a path

# 2. Create new environment
mamba create -n springtime python=3.9 ipykernel

# 3. Activate the environment
mamba activate springtime

# 4a. Install default springtime inside the environment
pip install git+https://github.com/phenology/springtime.git

# 4b. Developer installation
git clone git@github.com:phenology/springtime
cd springtime
pip install -e .

# 5. Possibly, use a custom start-kernel.sh script
# See the instructions here: https://github.com/ESMValGroup/ESMValTool-JupyterLab#using-a-custom-kernel-script

# 6. Possibly, if there is no, or you cannot use the default system R, you can
#    install it inside the conda environment:
mamba install r-base

# Enter an interactive R shell
R

# Install springtime dependencies inside the R shell
if(!require(devtools)){install.packages(devtools)}
devtools::install_github("bluegreen-labs/phenor@v1.3.1")
```
