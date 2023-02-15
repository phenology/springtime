
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
install.packages("daymetr")
```

## Install R and python with conda

```bash
# Create a basic conda environment with R and Python
mamba create -c conda-forge -n springtime python=3.9 r-base r-devtools  # TODO: try use r-essentials instead

# Install r-deps from command line (how to make this simpler?)
Rscript -e 'devtools::install_github("bluegreen-labs/phenor")'

# Install python package and verify installation
pip install git+https://github.com/phenology/springtime.git
python -m r2py.situation
> ...
> Calling `R RHOME`: /home/peter/miniconda3/envs/springtime/lib/R
> Environment variable R_LIBS_USER: None
> ...

# Or use hatch
pip install hatch
hatch env create
hatch run python -m rpy2.situation
```