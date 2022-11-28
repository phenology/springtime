# Springtime
Spatiotemporal phenology research with interpretable models.

Example task: predict the day of first bloom of the common
lilac given indirect observations (e.g. satellite data) and/or
other indicators (e.g. sunshine and temperature).

![illustration_example_use_case](illustration.png)

## Structure

This repository is structured as follows:

```
notebooks                  <-- Notebooks may be used for exploratory work
scripts                    <-- Use (Python) scripts to reproduce project outputs
packages                   <-- Reusable code snippets can be moved to, and imported from, packages
├── springtime             <-- Place project-specific snippets in a 'miscellaneous' package
└── py_ppo                 <-- Consider moving self-contained utilities/modules into their own package
data                       <-- Data directory. Excluded from version control
├── raw                    <-- Raw data retreived from source without modifying it
├── intermediate           <-- Partly processed data
└── processed              <-- Final results
Makefile                   <-- Use make to reproduce all project outputs, set up environment, run tests, etc.
LICENCE                    <-- Allow others to build upon our work
docs                       <-- Sphinx documentation - help others build upon our work
environment.yaml           <-- Keep track of, and easily (re-) install, project dependencies
```

Feel free to propose modifications to this structure by [opening an
issue](https://github.com/phenology/springtime/issues/new).

## Environment setup

The top level directory contains a file `environment.yml` which can be installed
with
[conda](https://docs.conda.io/en/latest/miniconda.html#latest-miniconda-installer-links)
or
[mamba](https://mamba.readthedocs.io/en/latest/installation.html#installation):

```
# Using conda:
conda env create

# Using mamba (newer, faster reimplementation of conda):
mamba env create
```

This will install a rich Python working environment with jupyterLab, matplotlib,
cartopy, etc. pre-installed. Moreover, this environment will install everyting
under the `packages` directory as editable packages. That means you can easily
import from these packages, but also modify them and see the changes right away.

For updating an existing environment, use

```
# conda or mamba
conda activate myenv
conda env update --prune
```
