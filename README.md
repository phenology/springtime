# Springtime
Spatiotemporal phenology research with interpretable models.

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
```

Feel free to propose modifications to this structure by [opening an
issue](https://github.com/phenology/springtime/issues/new).

## Environment

We recommend setting up a rich working environment, e.g. using
[mambaforge](https://mamba.readthedocs.io/en/latest/installation.html#installation):

```
mamba create -n springtime python=3 jupyterlab numpy pandas xarray matplotlib cartopy requests pytest isort black scipy joblib scikit-learn seaborn
```

We anticipate that parts of this repository might turn into self-contained
packages. At that stage we will clarify the exact reqruiements in those
packages.
