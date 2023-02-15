# Springtime
Spatiotemporal phenology research with interpretable models.

Example task: predict the day of first bloom of the common
lilac given indirect observations (e.g. satellite data) and/or
other indicators (e.g. sunshine and temperature).

![illustration_example_use_case](illustration.png)

## Project

Built with [hatch](https://hatch.pypa.io/latest/)

```bash
# Clone the repo
git clone  git@github.com:phenology/springtime
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

## R dependencies

Some datasets use [R](https://www.r-project.org/) libraries.

The R dependencies can be installed from R shell with
```R
if(!require(devtools)){install.packages(devtools)}
devtools::install_github("bluegreen-labs/phenor@v1.3.1")
install.packages("daymetr")
```

To complete installation you might need to install some OS dependencies.

The installation of the dependencies of the R dependencies can be cumbersome (takes a long time, lots of compilation with obscure errors, requires root access, etc.). To simplify installation of the dependencies of the R depencencies you can create a Anaconda environment using
 [Mamba forge](https://github.com/conda-forge/miniforge#mambaforge) with:

```shell
mamba env create --file environment.yml
conda activate springtime

# Install R dependencies with
R CMD INSTALL .

# Install Python code with
hatch env create
```
