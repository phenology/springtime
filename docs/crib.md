<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

# Install on CRIB or other managed JupyterHub service

If you want to run or develop springtime using JupyterHub on a machine that you
don't manage, you can add your own environment by making a new (conda or
virtualenv) environment, and adding it to the Jupyter kernelspec list. To this end:

```bash
# 0. Start Intel x86_64 machine

# 1. Make sure you have mamba
which mamba  # should return a path

# If not install mambaforge and activate it
# Make sure to append x86_64 to installation location.

# 2. Clone springtime repo
git clone https://github.com/phenology/springtime.git
cd springtime

# 2. Create new environment
mamba env create --file environment.yml

# 3. Activate the environment
mamba activate springtime

# 4. Developer installation
pip install -e .

# 5. Add Jupyter kernel
# See the instructions here: https://github.com/ESMValGroup/ESMValTool-JupyterLab#using-a-custom-kernel-script
pip install ipykernel
python -m ipykernel install --user --name springtime_x86 --display-name="Springtime x86" --env R_LIBS_USER $CONDA_PREFIX/lib/R/library -env PATH $PATH --env GDAL_DATA $GDAL_DATA --env UDUNITS2_XML_PATH $UDUNITS2_XML_PATH --env PROJ_DATA $PROJ_DATA --env GDAL_DRIVER_PATH $GDAL_DRIVER_PATH

# 6. Install direct R dependencies
unset R_LIBS_USER
Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
Rscript -e 'install.packages("daymetr", repos = "http://cran.us.r-project.org")'
```
