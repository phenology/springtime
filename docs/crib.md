<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

# Install on CRIB or other managed JupyterHub service

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
install.packages(c("daymetr", "MODISTools"))
devtools::install_github("ropensci/rppo")
```