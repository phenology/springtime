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

# 2. Clone sprintime repo
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
python -m ipykernel install --user --name springtime_x86 --display-name="Springtime x86"

# 6. Install direct R dependencies
# Enter an interactive R shell
R

# Install springtime dependencies inside the R shell
if(!require(devtools)){install.packages(devtools)}
devtools::install_github("bluegreen-labs/phenor@v1.3.1")
install.packages(c("daymetr", "MODISTools"))
devtools::install_github("ropensci/rppo")
```
