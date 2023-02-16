# Install R and python with conda

```bash
# Create a basic conda environment with R and Python
mamba create -c conda-forge -n springtime python=3.9 r-base r-devtools  # TODO: try use r-essentials instead

# Install r-deps from command line (how to make this simpler?)
Rscript -e 'devtools::install_github("bluegreen-labs/phenor")'
Rscript -e 'devtools::install_github("ropensci/rppo")'
Rscript -e 'install.packages(c("daymetr", "MODISTools"))'

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