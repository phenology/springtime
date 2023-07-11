FROM --platform=amd64 mambaorg/micromamba:1.4.6

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && \
    micromamba clean --all --yes

# Make sure the conda environment is activated for the remaining build
# instructions below
ARG MAMBA_DOCKERFILE_ACTIVATE=1  # (otherwise python will not be found)

WORKDIR /repo
COPY . .
RUN pip install .[r]

RUN Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
RUN Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
RUN Rscript -e 'install.packages(c("daymetr", "MODISTools", "phenocamr", "rnpn"), repos = "http://cran.us.r-project.org")'

# To add to an existing jupyterhub
RUN pip install ipykernel
