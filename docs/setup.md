<!--
SPDX-FileCopyrightText: 2023 Springtime authors

SPDX-License-Identifier: Apache-2.0
-->

# Getting started

This project requires python and R. We provide two ways to simplify the
installation: [using mamba](#installing-with-mamba) and [using docker](#installing-with-docker).

## Installing with mamba

### Create environment

Create a new Anaconda environment using [Mamba
forge](https://github.com/conda-forge/miniforge#mambaforge) and our environment
file:

```shell
curl -o environment.yml https://raw.githubusercontent.com/phenology/springtime/main/environment.yml
mamba env create --file environment.yml
conda activate springtime
```

This environment contains python, R, and some of the dependencies of our project
that are available on conda-forge.

You can verify that the environment is configured correctly by running

```bash
python -m rpy2.situation
```

If everything is okay, it should print something like:

```bash
...
Calling `R RHOME`: /home/peter/miniconda3/envs/springtime/lib/R
Environment variable R_LIBS_USER: None
...
```

### Install R dependencies

Additional [R](https://www.r-project.org/) libraries that are not available from
conda-forge need to be added manuallyL

```bash
Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
Rscript -e 'install.packages(c("daymetr", "MODISTools", "phenocamr", "rnpn"), repos = "http://cran.us.r-project.org")'
```

### Install springtime

Now, you can install springtime along with it's (python) dependencies like so:

```bash
pip install git+https://github.com/phenology/springtime.git
```

## Installing with docker

An alternative way to use springtime is via docker. We have prepared a docker
image that can be found
[here](https://github.com/phenology/springtime/pkgs/container/springtime). This
image contains the same installation as detailed above, with everything already
installed for you. To use it, you need to have docker installed on your system.

Official instructions for installing docker desktop can be found
[here](https://docs.docker.com/engine/install/). Alternatively, many third-party
instructions can be found online to install docker without docker
desktop, for example in WSL.

Once you have docker, you can pull the springtime image using

```bash
docker pull ghcr.io/phenology/springtime:latest
```

After the download completes, the image should be listed when you type `docker images`.

Now, you should be able to test your installation of springtime with:

```bash
docker run -v $PWD:/repo -v /home/peter/springtime/data:/tmp --rm springtime springtime --help
```

### Understanding the docker command

Essentially, the command above can be split into a few parts:

```
docker run <OPTIONS> springtime <COMMAND>
```

`docker run springimte` starts a container based on the `springtime` image you just pulled.
The `--rm` option makes sure it is deleted again after it is done executing
`<COMMAND>`. The `-v $PWD:/repo` tells docker to make your current working
directory (on your own system) available to the docker container. Inside the
container this directory will be available as `/repo`.

TODO: specify an additional mountpoint for data directory.

Additional options may be added to the docker command as well. For example, to
run on a macbook, we had to use `--platform linux/amd64` and specify the full
path to the container, instead of just the name:

```
docker run -v $PWD:/repo --rm  ghcr.io/phenology/springtime springtime
```

Be aware that docker containers can consume significant resources on your
system. Make sure that they're always properly removed when you're ready. You
can run `docker ps -a` to see all containers lingering around on your system,
and you can remove them with `docker rm <ID OR NAME>`.

### Running springtime through docker

The `<COMMAND>` part is similar to how you would use springtime on the command
line in the rest of the documentation. That means that the following alias would
make the "docker powered" version act as a drop-in replacement of the "local" springtime.

```bash
# By setting this alias
alias springtime="docker run -v $PWD:/repo --rm springtime springtime"

# you can now run
springtime --help

# and it will be the same as
docker run -v $PWD:/repo --rm springtime springtime --help
```

### Starting jupyter lab or IPython from the container

To start an interactive python session, you can run

```bash
docker run -v $PWD:/repo --rm -it springtime ipython
```

The `-it` makes it possible to interact with the running program, intead of
simply executing and exiting. Note that the container will terminate once you
terminate your ipython session.

TODO: add jupyter lab to container so we can use that as well


## Install on CRIB or other managed JupyterHub service

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
pip install ipykernel kernda
python -m ipykernel install --user --name springtime_x86 --display-name="Springtime x86" --env R_LIBS_USER $CONDA_PREFIX/lib/R/library
kernda -o ~/.local/share/jupyter/kernels/springtime_x86/kernel.json -o

# 6. Install direct R dependencies
unset R_LIBS_USER
Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
Rscript -e 'install.packages("daymetr", repos = "http://cran.us.r-project.org")'
```
