# Getting started

Springtime is a Python library, but it relies on R packages for some of the data
downloads. Depending on your situation, the options below may be applicable:

1. You want to [create a new (conda/mamba)
   environment](#create-a-new-condamamba-environment) that contains both R and
   python, and [install springtime](#install-springtime) and [the relevant R
   packages](#install-r-dependencies) inside it.
2. You want to [install springtime](#install-springtime) and [its R
   dependencies](#install-r-dependencies) in an existing environment where R and
   Python are already available
3. You want to [use a docker container](#use-springtime-through-docker) where
   everything is already pre-installed.
4. You want to [install springtime in an isolated environment on
   CRIB](#install-on-crib-or-other-managed-jupyterhub-service) or a similar
   JupyterHub service.

## Create a new (conda/mamba) environment

You need to have mamba/conda available on your system. To create a new environment
and activate it, run

```bash
mamba create --name springtime python="3.10"
mamba activate springtime
```

### Add pre-compiled R dependencies

In the next step, we will install the R dependencies. However, these can take a long time to compile. Alternatively, most of the dependencies are available as pre-compiled binaries via conda. You may choose to install these in your new environment by using the environment shipped with springtime.

```shell
curl -o environment.yml https://raw.githubusercontent.com/phenology/springtime/main/environment.yml
mamba env update -n springtime -f environment.yml
```

## Install springtime

Springtime is available on PyPI and can be installed with pip:

```bash
pip install springtime
```

This only installs the bare package. We provide an 'extras' option that additionally installs some ML models, ipykernel for working in notebooks. To install these extras along with springtime, run

```bash
pip install springtime[extras]
```

## Install R dependencies

R dependencies can be installed with the following:

```bash
Rscript -e 'devtools::install_github("bluegreen-labs/phenor", upgrade="never")'
Rscript -e 'devtools::install_github("ropensci/rppo", upgrade="never")'
Rscript -e 'install.packages(c("daymetr", "MODISTools", "phenocamr", "rnpn"), repos = "http://cran.us.r-project.org")'
```

## Use springtime through docker

An alternative way to use springtime is via docker. We have prepared a docker
image that can be found
[here](https://github.com/phenology/springtime/pkgs/container/springtime). This
image is based on [the official jupyter docker
stack](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-r-notebook)
with R already installed. On top of that, we have already installed springtime
with all its dependencies.

To use it, you need to have docker installed on your system.

Official instructions for installing docker desktop can be found
[here](https://docs.docker.com/engine/install/). Alternatively, many third-party
instructions can be found online to install docker without docker
desktop, for example in WSL.

Once you have docker, you can pull the springtime image using

```bash
docker pull ghcr.io/phenology/springtime:latest
```

After the download completes, the image should be listed when you type `docker images`.

### Using the docker image

You can use the docker image in two ways. The following command will start a
jupyter lab instance in which the springtime environment is installed:

```bash
docker run --rm -it -p 8888:8888 -v "${PWD}":/home/jovyan/work ghcr.io/phenology/springtime:latest
```

Alternatively, you can use the docker image to use the springtime command on
your terminal:

```bash
docker run --rm ghcr.io/phenology/springtime:latest springtime --help
```

You could also set an alias like so:

```bash
# By setting this alias
alias springtime="docker run --rm ghcr.io/phenology/springtime:latest springtime"

# you can now run
springtime --help

# which will effectively execute
docker run --rm ghcr.io/phenology/springtime:latest springtime --help
```

As such, you can effectively use the docker version of springtime exactly like
you would use a local installation.

### Customizing the docker command to your needs

Essentially, the commands above can be split into a few parts:

```bash
docker run <OPTIONS> ghcr.io/phenology/springtime:latest <COMMAND>
```

The core command `docker run ghcr.io/phenology/springtime:latest` starts a container based on the
`springtime` image you just pulled. The `--rm` option makes sure it is deleted
again after it is done executing `<COMMAND>`. The default command is to start a
jupyter lab instance, so that's what happens if you don't specify `<COMMAND>`.
Above, we executed the command `springtime --help`.

The `-v "${PWD}":/home/jovyan/work` tells docker to make your current working
directory (on your own system) available to the docker container. Inside the
container this directory will be available as `/home/jovyan/work`, i.e. the
`work` folder you see by default in jupyter lab. Changes inside this folder will
remain available on your host system. Changes in any other directory will be
lost when the container is destroyed. Note that you can mount multiple
directories in this way.

The `-it` makes it possible to interact with the running program, intead of
simply executing and exiting. Thus container will terminate once you terminate
your jupyter lab session.

Additional options may be added to the docker command as well. For example, to
run on a macbook, we had to add `--platform linux/amd64`:

```
docker run --rm --platform linux/amd64 ghcr.io/phenology/springtime springtime --help
```

Be aware that docker containers can consume significant resources on your
system. Make sure that they're always properly removed when you're ready. You
can run `docker ps -a` to see all containers lingering around on your system,
and you can remove them with `docker rm <ID OR NAME>`.

### Note: tmp/data

By default, currently, springtime stores any data or output in /tmp/data. That
means it will be lost when the docker container is destroyed. To persist it, for
now, move it to the work folder. We are planning to add additional configuration
that should make specification of the output or data directories more flexible.

## Install mamba on CRIB

Select one of the CRIB Intel x86_64 machines. Then open a terminal and run the code below:

```bash
curl -L -O https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh
bash Mambaforge-Linux-x86_64.sh
```
You may reply yes to this question:

```bash
Do you wish the installer to initialize Mambaforge
by running conda init? [yes|no]
[no] >>> yes
```

## Install on CRIB or other managed JupyterHub service

Sometimes the existing environment may clash with your springtime environment,
or you don't have complete control over the default environment. In that case,
it may be possible to create a custom kernel for springtime.

This worked for us on CRIB:

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
mamba env create --file environment.yml --name springtime_x86

# 3. Activate the environment
mamba activate springtime_x86

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
