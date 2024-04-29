# Age Of Dataframes

## Description

This repo accompanies the blogpost about using the Polars library, showcased on some Age of Empires II data.

## How-to use

The repo contains a `pyproject.toml` file, which can be used by the `poetry` utility to install all dependencies. Using `make` runs the `aoedata.py` script using poetry, which downloads and cleans the data for use in the included notebook, which contains just the code also given in the blogpost.

For reproduction purposes, the `MAX_DATE` variable in the script is set to limit the downloaded data to match what is used in the blogpost.

Have fun!
