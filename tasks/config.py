import os

import tomli

with open(os.path.join("pyproject.toml"), mode="rb") as fp:
    pyproject = tomli.load(fp)
PACKAGE_VERSION = pyproject["tool"]["poetry"]["version"]
SERVICE_NAME = pyproject["tool"]["poetry"]["name"]
