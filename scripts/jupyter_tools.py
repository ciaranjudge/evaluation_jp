# Standard library
import os
from pathlib import Path
import subprocess

# External packages
import yaml


def update_extensions_from_yml(jupyter_extensions_yml: Path, conda_env_name: str):
    """Given a jupyter_extensions yml file, update or install the specified extensions.
    """
    # *Get env name from supplied jupyter_extensions_yml
    # TODO Raise exception if jupyter_extensions_yml name field missing.
    with open(jupyter_extensions_yml) as f:
        jupyter_extensions_data = yaml.safe_load(f)
    env_name = jupyter_extensions_data['name']
    print(f"Found Jupyter extensions file for {env_name} at {jupyter_extensions_yml.resolve()}")

    for extension in jupyter_extensions_data['extensions']:
        subprocess.run(f"jupyter labextension install {extension}", shell=True)
    subprocess.run("jupyter lab build", shell=True)

