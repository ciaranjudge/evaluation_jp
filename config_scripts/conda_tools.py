# Standard library
from pathlib import Path
import os
from stat import S_IREAD, S_IWRITE  # File permission flag settings
import subprocess  # Run shell commands (cross-platform)
from contextlib import contextmanager
from functools import wraps
import json

# External packages
import yaml


@contextmanager
def writable(env_path, base_path, enforce_readonly=True):
    """Make a conda env temporarily writable and optionally read-only again.

    Envs should be read-only to enforce using environment.yml for specifying packages.
    If "conda-meta/history" is read-only, conda knows the env is read-only.
    (it's a "canary file" https://github.com/conda/conda/issues/4888#issuecomment-287201331)
    
    Always need to make base env writable - otherwise, conda treats child envs as read-only.
    If env_name is not "base", make the named env writable too.
    """
    # TODO Check that conda_prefix is a Path
    base_canaryfile = base_path / "conda-meta" / "history"
    if not env_path == base_path:  # Checks if this is base env!
        env_canaryfile = env_path / "conda-meta" / "history"
    try:
        # *Always make base env writable
        # TODO Raise NotwritableError if can't make env writable
        Path.touch(base_canaryfile)  # create canaryfile if it doesn't exist
        os.chmod(base_canaryfile, S_IWRITE)  # make it writable
        # *In the case of a non-base env, make it writable too
        if not env_path == base_path:
            if env_canaryfile.exists():
                os.chmod(env_canaryfile, S_IWRITE)  # make it writable
        yield
    finally:
        if enforce_readonly:
            # Make base read-only
            os.chmod(base_canaryfile, S_IREAD)
            if not env_path == base_path:
                # Create parent directory and canaryfile if they don't exist
                env_canaryfile.parent.mkdir(parents=True, exist_ok=True)
                Path.touch(env_canaryfile)
                os.chmod(env_canaryfile, S_IREAD)


def find_env_paths(env_name: str):
    """Given an env_name, return boolean env_exists, env_path, and base_path.
    """
    conda_info = json.loads(
        subprocess.run("conda info --json", shell=True, capture_output=True).stdout
    )
    base_path = Path(conda_info["root_prefix"])
    if env_name == "base":
        env_path = base_path
        env_exists = True
    else:
        env_path_str = next(
            (path for path in conda_info["envs"] if env_name in path), None
        )
        if env_path_str is not None:
            env_exists = True
            env_path = Path(env_path_str)
        else:
            env_exists = False
            env_path = base_path / "envs" / env_name
    return env_exists, env_path, base_path



# TODO Set environment variables from environment.yml
def set_env_vars(env_name: str, env_vars: dict):
    """Set environment variables for this conda environment.
    Based on "saving-environment-variables" section in 
    https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
    Easiest to run this from the conda env that env vars are being set for.
    Overwrites existing vars (assumes env vars are best stored in environment.yml)
    """

    # *Set up skeleton scripts with no env vars
    # Activate
    activate_sh = ["#!/bin/sh\n"]  # Unix
    activate_bat = [""]  # Windows
    # Deactivate
    deactivate_sh = ["#!/bin/sh\n"]  # Unix
    deactivate_bat = [""]  # Windows

    # *Format input env_vars dict for Unix/Windows activate and deactivate scripts
    for var, val in env_vars.items():
        # TODO Make sure paths are output correctly for OS, especially env vars ($ %%)
        # Need generic per-OS paths
        # Effectively means paths relative to predefined OS environ vars

        # TODO Make sure that any already set variables are kept when adding new ones!
        # Unix
        activate_sh += f"export {var}={val}\n"
        deactivate_sh += f"unset {var}\n"

        # Windows
        activate_bat += f"set {var}={val}\n"
        deactivate_bat += f"set {var}=\n"

    # *Write activate scripts to files
    # Create directory if it doesn't exist
    _, env_path, _ = find_env_paths(env_name)
    activate_dir = (env_path / "etc/conda/activate.d").mkdir(
        parents=True, exist_ok=True
    )
    with open(activate_dir / "env_vars.sh", "w+") as f:
        f.writelines(activate_sh)
    with open(activate_dir / "env_vars.bat", "w+") as f:
        f.writelines(activate_bat)

    # *Write deactivate scripts
    # Create directory if it doesn't exist
    deactivate_dir = (env_path / "etc/conda/deactivate.d").mkdir(
        parents=True, exist_ok=True
    )
    with open(deactivate_dir / "env_vars.sh", "w+") as f:
        f.writelines(deactivate_sh)
    with open(deactivate_dir / "env_vars.bat", "w+") as f:
        f.writelines(deactivate_bat)



def update_env_from_yml(environment_yml: Path, enforce_readonly: bool = True,) -> None:
    """Given an environment.yml file that specifies a named conda env,
    update the env based on the yml file packages (or create the env if it doesn't exist).
    Make the env temporarily writable while updating,
    then make it read-only again afterwards if enforce_readonly flag is set.
    """
    # *Get env name from environment_yml
    # TODO Raise exception if environment_yml name field missing.
    # TODO Raise exception if no dependencies specified. This creates blank env and is bad!
    with open(environment_yml) as f:
        environment_data = yaml.safe_load(f)
    env_name = environment_data["name"]
    print(f"Found environment file for {env_name} at {environment_yml.resolve()}")

    # *Make env writable and update or create it
    # TODO Suppress unhelpful "EnvironmentSectionNotValid" warning from conda
    env_exists, env_path, base_path = find_env_paths(env_name)
    with writable(env_path, base_path, enforce_readonly):
        if env_exists:
            # ?Not sure if '--prune' flag is doing anything...
            subprocess.run(
                f"conda env update --file {environment_yml} --prune", shell=True
            )
        else:
            print(f"Environment {env_name} doesn't exist, so create it...")
            subprocess.run(f"conda env create --file {environment_yml} -n {env_name}", shell=True)
        # subprocess.run(f"conda env update --all", shell=True)
    environment_lock_yml = environment_yml.parent / "environment-lock.yml"
    subprocess.run(
        f"conda env export --name {env_name} --file {environment_lock_yml}", shell=True
    )


def update_base_env(
    base_environment_yml: Path = Path.home() / ".conda" / "base-environment.yml",
    enforce_readonly: bool = True,
) -> None:
    """Update conda, and base environment if given base_environment_yml file.
    With no base_environment_yml, just update conda itself.
    Make base env temporarily writeable while updating,
    then make read-only again afterwards if enforce_readonly flag is set.
    """
    _, env_path, base_path = find_env_paths("base")
    with writable(env_path, base_path, enforce_readonly):
        subprocess.run("conda update conda --yes")
    update_env_from_yml(base_environment_yml, enforce_readonly)


def update_project_env(
    environment_yml: Path = Path("environment.yml"), enforce_readonly: bool = True,
) -> None:
    update_env_from_yml(environment_yml, enforce_readonly)



if __name__ == "__main__":
    update_project_env()
