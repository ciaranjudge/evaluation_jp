# -*- coding: utf-8 -*-
import conda_tools

# *Initial setup
# git
#   - unblock ssl
# VSCode 
#   - user extensions
#   - settings
# Conda
#   - Update base
#   - Fix ssl issue
#   - Base environment.yml in place
# Cookiecutter
# 

# TODO Limit frequency of updates (weekly/daily/monthly)
# *Routine update
def update_project():
    # conda_tools.update_base_env()
    conda_tools.update_project_env()
    # Conda env variables set ok
    # jupyter_tools.update_extensions()

"""Main module."""
if __name__ == "__main__":
    # *Get the user settings right

    # *Get the current project settings right
    update_project()
    # print(Path.cwd())
