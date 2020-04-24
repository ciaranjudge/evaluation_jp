from setuptools import find_packages, setup

setup(
    name='evaluation_jp',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    version='0.1.0',
    description='JobPath Evaluation',
    author='Ciaran Judge',
    license='AGPL',
)
