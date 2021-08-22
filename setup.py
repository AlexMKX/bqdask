import pathlib, os
from setuptools import setup, find_packages
from setuptools.command.install import install

with open(os.path.join(pathlib.Path(__file__).parent.resolve(), 'requirements.txt'), 'r') as req_file:
    req = req_file.readlines()
setup(
    name='bqdask',
    version='0.1.0',
    packages=find_packages(include=['bqdask']),
    python_requires='>=3.8',
    package_data={'': ['../requirements.txt']},

    install_requires=[
        req
    ],
)
