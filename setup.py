from setuptools import setup, find_packages

__version__ = "0.1.0"

with open("README.md") as description_file:
    readme = description_file.read()

with open("requirements.txt") as requirements_file:
    requirements = [line for line in requirements_file]

setup(
    name="meli-challenge",
    version=__version__,
    author="Victor Poglioni",
    python_requires=">=3.8.5",
    description="Meli Data Engineering Challenge",
    long_description=readme,
    url="https://github.com/victorrenop/meli-challenge",
    install_requires=requirements,
    packages=find_packages(),
)
