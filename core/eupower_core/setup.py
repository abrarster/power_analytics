from setuptools import find_packages, setup

with open("README.md") as f:
    readme = f.read()

setup(
    name="eupower_core",
    packages=find_packages(),
    long_description=readme,
    install_requires=[
        "pandas",
        "requests"
    ],
    python_requires=">=3.12",
)
