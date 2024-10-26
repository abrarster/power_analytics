from setuptools import find_packages, setup

setup(
    name="power_orchestration",
    packages=find_packages(exclude=["power_orchestration_tests"]),
    install_requires=[
        "dagster==1.7.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-mysql",
        "dagster-pandas",
        "dagster-polars",
        "dagster-dbt",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "shapely",
        "duckdb",
        "plotly",
        "geopandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
