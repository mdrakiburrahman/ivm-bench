from setuptools import setup, find_namespace_packages

setup(
    name="dbt-duckdb-openivm",
    version="0.1.0",
    description="Standalone dbt adapter for DuckDB-OpenIVM (CLI binary subprocess)",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    package_data={
        "dbt": [
            "include/openivm/**/*.yml",
            "include/openivm/**/*.sql",
        ],
    },
    install_requires=[
        "dbt-core~=1.11",
        "dbt-common>=1.0",
        "agate>=1.0",
    ],
    entry_points={
        "dbt.adapter": [
            "openivm = dbt.adapters.openivm",
        ],
    },
)
