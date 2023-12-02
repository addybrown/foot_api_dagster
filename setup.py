from setuptools import find_packages, setup

setup(
    name="dags",
    packages=find_packages(exclude=["dags_tests"]),
    install_requires=["dagster", "pandas", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
