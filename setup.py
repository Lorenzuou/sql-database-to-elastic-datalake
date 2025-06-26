from setuptools import setup, find_packages

setup(
    name="simplelake",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "sqlalchemy",
        "elasticsearch",
        "tqdm",
    ],
    extras_require={
        "test": [
            "pytest",
            "pytest-mock",
            "pytest-cov",
            "pandas-stubs",
            "sqlalchemy-stubs",
        ],
    },
) 