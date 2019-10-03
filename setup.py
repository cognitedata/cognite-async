import re

from setuptools import find_packages, setup

setup(
    name="cognite-async",
    version="0.1",
    url="",
    license="",
    author="Sander Land",
    author_email="sander.land@cognite.com",
    description="Extensions for asynchronous calls for the Cognite Data Fusion (CDF) Python SDK",
    install_requires=["pandas", "numpy", "cognite-sdk>=1.3.0"],
    python_requires=">=3.5",
    packages=["cognite." + p for p in find_packages(where="cognite")],
    include_package_data=True,
)
