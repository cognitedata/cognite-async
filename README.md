<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/cognite-async/master)](https://jenkins.cognite.ai/job/github-builds/job/cognite-async/job/master/)
[![tox](https://img.shields.io/badge/tox-3.6%2B-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

cognite-async
=============
Extension for the Python SDK for asynchronous operations.

## Usage
Import the cognite client from this package using `from cognite.async_client import CogniteClient`, and documented functions are added to the normal SDK end points automatically.


## Installation

The package is available from our private artifactory.

## Requirements

* up to date cognite-sdk for 1.0
* pandas
* numpy

## Documentation

Build documentation using:

* cd docs
* sphinx-build -W -b html ./source ./build




