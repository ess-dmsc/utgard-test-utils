# utgard-test-utils

Utilities for test automation at Utgård


## Installation

Run

    pip install .


## Developing

You can make the code available to your Python installation by performing an
editable install with

    pip install -e .

To run the tests, install the development requirements by running

    pip install -r requirements-dev.txt

The tests can then be run against the installed package with

    pytest [--cov --cov-report=xml:<path>] [--junit-xml=<path>]
