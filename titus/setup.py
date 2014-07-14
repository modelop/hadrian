#!/usr/bin/env python

from setuptools import setup

import titus.version

### To run the tests:
# python setup.py test

### To install on a machine:
# sudo python setup.py install

### To install in a home directory (~/lib):
# python setup.py install --home=~

setup(name="titus",
      version=titus.version.__version__,
      author="Open Data Group",
      author_email="support@opendatagroup.com",
      packages=["titus",
                "titus.producer",
                "titus.lib1",
                "titus.lib1.model",
                "titus.lib1.prob",
                "titus.lib1.stat",
                "titus.pmml"],
      description="Python implementation of Portable Format for Analytics (PFA): producer, converter, and consumer.",
      test_suite="test",
      install_requires=["python >= 2.7", "python < 3.0", "avro >= 1.7.6"],
      tests_require=["python >= 2.7", "python < 3.0", "avro >= 1.7.6", "PyYAML >= 3.11", "numpy >= 1.7.1"],
      )

### Details of dependencies:
# 
# Only tested in Python 2.7, but it ought to work in Python 2.6 (will be tested someday).
# Will not work in Python 3.x without 2to3 conversion (also untested).
# The module is pure-Python, so (for instance) it works in Jython 2.7.
# 
# Avro is required, only version 1.7.6 has been tested.
# 
# PyYAML is an optional dependency; it is only used by the titus.reader.yamlToAst function (and only version 3.11 has been tested).
# Numpy is an optional dependency; it is only used by the titus.producer modules (and only version 1.7.1 has been tested).
# 
# The test suite attempts to import all optional dependencies.
