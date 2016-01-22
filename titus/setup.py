#!/usr/bin/env python

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# 
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
                "titus.lib",
                "titus.lib.model",
                "titus.lib.prob",
                "titus.lib.stat",
                "titus.pmml",
                "titus.inspector"],
      scripts = ["scripts/pfainspector", "scripts/pfachain", "scripts/pfaexternalize", "scripts/pfarandom", "scripts/pfasize"],
      description="Python implementation of Portable Format for Analytics (PFA): producer, converter, and consumer.",
      test_suite="test",
      install_requires=["avro >= 1.7.6", "ply >= 3.4"],
      tests_require=["avro >= 1.7.6", "ply >= 3.4", "PyYAML >= 3.10", "numpy >= 1.6.1", "pytz == 2015.4"],
      )

### details of dependencies:
# 
# Only tested in Python 2.7, but it ought to work in Python 2.6 (will be tested someday).
# Will not work in Python 3.x without 2to3 conversion (also untested).
# The module is pure-Python, so (for instance) it works in Jython 2.7.
# 
# Avro is required; it is an integral part of PFA (only versions 1.7.6 -- 1.7.7 have been tested).
# PLY is required; it is used to parse PrettyPFA and Inspector commandlines (only version 3.4 has been tested).
# 
# PyYAML is an optional dependency; it is only used by the titus.reader.yamlToAst function (and only version 3.11 has been tested).
# Numpy is an optional dependency; it is only used by the "interp", "la", "stat.test", and "model.reg" PFA libraries, as well as Titus producers (and only version 1.7.1 has been tested).
# pytz is an optional dependency; it is only used by the "time" PFA library (2015.4 is required for adherence to PFA 0.8.1).
# 
# The test suite attempts to import all optional dependencies.
