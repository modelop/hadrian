#!/usr/bin/env python

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from titus.genpy import PFAEngine
from titus.errors import *

class TestLibKernel(unittest.TestCase):
    def testLinear(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.linear: [input, y]}
""")
        self.assertAlmostEqual(engine.action([1,2,3,4,5]), 55.0, places = 5)
        self.assertAlmostEqual(engine.action([1,1,1,1,1]), 15.0, places = 5)

    def testPoly(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: {gamma: 0.2}
  - let: {intercept: 0.4}
  - let: {degree: 3}
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.poly: [input, y, gamma, intercept, degree]}
""")
        self.assertAlmostEqual(engine.action([1,2,3,4,5]), 1481.544, places = 5)
        self.assertAlmostEqual(engine.action([1,1,1,1,1]), 39.304,   places = 5)

    def testRBF(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: {gamma: 0.2}
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.rbf: [input, y, gamma]}
""")
        self.assertAlmostEqual(engine.action([1,2,3,4,5]), 1.0,      places = 5)
        self.assertAlmostEqual(engine.action([1,1,1,1,1]), 0.002478, places = 5)

    def testSigmoid(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: {gamma: 0.2}
  - let: {intercept: 0.4}
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.sigmoid: [input, y, gamma, intercept]}
""")
        self.assertAlmostEqual(engine.action([1,2,3,4,5]), 1.0,     places = 5)
        self.assertAlmostEqual(engine.action([1,1,1,1,1]), 0.99777, places = 5)










