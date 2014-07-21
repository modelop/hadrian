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

import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1ProbDist(unittest.TestCase):
    def testNormalDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertAlmostEqual(engine.action(10.0), -1.612, places=3)
        self.assertAlmostEqual(engine.action(12.0), -2.112, places=3)
        self.assertAlmostEqual(engine.action(0.0),  -14.11, places=2)
        self.assertAlmostEqual(engine.action(15.0), -4.737, places=3)
        self.assertAlmostEqual(engine.action(8.0),  -2.112, places=3)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, 2.0]
''')
        self.assertAlmostEqual(engine.action(10.0), -1.612, places=3)
        self.assertAlmostEqual(engine.action(12.0), -2.112, places=3)
        self.assertAlmostEqual(engine.action(0.0),  -14.11, places=2)
        self.assertAlmostEqual(engine.action(15.0), -4.737, places=3)
        self.assertAlmostEqual(engine.action(8.0),  -2.112, places=3)
