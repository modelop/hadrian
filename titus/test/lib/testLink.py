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

class TestLib1Link(unittest.TestCase):
    def testSoftmax(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.softmax: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.03205860328008499, places = 5)
        self.assertAlmostEqual(out[1], 0.08714431874203257, places = 5)
        self.assertAlmostEqual(out[2], 0.23688281808991013, places = 5)
        self.assertAlmostEqual(out[3], 0.64391425988797220, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.softmax: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.03205860328008499, places = 5)
        self.assertAlmostEqual(out["two"],   0.08714431874203257, places = 5)
        self.assertAlmostEqual(out["three"], 0.23688281808991013, places = 5)
        self.assertAlmostEqual(out["four"],  0.64391425988797220, places = 5)

    def testLogit(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.logit: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9002495108803148, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.logit: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.7310585786300049, places = 5)
        self.assertAlmostEqual(out[1], 0.8807970779778823, places = 5)
        self.assertAlmostEqual(out[2], 0.9525741268224334, places = 5)
        self.assertAlmostEqual(out[3], 0.9820137900379085, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.logit: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],  0.7310585786300049, places = 5)
        self.assertAlmostEqual(out["two"],  0.8807970779778823, places = 5)
        self.assertAlmostEqual(out["three"],0.9525741268224334, places = 5)
        self.assertAlmostEqual(out["four"], 0.9820137900379085, places = 5)

    def testProbit(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.probit: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9860965524865013, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.probit: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.841344746068543 , places = 5)
        self.assertAlmostEqual(out[1], 0.9772498680518207, places = 5)
        self.assertAlmostEqual(out[2], 0.9986501019683699, places = 5)
        self.assertAlmostEqual(out[3], 0.9999683287581669, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.probit: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.841344746068543 , places = 5)
        self.assertAlmostEqual(out["two"],   0.9772498680518207, places = 5)
        self.assertAlmostEqual(out["three"], 0.9986501019683699, places = 5)
        self.assertAlmostEqual(out["four"],  0.9999683287581669, places = 5)

    def testCloglog(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.cloglog: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9998796388196516, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.cloglog: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.9340119641546875, places = 5)
        self.assertAlmostEqual(out[1], 0.9993820210106689, places = 5)
        self.assertAlmostEqual(out[2], 0.9999999981078213, places = 5)
        self.assertAlmostEqual(out[3], 1.0               , places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.cloglog: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.9340119641546875, places = 5)
        self.assertAlmostEqual(out["two"],   0.9993820210106689, places = 5)
        self.assertAlmostEqual(out["three"], 0.9999999981078213, places = 5)
        self.assertAlmostEqual(out["four"],  1.0               , places = 5)

    def testLoglog(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.loglog: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 1.203611803484212E-4,  places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.loglog: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.06598803584531254   , places = 5)
        self.assertAlmostEqual(out[1], 6.179789893310934E-4  , places = 5)
        self.assertAlmostEqual(out[2], 1.8921786948382924E-9 , places = 5)
        self.assertAlmostEqual(out[3], 1.9423376049564073E-24, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.loglog: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.06598803584531254   , places = 5)
        self.assertAlmostEqual(out["two"],   6.179789893310934E-4  , places = 5)
        self.assertAlmostEqual(out["three"], 1.8921786948382924E-9 , places = 5)
        self.assertAlmostEqual(out["four"],  1.9423376049564073E-24, places = 5)

    def testCauchit(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.cauchit: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.8642002512199081, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.cauchit: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.75              , places = 5)
        self.assertAlmostEqual(out[1], 0.8524163823495667, places = 5)
        self.assertAlmostEqual(out[2], 0.8975836176504333, places = 5)
        self.assertAlmostEqual(out[3], 0.9220208696226307, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.cauchit: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.75              , places = 5)
        self.assertAlmostEqual(out["two"],   0.8524163823495667, places = 5)
        self.assertAlmostEqual(out["three"], 0.8975836176504333, places = 5)
        self.assertAlmostEqual(out["four"],  0.9220208696226307, places = 5)

    def testSoftPlus(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.softplus: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 2.305083319768696, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.softplus: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 1.31326168752 , places = 5)
        self.assertAlmostEqual(out[1], 2.12692801104 , places = 5)
        self.assertAlmostEqual(out[2], 3.04858735157 , places = 5)
        self.assertAlmostEqual(out[3], 4.01814992792 , places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.softplus: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   1.31326168752, places = 5)
        self.assertAlmostEqual(out["two"],   2.12692801104, places = 5)
        self.assertAlmostEqual(out["three"], 3.04858735157, places = 5)
        self.assertAlmostEqual(out["four"],  4.01814992792, places = 5)

    def testReLu(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.relu: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 2.2, places = 1)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.relu: input}
""")
        out = engine.action([-1, -2, 3, 4])
        self.assertAlmostEqual(out[0], 0.0, places = 5)
        self.assertAlmostEqual(out[1], 0.0, places = 5)
        self.assertAlmostEqual(out[2], 3.0, places = 5)
        self.assertAlmostEqual(out[3], 4.0, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.relu: input}
""")
        out = engine.action({"one": -1, "two": -2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.0, places = 5)
        self.assertAlmostEqual(out["two"],   0.0, places = 5)
        self.assertAlmostEqual(out["three"], 3.0, places = 5)
        self.assertAlmostEqual(out["four"],  4.0, places = 5)

    def testtanh(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.tanh: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9757431300314515, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.tanh: input}
""")
        out = engine.action([-1, -2, 3, 4])
        self.assertAlmostEqual(out[0], -0.761594155956, places = 5)
        self.assertAlmostEqual(out[1], -0.964027580076, places = 5)
        self.assertAlmostEqual(out[2], 0.995054753687 , places = 5)
        self.assertAlmostEqual(out[3], 0.999329299739 , places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.tanh: input}
""")
        out = engine.action({"one": -1, "two": -2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   -0.761594155956, places = 5)
        self.assertAlmostEqual(out["two"],   -0.964027580076, places = 5)
        self.assertAlmostEqual(out["three"], 0.995054753687 , places = 5)
        self.assertAlmostEqual(out["four"],  0.999329299739 , places = 5)


