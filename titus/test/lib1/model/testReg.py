
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

class TestLib1ModelReg(unittest.TestCase):
    def testRegLinear(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: double}}
        - {name: const, type: double}
    init:
      coeff: [1, 2, 3, 0, 5]
      const: 100.0
action:
  model.reg.linear:
    - input
    - cell: model
""")
        self.assertAlmostEqual(engine.action([0.1, 0.2, 0.3, 0.4, 0.5]), 103.9, places=1)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: {type: array, items: double}}}
        - {name: const, type: {type: array, items: double}}
    init:
      coeff: [[1, 2, 3, 0, 5],
              [1, 1, 1, 1, 1],
              [0, 0, 0, 0, 1]]
      const: [0.0, 0.0, 100.0]
action:
  model.reg.linear:
    - input
    - cell: model
""")
        out = engine.action([0.1, 0.2, 0.3, 0.4, 0.5])
        self.assertAlmostEqual(out[0], 3.9,   places=1)
        self.assertAlmostEqual(out[1], 1.5,   places=1)
        self.assertAlmostEqual(out[2], 100.5, places=1)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: map, values: double}}
        - {name: const, type: double}
    init:
      coeff: {one: 1, two: 2, three: 3, four: 0, five: 5}
      const: 100.0
action:
  model.reg.linear:
    - input
    - cell: model
""")
        self.assertAlmostEqual(engine.action({"one": 0.1, "two": 0.2, "three": 0.3, "four": 0.4, "five": 0.5}), 103.9, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: map, values: {type: map, values: double}}}
        - {name: const, type: {type: map, values: double}}
    init:
      coeff:
        uno: {one: 1, two: 2, three: 3, four: 0, five: 5}
        dos: {one: 1, two: 1, three: 1, four: 1, five: 1}
        tres: {one: 0, two: 0, three: 0, four: 0, five: 1}
      const:
        {uno: 0.0, dos: 0.0, tres: 100.0}
action:
  model.reg.linear:
    - input
    - cell: model
""")
        out = engine.action({"one": 0.1, "two": 0.2, "three": 0.3, "four": 0.4, "five": 0.5})
        self.assertAlmostEqual(out["uno"],  3.9,   places=1)
        self.assertAlmostEqual(out["dos"],  1.5,   places=1)
        self.assertAlmostEqual(out["tres"], 100.5, places=1)



    def testRegNormSoftmax(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.softmax: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.03205860328008499, places = 5)
        self.assertAlmostEqual(out[1], 0.08714431874203257, places = 5)
        self.assertAlmostEqual(out[2], 0.23688281808991013, places = 5)
        self.assertAlmostEqual(out[3], 0.64391425988797220, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.softmax: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.03205860328008499, places = 5)
        self.assertAlmostEqual(out["two"],   0.08714431874203257, places = 5)
        self.assertAlmostEqual(out["three"], 0.23688281808991013, places = 5)
        self.assertAlmostEqual(out["four"],  0.64391425988797220, places = 5)



    def testRegNormLogit(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.logit: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9002495108803148, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.logit: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.7310585786300049, places = 5)
        self.assertAlmostEqual(out[1], 0.8807970779778823, places = 5)
        self.assertAlmostEqual(out[2], 0.9525741268224334, places = 5)
        self.assertAlmostEqual(out[3], 0.9820137900379085, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.logit: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],  0.7310585786300049, places = 5)
        self.assertAlmostEqual(out["two"],  0.8807970779778823, places = 5)
        self.assertAlmostEqual(out["three"],0.9525741268224334, places = 5)
        self.assertAlmostEqual(out["four"], 0.9820137900379085, places = 5)



    def testRegNormProbit(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.probit: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9860965524865013, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.probit: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.841344746068543 , places = 5)
        self.assertAlmostEqual(out[1], 0.9772498680518207, places = 5)
        self.assertAlmostEqual(out[2], 0.9986501019683699, places = 5)
        self.assertAlmostEqual(out[3], 0.9999683287581669, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.probit: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.841344746068543 , places = 5)
        self.assertAlmostEqual(out["two"],   0.9772498680518207, places = 5)
        self.assertAlmostEqual(out["three"], 0.9986501019683699, places = 5)
        self.assertAlmostEqual(out["four"],  0.9999683287581669, places = 5)



    def testRegNormCloglog(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.cloglog: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.9998796388196516, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.cloglog: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.9340119641546875, places = 5)
        self.assertAlmostEqual(out[1], 0.9993820210106689, places = 5)
        self.assertAlmostEqual(out[2], 0.9999999981078213, places = 5)
        self.assertAlmostEqual(out[3], 1.0               , places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.cloglog: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.9340119641546875, places = 5)
        self.assertAlmostEqual(out["two"],   0.9993820210106689, places = 5)
        self.assertAlmostEqual(out["three"], 0.9999999981078213, places = 5)
        self.assertAlmostEqual(out["four"],  1.0               , places = 5)



    def testRegNormLoglog(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.loglog: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 1.203611803484212E-4,  places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.loglog: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.06598803584531254   , places = 5)
        self.assertAlmostEqual(out[1], 6.179789893310934E-4  , places = 5)
        self.assertAlmostEqual(out[2], 1.8921786948382924E-9 , places = 5)
        self.assertAlmostEqual(out[3], 1.9423376049564073E-24, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.loglog: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.06598803584531254   , places = 5)
        self.assertAlmostEqual(out["two"],   6.179789893310934E-4  , places = 5)
        self.assertAlmostEqual(out["three"], 1.8921786948382924E-9 , places = 5)
        self.assertAlmostEqual(out["four"],  1.9423376049564073E-24, places = 5)



    def testRegNormCauchit(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.cauchit: input}
""")
        self.assertAlmostEqual(engine.action(2.2), 0.8642002512199081, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.cauchit: input}
""")
        out = engine.action([1, 2, 3, 4])
        self.assertAlmostEqual(out[0], 0.75              , places = 5)
        self.assertAlmostEqual(out[1], 0.8524163823495667, places = 5)
        self.assertAlmostEqual(out[2], 0.8975836176504333, places = 5)
        self.assertAlmostEqual(out[3], 0.9220208696226307, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.cauchit: input}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3, "four": 4})
        self.assertAlmostEqual(out["one"],   0.75              , places = 5)
        self.assertAlmostEqual(out["two"],   0.8524163823495667, places = 5)
        self.assertAlmostEqual(out["three"], 0.8975836176504333, places = 5)
        self.assertAlmostEqual(out["four"],  0.9220208696226307, places = 5)

    def testRegResidual(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action:
  model.reg.residual:
    - input
    - 3.0
""")
        self.assertAlmostEqual(engine.action(5.0), 2.0, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  model.reg.residual:
    - input
    - type: {type: array, items: double}
      value: [2.5, 2.5, 2.5]
""")
        out = engine.action([1, 2, 3])
        self.assertAlmostEqual(out[0], -1.5, places = 5)
        self.assertAlmostEqual(out[1], -0.5, places = 5)
        self.assertAlmostEqual(out[2],  0.5, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action:
  model.reg.residual:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3})
        self.assertAlmostEqual(out["one"],   -1.5, places = 5)
        self.assertAlmostEqual(out["two"],   -0.5, places = 5)
        self.assertAlmostEqual(out["three"],  0.5, places = 5)


    def testRegPull(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action:
  model.reg.pull:
    - input
    - 3.0
    - 2.0
""")
        self.assertAlmostEqual(engine.action(5.0), 1.0, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  model.reg.pull:
    - input
    - type: {type: array, items: double}
      value: [2.5, 2.5, 2.5]
    - type: {type: array, items: double}
      value: [2.0, 2.0, 2.0]
""")
        out = engine.action([1, 2, 3])
        self.assertAlmostEqual(out[0], -0.75, places = 5)
        self.assertAlmostEqual(out[1], -0.25, places = 5)
        self.assertAlmostEqual(out[2],  0.25, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action:
  model.reg.pull:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
    - type: {type: map, values: double}
      value: {one: 2.0, two: 2.0, three: 2.0}
""")
        out = engine.action({"one": 1, "two": 2, "three": 3})
        self.assertAlmostEqual(out["one"],   -0.75, places = 5)
        self.assertAlmostEqual(out["two"],   -0.25, places = 5)
        self.assertAlmostEqual(out["three"],  0.25, places = 5)


    def testRegMahalanobis(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  model.reg.mahalanobis:
    - input
    - type: {type: array, items: double}
      value: [2.5, 2.5, 2.5]
    - type: {type: array, items: {type: array, items: double}}
      value: [[2.0, 0.0, 0.0],
              [0.0, 4.0, 0.0],
              [0.0, 0.0, 1.0]]
""")
        self.assertAlmostEqual(engine.action([1, 2, 3]), 1.19895788083, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
action:
  model.reg.mahalanobis:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
    - type: {type: map, values: {type: map, values: double}}
      value: {one:   {one: 2.0, two: 0.0, three: 0.0},
              two:   {one: 0.0, two: 4.0, three: 0.0},
              three: {one: 0.0, two: 0.0, three: 1.0}}
""")
        self.assertAlmostEqual(engine.action({"one": 1, "two": 2, "three": 3}), 1.19895788083, places = 5)



    def testRegChi2(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - let:
      state:
        type:
          type: record
          name: Chi2
          fields:
            - {name: chi2, type: double}
            - {name: DOF, type: int}
        new:
          {chi2: 0.0, DOF: -4}
  - set: {state: {model.reg.updateChi2: [0.0, state]}}
  - set: {state: {model.reg.updateChi2: [1.0, state]}}
  - set: {state: {model.reg.updateChi2: [1.5, state]}}
  - set: {state: {model.reg.updateChi2: [-0.75, state]}}
  - set: {state: {model.reg.updateChi2: [-1.0, state]}}
  - set: {state: {model.reg.updateChi2: [0.5, state]}}
  - set: {state: {model.reg.updateChi2: [-1.5, state]}}
  - type: {type: array, items: double}
    new: [state.chi2, {model.reg.reducedChi2: state}, {model.reg.chi2Prob: state}]
""")
        out = engine.action(None)
        self.assertAlmostEqual(out[0], 7.3125, places = 4)
        self.assertAlmostEqual(out[1], 2.4375, places = 4)
        self.assertAlmostEqual(out[2], 0.9374, places = 4)




