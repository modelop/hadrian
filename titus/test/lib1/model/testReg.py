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

    def testRegLinearVariance1(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: array, items: {type: array, items: double}}}
    init:
      covar: [[ 1.0, -0.1, 0.0],
              [-0.1,  2.0, 0.0],
              [ 0.0,  0.0, 0.0]]
action:
  model.reg.linearVariance:
    - input
    - cell: model
""")
        self.assertAlmostEqual(engine.action([0.1, 0.2]), 0.086, places=3)

    def testRegLinearVariance2(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: array, items: {type: array, items: {type: array, items: double}}}}
    init:
      covar:
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
action:
  model.reg.linearVariance:
    - input
    - cell: model
""")
        results = engine.action([0.1, 0.2])
        self.assertAlmostEqual(results[0], 0.086, places=3)
        self.assertAlmostEqual(results[1], 0.086, places=3)
        self.assertAlmostEqual(results[2], 0.086, places=3)

    def testRegLinearVariance3(self):
        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: map, values: {type: map, values: double}}}
    init:
      covar: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
action:
  model.reg.linearVariance:
    - input
    - cell: model
""")
        self.assertAlmostEqual(engine.action({"a": 0.1, "b": 0.2}), 0.086, places=3)

    def testRegLinearVariance4(self):
        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: map, values: {type: map, values: {type: map, values: double}}}}
    init:
      covar:
        one: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
        two: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
        three: {a: {a:  1.0, b: -0.1, "": 0.0},
                b: {a: -0.1, b:  2.0, "": 0.0},
               "": {a:  0.0, b:  0.0, "": 0.0}}
        four: {a: {a:  1.0, b: -0.1, "": 0.0},
               b: {a: -0.1, b:  2.0, "": 0.0},
              "": {a:  0.0, b:  0.0, "": 0.0}}
action:
  model.reg.linearVariance:
    - input
    - cell: model
""")
        results = engine.action({"a": 0.1, "b": 0.2})
        self.assertAlmostEqual(results["one"], 0.086, places=3)
        self.assertAlmostEqual(results["two"], 0.086, places=3)
        self.assertAlmostEqual(results["three"], 0.086, places=3)

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




