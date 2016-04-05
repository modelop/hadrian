#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

class TestLib1TestTest(unittest.TestCase):
    def testKSTwoSample(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let:
      y:
       type: {type: array, items: double}
       new:
          - -0.6852076
          - -0.62961294
          -  1.47603708
          - -1.66223465
          - -0.34015844
          -  1.50852341
          - -0.0348001
          - -0.59529466
          -  0.71956491
          - -0.77441149

  - {stat.test.kolmogorov: [input, y]}
""")
        x = [0.53535232,
             0.66523251,
             0.92733853,
             0.45348014,
            -0.37606127,
             1.22115272,
            -0.36264331,
             2.15954568,
             0.49463302,
            -0.81670101]
        self.assertAlmostEqual(engine.action(x), 0.31285267601695582, places=6)

    def testRegResidual(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action:
  stat.test.residual:
    - input
    - 3.0
""")
        self.assertAlmostEqual(engine.action(5.0), 2.0, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  stat.test.residual:
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
  stat.test.residual:
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
  stat.test.pull:
    - input
    - 3.0
    - 2.0
""")
        self.assertAlmostEqual(engine.action(5.0), 1.0, places = 5)

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  stat.test.pull:
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
  stat.test.pull:
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
  stat.test.mahalanobis:
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
  stat.test.mahalanobis:
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
            - {name: dof, type: int}
        new:
          {chi2: 0.0, dof: -4}
  - set: {state: {stat.test.updateChi2: [0.0, state]}}
  - set: {state: {stat.test.updateChi2: [1.0, state]}}
  - set: {state: {stat.test.updateChi2: [1.5, state]}}
  - set: {state: {stat.test.updateChi2: [-0.75, state]}}
  - set: {state: {stat.test.updateChi2: [-1.0, state]}}
  - set: {state: {stat.test.updateChi2: [0.5, state]}}
  - set: {state: {stat.test.updateChi2: [-1.5, state]}}
  - type: {type: array, items: double}
    new: [state.chi2, {stat.test.reducedChi2: state}, {stat.test.chi2Prob: state}]
""")
        out = engine.action(None)
        self.assertAlmostEqual(out[0], 7.3125, places = 4)
        self.assertAlmostEqual(out[1], 2.4375, places = 4)
        self.assertAlmostEqual(out[2], 0.9374, places = 4)
