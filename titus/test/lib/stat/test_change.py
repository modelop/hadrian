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
    
class TestLib1StatChange(unittest.TestCase):
    def testTriggerOnHandMadeValues(self):
        engine, = PFAEngine.fromYaml('''
input: boolean
output: History
cells:
  history:
    type:
      type: record
      name: History
      fields:
        - {name: numEvents, type: int}
        - {name: numRuns, type: int}
        - {name: currentRun, type: int}
        - {name: longestRun, type: int}
    init:
      numEvents: 0
      numRuns: 0
      currentRun: 0
      longestRun: 0
action:
  - cell: history
    to:
      params: [{history: History}]
      ret: History
      do: {stat.change.updateTrigger: [input, history]}
  - cell: history
''')
        self.assertEqual(engine.action(False), {"numEvents": 0, "numRuns": 0, "currentRun": 0, "longestRun": 0})
        self.assertEqual(engine.action(False), {"numEvents": 0, "numRuns": 0, "currentRun": 0, "longestRun": 0})
        self.assertEqual(engine.action(True), {"numEvents": 1, "numRuns": 1, "currentRun": 1, "longestRun": 1})
        self.assertEqual(engine.action(False), {"numEvents": 1, "numRuns": 1, "currentRun": 0, "longestRun": 1})
        self.assertEqual(engine.action(False), {"numEvents": 1, "numRuns": 1, "currentRun": 0, "longestRun": 1})
        self.assertEqual(engine.action(True), {"numEvents": 2, "numRuns": 2, "currentRun": 1, "longestRun": 1})
        self.assertEqual(engine.action(True), {"numEvents": 3, "numRuns": 2, "currentRun": 2, "longestRun": 2})
        self.assertEqual(engine.action(True), {"numEvents": 4, "numRuns": 2, "currentRun": 3, "longestRun": 3})
        self.assertEqual(engine.action(False), {"numEvents": 4, "numRuns": 2, "currentRun": 0, "longestRun": 3})
        self.assertEqual(engine.action(True), {"numEvents": 5, "numRuns": 3, "currentRun": 1, "longestRun": 3})
        self.assertEqual(engine.action(True), {"numEvents": 6, "numRuns": 3, "currentRun": 2, "longestRun": 3})
        self.assertEqual(engine.action(True), {"numEvents": 7, "numRuns": 3, "currentRun": 3, "longestRun": 3})
        self.assertEqual(engine.action(True), {"numEvents": 8, "numRuns": 3, "currentRun": 4, "longestRun": 4})
        self.assertEqual(engine.action(True), {"numEvents": 9, "numRuns": 3, "currentRun": 5, "longestRun": 5})
        self.assertEqual(engine.action(True), {"numEvents": 10, "numRuns": 3, "currentRun": 6, "longestRun": 6})
        self.assertEqual(engine.action(False), {"numEvents": 10, "numRuns": 3, "currentRun": 0, "longestRun": 6})
        self.assertEqual(engine.action(False), {"numEvents": 10, "numRuns": 3, "currentRun": 0, "longestRun": 6})
        self.assertEqual(engine.action(True), {"numEvents": 11, "numRuns": 4, "currentRun": 1, "longestRun": 6})
        self.assertEqual(engine.action(False), {"numEvents": 11, "numRuns": 4, "currentRun": 0, "longestRun": 6})

    def testDoAShewhartExample(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: boolean
cells:
  threshold:
    type: double
    init: 5.0
  minRunLength:
    type: int
    init: 5
  example:
    type:
      type: record
      name: MeanTrigger
      fields:
        - {name: count, type: double}
        - {name: mean, type: double}
        - {name: numEvents, type: int}
        - {name: numRuns, type: int}
        - {name: currentRun, type: int}
        - {name: longestRun, type: int}
    init:
      count: 0.0
      mean: 0.0
      numEvents: 0
      numRuns: 0
      currentRun: 0
      longestRun: 0
action:
  - cell: example
    to:
      params: [{x: MeanTrigger}]
      ret: MeanTrigger
      do:
        - let: {y: {stat.sample.update: [input, 1.0, x]}}
        - stat.change.updateTrigger: [{">": [y.mean, {cell: threshold}]}, y]
  - ">":
    - cell: example
      path: [[longestRun]]
    - cell: minRunLength
''')
        self.assertEqual(engine.action(1.3), False)
        self.assertEqual(engine.action(2.1), False)
        self.assertEqual(engine.action(0.5), False)
        self.assertEqual(engine.action(2.5), False)
        self.assertEqual(engine.action(1.6), False)
        self.assertEqual(engine.action(3.5), False)
        self.assertEqual(engine.action(8.9), False)
        self.assertEqual(engine.action(13.4), False)
        self.assertEqual(engine.action(15.2), False)
        self.assertEqual(engine.action(12.8), False)
        self.assertEqual(engine.action(14.2), False)
        self.assertEqual(engine.action(16.3), False)
        self.assertEqual(engine.action(14.4), False)
        self.assertEqual(engine.action(12.2), True)
        self.assertEqual(engine.action(16.8), True)
        self.assertEqual(engine.action(15.2), True)
        self.assertEqual(engine.action(14.3), True)

    def testComputeZValues(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - stat.change.zValue:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: R, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
      - false
''')
        self.assertAlmostEqual(engine.action(10.0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(12.0), 1.00, places=2)
        self.assertAlmostEqual(engine.action(0.0), -5.00, places=2)
        self.assertAlmostEqual(engine.action(15.0), 2.50, places=2)
        self.assertAlmostEqual(engine.action(8.0), -1.00, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - stat.change.zValue:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: R, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
      - true
''')
        self.assertAlmostEqual(engine.action(10.0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(12.0), 1.02, places=2)
        self.assertAlmostEqual(engine.action(0.0), -5.12, places=2)
        self.assertAlmostEqual(engine.action(15.0), 2.56, places=2)
        self.assertAlmostEqual(engine.action(8.0), -1.02, places=2)

    def testUpdateCUSUM(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  last:
    type: double
    init: 0.0
action:
  - cell: last
    to:
      params: [{x: double}]
      ret: double
      do: {stat.change.updateCUSUM: [input, x, 0.0]}
  - cell: last
''')
        self.assertAlmostEqual(engine.action(0.0), 0.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(2.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(2.0), 5.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 6.0, places=2)
        self.assertAlmostEqual(engine.action(0.0), 6.0, places=2)
        self.assertAlmostEqual(engine.action(0.0), 6.0, places=2)
        self.assertAlmostEqual(engine.action(-1.0), 5.0, places=2)
        self.assertAlmostEqual(engine.action(-2.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(-2.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(-2.0), 0.0, places=2)
        self.assertAlmostEqual(engine.action(-2.0), 0.0, places=2)
        self.assertAlmostEqual(engine.action(0.0), 0.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(2.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(2.0), 5.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 6.0, places=2)

    def testDoARealCUSUMExample(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: boolean
cells:
  threshold:
    type: double
    init: 5.0
  minRunLength:
    type: int
    init: 5
  example:
    type:
      type: record
      name: CusumTrigger
      fields:
        - {name: last, type: double}
        - {name: numEvents, type: int}
        - {name: numRuns, type: int}
        - {name: currentRun, type: int}
        - {name: longestRun, type: int}
    init:
      last: 0.0
      numEvents: 0
      numRuns: 0
      currentRun: 0
      longestRun: 0
action:
  - cell: example
    to:
      params: [{x: CusumTrigger}]
      ret: CusumTrigger
      do:
        - let:
            llr:
              "-":
                - prob.dist.gaussianLL: [input, 15.0, 3.0]
                - prob.dist.gaussianLL: [input, 2.0, 5.0]
        - let:
            y:
              attr: x
              path: [[last]]
              to: {stat.change.updateCUSUM: [llr, x.last, 0.0]}
        - stat.change.updateTrigger: [{">": [y.last, {cell: threshold}]}, y]
  - ">":
    - cell: example
      path: [[longestRun]]
    - cell: minRunLength
''')
        self.assertEqual(engine.action(1.3), False)
        self.assertEqual(engine.action(2.1), False)
        self.assertEqual(engine.action(0.5), False)
        self.assertEqual(engine.action(2.5), False)
        self.assertEqual(engine.action(1.6), False)
        self.assertEqual(engine.action(3.5), False)
        self.assertEqual(engine.action(8.9), False)
        self.assertEqual(engine.action(13.4), False)
        self.assertEqual(engine.action(15.2), False)
        self.assertEqual(engine.action(12.8), False)
        self.assertEqual(engine.action(14.2), False)
        self.assertEqual(engine.action(16.3), False)
        self.assertEqual(engine.action(14.4), False)
        self.assertEqual(engine.action(12.2), True)
        self.assertEqual(engine.action(16.8), True)
        self.assertEqual(engine.action(15.2), True)
        self.assertEqual(engine.action(14.3), True)
