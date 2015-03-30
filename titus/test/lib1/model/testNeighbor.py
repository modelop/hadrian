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
    
class TestLib1ModelNeighbor(unittest.TestCase):
    def testFindKNearestNeighbors(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: {type: array, items: double}}
cells:
  codebook:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.nearestK:
    - 2
    - input
    - cell: codebook
''')
        self.assertEqual(set(map(tuple, engine.action([1.2, 1.2, 1.2, 1.2, 1.2]))), set([(1.0, 1.0, 1.0, 1.0, 1.0), (2.0, 2.0, 2.0, 2.0, 2.0)]))
        self.assertEqual(set(map(tuple, engine.action([4.1, 4.1, 4.1, 4.1, 4.1]))), set([(4.0, 4.0, 4.0, 4.0, 4.0), (5.0, 5.0, 5.0, 5.0, 5.0)]))

    def testFindAllNeighborsInABall(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: {type: array, items: double}}
cells:
  codebook:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.ballR:
    - m.sqrt: 5
    - input
    - cell: codebook
''')
        self.assertEqual(set(map(tuple, engine.action([1.2, 1.2, 1.2, 1.2, 1.2]))), set([(1.0, 1.0, 1.0, 1.0, 1.0), (2.0, 2.0, 2.0, 2.0, 2.0)]))
        self.assertEqual(set(map(tuple, engine.action([4.1, 4.1, 4.1, 4.1, 4.1]))), set([(4.0, 4.0, 4.0, 4.0, 4.0), (5.0, 5.0, 5.0, 5.0, 5.0)]))

    def testAverageOfSomePoints(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  points:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.mean:
    - cell: points
''')
        self.assertEqual(engine.action([1.2, 1.2, 1.2, 1.2, 1.2]), [3.0, 3.0, 3.0, 3.0, 3.0])

        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  points:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.mean:
    - cell: points
    - params: [{point: {type: array, items: double}}]
      ret: double
      do: {m.exp: {u-: {metric.simpleEuclidean: [input, point]}}}
''')
        for x in engine.action([1.2, 1.2, 1.2, 1.2, 1.2]):
            self.assertAlmostEqual(x, 1.253377, places=3)
