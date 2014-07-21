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
    
class TestLib1ModelCluster(unittest.TestCase):
    def testFindClosestCluster(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: string
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: {type: array, items: double}}
          - {name: id, type: string}
    init:
      - {id: one, center: [1, 1, 1, 1, 1]}
      - {id: two, center: [2, 2, 2, 2, 2]}
      - {id: three, center: [3, 3, 3, 3, 3]}
      - {id: four, center: [4, 4, 4, 4, 4]}
      - {id: five, center: [5, 5, 5, 5, 5]}
action:
  attr:
    model.cluster.closest:
      - input
      - cell: clusters
      - params:
          - x: {type: array, items: double}
          - y: {type: array, items: double}
        ret: double
        do:
          metric.euclidean:
            - fcnref: metric.absDiff
            - x
            - y
  path: [[id]]
''')
        self.assertEqual(engine.action([1.2, 1.2, 1.2, 1.2, 1.2]), "one")
        self.assertEqual(engine.action([1.8, 1.8, 1.8, 1.8, 1.8]), "two")
        self.assertEqual(engine.action([2.2, 2.2, 2.2, 2.2, 2.2]), "two")
        self.assertEqual(engine.action([5.0, 5.0, 5.0, 5.0, 5.0]), "five")
        self.assertEqual(engine.action([-1000.0, -1000.0, -1000.0, -1000.0, -1000.0]), "one")

    def testFindClosestNClusters(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: string}
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: {type: array, items: double}}
          - {name: id, type: string}
    init:
      - {id: one, center: [1, 1, 1, 1, 1]}
      - {id: two, center: [2, 2, 2, 2, 2]}
      - {id: three, center: [3, 3, 3, 3, 3]}
      - {id: four, center: [4, 4, 4, 4, 4]}
      - {id: five, center: [5, 5, 5, 5, 5]}
action:
  a.map:
    - model.cluster.closestN:
        - input
        - cell: clusters
        - params:
            - x: {type: array, items: double}
            - y: {type: array, items: double}
          ret: double
          do:
            metric.euclidean:
              - fcnref: metric.absDiff
              - x
              - y
        - 3
    - params: [{cluster: Cluster}]
      ret: string
      do: cluster.id
''')
        self.assertEqual(engine.action([1.2, 1.2, 1.2, 1.2, 1.2]), ["one", "two", "three"])
        self.assertEqual(engine.action([1.8, 1.8, 1.8, 1.8, 1.8]), ["two", "one", "three"])
        self.assertEqual(engine.action([2.2, 2.2, 2.2, 2.2, 2.2]), ["two", "three", "one"])
        self.assertEqual(engine.action([5.0, 5.0, 5.0, 5.0, 5.0]), ["five", "four", "three"])
        self.assertEqual(engine.action([-1000.0, -1000.0, -1000.0, -1000.0, -1000.0]), ["one", "two", "three"])
