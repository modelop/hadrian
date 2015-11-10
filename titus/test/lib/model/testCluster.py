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
            - fcn: metric.absDiff
            - x
            - y
  path: [[id]]
''')
        self.assertEqual(engine.action([1.2, 1.2, 1.2, 1.2, 1.2]), "one")
        self.assertEqual(engine.action([1.8, 1.8, 1.8, 1.8, 1.8]), "two")
        self.assertEqual(engine.action([2.2, 2.2, 2.2, 2.2, 2.2]), "two")
        self.assertEqual(engine.action([5.0, 5.0, 5.0, 5.0, 5.0]), "five")
        self.assertEqual(engine.action([-1000.0, -1000.0, -1000.0, -1000.0, -1000.0]), "one")

    def testFindClosestClusterWithStrings(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: string}
    init:
      - {center: "a"}
      - {center: "aa"}
      - {center: "aaa"}
      - {center: "aaaa"}
      - {center: "aaaaa"}
action:
  attr:
    model.cluster.closest:
      - input
      - cell: clusters
      - params:
          - x: string
          - y: string
        ret: double
        do: {"m.abs": {"-": [{s.len: x}, {s.len: y}]}}
  path: [[center]]
''')
        self.assertEqual(engine.action(""), "a")
        self.assertEqual(engine.action("b"), "a")
        self.assertEqual(engine.action("bb"), "aa")
        self.assertEqual(engine.action("bbb"), "aaa")
        self.assertEqual(engine.action("bbbb"), "aaaa")
        self.assertEqual(engine.action("bbbbb"), "aaaaa")
        self.assertEqual(engine.action("bbbbbb"), "aaaaa")

    def testFindClosestClusterSimplifiedSignature(self):
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
        - 3
        - input
        - cell: clusters
        - params:
            - x: {type: array, items: double}
            - y: {type: array, items: double}
          ret: double
          do:
            metric.euclidean:
              - fcn: metric.absDiff
              - x
              - y
    - params: [{cluster: Cluster}]
      ret: string
      do: cluster.id
''')
        self.assertEqual(engine.action([1.2, 1.2, 1.2, 1.2, 1.2]), ["one", "two", "three"])
        self.assertEqual(engine.action([1.8, 1.8, 1.8, 1.8, 1.8]), ["two", "one", "three"])
        self.assertEqual(engine.action([2.2, 2.2, 2.2, 2.2, 2.2]), ["two", "three", "one"])
        self.assertEqual(engine.action([5.0, 5.0, 5.0, 5.0, 5.0]), ["five", "four", "three"])
        self.assertEqual(engine.action([-1000.0, -1000.0, -1000.0, -1000.0, -1000.0]), ["one", "two", "three"])

    def testFindClosestNClustersWithStrings(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: string}
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: string}
    init:
      - {center: "a"}
      - {center: "aa"}
      - {center: "aaa"}
      - {center: "aaaa"}
      - {center: "aaaaa"}
action:
  a.map:
    - model.cluster.closestN:
        - 3
        - input
        - cell: clusters
        - params:
            - x: string
            - y: string
          ret: double
          do: {"m.abs": {"-": [{s.len: x}, {s.len: y}]}}
    - params: [{c: Cluster}]
      ret: string
      do: {attr: c, path: [[center]]}
''')
        self.assertEqual(engine.action(""), ["a", "aa", "aaa"])
        self.assertEqual(engine.action("b"), ["a", "aa", "aaa"])
        self.assertEqual(engine.action("bb"), ["aa", "a", "aaa"])
        self.assertEqual(engine.action("bbb"), ["aaa", "aa", "aaaa"])
        self.assertEqual(engine.action("bbbb"), ["aaaa", "aaa", "aaaaa"])
        self.assertEqual(engine.action("bbbbb"), ["aaaaa", "aaaa", "aaa"])
        self.assertEqual(engine.action("bbbbbb"), ["aaaaa", "aaaa", "aaa"])

    def testFindClosestNClustersSimplifiedSignature(self):
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
        - 3
        - input
        - cell: clusters
    - params: [{cluster: Cluster}]
      ret: string
      do: cluster.id
''')
        self.assertEqual(engine.action([1.2, 1.2, 1.2, 1.2, 1.2]), ["one", "two", "three"])
        self.assertEqual(engine.action([1.8, 1.8, 1.8, 1.8, 1.8]), ["two", "one", "three"])
        self.assertEqual(engine.action([2.2, 2.2, 2.2, 2.2, 2.2]), ["two", "three", "one"])
        self.assertEqual(engine.action([5.0, 5.0, 5.0, 5.0, 5.0]), ["five", "four", "three"])
        self.assertEqual(engine.action([-1000.0, -1000.0, -1000.0, -1000.0, -1000.0]), ["one", "two", "three"])

    def testCreateSomeRandomSeeds(self):
        engine, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items:
    type: record
    name: Cluster
    fields:
      - {name: id, type: int}
      - {name: center, type: {type: array, items: double}}
      - {name: onemore, type: string}
cells:
  dataset:
    type: {type: array, items: {type: array, items: double}}
    init:
      - [1.1, 1.2, 1.3]
      - [2.1, 2.2, 2.3]
      - [3.1, 3.2, 3.3]
      - [4.1, 4.2, 4.3]
      - [5.1, 5.2, 5.3]
      - [5.1, 5.2, 5.3]
      - [5.1, 5.2, 5.3]
      - [1.1, 1.2, 1.3]
      - [1.1, 1.2, 1.3]
action:
  model.cluster.randomSeeds:
    - {cell: dataset}
    - input
    - params: [{i: int}, {vec: {type: array, items: double}}]
      ret: Cluster
      do:
        new: {id: i, center: vec, onemore: {string: hello}}
        type: Cluster
''')

        result = engine.action(5)
        
        self.assertEqual(set(tuple(x["center"]) for x in result), set([(1.1, 1.2, 1.3), (2.1, 2.2, 2.3), (3.1, 3.2, 3.3), (4.1, 4.2, 4.3), (5.1, 5.2, 5.3)]))

    def testPerformStandardKMeans(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output:
  type: array
  items:
    type: record
    name: Cluster
    fields:
      - {name: id, type: int}
      - {name: center, type: {type: array, items: double}}
cells:
  counter:
    type: int
    init: 0
  dataset:
    type: {type: array, items: {type: array, items: double}}
    init: []
method: emit
action:
  if:
    "<":
      - cell: counter
        to:
          params: [{x: int}]
          ret: int
          do: {"+": [x, 1]}
      - 81
  then:
    cell: dataset
    to:
      params: [{x: {type: array, items: {type: array, items: double}}}]
      ret: {type: array, items: {type: array, items: double}}
      do:
        a.append: [x, input]
  else:
    - let:
        clusters:
          value: [{id: 0, center: [1.1, 2.1, 3.1]}, {id: 0, center: [3.1, 1.1, 2.1]}, {id: 0, center: [2.1, 3.1, 1.1]}]
          type: {type: array, items: Cluster}
    - for: {i: 0}
      while: {"<": [i, 10]}
      step: {i: {+: [i, 1]}}
      do:
        set:
          clusters:
            model.cluster.kmeansIteration:
              - {cell: dataset}
              - clusters
              - {fcn: metric.simpleEuclidean}
              - params: [{data: {type: array, items: {type: array, items: double}}}, {cluster: Cluster}]
                ret: Cluster
                do: {model.cluster.updateMean: [data, cluster, 0.0]}
    - emit: clusters
''')

        def emitFunction(x):
            results = [_["center"] for _ in x]

            self.assertAlmostEqual(results[0][0], 1.0, places=2)
            self.assertAlmostEqual(results[0][1], 2.0, places=2)
            self.assertAlmostEqual(results[0][2], 3.0, places=2)

            self.assertAlmostEqual(results[1][0], 3.0, places=2)
            self.assertAlmostEqual(results[1][1], 1.0, places=2)
            self.assertAlmostEqual(results[1][2], 2.0, places=2)

            self.assertAlmostEqual(results[2][0], 2.0, places=2)
            self.assertAlmostEqual(results[2][1], 3.0, places=2)
            self.assertAlmostEqual(results[2][2], 1.0, places=2)

        engine.emit = emitFunction

        engine.action([1.1, 2.1, 3.1])
        engine.action([1.1, 2.1, 3.0])
        engine.action([1.1, 2.1, 2.9])
        engine.action([1.1, 2.0, 3.1])
        engine.action([1.1, 2.0, 3.0])
        engine.action([1.1, 2.0, 2.9])
        engine.action([1.1, 1.9, 3.1])
        engine.action([1.1, 1.9, 3.0])
        engine.action([1.1, 1.9, 2.9])
        engine.action([1.0, 2.1, 3.1])
        engine.action([1.0, 2.1, 3.0])
        engine.action([1.0, 2.1, 2.9])
        engine.action([1.0, 2.0, 3.1])
        engine.action([1.0, 2.0, 3.0])
        engine.action([1.0, 2.0, 2.9])
        engine.action([1.0, 1.9, 3.1])
        engine.action([1.0, 1.9, 3.0])
        engine.action([1.0, 1.9, 2.9])
        engine.action([0.9, 2.1, 3.1])
        engine.action([0.9, 2.1, 3.0])
        engine.action([0.9, 2.1, 2.9])
        engine.action([0.9, 2.0, 3.1])
        engine.action([0.9, 2.0, 3.0])
        engine.action([0.9, 2.0, 2.9])
        engine.action([0.9, 1.9, 3.1])
        engine.action([0.9, 1.9, 3.0])
        engine.action([0.9, 1.9, 2.9])

        engine.action([3.1, 1.1, 2.1])
        engine.action([3.0, 1.1, 2.1])
        engine.action([2.9, 1.1, 2.1])
        engine.action([3.1, 1.1, 2.0])
        engine.action([3.0, 1.1, 2.0])
        engine.action([2.9, 1.1, 2.0])
        engine.action([3.1, 1.1, 1.9])
        engine.action([3.0, 1.1, 1.9])
        engine.action([2.9, 1.1, 1.9])
        engine.action([3.1, 1.0, 2.1])
        engine.action([3.0, 1.0, 2.1])
        engine.action([2.9, 1.0, 2.1])
        engine.action([3.1, 1.0, 2.0])
        engine.action([3.0, 1.0, 2.0])
        engine.action([2.9, 1.0, 2.0])
        engine.action([3.1, 1.0, 1.9])
        engine.action([3.0, 1.0, 1.9])
        engine.action([2.9, 1.0, 1.9])
        engine.action([3.1, 0.9, 2.1])
        engine.action([3.0, 0.9, 2.1])
        engine.action([2.9, 0.9, 2.1])
        engine.action([3.1, 0.9, 2.0])
        engine.action([3.0, 0.9, 2.0])
        engine.action([2.9, 0.9, 2.0])
        engine.action([3.1, 0.9, 1.9])
        engine.action([3.0, 0.9, 1.9])
        engine.action([2.9, 0.9, 1.9])

        engine.action([2.1, 3.1, 1.1])
        engine.action([2.1, 3.0, 1.1])
        engine.action([2.1, 2.9, 1.1])
        engine.action([2.0, 3.1, 1.1])
        engine.action([2.0, 3.0, 1.1])
        engine.action([2.0, 2.9, 1.1])
        engine.action([1.9, 3.1, 1.1])
        engine.action([1.9, 3.0, 1.1])
        engine.action([1.9, 2.9, 1.1])
        engine.action([2.1, 3.1, 1.0])
        engine.action([2.1, 3.0, 1.0])
        engine.action([2.1, 2.9, 1.0])
        engine.action([2.0, 3.1, 1.0])
        engine.action([2.0, 3.0, 1.0])
        engine.action([2.0, 2.9, 1.0])
        engine.action([1.9, 3.1, 1.0])
        engine.action([1.9, 3.0, 1.0])
        engine.action([1.9, 2.9, 1.0])
        engine.action([2.1, 3.1, 0.9])
        engine.action([2.1, 3.0, 0.9])
        engine.action([2.1, 2.9, 0.9])
        engine.action([2.0, 3.1, 0.9])
        engine.action([2.0, 3.0, 0.9])
        engine.action([2.0, 2.9, 0.9])
        engine.action([1.9, 3.1, 0.9])
        engine.action([1.9, 3.0, 0.9])
        engine.action([1.9, 2.9, 0.9])

