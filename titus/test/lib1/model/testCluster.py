#!/usr/bin/env python

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
