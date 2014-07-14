#!/usr/bin/env python

import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1Metric(unittest.TestCase):
    def testSimpleEuclideanWork(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.simpleEuclidean:
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 3.74, places=2)

    def testEuclideanWork(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 3.74, places=2)

    def testEuclideanWorkWithIntegers(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: int}
      - value: [0, 0, 0]
        type: {type: array, items: int}
''')
        self.assertAlmostEqual(engine.action(None), 3.74, places=2)

    def testEuclideanWorkWithIntegers2(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - params: [{x: int}, {y: int}]
        ret: double
        do: {m.abs: {"-": [x, y]}}
      - value: [1, 2, 3]
        type: {type: array, items: int}
      - value: [0, 0, 0]
        type: {type: array, items: int}
''')
        self.assertAlmostEqual(engine.action(None), 3.74, places=2)

    def testEuclideanWorkWithMissingValues(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcnref: metric.absDiff
      - value: [null, {double: 2}, {double: 3}]
        type: {type: array, items: ["null", double]}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 4.42, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [null, {double: 0}, {double: 0}]
        type: {type: array, items: ["null", double]}
''')
        self.assertAlmostEqual(engine.action(None), 4.42, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcnref: metric.absDiff
      - value: [null, {double: 2}, {double: 3}]
        type: {type: array, items: ["null", double]}
      - value: [null, {double: 0}, {double: 0}]
        type: {type: array, items: ["null", double]}
''')
        self.assertAlmostEqual(engine.action(None), 4.42, places=2)

    def testEuclideanWorkWithMissingValueWeights(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcnref: metric.absDiff
      - value: [null, {double: 2}, {double: 3}]
        type: {type: array, items: ["null", double]}
      - value: [0, 0, 0]
        type: {type: array, items: double}
      - value: [5.0, 1.0, 1.0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 6.75, places=2)

    def testEuclideanWorkWithGaussianSimilarity(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - params: [{x: double}, {y: double}]
        ret: double
        do: {metric.gaussianSimilarity: [x, y, 1.5]}
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 0.79, places=2)

    def testEuclideanWorkWithCategoricalInputs(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.euclidean: 
      - params: [{x: string}, {y: string}]
        ret: double
        do:
          if: {"==": [x, y]}
          then: 0.0
          else: 1.0
      - value: ["one", "two", "three"]
        type: {type: array, items: string}
      - value: ["one", "two", "THREE"]
        type: {type: array, items: string}
''')
        self.assertAlmostEqual(engine.action(None), 1.00, places=2)

    def testSquaredEuclideanWork(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.squaredEuclidean: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 14.00, places=2)

    def testChebyshevWork(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.chebyshev: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 3.00, places=2)

    def testTaxicabWork(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.taxicab: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
''')
        self.assertAlmostEqual(engine.action(None), 6.00, places=2)

    def testMinkowskiReproduceChebyshev(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.minkowski: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
      - 10.0
''')
        self.assertAlmostEqual(engine.action(None), 3.00, places=1)

    def testMinkowskiReproduceTaxicab(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.minkowski: 
      - fcnref: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
      - 1.0
''')
        self.assertAlmostEqual(engine.action(None), 6.00, places=2)

    def testBinaryMetricsDoSimpleMatching(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.simpleMatching: 
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
''')
        self.assertAlmostEqual(engine.action(None), 0.333, places=2)

    def testBinaryMetricsDoJaccard(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.jaccard: 
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
''')
        self.assertAlmostEqual(engine.action(None), 0.333, places=2)

    def testBinaryMetricsDoTanimoto(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.tanimoto: 
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
''')
        self.assertAlmostEqual(engine.action(None), 0.2, places=2)

    def testBinaryMetricsDoBinarySimilarity(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - metric.binarySimilarity:
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
      - 1.0
      - 2.0
      - 3.0
      - 4.0
      - 4.0
      - 3.0
      - 2.0
      - 1.0
''')
        self.assertAlmostEqual(engine.action(None), 1.5, places=2)

if __name__ == "__main__":
    unittest.main()
