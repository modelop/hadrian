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
    
class TestLib1La(unittest.TestCase):
    def chi2Vector(self, x, y):
        if isinstance(x, (list, tuple)) and isinstance(y, (list, tuple)):
            self.assertEqual(len(x), len(y))
            return sum((xi - yi)**2 for xi, yi in zip(x, y))
        elif isinstance(x, dict) and isinstance(y, dict):
            self.assertEqual(set(x.keys()), set(y.keys()))
            return sum((x[k] - y[k])**2 for k in x.keys())
        else:
            raise Exception

    def chi2(self, x, y):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x) and \
           isinstance(y, (list, tuple)) and all(isinstance(yi, (list, tuple)) for yi in y):
            self.assertEqual(len(x), len(y))
            for xi, yi in zip(x, y):
                self.assertEqual(len(xi), len(yi))
            return sum(sum((xj - yj)**2 for xj, yj in zip(xi, yi)) for xi, yi in zip(x, y))
        elif isinstance(x, dict) and all(isinstance(xi, dict) for xi in x.values()) and \
             isinstance(y, dict) and all(isinstance(yi, dict) for yi in y.values()):
            self.assertEqual(set(x.keys()), set(y.keys()))
            for k in x.keys():
                self.assertEqual(set(x[k].keys()), set(y[k].keys()))
            return sum(sum((x[i][j] - y[i][j])**2 for j in x[i].keys()) for i in x.keys())

    def testMapArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.map:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - params: [{x: double}]
      ret: double
      do: {u-: x}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[-1.0, -2.0, -3.0], [-4.0, -5.0, -6.0], [-7.0, -8.0, -9.0]]), 0.0, places=2)

    def testMapMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.map:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {four: 4, five: 5, six: 6},
              tres: {seven: 7, eight: 8, nine: 9}}
    - params: [{x: double}]
      ret: double
      do: {u-: x}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None),
                                         {"uno": {"one": -1.0, "two": -2.0, "three": -3.0},
                                          "dos": {"four": -4.0, "five": -5.0, "six": -6.0},
                                          "tres": {"seven": -7.0, "eight": -8.0, "nine": -9.0}}), 0.0, places=2)

    def testScaleArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: double}
action:
  la.scale:
    - type: {type: array, items: double}
      value: [1, 2, 3, 4, 5, 6, 7, 8, 9]
    - 10
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None), [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0]), 0.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.scale:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - 10
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]), 0.0, places=2)

    def testScaleMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: double}
action:
  la.scale:
    - type: {type: map, values: double}
      value: {one: 1, two: 2, three: 3, four: 4, five: 5}
    - 10
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None),
            {"one": 10.0, "two": 20.0, "three": 30.0, "four": 40.0, "five": 50.0}), 0.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.scale:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {four: 4, five: 5, six: 6},
              tres: {seven: 7, eight: 8, nine: 9}}
    - 10
''')
        self.assertAlmostEqual(self.chi2(engine.action(None),
                                         {"uno": {"one": 10.0, "two": 20.0, "three": 30.0},
                                          "dos": {"four": 40.0, "five": 50.0, "six": 60.0},
                                          "tres": {"seven": 70.0, "eight": 80.0, "nine": 90.0}}), 0.0, places=2)

    def testZipapArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.zipmap:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - type: {type: array, items: {type: array, items: double}}
      value: [[101, 102, 103], [104, 105, 106], [107, 108, 109]]
    - params: [{x: double}, {y: double}]
      ret: double
      do: {+: [x, y]}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[102.0, 104.0, 106.0], [108.0, 110.0, 112.0], [114.0, 116.0, 118.0]]), 0.0, places=2)

    def testZipmapMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.zipmap:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {one: 4, two: 5, three: 6},
              tres: {one: 7, two: 8, three: 9}}
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 101, two: 102, three: 103},
              dos: {one: 104, two: 105, three: 106},
              tres: {one: 107, two: 108, three: 109, four: 999.0}}
    - params: [{x: double}, {y: double}]
      ret: double
      do: {+: [x, y]}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), {"uno": {"one": 102.0, "two": 104.0, "three": 106.0, "four": 0.0},
                                                          "dos": {"one": 108.0, "two": 110.0, "three": 112.0, "four": 0.0},
                                                          "tres": {"one": 114.0, "two": 116.0, "three": 118.0, "four": 999.0}}), 0.0, places=2)

    def testAddArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: double}
action:
  la.add:
    - type: {type: array, items: double}
      value: [1, 2, 3, 4, 5, 6, 7, 8, 9]
    - type: {type: array, items: double}
      value: [101, 102, 103, 104, 105, 106, 107, 108, 109]
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None), [102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 114.0, 116.0, 118.0]), 0.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.add:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - type: {type: array, items: {type: array, items: double}}
      value: [[101, 102, 103], [104, 105, 106], [107, 108, 109]]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[102.0, 104.0, 106.0], [108.0, 110.0, 112.0], [114.0, 116.0, 118.0]]), 0.0, places=2)

    def testAddMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: double}
action:
  la.add:
    - type: {type: map, values: double}
      value: {one: 1, two: 2, three: 3, four: 4, five: 5}
    - type: {type: map, values: double}
      value: {one: 101, two: 102, three: 103, four: 104, five: 105, six: 999}
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None),
            {"one": 102.0, "two": 104.0, "three": 106.0, "four": 108.0, "five": 110.0, "six": 999.0}), 0.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.add:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {one: 4, two: 5, three: 6},
              tres: {one: 7, two: 8, three: 9}}
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 101, two: 102, three: 103},
              dos: {one: 104, two: 105, three: 106},
              tres: {one: 107, two: 108, three: 109, four: 999.0}}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None),
                                         {"uno": {"one": 102.0, "two": 104.0, "three": 106.0, "four": 0.0},
                                          "dos": {"one": 108.0, "two": 110.0, "three": 112.0, "four": 0.0},
                                          "tres": {"one": 114.0, "two": 116.0, "three": 118.0, "four": 999.0}}), 0.0, places=2)

    def testSubArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: double}
action:
  la.sub:
    - type: {type: array, items: double}
      value: [1, 2, 3, 4, 5, 6, 7, 8, 9]
    - type: {type: array, items: double}
      value: [101, 102, 103, 104, 105, 106, 107, 108, 109]
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None), [-100.0, -100.0, -100.0, -100.0, -100.0, -100.0, -100.0, -100.0, -100.0]), 0.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.sub:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - type: {type: array, items: {type: array, items: double}}
      value: [[101, 102, 103], [104, 105, 106], [107, 108, 109]]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[-100.0, -100.0, -100.0], [-100.0, -100.0, -100.0], [-100.0, -100.0, -100.0]]), 0.0, places=2)

    def testSubMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: double}
action:
  la.sub:
    - type: {type: map, values: double}
      value: {one: 1, two: 2, three: 3, four: 4, five: 5}
    - type: {type: map, values: double}
      value: {one: 101, two: 102, three: 103, four: 104, five: 105, six: 999}
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None),
            {"one": -100.0, "two": -100.0, "three": -100.0, "four": -100.0, "five": -100.0, "six": -999.0}), 0.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.sub:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {one: 4, two: 5, three: 6},
              tres: {one: 7, two: 8, three: 9}}
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 101, two: 102, three: 103},
              dos: {one: 104, two: 105, three: 106},
              tres: {one: 107, two: 108, three: 109, four: 999.0}}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None),
                                         {"uno": {"one": -100.0, "two": -100.0, "three": -100.0, "four": 0.0},
                                          "dos": {"one": -100.0, "two": -100.0, "three": -100.0, "four": 0.0},
                                          "tres": {"one": -100.0, "two": -100.0, "three": -100.0, "four": -999.0}}), 0.0, places=2)

    def testMultiplyAMatrixAndAVectorAsArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: double}
action:
  la.dot:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2], [3, 4], [5, 6]]
    - type: {type: array, items: double}
      value: [1, -2]
''')        
        self.assertAlmostEqual(self.chi2Vector(engine.action(None), [-3.0, -5.0, -7.0]), 0.0, places=2)

    def testMultiplyMatricesAsArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.dot:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2], [3, 4], [5, 6]]
    - type: {type: array, items: {type: array, items: double}}
      value: [[7, 8, 9], [10, 11, 12]]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[27.0, 30.0, 33.0], [61.0, 68.0, 75.0], [95.0, 106.0, 117.0]]), 0.0, places=2)

    def testMultiplyAMatrixAndAVectorAsMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: double}
action:
  la.dot:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2}, b: {x: 3, y: 4}, c: {x: 5, y: 6}}
    - type: {type: map, values: double}
      value: {x: 1, y: -2}
''')
        self.assertAlmostEqual(self.chi2Vector(engine.action(None), {"a": -3.0, "b": -5.0, "c": -7.0}), 0.0, places=2)

    def testMultiplyMatricesAsMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.dot:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2}, b: {x: 3, y: 4}, c: {x: 5, y: 6}}
    - type: {type: map, values: {type: map, values: double}}
      value: {x: {alpha: 7, beta: 8, gamma: 9}, y: {alpha: 10, beta: 11, gamma: 12}}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), {"a": {"alpha": 27.0, "beta": 30.0, "gamma": 33.0},
                                                               "b": {"alpha": 61.0, "beta": 68.0, "gamma": 75.0},
                                                               "c": {"alpha": 95.0, "beta": 106.0, "gamma": 117.0}}), 0.0, places=2)

    def testTransposeArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.transpose:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6]]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]]), 0.0, places=2)

    def testTransposeMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.transpose:
    - type: {type: map, values: {type: map, values: double}}
      value: {"a": {"x": 1, "y": 2, "z": 3}, "b": {"x": 4, "y": 5, "z": 6}}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), {"x": {"a": 1.0, "b": 4.0},
                                                          "y": {"a": 2.0, "b": 5.0},
                                                          "z": {"a": 3.0, "b": 6.0}}), 0.0, places=2)

    def testComputeAPseudoInverseFromArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.inverse:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6]]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[-0.944, 0.444], [-0.111, 0.111], [0.722, -0.222]]), 0.0, places=2)

    def testComputeAPseudoInverseFromMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.inverse:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2, z: 3}, b: {x: 4, y: 5, z: 6}}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), {"x": {"a": -0.944, "b": 0.444},
                                                          "y": {"a": -0.111, "b": 0.111},
                                                          "z": {"a": 0.722, "b": -0.222}}), 0.0, places=2)

    def testComputeATraceFromArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  la.trace:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
''')
        self.assertAlmostEqual(engine.action(None), 15.0, places=2)

    def testComputeATraceFromMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  la.trace:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 4, b: 5, c: 6}, c: {a: 7, b: 8, c: 9}}
''')
        self.assertAlmostEqual(engine.action(None), 15.0, places=2)

    def testComputeADeterminantFromArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  la.det:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, -5, 6], [7, 8, 9]]
''')
        self.assertAlmostEqual(engine.action(None), 120.0, places=2)

    def testComputeADeterminantFromMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  la.det:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 4, b: -5, c: 6}, c: {a: 7, b: 8, c: 9}}
''')
        self.assertAlmostEqual(engine.action(None), 120.0, places=2)

    def testSymmetricMatricesAsArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [2, 4, 5], [3, 5, 6]]
    - 0.01
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [2, 4, 5], [3, 6, 5]]
    - 0.01
''')
        self.assertFalse(engine.action(None))

    def testSymmetricMatricesAsMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 2, b: 4, c: 5}, c: {a: 3, b: 5, c: 6}}
    - 0.01
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 2, b: 4, c: 5}, c: {a: 3, b: 6, c: 5}}
    - 0.01
''')
        self.assertFalse(engine.action(None))

    def testComputeAnEigenBasisFromArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.eigenBasis:
    - type: {type: array, items: {type: array, items: double}}
      value: [[898.98, -1026.12], [-1026.12, 1309.55]]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[0.014, -0.017], [0.102, 0.083]]), 0.0, places=2)

    def testComputeAnEigenBasisFromMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.eigenBasis:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 898.98, b: -1026.12}, b: {a: -1026.12, b: 1309.55}}
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), {"0": {"a": 0.014, "b": -0.017}, "1": {"a": 0.102, "b": 0.083}}), 0.0, places=2)

    def testAccumulateACovarianceAndComputeEigenbasisUsingArrays(self):
        engine, = PFAEngine.fromYaml('''
input:
  type: record
  name: Input
  fields:
    - {name: datum, type: {type: array, items: double}}
    - {name: update, type: boolean}
output: {type: array, items: double}
cells:
  state:
    type:
      type: record
      name: State
      fields:
        - {name: count, type: double}
        - {name: mean, type: {type: array, items: double}}
        - {name: covariance, type: {type: array, items: {type: array, items: double}}}
    init:
      count: 0
      mean: [0, 0]
      covariance: [[0, 0], [0, 0]]
action:
  - if: input.update
    then:
      cell: state
      to:
        params: [{state: State}]
        ret: State
        do:
          stat.sample.updateCovariance:
            - input.datum
            - 1.0
            - state
  - if: {">=": [{cell: state, path: [[count]]}, 3]}
    then:
      la.dot:
        - la.eigenBasis:
            - cell: state
              path: [[covariance]]
        - type: {type: array, items: double}
          new:
            - {"-": [input.datum.0, {cell: state, path: [[mean], 0]}]}
            - {"-": [input.datum.1, {cell: state, path: [[mean], 1]}]}
    else:
      type: {type: array, items: double}
      value: []
''')
        engine.action({"datum": [12, 85], "update": True})
        engine.action({"datum": [32, 40], "update": True})
        engine.action({"datum": [4, 90], "update": True})
        engine.action({"datum": [3, 77], "update": True})
        engine.action({"datum": [7, 87], "update": True})
        engine.action({"datum": [88, 2], "update": True})
        engine.action({"datum": [56, 5], "update": True})

        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [12, 85], "update": False}), [-0.728, 0.775]), 0.0, places=2)
        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [32, 40], "update": False}), [0.295, -0.943]), 0.0, places=2)
        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [4, 90], "update": False}), [-0.921, 0.378]), 0.0, places=2)
        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [3, 77], "update": False}), [-0.718, -0.808]), 0.0, places=2)
        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [7, 87], "update": False}), [-0.830, 0.433]), 0.0, places=2)
        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [88, 2], "update": False}), [1.695, 1.585]), 0.0, places=2)
        self.assertAlmostEqual(self.chi2Vector(engine.action({"datum": [56, 5], "update": False}), [1.207, -1.420]), 0.0, places=2)

    def testTruncateAMatrixUsingArrays(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.truncate:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - 2
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]), 0.0, places=2)

    def testTruncateAMatrixUsingMaps(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.truncate:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2, z: 3}, b: {x: 4, y: 5, z: 6}, c: {x: 7, y: 8, z: 9}}
    - type: {type: array, items: string}
      value: [a, b, q]
''')
        self.assertAlmostEqual(self.chi2(engine.action(None), {"a": {"x": 1.0, "y": 2.0, "z": 3.0}, "b": {"x": 4.0, "y": 5.0, "z": 6.0}}), 0.0, places=2)

    def testAccumulateACovarianceAndUseThatToUpdateAPCAMatrixUsingArrays(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  state:
    type:
      type: record
      name: State
      fields:
        - {name: count, type: double}
        - {name: mean, type: {type: array, items: double}}
        - {name: covariance, type: {type: array, items: {type: array, items: double}}}
    init:
      count: 0
      mean: [0, 0, 0, 0, 0]
      covariance: [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]
action:
  - let:
      mat:
        la.truncate:
          - la.eigenBasis:
              - attr:
                  cell: state
                  to:
                    params: [{state: State}]
                    ret: State
                    do:
                      stat.sample.updateCovariance:
                        - input
                        - 1.0
                        - state
                path: [[covariance]]
          - 2
  - let:
      vec:
        a.mapWithIndex:
          - input
          - params: [{i: int}, {x: double}]
            ret: double
            do: {"-": [x, {cell: state, path: [[mean], i]}]}
  - if: {">=": [{cell: state, path: [[count]]}, 2]}
    then: {la.dot: [mat, vec]}
    else: {type: {type: array, items: double}, value: []}
''')
        engine.action([23, 56, 12, 34, 72])
        engine.action([52, 61, 12, 71, 91])
        engine.action([15, 12, 89, 23, 48])

        self.assertAlmostEqual(self.chi2Vector(engine.action([16, 27, 36, 84, 52]), [-0.038, -1.601]), 0.0, places=2)

if __name__ == "__main__":
    unittest.main()
