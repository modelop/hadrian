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
import math

from titus.genpy import PFAEngine
from titus.errors import *

class TestLib1ProbDist(unittest.TestCase):
    def testNormalDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertAlmostEqual(engine.action(10.0), -1.612, places=3)
        self.assertAlmostEqual(engine.action(12.0), -2.112, places=3)
        self.assertAlmostEqual(engine.action(0.0),  -14.11, places=2)
        self.assertAlmostEqual(engine.action(15.0), -4.737, places=3)
        self.assertAlmostEqual(engine.action(8.0),  -2.112, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, 2.0]
''')
        self.assertAlmostEqual(engine.action(10.0), -1.612, places=3)
        self.assertAlmostEqual(engine.action(12.0), -2.112, places=3)
        self.assertAlmostEqual(engine.action(0.0),  -14.11, places=2)
        self.assertAlmostEqual(engine.action(15.0), -4.737, places=3)
        self.assertAlmostEqual(engine.action(8.0),  -2.112, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianCDF:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertAlmostEqual(engine.action(10.0), 0.5,    places=3)
        self.assertAlmostEqual(engine.action(12.0), 0.8413, places=3)
        self.assertAlmostEqual(engine.action(5.0 ), 0.0062, places=3)
        self.assertAlmostEqual(engine.action(15.0), 0.9938, places=3)
        self.assertAlmostEqual(engine.action(8.0 ), 0.1586, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianCDF: [input, 10.0, 2.0]
''')
        self.assertAlmostEqual(engine.action(10.0), 0.5,    places=3)
        self.assertAlmostEqual(engine.action(12.0), 0.8413, places=3)
        self.assertAlmostEqual(engine.action(5.0),  0.0062, places=3)
        self.assertAlmostEqual(engine.action(15.0), 0.9938, places=3)
        self.assertAlmostEqual(engine.action(8.0),  0.1586, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianQF:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertEqual(engine.action(0.0),         float('-inf'))
        self.assertAlmostEqual(engine.action(0.01),  5.3473 , places=1)
        self.assertAlmostEqual(engine.action(0.4 ),  9.4933 , places=2)
        self.assertAlmostEqual(engine.action(0.5 ),  10.0000, places=2)
        self.assertAlmostEqual(engine.action(0.99),  14.6527, places=1)
        self.assertEqual(engine.action(1.0),         float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianQF: [input, 10.0, 2.0]
''')
        self.assertEqual(engine.action(0.0),         float('-inf'))
        self.assertAlmostEqual(engine.action(0.01),  5.3473, places=1)
        self.assertAlmostEqual(engine.action(0.4 ),  9.4933, places=2)
        self.assertAlmostEqual(engine.action(0.5 ),  10.0000, places=2)
        self.assertAlmostEqual(engine.action(0.99),  14.6527, places=1)
        self.assertEqual(engine.action(1.0),         float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))


### Handle the right edge cases ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: 0.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertEqual(engine.action(9.00), float('-inf'))
        self.assertEqual(engine.action(10.0), float('inf'))
        self.assertEqual(engine.action(11.0), float('-inf'))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, 0.0]
''')
        self.assertEqual(engine.action(9.00), float('-inf'))
        self.assertEqual(engine.action(10.0), float('inf'))
        self.assertEqual(engine.action(11.0), float('-inf'))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianCDF:
      - input
      - value: {count: 21, mean: 10, variance: 0.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertEqual(engine.action(9.00),  0.0)
        self.assertEqual(engine.action(10.0),  1.0)
        self.assertEqual(engine.action(11.0),  1.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianCDF: [input, 10.0, 0.0]
''')
        self.assertEqual(engine.action(9.00),  0.0)
        self.assertEqual(engine.action(10.0),  1.0)
        self.assertEqual(engine.action(11.0),  1.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianQF:
      - input
      - value: {count: 21, mean: 10, variance: 0.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertEqual(engine.action(0.0),  float('-inf'))
        self.assertEqual(engine.action(1.0),  float('inf'))
        self.assertEqual(engine.action(0.4),  10.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianQF: [input, 10.0, 0.0]
''')
        self.assertEqual(engine.action(0.0),  float('-inf'))
        self.assertEqual(engine.action(1.0),  float('inf'))
        self.assertEqual(engine.action(0.4),  10.0)


### raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: -3.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, -3.0]
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianCDF:
      - input
      - value: {count: 21, mean: 10, variance: -3.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianCDF: [input, 10.0, -3.0]
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianQF:
      - input
      - value: {count: 21, mean: 10, variance: -3.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gaussianQF: [input, 10.0, 3.0]
''')
        self.assertRaises(PFAException, lambda: engine.action(1.3))
        self.assertRaises(PFAException, lambda: engine.action(-0.3))


############## EXPONENTIAL DISTRIBUTION #####################
    def testExponentialDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialPDF: [input, 1] #[input, rate]
''')
        self.assertEqual(      engine.action(0.000),   1.0)
        self.assertAlmostEqual(engine.action(1.000),   0.368, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.135, places=3)
        self.assertAlmostEqual(engine.action(2.500),   0.082, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialCDF: [input, 1] #[input, rate]
''')
        self.assertEqual(      engine.action(0.000),   0.0)
        self.assertAlmostEqual(engine.action(1.000),   0.632 , places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.865 , places=3)
        self.assertAlmostEqual(engine.action(2.500),   0.918 , places=3)
        self.assertEqual(      engine.action(-20.0),   0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, 1] #[input, rate]
''')
        self.assertEqual(      engine.action(0.0),   0.0)
        self.assertAlmostEqual(engine.action(0.3),   0.3567, places=3)
        self.assertAlmostEqual(engine.action(0.5),   0.6931, places=3)
        self.assertAlmostEqual(engine.action(0.8),   1.6094, places=3)
        self.assertEqual(      engine.action(1.0),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### handle edge cases properly ###
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialPDF: [input, 0] #[input, rate]
''')
        self.assertEqual(engine.action(0.000),   0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialCDF: [input, 0] #[input, rate]
''')
        self.assertEqual(engine.action(0.000),   0.0)
        self.assertEqual(engine.action(-1.30),   0.0)
        self.assertEqual(engine.action(1.300),   0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, 0.9] #[input, rate]
''')
        self.assertEqual(engine.action(0.0),   0.0)


### raise the right exceptions ###
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialPDF: [input, -1] #[input, rate]
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialCDF: [input, -1] #[input, rate]
''')
        self.assertRaises(PFAException, lambda: engine.action(3.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, -1] #[input, rate]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, 1.5] #[input, rate]
''')
        self.assertRaises(PFAException, lambda: engine.action(-1.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))



############## POISSON DISTRIBUTION #####################
    def testPoissonDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.poissonPDF: [input, 4] #[input, lambda]
''')
        self.assertAlmostEqual(engine.action(0),      0.0183, places=3)
        self.assertAlmostEqual(engine.action(1),      0.0733, places=3)
        self.assertAlmostEqual(engine.action(2),      0.1465, places=3)
        self.assertAlmostEqual(engine.action(10),     0.0053, places=3)
        self.assertEqual(      engine.action(-20),  0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonCDF: [input, 4] #[input, lambda]
''')
        self.assertAlmostEqual(engine.action(0.0),     0.0183, places=3)
        self.assertAlmostEqual(engine.action(2.0),     0.2381, places=3)
        self.assertAlmostEqual(engine.action(2.5),     0.2381, places=3)
        self.assertAlmostEqual(engine.action(10.0),    0.9972, places=3)
        self.assertEqual(      engine.action(-10.0),   0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonQF: [input, 4]  #[input, lambda]
''')
        self.assertEqual(engine.action(0.0),   0.0)
        self.assertEqual(engine.action(0.3),   3.0)
        self.assertEqual(engine.action(0.5),   4.0)
        self.assertEqual(engine.action(0.8),   6.0)
        self.assertEqual(engine.action(1.0),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### it must handle edge cases properly ###

        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.poissonPDF: [input, 0] #[input, lambda]
''')
        self.assertEqual(engine.action(0), 1.0)
        self.assertEqual(engine.action(4), 0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonCDF: [input, 0] #[input, lambda]
''')
        self.assertEqual(engine.action(0.0),  1.0)
        self.assertEqual(engine.action(-1.3), 0.0)
        self.assertEqual(engine.action(1.3),  1.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonQF: [input, 0] #[input, lambda]
''')
        self.assertEqual(engine.action(0.0),   0.0)
        self.assertEqual(engine.action(0.8),   0.0)
        self.assertEqual(engine.action(1.0),   0.0)


### it must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.poissonPDF: [input, -4] #[input, lambda]
''')
        self.assertRaises(PFAException, lambda: engine.action(4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonCDF: [input, -3] #[input, lambda]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonQF: [input, -2] #[input, lambda]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.poissonQF: [input, 2] #[input, lambda]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## CHI2 DISTRIBUTION #####################
    def testChi2Distribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2PDF: [input, 4] #[input, degrees of freedom]
''')
        self.assertEqual(      engine.action(0.000),   0.0)
        self.assertAlmostEqual(engine.action(1.000),   0.1516, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.1839, places=3)
        self.assertAlmostEqual(engine.action(2.500),   0.1791, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2CDF: [input, 4] #[input, degrees of freedom]
''')
        self.assertEqual(      engine.action(0.0),     0.0)
        self.assertAlmostEqual(engine.action(1.0),     0.0902 , places=3)
        self.assertAlmostEqual(engine.action(5.0),     0.7127 , places=3)
        self.assertAlmostEqual(engine.action(8.5),     0.9251 , places=3)
        self.assertEqual(      engine.action(-20.0),   0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2QF: [input, 4] #[input, degrees of freedom]
''')
        self.assertEqual(      engine.action(0.0),   0.0)
        self.assertAlmostEqual(engine.action(0.3),   2.1947, places=3)
        self.assertAlmostEqual(engine.action(0.5),   3.3567, places=3)
        self.assertAlmostEqual(engine.action(0.8),   5.9886, places=3)
        self.assertEqual(      engine.action(1.0),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### it must handle edge cases ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2PDF: [input, 0] #[input, degrees of freedom]
''')
        self.assertEqual(engine.action(0.000),   float('inf'))
        self.assertEqual(engine.action(1.600),   0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2CDF: [input, 0] #[input, degrees of freedom]
''')
        self.assertEqual(engine.action(0.000),   0.0 )
        self.assertEqual(engine.action(1.600),   1.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2QF: [input, 0] #[input, degrees of freedom]
''')
        self.assertEqual(      engine.action(0.4),   0.0)
        self.assertEqual(      engine.action(1.0),   float('inf'))

### it must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2PDF: [input, -1] #[input, degrees of freedom]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2CDF: [input, -3] #[input, degrees of freedom]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2QF: [input, -3] #[input, degrees of freedom]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.chi2QF: [input, 3] #[input, degrees of freedom]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############# TEST F DISTRIBUTION ########################
    def testFDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fPDF: [input, 4, 10]
''')
        self.assertEqual(      engine.action(0.0),         0.0)
        self.assertAlmostEqual(engine.action(1.5),   0.2682, places=3)
        self.assertAlmostEqual(engine.action(2.0),   0.1568, places=3)
        self.assertAlmostEqual(engine.action(10.0),  0.000614, places=4)
        self.assertEqual(      engine.action(-20.0),       0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fCDF: [input, 4, 10]
''')
        self.assertEqual(      engine.action(0.0),   0.0)
        self.assertAlmostEqual(engine.action(0.1),   0.0200, places=3)
        self.assertAlmostEqual(engine.action(0.9),   0.5006, places=3)
        self.assertAlmostEqual(engine.action(4.0),   0.9657, places=3)
        self.assertAlmostEqual(engine.action(100.0), 0.9999, places=3)
        self.assertEqual(      engine.action(-20.0), 0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fQF: [input, 4, 10]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.001),   0.0208, places=3)
        self.assertAlmostEqual(engine.action(0.400),   0.7158, places=3)
        self.assertAlmostEqual(engine.action(0.999),   11.282, places=2)
        self.assertEqual(      engine.action(1.000),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### check edge case handling ###
# no real edge cases (doesnt act like a delta anywhere)

### must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fPDF: [input, 0, 10]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fCDF: [input, 4, 0]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fQF: [input, 0, 10]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.fQF: [input, 4, 10]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))




############## GAMMA DISTRIBUTION #####################
    def testGammaDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaPDF: [input, 3.0, 3.0] #[input, shape, scale]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(1.000),   0.0133, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.0380, places=3)
        self.assertAlmostEqual(engine.action(4.000),   0.0781, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaCDF: [input, 3.0, 3.0] #[input, shape, scale]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(3.000),   0.0803, places=3)
        self.assertAlmostEqual(engine.action(6.000),   0.3233, places=3)
        self.assertAlmostEqual(engine.action(10.00),   0.6472, places=3)
        self.assertAlmostEqual(engine.action(100.0),   1.0000, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaQF: [input, 3.0, 3.0] #[input, shape, scale]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.001),   0.5716, places=3)
        self.assertAlmostEqual(engine.action(0.400),   6.8552, places=3)
        self.assertAlmostEqual(engine.action(0.999),   33.687, places=2)
        self.assertEqual(      engine.action(1.000),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must handle edge cases properly ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaPDF: [input, 0, 3.0] #[input, shape, scale]
''')
        self.assertEqual(engine.action(0.000),   float('inf'))
        self.assertEqual(engine.action(1.3),     0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaCDF: [input, 3.0, 0.0] #[input, shape, scale]
''')
        self.assertEqual(engine.action(0.000),   0.0000)
        self.assertEqual(engine.action(1.30),    1.0000)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaQF: [input, 0.0, 3.0] #[input, shape, scale]
''')
        self.assertEqual(engine.action(0.00),    0.0000)
        self.assertEqual(engine.action(1.00),    float('inf'))

### it must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaPDF: [input, -1.3, -3.0] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaCDF: [input, -3.0, 1.0] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaQF: [input, -1.0, 3.0] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.gammaQF: [input, 2.0, 3.0] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## BETA DISTRIBUTION #####################
    def testBetaDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaPDF: [input, 4, 3] #[input, shape1, shape2]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.100),   0.0486, places=3)
        self.assertAlmostEqual(engine.action(0.800),   1.2288, places=3)
        self.assertAlmostEqual(engine.action(-20.0),   0.0000, places=3)
        self.assertEqual(      engine.action(9.000),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaCDF: [input, 4, 3] #[input, shape1, shape2]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.100),   0.0013, places=3)
        self.assertAlmostEqual(engine.action(0.900),   0.9842, places=3)
        self.assertAlmostEqual(engine.action(4.000),   1.0000, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaQF: [input, 4, 3] #[input, shape1, shape2]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.001),   0.0939, places=3)
        self.assertAlmostEqual(engine.action(0.400),   0.5292, places=3)
        self.assertAlmostEqual(engine.action(0.999),   0.9621, places=3)
        self.assertEqual(      engine.action(1.000),   1.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### it must handle edge cases properly ###
## no real edge cases

### it must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaPDF: [input, 0, 3] #[input, shape1, shape2]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaCDF: [input, 4, -3] #[input, shape1, shape2]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaQF: [input, -4, 0] #[input, shape1, shape2]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.betaQF: [input, 4, 3] #[input, shape1, shape2]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))



############## CAUCHY DISTRIBUTION #####################
    def testCauchyDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyPDF: [input, 4, 3] #[input, location, scale]
''')
        self.assertAlmostEqual(engine.action(-3.00),   0.0165, places=3)
        self.assertAlmostEqual(engine.action(0.000),   0.0382, places=3)
        self.assertAlmostEqual(engine.action(0.500),   0.0449, places=3)
        self.assertAlmostEqual(engine.action(10.00),   0.0212, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyCDF: [input, 4, 3] #[input, location, scale]
''')
        self.assertAlmostEqual(engine.action(0.000),   0.2048, places=3)
        self.assertAlmostEqual(engine.action(0.100),   0.2087, places=3)
        self.assertAlmostEqual(engine.action(0.900),   0.2448, places=3)
        self.assertAlmostEqual(engine.action(4.000),   0.5000, places=3)
        self.assertAlmostEqual(engine.action(-20.0),   0.0396, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyQF: [input, 4, 3] #[input, location, scale]
''')
        self.assertEqual(      engine.action(0.000),   float('-inf'))
        self.assertAlmostEqual(engine.action(0.001),   -950.926, places=1)
        self.assertAlmostEqual(engine.action(0.400),   3.0252, places=3)
        self.assertAlmostEqual(engine.action(0.999),   958.926, places=2)
        self.assertEqual(      engine.action(1.000),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must handle edge cases ###
## cauchy distribution DOESNT become a delta fcn when scale=0

### must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyPDF: [input, 4, -3] #[input, location, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyCDF: [input, 4, 0] #[input, location, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyQF: [input, 4, -1] #[input, location, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.cauchyQF: [input, 4, 3] #[input, location, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(1.4))
        self.assertRaises(PFAException, lambda: engine.action(-.4))



############## LOGNORMAL DISTRIBUTION #####################
    def testLogNormalDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalPDF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(1.000),   0.0539, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.0849, places=3)
        self.assertAlmostEqual(engine.action(4.000),   0.0826, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalCDF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.900),   0.0176, places=3)
        self.assertAlmostEqual(engine.action(4.000),   0.2697, places=3)
        self.assertAlmostEqual(engine.action(100.0),   0.9954, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalQF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.001),   0.3361, places=3)
        self.assertAlmostEqual(engine.action(0.400),   5.7354, places=3)
        self.assertAlmostEqual(engine.action(0.999),   162.43, places=2)
        self.assertEqual(      engine.action(1.000),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must raise the right exceptions ###

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalPDF: [input, 2.0, -3.0] #[input, meanlog, sdlog]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalCDF: [input, 2.0, 0.0] #[input, meanlog, sdlog]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalQF: [input, 2.0, -1.0] #[input, meanlog, sdlog]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.lognormalQF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## STUDENTT DISTRIBUTION #####################
    def testStudentTDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tPDF: [input, 2] #[input, degrees of freedom, noncentrality]
''')
        self.assertAlmostEqual(engine.action(-1.00),   0.1924, places=3)
        self.assertAlmostEqual(engine.action(1.000),   0.1924, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.0680, places=3)
        self.assertAlmostEqual(engine.action(4.000),   0.0131, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tCDF: [input, 2] #[input, degrees of freedom, noncentrality]
''')
        self.assertAlmostEqual(engine.action(-0.90),   0.2315, places=3)
        self.assertAlmostEqual(engine.action(0.000),   0.5000, places=3)
        self.assertAlmostEqual(engine.action(0.900),   0.7684, places=3)
        self.assertAlmostEqual(engine.action(100.0),   0.9999, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tQF: [input, 2] #[input, degrees of freedom, noncentrality]
''')
        self.assertEqual(      engine.action(0.000),   float('-inf'))
        self.assertAlmostEqual(engine.action(0.001),   -22.33, places=2)
        self.assertAlmostEqual(engine.action(0.400),   -.2887, places=3)
        self.assertAlmostEqual(engine.action(0.999),   22.327, places=2)
        self.assertEqual(      engine.action(1.000),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must handle exceptions properly ###
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tPDF: [input, -2] #[input, degrees of freedom, noncentrality]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tCDF: [input, -1] #[input, degrees of freedom, noncentrality]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tQF: [input, 0] #[input, degrees of freedom, noncentrality]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.tQF: [input, 2] #[input, degrees of freedom, noncentrality]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## BINOMIAL DISTRIBUTION #####################
    def testBinomialDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.binomialPDF: [input, 4, .4]  #[input, size, prob]
''')
        self.assertEqual(      engine.action(0),      0.1296)
        self.assertAlmostEqual(engine.action(1),      0.3456, places=3)
        self.assertAlmostEqual(engine.action(2),      0.3456, places=3)
        self.assertAlmostEqual(engine.action(10),     0.0000, places=3)
        self.assertEqual(      engine.action(-20),    0.0000)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.binomialCDF: [input, 4, .4]  #[input, size, prob]
''')
        self.assertAlmostEqual(engine.action(0.0),     0.1296, places=3)
        self.assertAlmostEqual(engine.action(2.0),     0.8208, places=3)
        self.assertAlmostEqual(engine.action(2.5),     0.8208, places=3)
        self.assertAlmostEqual(engine.action(10.0),    1.0000, places=3)
        self.assertEqual(      engine.action(-10.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.binomialQF: [input, 4, .4]  #[input, size, prob]
''')
        self.assertEqual(engine.action(0.0),   0.0)
        self.assertEqual(engine.action(0.3),   1.0)
        self.assertEqual(engine.action(0.5),   2.0)
        self.assertEqual(engine.action(0.8),   2.0)
        self.assertEqual(engine.action(1.0),   4.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must handle edge cases properly ###
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.binomialPDF: [input, 4, 0.0]  #[input, size, prob]
''')
        self.assertEqual(engine.action(0),      1.0)
        self.assertEqual(engine.action(1),      0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.binomialCDF: [input, 4, 0.0]  #[input, size, prob]
''')
        self.assertEqual(engine.action(0.0),   1.0000)
        self.assertEqual(engine.action(-1.0),  0.0000)
        self.assertEqual(engine.action(2.0),   1.0000)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.binomialQF: [input, 4, 0.0]  #[input, size, prob]
''')
        self.assertEqual(engine.action(0.0),   0.0000)
        self.assertEqual(engine.action(0.3),   0.0000)
        self.assertEqual(engine.action(1.0),   4.0000)

### must raise the right exceptions ###
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.binomialPDF: [input, -4, 0.4]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(5))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.binomialCDF: [input, 4, 1.1]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.binomialQF: [input, 4, 0.4]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## UNIFORM DISTRIBUTION #####################
    def testUniformDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformPDF: [input, 1.0, 3.0] #[input, min, max]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(1.000),   0.5000, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.5000, places=3)
        self.assertEqual(      engine.action(4.000),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformCDF: [input, 1.0, 3.0] #[input, min, max]
''')
        self.assertEqual(      engine.action(1.000),   0.0000)
        self.assertAlmostEqual(engine.action(1.500),   0.2500, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.5000, places=3)
        self.assertAlmostEqual(engine.action(2.300),   0.6500, places=3)
        self.assertEqual(      engine.action(5.000),   1.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformQF: [input, 1.0, 3.0] #[input, min, max]
''')
        self.assertEqual(      engine.action(0.000),   1.0000)
        self.assertAlmostEqual(engine.action(0.001),   1.0020, places=3)
        self.assertAlmostEqual(engine.action(0.400),   1.8000, places=3)
        self.assertAlmostEqual(engine.action(0.999),   2.9980, places=2)
        self.assertEqual(      engine.action(1.000),   3.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must handle exceptions correctly ###
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformPDF: [input, 5.0, 3.0] #[input, min, max]
''')
        self.assertRaises(PFAException, lambda: engine.action(2.0))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformCDF: [input, 4.0, 3.0] #[input, min, max]
''')
        self.assertRaises(PFAException, lambda: engine.action(2.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformQF: [input, 3.0, 3.0] #[input, min, max]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.uniformQF: [input, 1.0, 3.0] #[input, min, max]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## GEOMETRIC DISTRIBUTION #####################
    def testGeometricDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.geometricPDF: [input, 0.4] #[input, probability of success]
''')
        self.assertEqual(      engine.action(0),   0.4000)
        self.assertAlmostEqual(engine.action(1),   0.2400, places=3)
        self.assertAlmostEqual(engine.action(4),   0.0518, places=3)
        self.assertEqual(      engine.action(-20),   0.0)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.geometricCDF: [input, 0.4] #[input, probability of success]
''')
        self.assertEqual(      engine.action(0.000),   0.4)
        self.assertAlmostEqual(engine.action(1.000),   0.640 , places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.784 , places=3)
        self.assertAlmostEqual(engine.action(2.500),   0.784 , places=3)
        self.assertEqual(      engine.action(-20.0),   0.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.geometricQF: [input, 0.4] #[input, probability of success]
''')
        self.assertEqual(engine.action(0.0),   0.0)
        self.assertEqual(engine.action(0.3),   0.0)
        self.assertEqual(engine.action(0.5),   1.0)
        self.assertEqual(engine.action(0.8),   3.0)
        self.assertEqual(engine.action(1.0),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must raise the right exceptions ###
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.geometricPDF: [input, 1.4] #[input, probability of success]
''')
        self.assertRaises(PFAException, lambda: engine.action(2))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.geometricCDF: [input, -0.4] #[input, probability of success]
''')
        self.assertRaises(PFAException, lambda: engine.action(2.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.geometricQF: [input, -0.4] #[input, probability of success]
''')
        self.assertRaises(PFAException, lambda: engine.action(0.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.geometricQF: [input, 0.4] #[input, probability of success]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))


############## HYPERGEOMETRIC DISTRIBUTION #####################
    def testHypergeometricDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.hypergeometricPDF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
''')
        self.assertAlmostEqual(engine.action(0),       0.0219, places=3)
        self.assertAlmostEqual(engine.action(1),       0.2198, places=3)
        self.assertAlmostEqual(engine.action(4),       0.0000, places=3)
        self.assertEqual(      engine.action(-20),     0.0000)

        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.hypergeometricCDF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
''')
        self.assertAlmostEqual(engine.action(0),   0.0219, places=3)
        self.assertAlmostEqual(engine.action(1),   0.2418, places=3)
        self.assertAlmostEqual(engine.action(2),   0.7363, places=3)
        self.assertAlmostEqual(engine.action(2),   0.7363, places=3)
        self.assertEqual(      engine.action(-20),   0.0000)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.hypergeometricQF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
''')
        self.assertEqual(engine.action(0.0),   0.0)
        self.assertEqual(engine.action(0.3),   2.0)
        self.assertEqual(engine.action(0.5),   2.0)
        self.assertEqual(engine.action(0.8),   3.0)
        self.assertEqual(engine.action(0.99),  3.0)
        self.assertEqual(engine.action(1.0),   3.0)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### must raise the right exceptions ###
# 1. you cant draw more balls than are in the urn
# 2. you cant draw more white balls than are in the urn (this happens with probability zero)
# 3. in QF: you cant input probabilities greater than 1, less than 0
# check 1
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.hypergeometricPDF: [input, 4, 4, 20] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
''')
        self.assertRaises(PFAException, lambda: engine.action(3))

# check 2
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.hypergeometricCDF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
''')
        self.assertEqual(engine.action(2000),   0.0)
# check 3
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.hypergeometricQF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))

############## NEGATIVE BINOMIAL DISTRIBUTION #####################
    def testNegativeBinomialDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.negativeBinomialPDF: [input, 5, .7] #[input, size, probability ]
''')
        self.assertAlmostEqual(engine.action(0),   0.1681, places=3)
        self.assertAlmostEqual(engine.action(3),   0.1588, places=3)
        self.assertAlmostEqual(engine.action(6),   0.0257, places=3)
        self.assertEqual(      engine.action(-20),   0.0000)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.negativeBinomialCDF: [input, 5, .7] #[input, size, probability ]
''')
        self.assertAlmostEqual(engine.action(0.000),   0.1681, places=3)
        self.assertAlmostEqual(engine.action(1.000),   0.4202, places=3)
        self.assertAlmostEqual(engine.action(2.000),   0.6471, places=3)
        self.assertAlmostEqual(engine.action(2.500),   0.6471, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.negativeBinomialQF: [input, 5, .7] #[input, size, probability ]
''')
        self.assertEqual(engine.action(0.0),   0.0)
        self.assertEqual(engine.action(0.3),   1.0)
        self.assertEqual(engine.action(0.5),   2.0)
        self.assertEqual(engine.action(0.8),   3.0)
        self.assertEqual(engine.action(1.0),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))


### must handle edge cases properly ###
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.negativeBinomialPDF: [input, 0, .7]  #[input, size, prob]
''')
        self.assertEqual(engine.action(0),   1.0)
        self.assertEqual(engine.action(3),   0.0)
        self.assertEqual(engine.action(6),   0.0)
        self.assertEqual(engine.action(-20), 0.0)

### must raise the right exceptions ###
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - prob.dist.negativeBinomialPDF: [input, 4, 0.0]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(5))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.negativeBinomialCDF: [input, 4, 1.1]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.negativeBinomialQF: [input, 0, -0.4]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.negativeBinomialQF: [input, 4, 0.4]  #[input, size, prob]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))



############## WEIBULL DISTRIBUTION #####################
    def testWeibullDistribution(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullPDF: [input, 2, 4] #[input, shape, scale]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.300),   0.0373, places=3)
        self.assertAlmostEqual(engine.action(5.000),   0.1310, places=3)
        self.assertAlmostEqual(engine.action(-20.0),   0.0000, places=3)
        self.assertAlmostEqual(engine.action(9.000),   0.0071, places=3)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullCDF: [input, 2, 4] #[input, shape, scale]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.100),   0.0006, places=3)
        self.assertAlmostEqual(engine.action(0.900),   0.0494, places=3)
        self.assertAlmostEqual(engine.action(4.000),   0.6321, places=3)
        self.assertEqual(      engine.action(-20.0),   0.0000)
        self.assertTrue(math.isnan(engine.action(float('nan'))))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullQF: [input, 2, 4] #[input, shape, scale]
''')
        self.assertEqual(      engine.action(0.000),   0.0000)
        self.assertAlmostEqual(engine.action(0.001),   0.1265, places=3)
        self.assertAlmostEqual(engine.action(0.400),   2.8589, places=3)
        self.assertAlmostEqual(engine.action(0.999),   10.513, places=3)
        self.assertEqual(      engine.action(1.000),   float('inf'))
        self.assertTrue(math.isnan(engine.action(float('nan'))))

### it must raise the righte exceptions ###
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullPDF: [input, -2, 4] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(1.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullCDF: [input, 2, 0] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(1.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullQF: [input, 0, 4] #[input, shape, scale]
''')

        self.assertRaises(PFAException, lambda: engine.action(1.4))

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - prob.dist.weibullQF: [input, 2, 4] #[input, shape, scale]
''')
        self.assertRaises(PFAException, lambda: engine.action(-.4))
        self.assertRaises(PFAException, lambda: engine.action(1.4))
