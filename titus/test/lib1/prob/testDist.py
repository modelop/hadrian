#!/usr/bin/env python

import unittest

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
