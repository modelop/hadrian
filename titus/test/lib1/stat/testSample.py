#!/usr/bin/env python

import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1StatSample(unittest.TestCase):
    def testAccumulateACounter(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: ["null", {type: record, name: State, fields: [{name: count, type: double}]}]
    init: null
action:
  - cell: state
    to:
      params: [{state: ["null", State]}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - ifnotnull: {x: {cell: state}}
    then: {attr: x, path: [[count]]}
    else: -999
''')
        self.assertAlmostEqual(engine.action(1.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 2.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 4.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 5.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}]}
    init: {"count": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[count]]}
''')
        self.assertAlmostEqual(engine.action(1.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 2.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 4.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 5.0, places=2)

    def testAccumulateAMean(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: double}]}
    init: {"count": 0.0, "mean": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[mean]]}
''')
        self.assertAlmostEqual(engine.action(3.2), 3.2, places=2)
        self.assertAlmostEqual(engine.action(3.4), 3.3, places=2)
        self.assertAlmostEqual(engine.action(4.5), 3.7, places=2)
        self.assertAlmostEqual(engine.action(2.2), 3.325, places=2)
        self.assertAlmostEqual(engine.action(9.7), 4.6, places=2)
        self.assertAlmostEqual(engine.action(3.4), 4.4, places=2)
        self.assertAlmostEqual(engine.action(5.5), 4.557, places=2)
        self.assertAlmostEqual(engine.action(2.1), 4.25, places=2)

    def testAccumulateAVariance(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
''')
        self.assertAlmostEqual(engine.action(3.2), 0.0, places=2)
        self.assertAlmostEqual(engine.action(3.4), 0.01, places=2)
        self.assertAlmostEqual(engine.action(4.5), 0.326, places=2)
        self.assertAlmostEqual(engine.action(2.2), 0.6668, places=2)
        self.assertAlmostEqual(engine.action(9.7), 7.036, places=2)
        self.assertAlmostEqual(engine.action(3.4), 6.0633, places=2)
        self.assertAlmostEqual(engine.action(5.5), 5.345, places=2)
        self.assertAlmostEqual(engine.action(2.1), 5.3375, places=2)

    def testHandleErrorCases(self):
        def bad():
            PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: int}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
''')
        self.assertRaises(PFASemanticException, bad)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
cells:
  state:
    type: ["null", {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}, {name: hello, type: double}]}]
    init: null
action:
  - cell: state
    to:
      params: [{state: ["null", State]}]
      ret: State
      do: {stat.sample.update: [1.0, 1.0, state]}
  - null
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(None))

    def testUpdateWindowAccumulateACounter(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}]}}
    init: []
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[count]]}
''')
        self.assertAlmostEqual(engine.action(1.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 2.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}]}}
    init: [{x: 0.0, w: 0.0, count: 0.0}]
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[count]]}
''')
        self.assertAlmostEqual(engine.action(1.0), 1.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 2.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)
        self.assertAlmostEqual(engine.action(1.0), 3.0, places=2)

    def testUpdateWindowAccumulateAMean(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}]}}
    init: []
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[mean]]}
''')
        self.assertAlmostEqual(engine.action(3.2), 3.2, places=2)
        self.assertAlmostEqual(engine.action(3.4), 3.3, places=2)
        self.assertAlmostEqual(engine.action(4.5), 3.7, places=2)
        self.assertAlmostEqual(engine.action(2.2), 3.3666, places=2)
        self.assertAlmostEqual(engine.action(9.7), 5.4666, places=2)
        self.assertAlmostEqual(engine.action(3.4), 5.1, places=2)
        self.assertAlmostEqual(engine.action(5.5), 6.2, places=2)
        self.assertAlmostEqual(engine.action(2.1), 3.666, places=2)

    def testUpdateWindowAccumulateAVariance(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}}
    init: []
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[variance]]}
''')
        self.assertAlmostEqual(engine.action(3.2), 0.0, places=2)
        self.assertAlmostEqual(engine.action(3.4), 0.01, places=2)
        self.assertAlmostEqual(engine.action(4.5), 0.326, places=2)
        self.assertAlmostEqual(engine.action(2.2), 0.8822, places=2)
        self.assertAlmostEqual(engine.action(9.7), 9.8422, places=2)
        self.assertAlmostEqual(engine.action(3.4), 10.82, places=2)
        self.assertAlmostEqual(engine.action(5.5), 6.86, places=2)
        self.assertAlmostEqual(engine.action(2.1), 1.96, places=2)

    def testUpdateWindowAccumulateAMeanWithASuddenWindowShrink(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  counter:
    type: int
    init: 0
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}]}}
    init: []
action:
  - cell: counter
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, 1]}
  - let:
      windowSize:
        if: {"<": [{cell: counter}, 6]}
        then: 1000
        else: 3
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, windowSize]}
  - {attr: {a.last: {cell: state}}, path: [[mean]]}
''')
        self.assertAlmostEqual(engine.action(3.2), 3.2, places=2)
        self.assertAlmostEqual(engine.action(3.4), 3.3, places=2)
        self.assertAlmostEqual(engine.action(4.5), 3.7, places=2)
        self.assertAlmostEqual(engine.action(2.2), 3.325, places=2)
        self.assertAlmostEqual(engine.action(9.7), 4.6, places=2)
        self.assertAlmostEqual(engine.action(3.4), 5.1, places=2)
        self.assertAlmostEqual(engine.action(5.5), 6.2, places=2)
        self.assertAlmostEqual(engine.action(2.1), 3.666, places=2)

    def testUpdateWindowAccumulateAVarianceWithASuddenWindowShrink(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  counter:
    type: int
    init: 0
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}}
    init: []
action:
  - cell: counter
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, 1]}
  - let:
      windowSize:
        if: {"<": [{cell: counter}, 6]}
        then: 1000
        else: 3
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, windowSize]}
  - {attr: {a.last: {cell: state}}, path: [[variance]]}
''')
        self.assertAlmostEqual(engine.action(3.2), 0.0, places=2)
        self.assertAlmostEqual(engine.action(3.4), 0.01, places=2)
        self.assertAlmostEqual(engine.action(4.5), 0.326, places=2)
        self.assertAlmostEqual(engine.action(2.2), 0.6668, places=2)
        self.assertAlmostEqual(engine.action(9.7), 7.036, places=2)
        self.assertAlmostEqual(engine.action(3.4), 10.82, places=2)
        self.assertAlmostEqual(engine.action(5.5), 6.86, places=2)
        self.assertAlmostEqual(engine.action(2.1), 1.96, places=2)

    def testAccumulateAnEWMA(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: ["null", {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}]
    init: null
action:
  - cell: state
    to:
      params: [{state: ["null", State]}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
  - ifnotnull: {x: {cell: state}}
    then: {attr: x, path: [[mean]]}
    else: -999
''')
        self.assertAlmostEqual(engine.action(50.0), 50.00, places=2)
        self.assertAlmostEqual(engine.action(52.0), 50.60, places=2)
        self.assertAlmostEqual(engine.action(47.0), 49.52, places=2)
        self.assertAlmostEqual(engine.action(53.0), 50.56, places=2)
        self.assertAlmostEqual(engine.action(49.3), 50.18, places=2)
        self.assertAlmostEqual(engine.action(50.1), 50.16, places=2)
        self.assertAlmostEqual(engine.action(47.0), 49.21, places=2)
        self.assertAlmostEqual(engine.action(51.0), 49.75, places=2)
        self.assertAlmostEqual(engine.action(50.1), 49.85, places=2)
        self.assertAlmostEqual(engine.action(51.2), 50.26, places=2)
        self.assertAlmostEqual(engine.action(50.5), 50.33, places=2)
        self.assertAlmostEqual(engine.action(49.6), 50.11, places=2)
        self.assertAlmostEqual(engine.action(47.6), 49.36, places=2)
        self.assertAlmostEqual(engine.action(49.9), 49.52, places=2)
        self.assertAlmostEqual(engine.action(51.3), 50.05, places=2)
        self.assertAlmostEqual(engine.action(47.8), 49.38, places=2)
        self.assertAlmostEqual(engine.action(51.2), 49.92, places=2)
        self.assertAlmostEqual(engine.action(52.6), 50.73, places=2)
        self.assertAlmostEqual(engine.action(52.4), 51.23, places=2)
        self.assertAlmostEqual(engine.action(53.6), 51.94, places=2)
        self.assertAlmostEqual(engine.action(52.1), 51.99, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {mean: 50.0, variance: 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
  - {cell: state, path: [[mean]]}
''')
        self.assertAlmostEqual(engine.action(52.0), 50.60, places=2)
        self.assertAlmostEqual(engine.action(47.0), 49.52, places=2)
        self.assertAlmostEqual(engine.action(53.0), 50.56, places=2)
        self.assertAlmostEqual(engine.action(49.3), 50.18, places=2)
        self.assertAlmostEqual(engine.action(50.1), 50.16, places=2)
        self.assertAlmostEqual(engine.action(47.0), 49.21, places=2)
        self.assertAlmostEqual(engine.action(51.0), 49.75, places=2)
        self.assertAlmostEqual(engine.action(50.1), 49.85, places=2)
        self.assertAlmostEqual(engine.action(51.2), 50.26, places=2)
        self.assertAlmostEqual(engine.action(50.5), 50.33, places=2)
        self.assertAlmostEqual(engine.action(49.6), 50.11, places=2)
        self.assertAlmostEqual(engine.action(47.6), 49.36, places=2)
        self.assertAlmostEqual(engine.action(49.9), 49.52, places=2)
        self.assertAlmostEqual(engine.action(51.3), 50.05, places=2)
        self.assertAlmostEqual(engine.action(47.8), 49.38, places=2)
        self.assertAlmostEqual(engine.action(51.2), 49.92, places=2)
        self.assertAlmostEqual(engine.action(52.6), 50.73, places=2)
        self.assertAlmostEqual(engine.action(52.4), 51.23, places=2)
        self.assertAlmostEqual(engine.action(53.6), 51.94, places=2)
        self.assertAlmostEqual(engine.action(52.1), 51.99, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {mean: 50.0, variance: 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
  - {cell: state, path: [[variance]]}
''')
        self.assertAlmostEqual(engine.action(52.0), 0.840, places=2)
        self.assertAlmostEqual(engine.action(47.0), 3.309, places=2)
        self.assertAlmostEqual(engine.action(53.0), 4.859, places=2)
        self.assertAlmostEqual(engine.action(49.3), 3.737, places=2)
        self.assertAlmostEqual(engine.action(50.1), 2.617, places=2)
        self.assertAlmostEqual(engine.action(47.0), 3.928, places=2)
        self.assertAlmostEqual(engine.action(51.0), 3.421, places=2)
        self.assertAlmostEqual(engine.action(50.1), 2.421, places=2)
        self.assertAlmostEqual(engine.action(51.2), 2.075, places=2)
        self.assertAlmostEqual(engine.action(50.5), 1.465, places=2)
        self.assertAlmostEqual(engine.action(49.6), 1.137, places=2)
        self.assertAlmostEqual(engine.action(47.6), 2.120, places=2)
        self.assertAlmostEqual(engine.action(49.9), 1.546, places=2)
        self.assertAlmostEqual(engine.action(51.3), 1.747, places=2)
        self.assertAlmostEqual(engine.action(47.8), 2.290, places=2)
        self.assertAlmostEqual(engine.action(51.2), 2.300, places=2)
        self.assertAlmostEqual(engine.action(52.6), 3.113, places=2)
        self.assertAlmostEqual(engine.action(52.4), 2.766, places=2)
        self.assertAlmostEqual(engine.action(53.6), 3.117, places=2)
        self.assertAlmostEqual(engine.action(52.1), 2.187, places=2)

    def testHoltWintersAccumulateATrend(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - cell: state
    path: [{string: level}]
''')
        self.assertAlmostEqual(engine.action(50.0), 40.00, places=2)
        self.assertAlmostEqual(engine.action(45.0), 50.40, places=2)
        self.assertAlmostEqual(engine.action(40.0), 45.02, places=2)
        self.assertAlmostEqual(engine.action(35.0), 36.73, places=2)
        self.assertAlmostEqual(engine.action(30.0), 29.97, places=2)
        self.assertAlmostEqual(engine.action(25.0), 24.63, places=2)
        self.assertAlmostEqual(engine.action(20.0), 19.80, places=2)
        self.assertAlmostEqual(engine.action(15.0), 14.96, places=2)
        self.assertAlmostEqual(engine.action(10.0), 10.02, places=2)
        self.assertAlmostEqual(engine.action(5.0), 5.02, places=2)
        self.assertAlmostEqual(engine.action(0.0), 0.01, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - cell: state
    path: [{string: trend}]
''')
        self.assertAlmostEqual(engine.action(50.0), 32.00, places=2)
        self.assertAlmostEqual(engine.action(45.0), 14.72, places=2)
        self.assertAlmostEqual(engine.action(40.0), -1.36, places=2)
        self.assertAlmostEqual(engine.action(35.0), -6.90, places=2)
        self.assertAlmostEqual(engine.action(30.0), -6.79, places=2)
        self.assertAlmostEqual(engine.action(25.0), -5.62, places=2)
        self.assertAlmostEqual(engine.action(20.0), -4.99, places=2)
        self.assertAlmostEqual(engine.action(15.0), -4.87, places=2)
        self.assertAlmostEqual(engine.action(10.0), -4.93, places=2)
        self.assertAlmostEqual(engine.action(5.0), -4.99, places=2)
        self.assertAlmostEqual(engine.action(0.0), -5.01, places=2)

    def testHoltWintersForecastATrend(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - stat.sample.forecast1HoltWinters: {cell: state}
''')
        self.assertAlmostEqual(engine.action(50.0), 72.00, places=2)
        self.assertAlmostEqual(engine.action(45.0), 65.12, places=2)
        self.assertAlmostEqual(engine.action(40.0), 43.67, places=2)
        self.assertAlmostEqual(engine.action(35.0), 29.83, places=2)
        self.assertAlmostEqual(engine.action(30.0), 23.17, places=2)
        self.assertAlmostEqual(engine.action(25.0), 19.01, places=2)
        self.assertAlmostEqual(engine.action(20.0), 14.81, places=2)
        self.assertAlmostEqual(engine.action(15.0), 10.09, places=2)
        self.assertAlmostEqual(engine.action(10.0), 5.09, places=2)
        self.assertAlmostEqual(engine.action(5.0), 0.03, places=2)
        self.assertAlmostEqual(engine.action(0.0), -5.00, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - a.last: {stat.sample.forecastHoltWinters: [3, {cell: state}]}
''')
        self.assertAlmostEqual(engine.action(50.0), 136.00, places=2)
        self.assertAlmostEqual(engine.action(45.0), 94.56, places=2)
        self.assertAlmostEqual(engine.action(40.0), 40.95, places=2)
        self.assertAlmostEqual(engine.action(35.0), 16.02, places=2)
        self.assertAlmostEqual(engine.action(30.0), 9.58, places=2)
        self.assertAlmostEqual(engine.action(25.0), 7.76, places=2)
        self.assertAlmostEqual(engine.action(20.0), 4.83, places=2)
        self.assertAlmostEqual(engine.action(15.0), 0.35, places=2)
        self.assertAlmostEqual(engine.action(10.0), -4.77, places=2)
        self.assertAlmostEqual(engine.action(5.0), -9.94, places=2)
        self.assertAlmostEqual(engine.action(0.0), -15.01, places=2)

    def testHoltWintersPeriodicAccumulateAnAdditiveTrend(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: HoltWinters
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: false}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - cell: state
''')
        inputSequence = [8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0]
        expectedLevels = [4.41, 5.0301, 4.861461, 3.9758822099999995, 4.188847658099999, 4.488564182540999, 4.641300543221009, 4.452171014507124, 4.417702083098764, 4.468964465539193, 4.542144404518389, 4.521624943415324, 4.501496029430319, 4.50124424117252, 4.521651020236533, 4.5250653738300315, 4.52113444671092, 4.517931202495852, 4.521755238457936, 4.5242575202865805, 4.524388376599926, 4.5232770868680205, 4.523657174863217, 4.524410223942632]
        expectedTrends = [0.633, 0.62913, 0.3897993, 0.007185872999999787, 0.0689197455299998, 0.13815877920329986, 0.14253205364631272, 0.04303357893825353, 0.01978282583426929, 0.029226692816117207, 0.04241266666504093, 0.023533028334609074, 0.010434445638724935, 0.007228575469767743, 0.011182036548041254, 0.008851731661678497, 0.005016934027441439, 0.0025508805546886693, 0.00293282717690721, 0.002803663572428441, 0.0020018213947037003, 0.0010678880567208248, 8.615480382635968E-4, 8.289983506089451E-4]
        expectedCycle0s = [3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924, 1.4774493057281883, -0.523496688182195, -2.524384907518901]
        expectedCycle1s = [1.0, 1.0, 1.0, 3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924]
        expectedCycle2s = [1.0, 1.0, 3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924, 1.4774493057281883]
        expectedCycle3s = [1.0, 3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924, 1.4774493057281883, -0.523496688182195]

        for i, x in enumerate(inputSequence):
            result = engine.action(x)
            self.assertAlmostEqual(result["level"], expectedLevels[i], places=2)
            self.assertAlmostEqual(result["trend"], expectedTrends[i], places=2)
            self.assertAlmostEqual(result["cycle"][0], expectedCycle0s[i], places=2)
            self.assertAlmostEqual(result["cycle"][1], expectedCycle1s[i], places=2)
            self.assertAlmostEqual(result["cycle"][2], expectedCycle2s[i], places=2)
            self.assertAlmostEqual(result["cycle"][3], expectedCycle3s[i], places=2)

    def testHoltWintersPeriodicForecastAnAdditiveTrend(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: false}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - stat.sample.forecast1HoltWinters: {cell: state}
''')
        self.assertAlmostEqual(engine.action(8.0), 6.04, places=2)
        self.assertAlmostEqual(engine.action(6.0), 6.66, places=2)
        self.assertAlmostEqual(engine.action(4.0), 6.25, places=2)
        self.assertAlmostEqual(engine.action(2.0), 7.31, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.23, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.95, places=2)
        self.assertAlmostEqual(engine.action(4.0), 3.11, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.26, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.90, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.85, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.21, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.15, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.04, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.96, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.03, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.04, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.03, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.01, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.01, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.00, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: false}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - a.head: {stat.sample.forecastHoltWinters: [1, {cell: state}]}
''')
        self.assertAlmostEqual(engine.action(8.0), 6.04, places=2)
        self.assertAlmostEqual(engine.action(6.0), 6.66, places=2)
        self.assertAlmostEqual(engine.action(4.0), 6.25, places=2)
        self.assertAlmostEqual(engine.action(2.0), 7.31, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.23, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.95, places=2)
        self.assertAlmostEqual(engine.action(4.0), 3.11, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.26, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.90, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.85, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.21, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.15, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.04, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.96, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.03, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.04, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.03, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.01, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.01, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.00, places=2)

    def testHoltWintersPeriodicAccumulateAMultiplicativeTrend(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: HoltWinters
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: true}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - cell: state
''')
        inputSequence = [8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0]
        expectedLevels = [4.709999999999999, 5.603099999999999, 5.663990999999998, 4.956855509999998, 5.069345548299841, 5.352449557989157, 5.515749692921031, 5.2872313440581244, 5.260499462297407, 5.321153447406931, 5.407280797054145, 5.375630707249961, 5.355414240440458, 5.3571763764799645, 5.382751618896728, 5.384939928849279, 5.380451760749505, 5.377167967546183, 5.382406885399155, 5.385124017416345, 5.385192034968023, 5.383914333497006, 5.384582834532246, 5.38549356133603]
        expectedTrends = [0.7229999999999996, 0.7740299999999996, 0.5600882999999997, 0.17992116299999955, 0.15969182558995274, 0.19671548081976165, 0.18669087705339538, 0.062128109278504734, 0.03547011196673803, 0.04302527390957382, 0.055955896630866045, 0.029674100700350883, 0.01470693044739483, 0.010823492125028223, 0.015249017212548925, 0.011330805034549559, 0.00658511309425238, 0.003624441204979981, 0.004108784199377763, 0.0036912885447213458, 0.002604307246808362, 0.0014397046314606797, 0.0012083435525944988, 0.001119058527951486]
        expectedCycle0s = [1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833, 1.1146186181950057, 0.7428864674776158, 0.3713727943024939]
        expectedCycle1s = [1.0, 1.0, 1.0, 1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833]
        expectedCycle2s = [1.0, 1.0, 1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833, 1.1146186181950057]
        expectedCycle3s = [1.0, 1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833, 1.1146186181950057, 0.7428864674776158]

        for i, x in enumerate(inputSequence):
            result = engine.action(x)
            self.assertAlmostEqual(result["level"], expectedLevels[i], places=2)
            self.assertAlmostEqual(result["trend"], expectedTrends[i], places=2)
            self.assertAlmostEqual(result["cycle"][0], expectedCycle0s[i], places=2)
            self.assertAlmostEqual(result["cycle"][1], expectedCycle1s[i], places=2)
            self.assertAlmostEqual(result["cycle"][2], expectedCycle2s[i], places=2)
            self.assertAlmostEqual(result["cycle"][3], expectedCycle3s[i], places=2)

    def testHoltWintersPeriodicForecastAMultiplicativeTrend(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: true}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - stat.sample.forecast1HoltWinters: {cell: state}
''')
        self.assertAlmostEqual(engine.action(8.0), 5.43, places=2)
        self.assertAlmostEqual(engine.action(6.0), 6.38, places=2)
        self.assertAlmostEqual(engine.action(4.0), 6.22, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.37, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.56, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.08, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.64, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.47, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.91, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.90, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.11, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.25, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.05, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.96, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.02, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.08, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.04, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.02, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.01, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.00, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: true}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - a.head: {stat.sample.forecastHoltWinters: [1, {cell: state}]}
''')
        self.assertAlmostEqual(engine.action(8.0), 5.43, places=2)
        self.assertAlmostEqual(engine.action(6.0), 6.38, places=2)
        self.assertAlmostEqual(engine.action(4.0), 6.22, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.37, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.56, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.08, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.64, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.47, places=2)
        self.assertAlmostEqual(engine.action(8.0), 5.91, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.90, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.11, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.25, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.05, places=2)
        self.assertAlmostEqual(engine.action(6.0), 3.96, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.02, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.08, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.04, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.02, places=2)
        self.assertAlmostEqual(engine.action(8.0), 6.01, places=2)
        self.assertAlmostEqual(engine.action(6.0), 4.00, places=2)
        self.assertAlmostEqual(engine.action(4.0), 2.00, places=2)
        self.assertAlmostEqual(engine.action(2.0), 8.00, places=2)
