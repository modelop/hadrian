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
    
class TestLib1Interp(unittest.TestCase):
    def testNearestPick1d(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: int
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: int}
    init:
      - {x: 1.0, to: 7}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  +:
    - interp.nearest:
        - input
        - cell: table
    - 100
''')
        self.assertEqual(engine.action(-0.2), 105)
        self.assertEqual(engine.action(-0.1), 105)
        self.assertEqual(engine.action(0.0), 105)
        self.assertEqual(engine.action(0.1), 105)
        self.assertEqual(engine.action(0.2), 105)
        self.assertEqual(engine.action(0.3), 106)
        self.assertEqual(engine.action(0.4), 106)
        self.assertEqual(engine.action(0.5), 106)
        self.assertEqual(engine.action(0.6), 106)
        self.assertEqual(engine.action(0.7), 106)
        self.assertEqual(engine.action(0.8), 107)
        self.assertEqual(engine.action(0.9), 107)
        self.assertEqual(engine.action(1.0), 107)
        self.assertEqual(engine.action(1.1), 107)
        self.assertEqual(engine.action(1.2), 107)

    def testNearestPickNd(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: int
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: {type: array, items: double}}
          - {name: to, type: int}
    init:
      - {x: [0.0, 0.0, 1.0], to: 7}
      - {x: [0.0, 0.0, 0.0], to: 5}
      - {x: [0.0, 0.0, 0.5], to: 6}
action:
  +:
    - interp.nearest:
        - type:
            type: array
            items: double
          new:
            - 0.0
            - 0.0
            - input
        - cell: table
    - 100
''')
        self.assertEqual(engine.action(-0.2), 105)
        self.assertEqual(engine.action(-0.1), 105)
        self.assertEqual(engine.action(0.0), 105)
        self.assertEqual(engine.action(0.1), 105)
        self.assertEqual(engine.action(0.2), 105)
        self.assertEqual(engine.action(0.3), 106)
        self.assertEqual(engine.action(0.4), 106)
        self.assertEqual(engine.action(0.5), 106)
        self.assertEqual(engine.action(0.6), 106)
        self.assertEqual(engine.action(0.7), 106)
        self.assertEqual(engine.action(0.8), 107)
        self.assertEqual(engine.action(0.9), 107)
        self.assertEqual(engine.action(1.0), 107)
        self.assertEqual(engine.action(1.1), 107)
        self.assertEqual(engine.action(1.2), 107)

    def testNearestAbstractMetric(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: string}
          - {name: to, type: int}
    init:
      - {x: "aaa", to: 3}
      - {x: "aaaaaa", to: 6}
      - {x: "aaaaaaaaa", to: 9}
action:
  +:
    - interp.nearest:
        - input
        - cell: table
        - params: [{a: string}, {b: string}]
          ret: double
          do: {m.abs: {"-": [{s.len: a}, {s.len: b}]}}
    - 100
''')
        self.assertEqual(engine.action("b"), 103)
        self.assertEqual(engine.action("bb"), 103)
        self.assertEqual(engine.action("bbb"), 103)
        self.assertEqual(engine.action("bbbb"), 103)
        self.assertEqual(engine.action("bbbbb"), 106)
        self.assertEqual(engine.action("bbbbbb"), 106)
        self.assertEqual(engine.action("bbbbbbb"), 106)
        self.assertEqual(engine.action("bbbbbbbb"), 109)
        self.assertEqual(engine.action("bbbbbbbbb"), 109)
        self.assertEqual(engine.action("bbbbbbbbbb"), 109)
        self.assertEqual(engine.action("bbbbbbbbbbb"), 109)

    def testLinearScalars(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
    init:
      - {x: 1.0, to: 4}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  +:
    - interp.linear:
        - input
        - cell: table
    - 100
''')
        self.assertAlmostEqual(engine.action(-0.2), 104.6, places=3)
        self.assertAlmostEqual(engine.action(-0.1), 104.8, places=3)
        self.assertAlmostEqual(engine.action(0.0), 105.0, places=3)
        self.assertAlmostEqual(engine.action(0.1), 105.2, places=3)
        self.assertAlmostEqual(engine.action(0.2), 105.4, places=3)
        self.assertAlmostEqual(engine.action(0.3), 105.6, places=3)
        self.assertAlmostEqual(engine.action(0.4), 105.8, places=3)
        self.assertAlmostEqual(engine.action(0.5), 106.0, places=3)
        self.assertAlmostEqual(engine.action(0.6), 105.6, places=3)
        self.assertAlmostEqual(engine.action(0.7), 105.2, places=3)
        self.assertAlmostEqual(engine.action(0.8), 104.8, places=3)
        self.assertAlmostEqual(engine.action(0.9), 104.4, places=3)
        self.assertAlmostEqual(engine.action(1.0), 104.0, places=3)
        self.assertAlmostEqual(engine.action(1.1), 103.6, places=3)
        self.assertAlmostEqual(engine.action(1.2), 103.2, places=3)

    def testLinearVectors(self):
        engine, = PFAEngine.fromYaml('''
input: double
output:
  type: array
  items: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: {type: array, items: double}}
    init:
      - {x: 1.0, to: [1, 4]}
      - {x: 0.0, to: [1, 5]}
      - {x: 0.5, to: [1, 6]}
action:
  interp.linear:
    - input
    - cell: table
''')
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(-0.2), [1.0, 4.6])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(-0.1), [1.0, 4.8])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.0), [1.0, 5.0])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.1), [1.0, 5.2])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.2), [1.0, 5.4])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.3), [1.0, 5.6])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.4), [1.0, 5.8])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.5), [1.0, 6.0])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.6), [1.0, 5.6])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.7), [1.0, 5.2])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.8), [1.0, 4.8])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(0.9), [1.0, 4.4])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(1.0), [1.0, 4.0])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(1.1), [1.0, 3.6])), 0.0, places=3)
        self.assertAlmostEqual(sum((x - y)**2 for x, y in zip(engine.action(1.2), [1.0, 3.2])), 0.0, places=3)

    def testLinearScalarsFlat(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
    init:
      - {x: 1.0, to: 4}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  +:
    - interp.linearFlat:
        - input
        - cell: table
    - 100
''')
        self.assertAlmostEqual(engine.action(-0.2), 105.0, places=3)
        self.assertAlmostEqual(engine.action(-0.1), 105.0, places=3)
        self.assertAlmostEqual(engine.action(0.0), 105.0, places=3)
        self.assertAlmostEqual(engine.action(0.1), 105.2, places=3)
        self.assertAlmostEqual(engine.action(0.2), 105.4, places=3)
        self.assertAlmostEqual(engine.action(0.3), 105.6, places=3)
        self.assertAlmostEqual(engine.action(0.4), 105.8, places=3)
        self.assertAlmostEqual(engine.action(0.5), 106.0, places=3)
        self.assertAlmostEqual(engine.action(0.6), 105.6, places=3)
        self.assertAlmostEqual(engine.action(0.7), 105.2, places=3)
        self.assertAlmostEqual(engine.action(0.8), 104.8, places=3)
        self.assertAlmostEqual(engine.action(0.9), 104.4, places=3)
        self.assertAlmostEqual(engine.action(1.0), 104.0, places=3)
        self.assertAlmostEqual(engine.action(1.1), 104.0, places=3)
        self.assertAlmostEqual(engine.action(1.2), 104.0, places=3)

    def testLinearScalarsMissing(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: ["null", double]
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
    init:
      - {x: 1.0, to: 4}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  interp.linearMissing:
    - input
    - cell: table
''')
        self.assertEqual(engine.action(-0.2), None)
        self.assertEqual(engine.action(-0.1), None)
        self.assertAlmostEqual(engine.action(0.0), 5.0, places=3)
        self.assertAlmostEqual(engine.action(0.1), 5.2, places=3)
        self.assertAlmostEqual(engine.action(0.2), 5.4, places=3)
        self.assertAlmostEqual(engine.action(0.3), 5.6, places=3)
        self.assertAlmostEqual(engine.action(0.4), 5.8, places=3)
        self.assertAlmostEqual(engine.action(0.5), 6.0, places=3)
        self.assertAlmostEqual(engine.action(0.6), 5.6, places=3)
        self.assertAlmostEqual(engine.action(0.7), 5.2, places=3)
        self.assertAlmostEqual(engine.action(0.8), 4.8, places=3)
        self.assertAlmostEqual(engine.action(0.9), 4.4, places=3)
        self.assertAlmostEqual(engine.action(1.0), 4.0, places=3)
        self.assertEqual(engine.action(1.1), None)
        self.assertEqual(engine.action(1.2), None)

if __name__ == "__main__":
    unittest.main()
