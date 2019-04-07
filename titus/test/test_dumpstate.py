#!/usr/bin/env python

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import unittest

from titus.genpy import PFAEngine

class TestDumpstate(unittest.TestCase):
    def testPrivateCellsInt(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: int
    init: 0
action:
  - cell: test
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, "0")
        engine.action(3)
        self.assertEqual(engine.snapshot().cells["test"].init, "3")
        engine.action(2)
        self.assertEqual(engine.snapshot().cells["test"].init, "5")

    def testPrivateCellsString(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: "null"
cells:
  test:
    type: string
    init: ""
action:
  - cell: test
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, '""')
        engine.action("hey")
        self.assertEqual(engine.snapshot().cells["test"].init, '"hey"')
        engine.action("there")
        self.assertEqual(engine.snapshot().cells["test"].init, '"heythere"')

    def testPrivateCellsArray(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: {type: array, items: int}
    init: []
action:
  - cell: test
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, "[]")
        engine.action(3)
        self.assertEqual(engine.snapshot().cells["test"].init, "[3]")
        engine.action(2)
        self.assertEqual(engine.snapshot().cells["test"].init, "[3, 2]")

    def testPrivateCellsMap(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: {type: map, values: int}
    init: {"a": 0, "b": 0}
action:
  - cell: test
    path: [{string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
''')
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["b"], 0)
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["b"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["b"], 5)

    def testPrivateCellsRecord(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
    init: {a: 0, b: hey}
action:
  - cell: test
    path: [{string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
''')
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["a"], 0)
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["a"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["a"], 5)

    def testPrivateCellsUnion(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: [int, string]
    init: {int: 0}
action:
  - cell: test
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, '{"int": 0}')
        engine.action(3)
        self.assertEqual(engine.snapshot().cells["test"].init, '3')
        engine.action(2)
        self.assertEqual(engine.snapshot().cells["test"].init, '5')

    def testPublicCellsInt(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: int
    init: 0
    shared: true
action:
  - cell: test
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, "0")
        engine.action(3)
        self.assertEqual(engine.snapshot().cells["test"].init, "3")
        engine.action(2)
        self.assertEqual(engine.snapshot().cells["test"].init, "5")

    def testPublicCellsString(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: "null"
cells:
  test:
    type: string
    init: ""
    shared: true
action:
  - cell: test
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, '""')
        engine.action("hey")
        self.assertEqual(engine.snapshot().cells["test"].init, '"hey"')
        engine.action("there")
        self.assertEqual(engine.snapshot().cells["test"].init, '"heythere"')

    def testPublicCellsArray(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: {type: array, items: int}
    init: []
    shared: true
action:
  - cell: test
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, "[]")
        engine.action(3)
        self.assertEqual(engine.snapshot().cells["test"].init, "[3]")
        engine.action(2)
        self.assertEqual(engine.snapshot().cells["test"].init, "[3, 2]")

    def testPublicCellsMap(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: {type: map, values: int}
    init: {"a": 0, "b": 0}
    shared: true
action:
  - cell: test
    path: [{string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
''')
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["b"], 0)
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["b"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["b"], 5)

    def testPublicCellsRecord(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
    init: {a: 0, b: hey}
    shared: true
action:
  - cell: test
    path: [{string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
''')
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["a"], 0)
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["a"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().cells["test"].init)["a"], 5)

    def testPublicCellsUnion(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
cells:
  test:
    type: [int, string]
    init: {int: 0}
    shared: true
action:
  - cell: test
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
  - null
''')
        self.assertEqual(engine.snapshot().cells["test"].init, '{"int": 0}')
        engine.action(3)
        self.assertEqual(engine.snapshot().cells["test"].init, '3')
        engine.action(2)
        self.assertEqual(engine.snapshot().cells["test"].init, '5')

    def testPrivatePoolsInt(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: int
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: 0
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "3")
        engine.action(2)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "5")

    def testPrivatePoolsString(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: "null"
pools:
  test:
    type: string
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
    init: {string: ""}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action("hey")
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], '"hey"')
        engine.action("there")
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], '"heythere"')

    def testPrivatePoolsArray(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: {type: array, items: int}
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
    init: {value: [], type: {type: array, items: int}}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "[3]")
        engine.action(2)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "[3, 2]")

    def testPrivatePoolsMap(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: {type: map, values: int}
    init: {"zzz": {"a": 0, "b": 0}}
action:
  - pool: test
    path: [{string: zzz}, {string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {}, type: {type: map, values: int}}
  - null
''')
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["b"], 0)
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["b"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["b"], 5)

    def testPrivatePoolsRecord(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
action:
  - pool: test
    path: [{string: zzz}, {string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {a: 0, b: hey}, type: MyRecord}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["a"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["a"], 5)

    def testPrivatePoolsUnion(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: [int, string]
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
    init: {int: 0}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "3")
        engine.action(2)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "5")

    def testPublicPoolsInt(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: int
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: 0
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "3")
        engine.action(2)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "5")

    def testPublicPoolsString(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: "null"
pools:
  test:
    type: string
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
    init: {string: ""}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action("hey")
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], '"hey"')
        engine.action("there")
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], '"heythere"')

    def testPublicPoolsArray(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: {type: array, items: int}
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
    init: {value: [], type: {type: array, items: int}}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "[3]")
        engine.action(2)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "[3, 2]")

    def testPublicPoolsMap(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: {type: map, values: int}
    init: {"zzz": {"a": 0, "b": 0}}
    shared: true
action:
  - pool: test
    path: [{string: zzz}, {string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {}, type: {type: map, values: int}}
  - null
''')
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["b"], 0)
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["b"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["b"], 5)

    def testPublicPoolsRecord(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
    shared: true
action:
  - pool: test
    path: [{string: zzz}, {string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {a: 0, b: hey}, type: MyRecord}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["a"], 3)
        engine.action(2)
        self.assertEqual(json.loads(engine.snapshot().pools["test"].init["zzz"])["a"], 5)

    def testPublicPoolsUnion(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: "null"
pools:
  test:
    type: [int, string]
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
    init: {int: 0}
  - null
''')
        self.assertEqual(engine.snapshot().pools["test"].init, {})
        engine.action(3)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "3")
        engine.action(2)
        self.assertEqual(engine.snapshot().pools["test"].init["zzz"], "5")

if __name__ == "__main__":
    unittest.main()
