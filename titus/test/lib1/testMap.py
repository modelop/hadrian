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
    
class TestLib1Map(unittest.TestCase):
    def testGetLength(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: int
action:
  - {map.len: [input]}
''')
        self.assertEqual(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), 5)

        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: int
action:
  - {map.len: [input]}
''')
        self.assertEqual(engine.action({}), 0)

    def testGetKeys(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: array, items: string}
action:
  - {map.keys: [input]}
''')
        self.assertEqual(set(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})), set(["a", "b", "c", "d", "e"]))

        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: array, items: string}
action:
  - {map.keys: [input]}
''')
        self.assertEqual(engine.action({}), [])

    def testGetValues(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: array, items: int}
action:
  - {map.values: [input]}
''')
        self.assertEqual(set(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})), set([1, 2, 3, 4, 5]))

        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: array, items: int}
action:
  - {map.values: [input]}
''')
        self.assertEqual(engine.action({}), [])

    def testCheckContainsKey(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  map.containsKey:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
''')
        self.assertTrue(engine.action("a"))
        self.assertFalse(engine.action("z"))

        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  map.containsKey:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - params: [{x: string}]
      ret: boolean
      do: {"==": [x, input]}
''')
        self.assertTrue(engine.action("a"))
        self.assertFalse(engine.action("z"))

    def testCheckContainsKey(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: boolean
action:
  map.containsValue:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
''')
        self.assertTrue(engine.action(1))
        self.assertFalse(engine.action(9))

        engine, = PFAEngine.fromYaml('''
input: int
output: boolean
action:
  map.containsValue:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [x, input]}
''')
        self.assertTrue(engine.action(1))
        self.assertFalse(engine.action(9))

    def testAddKeyValuePairs(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: map, values: int}
action:
  map.add:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
    - 999
''')
        self.assertEqual(engine.action("a"), {"a": 999, "b": 2, "c": 3, "d": 4, "e": 5})
        self.assertEqual(engine.action("z"), {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "z": 999})

        engine, = PFAEngine.fromYaml('''
input: int
output: {type: map, values: int}
action:
  map.add:
    - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
    - input
''')
        self.assertEqual(engine.action(1), {"BA==": 2, "Ag==": 1, "Bg==": 3, "Cg==": 5, "CA==": 4})
        self.assertEqual(engine.action(999), {"BA==": 2, "Ag==": 1, "Bg==": 3, "Cg==": 5, "CA==": 4, "zg8=": 999})

    def testRemoveKeys(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: map, values: int}
action:
  map.remove:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
''')
        self.assertEqual(engine.action("a"), {"b": 2, "c": 3, "d": 4, "e": 5})
        self.assertEqual(engine.action("z"), {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    def testKeepOnlyCertainKeys(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: string}
output: {type: map, values: int}
action:
  map.only:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
''')
        self.assertEqual(engine.action(["b", "c", "e"]), {"b": 2, "c": 3, "e": 5})
        self.assertEqual(engine.action(["b", "c", "e", "z"]), {"b": 2, "c": 3, "e": 5})
        self.assertEqual(engine.action([]), {})

        engine, = PFAEngine.fromYaml('''
input: {type: array, items: string}
output: {type: map, values: int}
action:
  map.only:
    - {value: {}, type: {type: map, values: int}}
    - input
''')
        self.assertEqual(engine.action(["b", "c", "e"]), {})
        self.assertEqual(engine.action([]), {})

    def testEliminateOnlyCertainKeys(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: string}
output: {type: map, values: int}
action:
  map.except:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
''')
        self.assertEqual(engine.action(["b", "c", "e"]), {"a": 1, "d": 4})
        self.assertEqual(engine.action(["b", "c", "e", "z"]), {"a": 1, "d": 4})
        self.assertEqual(engine.action([]), {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

        engine, = PFAEngine.fromYaml('''
input: {type: array, items: string}
output: {type: map, values: int}
action:
  map.except:
    - {value: {}, type: {type: map, values: int}}
    - input
''')
        self.assertEqual(engine.action(["b", "c", "e"]), {})
        self.assertEqual(engine.action([]), {})

    def testUpdateWithAnOverlay(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: map, values: int}
action:
  map.update:
    - {value: {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, type: {type: map, values: int}}
    - input
''')
        self.assertEqual(engine.action({"b": 102, "c": 103, "z": 999}), {"a": 1, "b": 102, "c": 103, "d": 4, "e": 5, "z": 999})

    def testSplit(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: array, items: {type: map, values: int}}
action:
  map.split: input
''')
        self.assertEqual(sorted(engine.action({"a": 1, "b": 2, "c": 3})), sorted([{"a": 1}, {"b": 2}, {"c": 3}]))

    def testJoin(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: {type: map, values: int}}
output: {type: map, values: int}
action:
  map.join: input
''')
        self.assertEqual(sorted(engine.action([{"a": 1}, {"b": 2}, {"c": 3}])), sorted({"a": 1, "b": 2, "c": 3}))

    def testToSet(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: int}
output: {type: map, values: int}
action:
  - {map.toset: [input]}
''')
        self.assertEqual(engine.action([1, 2, 3, 4, 5]), {"BA==": 2, "Ag==": 1, "Bg==": 3, "Cg==": 5, "CA==": 4})

    def testFromSet(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: array, items: int}
action:
  - {map.fromset: [input]}
''')
        self.assertEqual(set(engine.action({"BA==": 2, "Ag==": 1, "Bg==": 3, "Cg==": 5, "CA==": 4})), set([1, 2, 3, 4, 5]))

        engine, = PFAEngine.fromYaml('''
input: {type: map, values: string}
output: {type: array, items: string}
action:
  - {map.fromset: [input]}
''')
        self.assertEqual(set(engine.action({"BA==": "two", "Ag==": "one", "Bg==": "three", "Cg==": "five", "CA==": "four"})), set(["one", "two", "three", "four", "five"]))

    def testIn(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: boolean
action:
  map.in:
    - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
    - input
''')
        self.assertTrue(engine.action(2))
        self.assertFalse(engine.action(0))

    def testUnion(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  map.fromset:
    map.union:
      - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
      - {map.toset: {value: [4, 5, 6, 7, 8], type: {type: array, items: int}}}
''')
        self.assertEqual(set(engine.action(None)), set([1, 2, 3, 4, 5, 6, 7, 8]))

    def testIntersection(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  map.fromset:
    map.intersection:
      - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
      - {map.toset: {value: [4, 5, 6, 7, 8], type: {type: array, items: int}}}
''')
        self.assertEqual(set(engine.action(None)), set([4, 5]))

    def testDiff(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  map.fromset:
    map.diff:
      - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
      - {map.toset: {value: [4, 5, 6, 7, 8], type: {type: array, items: int}}}
''')
        self.assertEqual(set(engine.action(None)), set([1, 2, 3]))

    def testSymDiff(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  map.fromset:
    map.symdiff:
      - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
      - {map.toset: {value: [4, 5, 6, 7, 8], type: {type: array, items: int}}}
''')
        self.assertEqual(set(engine.action(None)), set([1, 2, 3, 6, 7, 8]))

    def testSubset(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: int}
output: boolean
action:
  map.subset:
    - {map.toset: input}
    - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
''')
        self.assertTrue(engine.action([1, 2, 3]))
        self.assertFalse(engine.action([1, 2, 3, 999]))
        self.assertFalse(engine.action([888, 999]))

    def testDisjoint(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: int}
output: boolean
action:
  map.disjoint:
    - {map.toset: input}
    - {map.toset: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}}
''')
        self.assertFalse(engine.action([1, 2, 3]))
        self.assertFalse(engine.action([1, 2, 3, 999]))
        self.assertTrue(engine.action([888, 999]))

    def testMap(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: string}
output: {type: map, values: int}
action:
  map.map:
    - input
    - params: [{x: string}]
      ret: int
      do: {parse.int: [x, 10]}
''')
        self.assertEqual(engine.action({"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}), {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    def testMapWithKey(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: string}
output: {type: map, values: int}
action:
  map.mapWithKey:
    - input
    - params: [{key: string}, {value: string}]
      ret: int
      do:
        if: {">": [key, {string: "c"}]}
        then: {+: [{parse.int: [value, 10]}, 1000]}
        else: {parse.int: [value, 10]}
''')
        self.assertEqual(engine.action({"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}), {"a": 1, "b": 2, "c": 3, "d": 1004, "e": 1005})

    def testFilter(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: map, values: int}
action:
  map.filter:
    - input
    - params: [{x: int}]
      ret: boolean
      do: {"<": [x, 3]}
''')
        self.assertEqual(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), {"a": 1, "b": 2})

    def testFilterWithKey(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: map, values: int}
action:
  map.filterWithKey:
    - input
    - params: [{key: string}, {value: int}]
      ret: boolean
      do: {"&&": [{"<": [value, 3]}, {"==": [key, {string: "a"}]}]}
''')
        self.assertEqual(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), {"a": 1})

    def testFilterMap(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: map, values: int}
action:
  map.filterMap:
    - input
    - params: [{value: int}]
      ret: [int, "null"]
      do:
        if: {"==": [{"%": [value, 2]}, 0]}
        then: {"+": [value, 1000]}
        else: null
''')
        self.assertEqual(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), {"b": 1002, "d": 1004})

    def testFilterMapWithKey(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: map, values: int}
action:
  map.filterMapWithKey:
    - input
    - params: [{key: string}, {value: int}]
      ret: [int, "null"]
      do:
        if: {"&&": [{"==": [{"%": [value, 2]}, 0]}, {"==": [key, {string: "b"}]}]}
        then: {"+": [value, 1000]}
        else: null
''')
        self.assertEqual(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), {"b": 1002})

    def testFlatMapWithKey(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: {type: map, values: int}
action:
  map.flatMapWithKey:
    - input
    - params: [{key: string}, {value: int}]
      ret: {type: map, values: int}
      do:
        map.add:
          - map.add:
              - {value: {}, type: {type: map, values: int}}
              - key
              - value
          - {s.concat: [key, key]}
          - {+: [100, value]}
''')
        self.assertEqual(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}), {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "aa": 101, "bb": 102, "cc": 103, "dd": 104, "ee": 105})

    def testCorresponds(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: boolean
action:
  map.corresponds:
    - input
    - {value: {"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}, type: {type: map, values: string}}
    - params: [{x: int}, {y: string}]
      ret: boolean
      do: {"==": [x, {parse.int: [y, 10]}]}
''')
        self.assertTrue(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}))
        self.assertFalse(engine.action({"a": 111, "b": 2, "c": 3, "d": 4, "e": 5}))

    def testCorrespondsWithKey(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: int}
output: boolean
action:
  map.correspondsWithKey:
    - input
    - {value: {"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}, type: {type: map, values: string}}
    - params: [{k: string}, {x: int}, {y: string}]
      ret: boolean
      do:
        if: {"==": [k, {string: "a"}]}
        then: true
        else: {"==": [x, {parse.int: [y, 10]}]}
''')
        self.assertTrue(engine.action({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}))
        self.assertTrue(engine.action({"a": 111, "b": 2, "c": 3, "d": 4, "e": 5}))
        self.assertFalse(engine.action({"a": 1, "b": 222, "c": 3, "d": 4, "e": 5}))
