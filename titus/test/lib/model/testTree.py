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
    
class TestLib1ModelTree(unittest.TestCase):
    def testNumericalComparisons(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action("<="))
        self.assertFalse(engine.action("<"))
        self.assertTrue(engine.action("!="))
        self.assertFalse(engine.action("=="))
        self.assertTrue(engine.action(">"))
        self.assertTrue(engine.action(">="))

        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: int}
        new: {field: {type: Fields, value: two}, operator: input, value: 2}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action("<="))
        self.assertFalse(engine.action("<"))
        self.assertTrue(engine.action("!="))
        self.assertFalse(engine.action("=="))
        self.assertTrue(engine.action(">"))
        self.assertTrue(engine.action(">="))

        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 3, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: one}, operator: input, value: 2.0}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action("<="))
        self.assertFalse(engine.action("<"))
        self.assertTrue(engine.action("!="))
        self.assertFalse(engine.action("=="))
        self.assertTrue(engine.action(">"))
        self.assertTrue(engine.action(">="))

    def testStringComparisons(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: string}
        new: {field: {type: Fields, value: three}, operator: input, value: ["GZK"]}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action("<="))
        self.assertFalse(engine.action("<"))
        self.assertTrue(engine.action("!="))
        self.assertFalse(engine.action("=="))
        self.assertTrue(engine.action(">"))
        self.assertTrue(engine.action(">="))

    def testOtherComparisons(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: bytes}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: bytes}
        new: {field: {type: Fields, value: three}, operator: input, value: {type: bytes, value: "GZK"}}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action("<="))
        self.assertFalse(engine.action("<"))
        self.assertTrue(engine.action("!="))
        self.assertFalse(engine.action("=="))
        self.assertTrue(engine.action(">"))
        self.assertTrue(engine.action(">="))

    def testDoSetInclusion(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: string}}
        value: {field: three, operator: in, value: ["IBM", "HAL", "GZK"]}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: string}}
        value: {field: three, operator: in, value: ["IBM", "HALLY", "GZK"]}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: int}}
        value: {field: one, operator: in, value: [0, 1, 2]}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: int}}
        value: {field: one, operator: in, value: [0, 2, 3]}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertFalse(engine.action(None))

    def testPassThroughMissingValues(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: ["null", double]}, {name: three, type: string}]}
        value: {one: 1, two: null, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.missingTest: [datum, tree]
''')
        self.assertEqual(engine.action("<="), None)

#     def testNotWorkForSimpleTest(self):
#         engine, = PFAEngine.fromYaml('''
# input: string
# output: ["null", boolean]
# action:
#   - let:
#       datum:
#         type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: ["null", double]}, {name: three, type: string}]}
#         value: {one: 1, two: null, three: "HAL"}
#       tree:
#         type:
#           type: record
#           name: Tree
#           fields:
#             - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
#             - {name: operator, type: string}
#             - {name: value, type: double}
#         new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
#   - model.tree.simpleTest: [datum, tree]
# ''')
#         self.assertRaises(PFARuntimeException, lambda: engine.action("<="))

    def testBeOkayForSimpleTestIfTheTypesMatch(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: ["null", double]}, {name: three, type: string}]}
        value: {one: 1, two: null, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: ["null", double]}
        value: {field: two, operator: "<=", value: null}
  - model.tree.simpleTest: [datum, tree]
''')
        self.assertTrue(engine.action("=="))

    def testNotActLikeSimpleTestWhenNothingIsMissing(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.missingTest: [datum, tree]
''')
        self.assertEqual(engine.action("<="), {"boolean": False})

    def testEvaluateAChainOfComparisons(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: ["null", int]}, {name: two, type: ["null", double]}, {name: three, type: ["null", string]}]}
        value: {one: null, two: null, three: null}
      comparisons:
        type:
          type: array
          items:
            type: record
            name: Comparison
            fields:
              - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
              - {name: operator, type: string}
              - {name: value, type: [int, double, string]}
        value:
          - {field: one, operator: "==", value: {int: 1}}
          - {field: two, operator: "==", value: {double: 2.2}}
          - {field: three, operator: "==", value: {string: "THREE"}}
  - model.tree.surrogateTest:
      - datum
      - comparisons
      - params: [{d: Datum}, {c: Comparison}]
        ret: ["null", boolean]
        do: {model.tree.missingTest: [d, c]}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: ["null", int]}, {name: two, type: ["null", double]}, {name: three, type: ["null", string]}]}
        value: {one: null, two: null, three: {string: "THREE"}}
      comparisons:
        type:
          type: array
          items:
            type: record
            name: Comparison
            fields:
              - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
              - {name: operator, type: string}
              - {name: value, type: [int, double, string]}
        value:
          - {field: one, operator: "==", value: {int: 1}}
          - {field: two, operator: "==", value: {double: 2.2}}
          - {field: three, operator: "==", value: {string: "THREE"}}
  - model.tree.surrogateTest:
      - datum
      - comparisons
      - params: [{d: Datum}, {c: Comparison}]
        ret: ["null", boolean]
        do: {model.tree.missingTest: [d, c]}
''')
        engine.action(None)

    def testWorkForATypicalExample(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - name: field
          type:
            type: enum
            name: Fields
            symbols: [one, two, three]
        - {name: operator, type: string}
        - {name: value, type: [int, double, string]}
        - {name: pass, type: [string, TreeNode]}
        - {name: fail, type: [string, TreeNode]}
    init:
      field: one
      operator: "<"
      value: {double: 12}
      pass:
        TreeNode:
          field: two
          operator: ">"
          value: {double: 3.5}
          pass: {string: yes-yes}
          fail: {string: yes-no}
      fail:
        TreeNode:
          field: three
          operator: ==
          value: {string: TEST}
          pass: {string: no-yes}
          fail: {string: no-no}
action:
  - model.tree.simpleWalk:
      - input
      - cell: tree
      - params: [{d: Datum}, {t: TreeNode}]
        ret: boolean
        do: {model.tree.simpleTest: [d, t]}
''')
        self.assertEqual(engine.action({"one": 1, "two": 7, "three": "whatever"}), "yes-yes")
        self.assertEqual(engine.action({"one": 1, "two": 0, "three": "whatever"}), "yes-no")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "TEST"}), "no-yes")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "ZEST"}), "no-no")

    def testSimpleTree(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - name: field
          type:
            type: enum
            name: Fields
            symbols: [one, two, three]
        - {name: operator, type: string}
        - {name: value, type: [int, double, string]}
        - {name: pass, type: [string, TreeNode]}
        - {name: fail, type: [string, TreeNode]}
    init:
      field: one
      operator: "<"
      value: {double: 12}
      pass:
        TreeNode:
          field: two
          operator: ">"
          value: {double: 3.5}
          pass: {string: yes-yes}
          fail: {string: yes-no}
      fail:
        TreeNode:
          field: three
          operator: ==
          value: {string: TEST}
          pass: {string: no-yes}
          fail: {string: no-no}
action:
  {model.tree.simpleTree: [input, cell: tree]}
''')
        self.assertEqual(engine.action({"one": 1, "two": 7, "three": "whatever"}), "yes-yes")
        self.assertEqual(engine.action({"one": 1, "two": 0, "three": "whatever"}), "yes-no")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "TEST"}), "no-yes")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "ZEST"}), "no-no")

    def testWorkForSameExampleUnionOrderSwitched(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - name: field
          type:
            type: enum
            name: Fields
            symbols: [one, two, three]
        - {name: operator, type: string}
        - {name: value, type: [int, double, string]}
        - {name: pass, type: [TreeNode, string]}
        - {name: fail, type: [TreeNode, string]}
    init:
      field: one
      operator: "<"
      value: {double: 12}
      pass:
        TreeNode:
          field: two
          operator: ">"
          value: {double: 3.5}
          pass: {string: yes-yes}
          fail: {string: yes-no}
      fail:
        TreeNode:
          field: three
          operator: ==
          value: {string: TEST}
          pass: {string: no-yes}
          fail: {string: no-no}
action:
  - model.tree.simpleWalk:
      - input
      - cell: tree
      - params: [{d: Datum}, {t: TreeNode}]
        ret: boolean
        do: {model.tree.simpleTest: [d, t]}
''')
        self.assertEqual(engine.action({"one": 1, "two": 7, "three": "whatever"}), "yes-yes")
        self.assertEqual(engine.action({"one": 1, "two": 0, "three": "whatever"}), "yes-no")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "TEST"}), "no-yes")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "ZEST"}), "no-no")

    def testWorkWithACompletelyUserDefinedPredicate(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - {name: field, type: string}
        - {name: pass, type: [string, TreeNode]}
        - {name: fail, type: [string, TreeNode]}
    init:
      field: one
      pass:
        TreeNode:
          field: two
          pass: {string: yes-yes}
          fail: {string: yes-no}
      fail:
        TreeNode:
          field: three
          pass: {string: no-yes}
          fail: {string: no-no}
action:
  - {model.tree.simpleWalk: [input, {cell: tree}, {fcn: u.myPredicate}]}
fcns:
  myPredicate:
    params:
      - datum: Datum
      - treeNode: TreeNode
    ret: boolean
    do:
      cond:
        - {if: {"==": [treeNode.field, [one]]}, then: {"<": [datum.one, 12]}}
        - {if: {"==": [treeNode.field, [two]]}, then: {">": [datum.two, 3.5]}}
      else: {"==": [datum.three, [TEST]]}
''')
        self.assertEqual(engine.action({"one": 1, "two": 7, "three": "whatever"}), "yes-yes")
        self.assertEqual(engine.action({"one": 1, "two": 0, "three": "whatever"}), "yes-no")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "TEST"}), "no-yes")
        self.assertEqual(engine.action({"one": 15, "two": 7, "three": "ZEST"}), "no-no")

    def testMissingWalk(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: Datum, fields: [{name: one, type: ["null", int]}, {name: two, type: ["null", double]}, {name: three, type: ["null", string]}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - name: field
          type:
            type: enum
            name: Fields
            symbols: [one, two, three]
        - {name: operator, type: string}
        - {name: value, type: ["null", int, double, string, {type: array, items: double}]}
        - {name: pass, type: [string, TreeNode]}
        - {name: fail, type: [string, TreeNode]}
        - {name: missing, type: [string, TreeNode]}
    init:
      field: one
      operator: "<"
      value: {double: 12}
      pass:
        TreeNode:
          field: two
          operator: ">"
          value: {double: 3.5}
          pass: {string: yes-yes}
          fail: {string: yes-no}
          missing: {string: yes-maybe}
      fail:
        TreeNode:
          field: two
          operator: "in"
          value: {array: [3.14, 6.28]}
          pass: {string: no-yes}
          fail: {string: no-no}
          missing: {string: no-maybe}
      missing:
        TreeNode:
          field: three
          operator: ==
          value: {string: TEST}
          pass: {string: maybe-yes}
          fail: {string: maybe-no}
          missing: {string: maybe-maybe}
action:
  - model.tree.missingWalk:
      - input
      - cell: tree
      - params: [{d: Datum}, {t: TreeNode}]
        ret: ["null", boolean]
        do: {model.tree.missingTest: [d, t]}
''')

        self.assertEqual(engine.action({"one": {"int": 1}, "two": {"double": 7}, "three": {"string": "whatever"}}), "yes-yes")
        self.assertEqual(engine.action({"one": {"int": 1}, "two": {"double": 0}, "three": {"string": "whatever"}}), "yes-no")
        self.assertEqual(engine.action({"one": {"int": 1}, "two": None, "three": {"string": "whatever"}}), "yes-maybe")

        self.assertEqual(engine.action({"one": {"int": 15}, "two": {"double": 3.14}, "three": {"string": "whatever"}}), "no-yes")
        self.assertEqual(engine.action({"one": {"int": 15}, "two": {"double": 1.61}, "three": {"string": "whatever"}}), "no-no")
        self.assertEqual(engine.action({"one": {"int": 15}, "two": None, "three": {"string": "whatever"}}), "no-maybe")

        self.assertEqual(engine.action({"one": None, "two": {"double": 7}, "three": {"string": "TEST"}}), "maybe-yes")
        self.assertEqual(engine.action({"one": None, "two": {"double": 7}, "three": {"string": "ZEST"}}), "maybe-no")
        self.assertEqual(engine.action({"one": None, "two": {"double": 7}, "three": None}), "maybe-maybe")
