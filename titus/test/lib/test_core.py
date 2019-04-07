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

import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1Core(unittest.TestCase):
    def testDoAddition(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {+: [input, input]}
''')
        self.assertEqual(engine.action(3.14), 6.28)

    def testAdditionIntOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {+: [input, 10]}
''')
        self.assertEqual(engine.action(2147483637), 2147483647)
        self.assertRaises(PFARuntimeException, lambda: engine.action(2147483638))

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {+: [input, -10]}
''')
        self.assertEqual(engine.action(-2147483638), -2147483648)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2147483639))

    def testAdditionLongOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {+: [input, 10]}
''')
        self.assertEqual(engine.action(9223372036854775797), 9223372036854775807)
        self.assertRaises(PFARuntimeException, lambda: engine.action(9223372036854775798))

        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {+: [input, -10]}
''')
        self.assertEqual(engine.action(-9223372036854775798), -9223372036854775808)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-9223372036854775799))

    def testDoSubtraction(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {-: [input, 1.1]}
''')
        self.assertEqual(engine.action(3.14), 2.04)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {-: [1.1, input]}
''')
        self.assertEqual(engine.action(3.14), -2.04)

    def testHandleSubtractionIntOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {-: [-10, input]}
''')
        self.assertEqual(engine.action(2147483638), -2147483648)
        self.assertRaises(PFARuntimeException, lambda: engine.action(2147483639))

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {-: [10, input]}
''')
        self.assertEqual(engine.action(-2147483637), 2147483647)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2147483638))

    def testHandleSubtractionLongOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {-: [-10, input]}
''')
        self.assertEqual(engine.action(9223372036854775798), -9223372036854775808)
        self.assertRaises(PFARuntimeException, lambda: engine.action(9223372036854775799))

        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {-: [10, input]}
''')
        self.assertEqual(engine.action(-9223372036854775797), 9223372036854775807)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-9223372036854775798))

    def testHandleNegativeIntOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {u-: [input]}
''')
        self.assertEqual(engine.action(2147483647), -2147483647)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2147483648))

    def testHandleNegativeLongOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {u-: [input]}
''')
        self.assertEqual(engine.action(9223372036854775807), -9223372036854775807)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-9223372036854775808))

    def testDoMultiplication(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"*": [input, input]}
''')
        self.assertAlmostEqual(engine.action(3.14), 9.86, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"*": [{"*": [input, input]}, input]}
''')
        self.assertAlmostEqual(engine.action(3.14), 30.96, places=2)

    def testMultiplicationIntOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"*": [input, 2]}
''')
        self.assertEqual(engine.action(1073741823), 2147483646)
        self.assertRaises(PFARuntimeException, lambda: engine.action(1073741824))

    def testMultiplicationLongOverflows(self):
        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {"*": [input, 2]}
''')
        self.assertEqual(engine.action(4611686018427387903), 9223372036854775806)
        self.assertRaises(PFARuntimeException, lambda: engine.action(4611686018427387904))
        self.assertEqual(engine.action(-4611686018427387904), -9223372036854775808)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-4611686018427387905))

    def testFloatingPointDivision(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - {/: [5, 3]}
''')
        self.assertAlmostEqual(engine.action(None), 1.67, places=2)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {/: [5, 3]}
'''))

    def testIntegerDivision(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {//: [5, 3]}
''')
        self.assertEqual(engine.action(None), 1)

    def testNegationsWithTheRightOverflowHandling(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {u-: [input]}
''')
        self.assertEqual(engine.action(12), -12)
        self.assertEqual(engine.action(2147483647), -2147483647)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2147483648))

        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {u-: [input]}
''')
        self.assertEqual(engine.action(12), -12)
        self.assertEqual(engine.action(9223372036854775807), -9223372036854775807)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-9223372036854775808))

    def testInterpretPercentAsAModuloOperatorNotADivisionRemainder(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"%": [input, 6]}
''')
        self.assertEqual(engine.action(15), 3)
        self.assertEqual(engine.action(-15), 3)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"%": [input, -6]}
''')
        self.assertEqual(engine.action(15), -3)
        self.assertEqual(engine.action(-15), -3)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%": [input, 6]}
''')
        self.assertAlmostEqual(engine.action(15.2), 3.2, places=2)
        self.assertAlmostEqual(engine.action(-15.2), 2.8, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%": [input, -6]}
''')
        self.assertAlmostEqual(engine.action(15.2), -2.8, places=2)
        self.assertAlmostEqual(engine.action(-15.2), -3.2, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%": [input, 6.4]}
''')
        self.assertAlmostEqual(engine.action(15), 2.2, places=2)
        self.assertAlmostEqual(engine.action(-15), 4.2, places=2)
        self.assertAlmostEqual(engine.action(15.2), 2.4, places=2)
        self.assertAlmostEqual(engine.action(-15.2), 4.0, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%": [input, -6.4]}
''')
        self.assertAlmostEqual(engine.action(15), -4.2, places=2)
        self.assertAlmostEqual(engine.action(-15), -2.2, places=2)
        self.assertAlmostEqual(engine.action(15.2), -4.0, places=2)
        self.assertAlmostEqual(engine.action(-15.2), -2.4, places=2)

    def testInterpretPercentPercentAsADivisionRemainderNotAModuloOperator(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"%%": [input, 6]}
''')
        self.assertEqual(engine.action(15), 3)
        self.assertEqual(engine.action(-15), -3)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"%%": [input, -6]}
''')
        self.assertEqual(engine.action(15), 3)
        self.assertEqual(engine.action(-15), -3)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%%": [input, 6]}
''')
        self.assertAlmostEqual(engine.action(15.2), 3.2, places=2)
        self.assertAlmostEqual(engine.action(-15.2), -3.2, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%%": [input, -6]}
''')
        self.assertAlmostEqual(engine.action(15.2), 3.2, places=2)
        self.assertAlmostEqual(engine.action(-15.2), -3.2, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%%": [input, 6.4]}
''')
        self.assertAlmostEqual(engine.action(15), 2.2, places=2)
        self.assertAlmostEqual(engine.action(-15), -2.2, places=2)
        self.assertAlmostEqual(engine.action(15.2), 2.4, places=2)
        self.assertAlmostEqual(engine.action(-15.2), -2.4, places=2)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"%%": [input, -6.4]}
''')
        self.assertAlmostEqual(engine.action(15), 2.2, places=2)
        self.assertAlmostEqual(engine.action(-15), -2.2, places=2)
        self.assertAlmostEqual(engine.action(15.2), 2.4, places=2)
        self.assertAlmostEqual(engine.action(-15.2), -2.4, places=2)

    def testDoExponentiation(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {"**": [input, 30]}
''')
        self.assertAlmostEqual(engine.action(2.5), 867361737988.4036, places=2)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"**": [input, 30]}
''')
        self.assertEqual(engine.action(2), 1073741824)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {"**": [input, 30]}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(3))

        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {"**": [input, 30]}
''')
        self.assertEqual(engine.action(3), 205891132094649)

    def testDoCmp(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {cmp: [input, 5]}
''')
        self.assertEqual(engine.action(3), -1)
        self.assertEqual(engine.action(5), 0)
        self.assertEqual(engine.action(7), 1)

        engine, = PFAEngine.fromYaml('''
input: double
output: int
action:
  - {cmp: [input, 5.3]}
''')
        self.assertEqual(engine.action(5.2), -1)
        self.assertEqual(engine.action(5.3), 0)
        self.assertEqual(engine.action(5.4), 1)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cmp: [input, [HAL]]}
''')
        self.assertEqual(engine.action("GZK"), -1)
        self.assertEqual(engine.action("HAL"), 0)
        self.assertEqual(engine.action("IBM"), 1)

    def testSpecificNumericalOperators(self):
        LE, = PFAEngine.fromYaml('''
input: double
output: boolean
action:
  - {"<=": [input, 5.3]}
''')
        LT, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"<": [input, 5.3]}
""")
        NE, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"!=": [input, 5.3]}
""")
        EQ, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"==": [input, 5.3]}
""")
        GE, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {">=": [input, 5.3]}
""")
        GT, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {">": [input, 5.3]}
""")

        self.assertTrue(LE.action(5.2))
        self.assertTrue(LE.action(5.3))
        self.assertFalse(LE.action(5.4))

        self.assertTrue(LT.action(5.2))
        self.assertFalse(LT.action(5.3))
        self.assertFalse(LT.action(5.4))

        self.assertTrue(NE.action(5.2))
        self.assertFalse(NE.action(5.3))
        self.assertTrue(NE.action(5.4))

        self.assertFalse(EQ.action(5.2))
        self.assertTrue(EQ.action(5.3))
        self.assertFalse(EQ.action(5.4))

        self.assertFalse(GE.action(5.2))
        self.assertTrue(GE.action(5.3))
        self.assertTrue(GE.action(5.4))

        self.assertFalse(GT.action(5.2))
        self.assertFalse(GT.action(5.3))
        self.assertTrue(GT.action(5.4))

    def testSpecificNonNumericOperators(self):
        LE, = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"<=": [input, [HAL]]}
""")
        LT, = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"<": [input, [HAL]]}
""")
        NE, = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"!=": [input, [HAL]]}
""")
        EQ, = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"==": [input, [HAL]]}
""")
        GE, = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {">=": [input, [HAL]]}
""")
        GT, = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {">": [input, [HAL]]}
""")

        self.assertTrue(LE.action("GZK"))
        self.assertTrue(LE.action("HAL"))
        self.assertFalse(LE.action("IBM"))

        self.assertTrue(LT.action("GZK"))
        self.assertFalse(LT.action("HAL"))
        self.assertFalse(LT.action("IBM"))

        self.assertTrue(NE.action("GZK"))
        self.assertFalse(NE.action("HAL"))
        self.assertTrue(NE.action("IBM"))

        self.assertFalse(EQ.action("GZK"))
        self.assertTrue(EQ.action("HAL"))
        self.assertFalse(EQ.action("IBM"))

        self.assertFalse(GE.action("GZK"))
        self.assertTrue(GE.action("HAL"))
        self.assertTrue(GE.action("IBM"))

        self.assertFalse(GT.action("GZK"))
        self.assertFalse(GT.action("HAL"))
        self.assertTrue(GT.action("IBM"))

    def testRejectComparisonsBetweenDifferentTypesEvenIfTheyHaveTheSameStructure(self):
        engine, = PFAEngine.fromYaml('''
input: {type: enum, name: Category1, symbols: [z, y, x, w]}
output: boolean
action:
  - {"==": [input, {type: Category1, value: x}]}
''')
        self.assertFalse(engine.action("z"))
        self.assertFalse(engine.action("y"))
        self.assertTrue(engine.action("x"))
        self.assertFalse(engine.action("w"))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: {type: enum, name: Category1, symbols: [z, y, x, w]}
output: boolean
action:
  - {"==": [input, {type: {type: enum, name: Category2, symbols: [w, x, y, z]}, value: x}]}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: {type: enum, name: Category1, symbols: [z, y, x, w]}
output: boolean
action:
  - {"==": [input, {type: {type: enum, name: Category2, symbols: [w, x, y, whatever]}, value: x}]}
'''))

    def testDoMax(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {max: [input, 3.2]}
''')
        self.assertEqual(engine.action(2.2), 3.2)
        self.assertEqual(engine.action(4.2), 4.2)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {max: [input, 3]}
''')
        self.assertEqual(engine.action(2), 3)
        self.assertEqual(engine.action(4), 4)

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {max: [input, [HAL]]}
''')
        self.assertEqual(engine.action("GZK"), "HAL")
        self.assertEqual(engine.action("IBM"), "IBM")

    def testDoMin(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {min: [input, 3.2]}
''')
        self.assertEqual(engine.action(2.2), 2.2)
        self.assertEqual(engine.action(4.2), 3.2)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {min: [input, 3]}
''')
        self.assertEqual(engine.action(2), 2)
        self.assertEqual(engine.action(4), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {min: [input, [HAL]]}
''')
        self.assertEqual(engine.action("GZK"), "GZK")
        self.assertEqual(engine.action("IBM"), "HAL")

    def testLogicalOperators(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log:
      - {"&&": [true, true]}
      - {"&&": [true, false]}
      - {"&&": [false, true]}
      - {"&&": [false, false]}
''')
        engine.log = lambda message, ns: self.assertEqual(message, [True, False, False, False])
        engine.action(None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log:
      - {"||": [true, true]}
      - {"||": [true, false]}
      - {"||": [false, true]}
      - {"||": [false, false]}
''')
        engine.log = lambda message, ns: self.assertEqual(message, [True, True, True, False])
        engine.action(None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log:
      - {"^^": [true, true]}
      - {"^^": [true, false]}
      - {"^^": [false, true]}
      - {"^^": [false, false]}
''')
        engine.log = lambda message, ns: self.assertEqual(message, [False, True, True, False])
        engine.action(None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log:
      - {"!": [true]}
      - {"!": [false]}
''')
        engine.log = lambda message, ns: self.assertEqual(message, [False, True])
        engine.action(None)

    def testBitwiseOperators(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {"&": [85, 15]}
''')
        self.assertEqual(engine.action(None), 5)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {"|": [85, 15]}
''')
        self.assertEqual(engine.action(None), 95)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {"^": [85, 15]}
''')
        self.assertEqual(engine.action(None), 90)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {"~": [85]}
''')
        self.assertEqual(engine.action(None), -86)

    def testKleeneAnd(self):
        engine1, = PFAEngine.fromYaml('''
input:
  type: record
  name: Input
  fields:
    - {name: x, type: [boolean, "null"]}
    - {name: y, type: [boolean, "null"]}
output: [boolean, "null"]
action:
  {"&&&": [input.x, input.y]}
''')

        self.assertEqual(engine1.action({"x": False, "y": False}), {"boolean": False})
        self.assertEqual(engine1.action({"x": False, "y": None}), {"boolean": False})
        self.assertEqual(engine1.action({"x": False, "y": True}), {"boolean": False})

        self.assertEqual(engine1.action({"x": None, "y": False}), {"boolean": False})
        self.assertEqual(engine1.action({"x": None, "y": None}), None)
        self.assertEqual(engine1.action({"x": None, "y": True}), None)

        self.assertEqual(engine1.action({"x": True, "y": False}), {"boolean": False})
        self.assertEqual(engine1.action({"x": True, "y": None}), None)
        self.assertEqual(engine1.action({"x": True, "y": True}), {"boolean": True})

        engine2, = PFAEngine.fromYaml('''
input:
  type: record
  name: Input
  fields:
    - {name: x, type: [boolean, "null"]}
    - {name: y, type: [boolean, "null"]}
output: [boolean, "null"]
action:
  {"|||": [input.x, input.y]}
''')

        self.assertEqual(engine2.action({"x": False, "y": False}), {"boolean": False})
        self.assertEqual(engine2.action({"x": False, "y": None}), None)
        self.assertEqual(engine2.action({"x": False, "y": True}), {"boolean": True})

        self.assertEqual(engine2.action({"x": None, "y": False}), None)
        self.assertEqual(engine2.action({"x": None, "y": None}), None)
        self.assertEqual(engine2.action({"x": None, "y": True}), {"boolean": True})

        self.assertEqual(engine2.action({"x": True, "y": False}), {"boolean": True})
        self.assertEqual(engine2.action({"x": True, "y": None}), {"boolean": True})
        self.assertEqual(engine2.action({"x": True, "y": True}), {"boolean": True})

        engine3, = PFAEngine.fromYaml('''
input:
  type: record
  name: Input
  fields:
    - {name: x, type: [boolean, "null"]}
output: [boolean, "null"]
action:
  {"!!!": [input.x]}
''')

        self.assertEqual(engine3.action({"x": False}), {"boolean": True})
        self.assertEqual(engine3.action({"x": None}), None)
        self.assertEqual(engine3.action({"x": True}), {"boolean": False})

if __name__ == "__main__":
    unittest.main()
