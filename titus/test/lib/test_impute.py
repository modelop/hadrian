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
    
class TestLib1Impute(unittest.TestCase):
    def testErrorOnNull(self):
        engine, = PFAEngine.fromYaml('''
input: [int, "null"]
output: int
action:
  - {impute.errorOnNull: [input]}
''')
        self.assertEqual(engine.action(3), 3)
        self.assertEqual(engine.action(12), 12)
        self.assertRaises(PFARuntimeException, lambda: engine.action(None))
        self.assertEqual(engine.action(5), 5)

    def testDefaultOnNull(self):
        engine, = PFAEngine.fromYaml('''
input: [int, "null"]
output: int
action:
  - {impute.defaultOnNull: [input, 12]}
''')
        self.assertEqual(engine.action(3), 3)
        self.assertEqual(engine.action(12), 12)
        self.assertEqual(engine.action(None), 12)
        self.assertEqual(engine.action(5), 5)

        engine, = PFAEngine.fromYaml('''
input: [string, "null"]
output: string
action:
  - {impute.defaultOnNull: [input, [oops]]}
''')
        self.assertEqual(engine.action("one"), "one")
        self.assertEqual(engine.action("two"), "two")
        self.assertEqual(engine.action(None), "oops")
        self.assertEqual(engine.action("four"), "four")

        def bad():
            PFAEngine.fromYaml('''
input: ["null", "null"]
output: "null"
action:
  - {impute.defaultOnNull: [input, "null"]}
''')
        self.assertRaises(SchemaParseException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: [[int, string], "null"]
output: [int, string]
action:
  - {impute.defaultOnNull: [input, 12]}
''')
        self.assertRaises(SchemaParseException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: [int, string, "null"]
output: int
action:
  - {impute.defaultOnNull: [input, 12]}
''')
        self.assertRaises(PFASemanticException, bad)

    def testIsNan(self):
        floatEngine, = PFAEngine.fromYaml('''
input: float
output: boolean
action: {impute.isnan: input}
''')
        self.assertFalse(floatEngine.action(123.4))
        self.assertTrue(floatEngine.action(float("nan")))
        self.assertFalse(floatEngine.action(float("inf")))
        self.assertFalse(floatEngine.action(float("-inf")))

        doubleEngine, = PFAEngine.fromYaml('''
input: double
output: boolean
action: {impute.isnan: input}
''')
        self.assertFalse(doubleEngine.action(123.4))
        self.assertTrue(doubleEngine.action(float("nan")))
        self.assertFalse(doubleEngine.action(float("inf")))
        self.assertFalse(doubleEngine.action(float("-inf")))

    def testIsInf(self):
        floatEngine, = PFAEngine.fromYaml('''
input: float
output: boolean
action: {impute.isinf: input}
''')
        self.assertFalse(floatEngine.action(123.4))
        self.assertFalse(floatEngine.action(float("nan")))
        self.assertTrue(floatEngine.action(float("inf")))
        self.assertTrue(floatEngine.action(float("-inf")))

        doubleEngine, = PFAEngine.fromYaml('''
input: double
output: boolean
action: {impute.isinf: input}
''')
        self.assertFalse(doubleEngine.action(123.4))
        self.assertFalse(doubleEngine.action(float("nan")))
        self.assertTrue(doubleEngine.action(float("inf")))
        self.assertTrue(doubleEngine.action(float("-inf")))

    def testIsNum(self):
        floatEngine, = PFAEngine.fromYaml('''
input: float
output: boolean
action: {impute.isnum: input}
''')
        self.assertTrue(floatEngine.action(123.4))
        self.assertFalse(floatEngine.action(float("nan")))
        self.assertFalse(floatEngine.action(float("inf")))
        self.assertFalse(floatEngine.action(float("-inf")))

        doubleEngine, = PFAEngine.fromYaml('''
input: double
output: boolean
action: {impute.isnum: input}
''')
        self.assertTrue(doubleEngine.action(123.4))
        self.assertFalse(doubleEngine.action(float("nan")))
        self.assertFalse(doubleEngine.action(float("inf")))
        self.assertFalse(doubleEngine.action(float("-inf")))

    def testErrorOnNonNum(self):
        floatEngine, = PFAEngine.fromYaml('''
input: float
output: float
action: {impute.errorOnNonNum: input}
''')
        self.assertEqual(floatEngine.action(123.4), 123.4)
        self.assertRaises(PFARuntimeException, lambda: floatEngine.action(float("nan")))
        self.assertRaises(PFARuntimeException, lambda: floatEngine.action(float("inf")))
        self.assertRaises(PFARuntimeException, lambda: floatEngine.action(float("-inf")))

        doubleEngine, = PFAEngine.fromYaml('''
input: double
output: double
action: {impute.errorOnNonNum: input}
''')
        self.assertEqual(doubleEngine.action(123.4), 123.4)
        self.assertRaises(PFARuntimeException, lambda: doubleEngine.action(float("nan")))
        self.assertRaises(PFARuntimeException, lambda: doubleEngine.action(float("inf")))
        self.assertRaises(PFARuntimeException, lambda: doubleEngine.action(float("-inf")))

    def testDefaultOnNonNum(self):
        floatEngine, = PFAEngine.fromYaml('''
input: float
output: float
action: {impute.defaultOnNonNum: [input, {float: 999.0}]}
''')
        self.assertEqual(floatEngine.action(123.4), 123.4)
        self.assertEqual(floatEngine.action(float("nan")), 999.0)
        self.assertEqual(floatEngine.action(float("inf")), 999.0)
        self.assertEqual(floatEngine.action(float("-inf")), 999.0)

        doubleEngine, = PFAEngine.fromYaml('''
input: double
output: double
action: {impute.defaultOnNonNum: [input, 999.0]}
''')
        self.assertEqual(doubleEngine.action(123.4), 123.4)
        self.assertEqual(doubleEngine.action(float("nan")), 999.0)
        self.assertEqual(doubleEngine.action(float("inf")), 999.0)
        self.assertEqual(doubleEngine.action(float("-inf")), 999.0)

if __name__ == "__main__":
    unittest.main()
