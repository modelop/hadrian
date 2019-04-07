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

import math
import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1Parse(unittest.TestCase):
    def testParseInt(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action: {parse.int: [input, 10]}
''')
        self.assertEqual(engine.action("   123   "), 123)
        self.assertEqual(engine.action("   +123   "), 123)
        self.assertEqual(engine.action("   -123   "), -123)
        self.assertEqual(engine.action("   2147483647   "), 2147483647)
        self.assertRaises(PFARuntimeException, lambda: engine.action("   2147483648   "))
        self.assertEqual(engine.action("   -2147483648   "), -2147483648)
        self.assertRaises(PFARuntimeException, lambda: engine.action("   -2147483649   "))

    def testParseLong(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: long
action: {parse.long: [input, 10]}
''')
        self.assertEqual(engine.action("   123   "), 123)
        self.assertEqual(engine.action("   +123   "), 123)
        self.assertEqual(engine.action("   -123   "), -123)
        self.assertEqual(engine.action("   9223372036854775807   "), 9223372036854775807)
        self.assertRaises(PFARuntimeException, lambda: engine.action("   9223372036854775808   "))
        self.assertEqual(engine.action("   -9223372036854775808   "), -9223372036854775808)
        self.assertRaises(PFARuntimeException, lambda: engine.action("   -9223372036854775809   "))

    def testParseFloat(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: float
action: {parse.float: input}
''')
        self.assertEqual(engine.action("   123   "), 123.0)
        self.assertEqual(engine.action("   -123   "), -123.0)
        self.assertEqual(engine.action("   3.4028234e38   "), 3.4028234e38)
        self.assertEqual(engine.action("   -3.4028234e38   "), -3.4028234e38)
        self.assertEqual(engine.action("   3.4028236e38   "), float("inf"))
        self.assertEqual(engine.action("   -3.4028236e38   "), float("-inf"))
        self.assertEqual(engine.action("   1.4e-45   "), 1.4e-45)
        self.assertEqual(engine.action("   -1.4e-45   "), -1.4e-45)
        self.assertEqual(engine.action("   1e-46   "), 0.0)
        self.assertEqual(engine.action("   -1e-46   "), 0.0)
        self.assertTrue(math.isnan(engine.action("   nAN   ")))
        self.assertEqual(engine.action("   inf   "), float("inf"))
        self.assertEqual(engine.action("   +inf   "), float("inf"))
        self.assertEqual(engine.action("   -inf   "), float("-inf"))

    def testParseDouble(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: double
action: {parse.double: input}
''')
        self.assertEqual(engine.action("   123   "), 123.0)
        self.assertEqual(engine.action("   -123   "), -123.0)
        self.assertEqual(engine.action("   1.7976931348623157e308   "), 1.7976931348623157e308)
        self.assertEqual(engine.action("   -1.7976931348623157e308   "), -1.7976931348623157e308)
        self.assertEqual(engine.action("   1.7976931348623159e308   "), float("inf"))
        self.assertEqual(engine.action("   -1.7976931348623159e308   "), float("-inf"))
        self.assertEqual(engine.action("   4.9e-324   "), 4.9e-324)
        self.assertEqual(engine.action("   -4.9e-324   "), -4.9e-324)
        self.assertEqual(engine.action("   1e-324   "), 0.0)
        self.assertEqual(engine.action("   1e-324   "), 0.0)
        self.assertTrue(math.isnan(engine.action("   nAN   ")))
        self.assertEqual(engine.action("   inf   "), float("inf"))
        self.assertEqual(engine.action("   +inf   "), float("inf"))
        self.assertEqual(engine.action("   -inf   "), float("-inf"))

if __name__ == "__main__":
    unittest.main()
