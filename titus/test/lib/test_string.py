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
    
class TestLib1String(unittest.TestCase):
    def testGetLength(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.len: [input]}
''')
        self.assertEqual(engine.action("hello"), 5)

    def testGetSubstring(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: string
action:
  - {s.substr: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], 5, input]}
''')
        self.assertEqual(engine.action(10), "FGHIJ")
        self.assertEqual(engine.action(-10), "FGHIJKLMNOP")
        self.assertEqual(engine.action(0), "")
        self.assertEqual(engine.action(1), "")
        self.assertEqual(engine.action(100), "FGHIJKLMNOPQRSTUVWXYZ")

    def testGetSubstringTo(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: string
action:
  - {s.substrto: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], 5, input, [...]]}
''')
        self.assertEqual(engine.action(10), "ABCDE...KLMNOPQRSTUVWXYZ")
        self.assertEqual(engine.action(-10), "ABCDE...QRSTUVWXYZ")
        self.assertEqual(engine.action(0), "ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
        self.assertEqual(engine.action(1), "ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
        self.assertEqual(engine.action(100), "ABCDE...")

    def testDoContains(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {s.contains: [input, [DEFG]]}
''')
        self.assertTrue(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self.assertFalse(engine.action("ack! ack! ack!"))

    def testDoCount(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.count: [input, [ack!]]}
''')
        self.assertEqual(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), 0)
        self.assertEqual(engine.action("ack! ack! ack!"), 3)
        self.assertEqual(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"), 2)
        self.assertEqual(engine.action("adfasdfadack!asdfasdfasdf"), 1)

    def testDoIndex(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.index: [input, [ack!]]}
''')
        self.assertEqual(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), -1)
        self.assertEqual(engine.action("ack! ack! ack!"), 0)
        self.assertEqual(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"), 10)
        self.assertEqual(engine.action("adfasdfadack!asdfasdfasdf"), 9)

    def testDoRIndex(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.rindex: [input, [ack!]]}
''')
        self.assertEqual(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), -1)
        self.assertEqual(engine.action("ack! ack! ack!"), 10)
        self.assertEqual(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"), 36)
        self.assertEqual(engine.action("adfasdfadack!asdfasdfasdf"), 9)

    def testDoStartswith(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {s.startswith: [input, [ack!]]}
''')
        self.assertFalse(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self.assertTrue(engine.action("ack! ack! ack!"))
        self.assertFalse(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"))
        self.assertFalse(engine.action("adfasdfadack!asdfasdfasdf"))

    def testDoEndswith(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {s.endswith: [input, [ack!]]}
''')
        self.assertFalse(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self.assertTrue(engine.action("ack! ack! ack!"))
        self.assertTrue(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsfack!"))
        self.assertFalse(engine.action("adfasdfadack!asdfasdfasdf"))

    def testDoJoin(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: string}
output: string
action:
  - {s.join: [input, [", "]]}
''')
        self.assertEqual(engine.action(["one", "two", "three"]), "one, two, three")

    def testDoSplit(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: string}
action:
  - {s.split: [input, [", "]]}
''')
        self.assertEqual(engine.action("one, two, three"), ["one", "two", "three"])

    def testDoHex(self):
        engine1, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.hex: [input]}, {string: "|"}]
''')

        self.assertEqual(engine1.action(0), "0|")
        self.assertEqual(engine1.action(1), "1|")
        self.assertEqual(engine1.action(15), "f|")
        self.assertEqual(engine1.action(16), "10|")
        self.assertEqual(engine1.action(255), "ff|")
        self.assertEqual(engine1.action(256), "100|")
        self.assertEqual(engine1.action(65535), "ffff|")
        self.assertEqual(engine1.action(65536), "10000|")
        self.assertRaises(PFARuntimeException, lambda: engine1.action(-1))

        engine2, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.hex: [input, 8, false]}, {string: "|"}]
''')

        self.assertEqual(engine2.action(0), "       0|")
        self.assertEqual(engine2.action(1), "       1|")
        self.assertEqual(engine2.action(15), "       f|")
        self.assertEqual(engine2.action(16), "      10|")
        self.assertEqual(engine2.action(255), "      ff|")
        self.assertEqual(engine2.action(256), "     100|")
        self.assertEqual(engine2.action(65535), "    ffff|")
        self.assertEqual(engine2.action(65536), "   10000|")
        self.assertRaises(PFARuntimeException, lambda: engine2.action(-1))

        engine2a, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.hex: [input, 8, true]}, {string: "|"}]
''')

        self.assertEqual(engine2a.action(0), "00000000|")
        self.assertEqual(engine2a.action(1), "00000001|")
        self.assertEqual(engine2a.action(15), "0000000f|")
        self.assertEqual(engine2a.action(16), "00000010|")
        self.assertEqual(engine2a.action(255), "000000ff|")
        self.assertEqual(engine2a.action(256), "00000100|")
        self.assertEqual(engine2a.action(65535), "0000ffff|")
        self.assertEqual(engine2a.action(65536), "00010000|")
        self.assertRaises(PFARuntimeException, lambda: engine2a.action(-1))

        engine3, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.hex: [input, -8, false]}, {string: "|"}]
''')

        self.assertEqual(engine3.action(0), "0       |")
        self.assertEqual(engine3.action(1), "1       |")
        self.assertEqual(engine3.action(15), "f       |")
        self.assertEqual(engine3.action(16), "10      |")
        self.assertEqual(engine3.action(255), "ff      |")
        self.assertEqual(engine3.action(256), "100     |")
        self.assertEqual(engine3.action(65535), "ffff    |")
        self.assertEqual(engine3.action(65536), "10000   |")
        self.assertRaises(PFARuntimeException, lambda: engine3.action(-1))

    def testDoNumber(self):
        engine1, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.int: [input]}, {string: "|"}]
''')

        self.assertEqual(engine1.action(0), "0|")
        self.assertEqual(engine1.action(1), "1|")
        self.assertEqual(engine1.action(10), "10|")
        self.assertEqual(engine1.action(100), "100|")
        self.assertEqual(engine1.action(1000), "1000|")
        self.assertEqual(engine1.action(10000), "10000|")
        self.assertEqual(engine1.action(100000), "100000|")
        self.assertEqual(engine1.action(1000000), "1000000|")
        self.assertEqual(engine1.action(10000000), "10000000|")
        self.assertEqual(engine1.action(100000000), "100000000|")
        self.assertEqual(engine1.action(1000000000), "1000000000|")
        self.assertEqual(engine1.action(-1), "-1|")
        self.assertEqual(engine1.action(-10), "-10|")
        self.assertEqual(engine1.action(-100), "-100|")
        self.assertEqual(engine1.action(-1000), "-1000|")
        self.assertEqual(engine1.action(-10000), "-10000|")
        self.assertEqual(engine1.action(-100000), "-100000|")
        self.assertEqual(engine1.action(-1000000), "-1000000|")
        self.assertEqual(engine1.action(-10000000), "-10000000|")
        self.assertEqual(engine1.action(-100000000), "-100000000|")
        self.assertEqual(engine1.action(-1000000000), "-1000000000|")

        engine2, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.int: [input, 8, false]}, {string: "|"}]
''')

        self.assertEqual(engine2.action(0), "       0|")
        self.assertEqual(engine2.action(1), "       1|")
        self.assertEqual(engine2.action(10), "      10|")
        self.assertEqual(engine2.action(100), "     100|")
        self.assertEqual(engine2.action(1000), "    1000|")
        self.assertEqual(engine2.action(10000), "   10000|")
        self.assertEqual(engine2.action(100000), "  100000|")
        self.assertEqual(engine2.action(1000000), " 1000000|")
        self.assertEqual(engine2.action(10000000), "10000000|")
        self.assertEqual(engine2.action(100000000), "100000000|")
        self.assertEqual(engine2.action(1000000000), "1000000000|")
        self.assertEqual(engine2.action(-1), "      -1|")
        self.assertEqual(engine2.action(-10), "     -10|")
        self.assertEqual(engine2.action(-100), "    -100|")
        self.assertEqual(engine2.action(-1000), "   -1000|")
        self.assertEqual(engine2.action(-10000), "  -10000|")
        self.assertEqual(engine2.action(-100000), " -100000|")
        self.assertEqual(engine2.action(-1000000), "-1000000|")
        self.assertEqual(engine2.action(-10000000), "-10000000|")
        self.assertEqual(engine2.action(-100000000), "-100000000|")
        self.assertEqual(engine2.action(-1000000000), "-1000000000|")

        engine2a, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.int: [input, 8, true]}, {string: "|"}]
''')

        self.assertEqual(engine2a.action(0), "00000000|")
        self.assertEqual(engine2a.action(1), "00000001|")
        self.assertEqual(engine2a.action(10), "00000010|")
        self.assertEqual(engine2a.action(100), "00000100|")
        self.assertEqual(engine2a.action(1000), "00001000|")
        self.assertEqual(engine2a.action(10000), "00010000|")
        self.assertEqual(engine2a.action(100000), "00100000|")
        self.assertEqual(engine2a.action(1000000), "01000000|")
        self.assertEqual(engine2a.action(10000000), "10000000|")
        self.assertEqual(engine2a.action(100000000), "100000000|")
        self.assertEqual(engine2a.action(1000000000), "1000000000|")
        self.assertEqual(engine2a.action(-1), "-0000001|")
        self.assertEqual(engine2a.action(-10), "-0000010|")
        self.assertEqual(engine2a.action(-100), "-0000100|")
        self.assertEqual(engine2a.action(-1000), "-0001000|")
        self.assertEqual(engine2a.action(-10000), "-0010000|")
        self.assertEqual(engine2a.action(-100000), "-0100000|")
        self.assertEqual(engine2a.action(-1000000), "-1000000|")
        self.assertEqual(engine2a.action(-10000000), "-10000000|")
        self.assertEqual(engine2a.action(-100000000), "-100000000|")
        self.assertEqual(engine2a.action(-1000000000), "-1000000000|")

        engine3, = PFAEngine.fromYaml('''
input: int
output: string
action:
  s.concat: [{s.int: [input, -8, false]}, {string: "|"}]
''')

        self.assertEqual(engine3.action(0), "0       |")
        self.assertEqual(engine3.action(1), "1       |")
        self.assertEqual(engine3.action(10), "10      |")
        self.assertEqual(engine3.action(100), "100     |")
        self.assertEqual(engine3.action(1000), "1000    |")
        self.assertEqual(engine3.action(10000), "10000   |")
        self.assertEqual(engine3.action(100000), "100000  |")
        self.assertEqual(engine3.action(1000000), "1000000 |")
        self.assertEqual(engine3.action(10000000), "10000000|")
        self.assertEqual(engine3.action(100000000), "100000000|")
        self.assertEqual(engine3.action(1000000000), "1000000000|")
        self.assertEqual(engine3.action(-1), "-1      |")
        self.assertEqual(engine3.action(-10), "-10     |")
        self.assertEqual(engine3.action(-100), "-100    |")
        self.assertEqual(engine3.action(-1000), "-1000   |")
        self.assertEqual(engine3.action(-10000), "-10000  |")
        self.assertEqual(engine3.action(-100000), "-100000 |")
        self.assertEqual(engine3.action(-1000000), "-1000000|")
        self.assertEqual(engine3.action(-10000000), "-10000000|")
        self.assertEqual(engine3.action(-100000000), "-100000000|")
        self.assertEqual(engine3.action(-1000000000), "-1000000000|")

        engine4, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, null, null]}, {string: "|"}]
''')

        self.assertEqual(engine4.action(float("nan")), "nan|")
        self.assertEqual(engine4.action(float("inf")), "inf|")
        self.assertEqual(engine4.action(float("-inf")), "-inf|")
        self.assertEqual(engine4.action(0.0), "0.0|")
        self.assertEqual(engine4.action(-0.0), "0.0|")
        self.assertEqual(engine4.action(math.pi), "3.141593|")
        self.assertEqual(engine4.action(1.0e-10), "1.0e-10|")
        self.assertEqual(engine4.action(1.0e-09), "1.0e-09|")
        self.assertEqual(engine4.action(1.0e-08), "1.0e-08|")
        self.assertEqual(engine4.action(1.0e-07), "1.0e-07|")
        self.assertEqual(engine4.action(1.0e-06), "1.0e-06|")
        self.assertEqual(engine4.action(1.0e-05), "1.0e-05|")
        self.assertEqual(engine4.action(0.0001), "0.0001|")
        self.assertEqual(engine4.action(0.001), "0.001|")
        self.assertEqual(engine4.action(0.01), "0.01|")
        self.assertEqual(engine4.action(0.1), "0.1|")
        self.assertEqual(engine4.action(1.0), "1.0|")
        self.assertEqual(engine4.action(10.0), "10.0|")
        self.assertEqual(engine4.action(100.0), "100.0|")
        self.assertEqual(engine4.action(1000.0), "1000.0|")
        self.assertEqual(engine4.action(10000.0), "10000.0|")
        self.assertEqual(engine4.action(100000.0), "100000.0|")
        self.assertEqual(engine4.action(1.0e+06), "1.0e+06|")
        self.assertEqual(engine4.action(1.0e+07), "1.0e+07|")
        self.assertEqual(engine4.action(1.0e+08), "1.0e+08|")
        self.assertEqual(engine4.action(1.0e+09), "1.0e+09|")
        self.assertEqual(engine4.action(1.0e+10), "1.0e+10|")
        self.assertEqual(engine4.action(-1.0e-10), "-1.0e-10|")
        self.assertEqual(engine4.action(-1.0e-09), "-1.0e-09|")
        self.assertEqual(engine4.action(-1.0e-08), "-1.0e-08|")
        self.assertEqual(engine4.action(-1.0e-07), "-1.0e-07|")
        self.assertEqual(engine4.action(-1.0e-06), "-1.0e-06|")
        self.assertEqual(engine4.action(-1.0e-05), "-1.0e-05|")
        self.assertEqual(engine4.action(-0.0001), "-0.0001|")
        self.assertEqual(engine4.action(-0.001), "-0.001|")
        self.assertEqual(engine4.action(-0.01), "-0.01|")
        self.assertEqual(engine4.action(-0.1), "-0.1|")
        self.assertEqual(engine4.action(-1.0), "-1.0|")
        self.assertEqual(engine4.action(-10.0), "-10.0|")
        self.assertEqual(engine4.action(-100.0), "-100.0|")
        self.assertEqual(engine4.action(-1000.0), "-1000.0|")
        self.assertEqual(engine4.action(-10000.0), "-10000.0|")
        self.assertEqual(engine4.action(-100000.0), "-100000.0|")
        self.assertEqual(engine4.action(-1.0e+06), "-1.0e+06|")
        self.assertEqual(engine4.action(-1.0e+07), "-1.0e+07|")
        self.assertEqual(engine4.action(-1.0e+08), "-1.0e+08|")
        self.assertEqual(engine4.action(-1.0e+09), "-1.0e+09|")
        self.assertEqual(engine4.action(-1.0e+10), "-1.0e+10|")

        engine5, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, 10, null]}, {string: "|"}]
''')

        self.assertEqual(engine5.action(float("nan")), "       nan|")
        self.assertEqual(engine5.action(float("inf")), "       inf|")
        self.assertEqual(engine5.action(float("-inf")), "      -inf|")
        self.assertEqual(engine5.action(0.0), "       0.0|")
        self.assertEqual(engine5.action(-0.0), "       0.0|")
        self.assertEqual(engine5.action(math.pi), "  3.141593|")
        self.assertEqual(engine5.action(1.0e-10), "   1.0e-10|")
        self.assertEqual(engine5.action(1.0e-09), "   1.0e-09|")
        self.assertEqual(engine5.action(1.0e-08), "   1.0e-08|")
        self.assertEqual(engine5.action(1.0e-07), "   1.0e-07|")
        self.assertEqual(engine5.action(1.0e-06), "   1.0e-06|")
        self.assertEqual(engine5.action(1.0e-05), "   1.0e-05|")
        self.assertEqual(engine5.action(0.0001), "    0.0001|")
        self.assertEqual(engine5.action(0.001), "     0.001|")
        self.assertEqual(engine5.action(0.01), "      0.01|")
        self.assertEqual(engine5.action(0.1), "       0.1|")
        self.assertEqual(engine5.action(1.0), "       1.0|")
        self.assertEqual(engine5.action(10.0), "      10.0|")
        self.assertEqual(engine5.action(100.0), "     100.0|")
        self.assertEqual(engine5.action(1000.0), "    1000.0|")
        self.assertEqual(engine5.action(10000.0), "   10000.0|")
        self.assertEqual(engine5.action(100000.0), "  100000.0|")
        self.assertEqual(engine5.action(1.0e+06), "   1.0e+06|")
        self.assertEqual(engine5.action(1.0e+07), "   1.0e+07|")
        self.assertEqual(engine5.action(1.0e+08), "   1.0e+08|")
        self.assertEqual(engine5.action(1.0e+09), "   1.0e+09|")
        self.assertEqual(engine5.action(1.0e+10), "   1.0e+10|")
        self.assertEqual(engine5.action(-1.0e-10), "  -1.0e-10|")
        self.assertEqual(engine5.action(-1.0e-09), "  -1.0e-09|")
        self.assertEqual(engine5.action(-1.0e-08), "  -1.0e-08|")
        self.assertEqual(engine5.action(-1.0e-07), "  -1.0e-07|")
        self.assertEqual(engine5.action(-1.0e-06), "  -1.0e-06|")
        self.assertEqual(engine5.action(-1.0e-05), "  -1.0e-05|")
        self.assertEqual(engine5.action(-0.0001), "   -0.0001|")
        self.assertEqual(engine5.action(-0.001), "    -0.001|")
        self.assertEqual(engine5.action(-0.01), "     -0.01|")
        self.assertEqual(engine5.action(-0.1), "      -0.1|")
        self.assertEqual(engine5.action(-1.0), "      -1.0|")
        self.assertEqual(engine5.action(-10.0), "     -10.0|")
        self.assertEqual(engine5.action(-100.0), "    -100.0|")
        self.assertEqual(engine5.action(-1000.0), "   -1000.0|")
        self.assertEqual(engine5.action(-10000.0), "  -10000.0|")
        self.assertEqual(engine5.action(-100000.0), " -100000.0|")
        self.assertEqual(engine5.action(-1.0e+06), "  -1.0e+06|")
        self.assertEqual(engine5.action(-1.0e+07), "  -1.0e+07|")
        self.assertEqual(engine5.action(-1.0e+08), "  -1.0e+08|")
        self.assertEqual(engine5.action(-1.0e+09), "  -1.0e+09|")
        self.assertEqual(engine5.action(-1.0e+10), "  -1.0e+10|")

        engine6, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, -10, null]}, {string: "|"}]
''')

        self.assertEqual(engine6.action(float("nan")), "nan       |")
        self.assertEqual(engine6.action(float("inf")), "inf       |")
        self.assertEqual(engine6.action(float("-inf")), "-inf      |")
        self.assertEqual(engine6.action(0.0), "0.0       |")
        self.assertEqual(engine6.action(-0.0), "0.0       |")
        self.assertEqual(engine6.action(math.pi), "3.141593  |")
        self.assertEqual(engine6.action(1.0e-10), "1.0e-10   |")
        self.assertEqual(engine6.action(1.0e-09), "1.0e-09   |")
        self.assertEqual(engine6.action(1.0e-08), "1.0e-08   |")
        self.assertEqual(engine6.action(1.0e-07), "1.0e-07   |")
        self.assertEqual(engine6.action(1.0e-06), "1.0e-06   |")
        self.assertEqual(engine6.action(1.0e-05), "1.0e-05   |")
        self.assertEqual(engine6.action(0.0001), "0.0001    |")
        self.assertEqual(engine6.action(0.001), "0.001     |")
        self.assertEqual(engine6.action(0.01), "0.01      |")
        self.assertEqual(engine6.action(0.1), "0.1       |")
        self.assertEqual(engine6.action(1.0), "1.0       |")
        self.assertEqual(engine6.action(10.0), "10.0      |")
        self.assertEqual(engine6.action(100.0), "100.0     |")
        self.assertEqual(engine6.action(1000.0), "1000.0    |")
        self.assertEqual(engine6.action(10000.0), "10000.0   |")
        self.assertEqual(engine6.action(100000.0), "100000.0  |")
        self.assertEqual(engine6.action(1.0e+06), "1.0e+06   |")
        self.assertEqual(engine6.action(1.0e+07), "1.0e+07   |")
        self.assertEqual(engine6.action(1.0e+08), "1.0e+08   |")
        self.assertEqual(engine6.action(1.0e+09), "1.0e+09   |")
        self.assertEqual(engine6.action(1.0e+10), "1.0e+10   |")
        self.assertEqual(engine6.action(-1.0e-10), "-1.0e-10  |")
        self.assertEqual(engine6.action(-1.0e-09), "-1.0e-09  |")
        self.assertEqual(engine6.action(-1.0e-08), "-1.0e-08  |")
        self.assertEqual(engine6.action(-1.0e-07), "-1.0e-07  |")
        self.assertEqual(engine6.action(-1.0e-06), "-1.0e-06  |")
        self.assertEqual(engine6.action(-1.0e-05), "-1.0e-05  |")
        self.assertEqual(engine6.action(-0.0001), "-0.0001   |")
        self.assertEqual(engine6.action(-0.001), "-0.001    |")
        self.assertEqual(engine6.action(-0.01), "-0.01     |")
        self.assertEqual(engine6.action(-0.1), "-0.1      |")
        self.assertEqual(engine6.action(-1.0), "-1.0      |")
        self.assertEqual(engine6.action(-10.0), "-10.0     |")
        self.assertEqual(engine6.action(-100.0), "-100.0    |")
        self.assertEqual(engine6.action(-1000.0), "-1000.0   |")
        self.assertEqual(engine6.action(-10000.0), "-10000.0  |")
        self.assertEqual(engine6.action(-100000.0), "-100000.0 |")
        self.assertEqual(engine6.action(-1.0e+06), "-1.0e+06  |")
        self.assertEqual(engine6.action(-1.0e+07), "-1.0e+07  |")
        self.assertEqual(engine6.action(-1.0e+08), "-1.0e+08  |")
        self.assertEqual(engine6.action(-1.0e+09), "-1.0e+09  |")
        self.assertEqual(engine6.action(-1.0e+10), "-1.0e+10  |")

        engine7, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, null, 3]}, {string: "|"}]
''')

        self.assertEqual(engine7.action(float("nan")), "nan|")
        self.assertEqual(engine7.action(float("inf")), "inf|")
        self.assertEqual(engine7.action(float("-inf")), "-inf|")
        self.assertEqual(engine7.action(0.0), "0.000|")
        self.assertEqual(engine7.action(-0.0), "0.000|")
        self.assertEqual(engine7.action(math.pi), "3.142|")
        self.assertEqual(engine7.action(1.0e-10), "1.000e-10|")
        self.assertEqual(engine7.action(1.0e-09), "1.000e-09|")
        self.assertEqual(engine7.action(1.0e-08), "1.000e-08|")
        self.assertEqual(engine7.action(1.0e-07), "1.000e-07|")
        self.assertEqual(engine7.action(1.0e-06), "1.000e-06|")
        self.assertEqual(engine7.action(1.0e-05), "1.000e-05|")
        self.assertEqual(engine7.action(0.0001), "0.000|")
        self.assertEqual(engine7.action(0.001), "0.001|")
        self.assertEqual(engine7.action(0.01), "0.010|")
        self.assertEqual(engine7.action(0.1), "0.100|")
        self.assertEqual(engine7.action(1.0), "1.000|")
        self.assertEqual(engine7.action(10.0), "10.000|")
        self.assertEqual(engine7.action(100.0), "100.000|")
        self.assertEqual(engine7.action(1000.0), "1000.000|")
        self.assertEqual(engine7.action(10000.0), "10000.000|")
        self.assertEqual(engine7.action(100000.0), "100000.000|")
        self.assertEqual(engine7.action(1.0e+06), "1.000e+06|")
        self.assertEqual(engine7.action(1.0e+07), "1.000e+07|")
        self.assertEqual(engine7.action(1.0e+08), "1.000e+08|")
        self.assertEqual(engine7.action(1.0e+09), "1.000e+09|")
        self.assertEqual(engine7.action(1.0e+10), "1.000e+10|")
        self.assertEqual(engine7.action(-1.0e-10), "-1.000e-10|")
        self.assertEqual(engine7.action(-1.0e-09), "-1.000e-09|")
        self.assertEqual(engine7.action(-1.0e-08), "-1.000e-08|")
        self.assertEqual(engine7.action(-1.0e-07), "-1.000e-07|")
        self.assertEqual(engine7.action(-1.0e-06), "-1.000e-06|")
        self.assertEqual(engine7.action(-1.0e-05), "-1.000e-05|")
        self.assertEqual(engine7.action(-0.0001), "-0.000|")
        self.assertEqual(engine7.action(-0.001), "-0.001|")
        self.assertEqual(engine7.action(-0.01), "-0.010|")
        self.assertEqual(engine7.action(-0.1), "-0.100|")
        self.assertEqual(engine7.action(-1.0), "-1.000|")
        self.assertEqual(engine7.action(-10.0), "-10.000|")
        self.assertEqual(engine7.action(-100.0), "-100.000|")
        self.assertEqual(engine7.action(-1000.0), "-1000.000|")
        self.assertEqual(engine7.action(-10000.0), "-10000.000|")
        self.assertEqual(engine7.action(-100000.0), "-100000.000|")
        self.assertEqual(engine7.action(-1.0e+06), "-1.000e+06|")
        self.assertEqual(engine7.action(-1.0e+07), "-1.000e+07|")
        self.assertEqual(engine7.action(-1.0e+08), "-1.000e+08|")
        self.assertEqual(engine7.action(-1.0e+09), "-1.000e+09|")
        self.assertEqual(engine7.action(-1.0e+10), "-1.000e+10|")

        engine8, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, 10, 3]}, {string: "|"}]
''')

        self.assertEqual(engine8.action(float("nan")), "       nan|")
        self.assertEqual(engine8.action(float("inf")), "       inf|")
        self.assertEqual(engine8.action(float("-inf")), "      -inf|")
        self.assertEqual(engine8.action(0.0), "     0.000|")
        self.assertEqual(engine8.action(-0.0), "     0.000|")
        self.assertEqual(engine8.action(math.pi), "     3.142|")
        self.assertEqual(engine8.action(1.0e-10), " 1.000e-10|")
        self.assertEqual(engine8.action(1.0e-09), " 1.000e-09|")
        self.assertEqual(engine8.action(1.0e-08), " 1.000e-08|")
        self.assertEqual(engine8.action(1.0e-07), " 1.000e-07|")
        self.assertEqual(engine8.action(1.0e-06), " 1.000e-06|")
        self.assertEqual(engine8.action(1.0e-05), " 1.000e-05|")
        self.assertEqual(engine8.action(0.0001), "     0.000|")
        self.assertEqual(engine8.action(0.001), "     0.001|")
        self.assertEqual(engine8.action(0.01), "     0.010|")
        self.assertEqual(engine8.action(0.1), "     0.100|")
        self.assertEqual(engine8.action(1.0), "     1.000|")
        self.assertEqual(engine8.action(10.0), "    10.000|")
        self.assertEqual(engine8.action(100.0), "   100.000|")
        self.assertEqual(engine8.action(1000.0), "  1000.000|")
        self.assertEqual(engine8.action(10000.0), " 10000.000|")
        self.assertEqual(engine8.action(100000.0), "100000.000|")
        self.assertEqual(engine8.action(1.0e+06), " 1.000e+06|")
        self.assertEqual(engine8.action(1.0e+07), " 1.000e+07|")
        self.assertEqual(engine8.action(1.0e+08), " 1.000e+08|")
        self.assertEqual(engine8.action(1.0e+09), " 1.000e+09|")
        self.assertEqual(engine8.action(1.0e+10), " 1.000e+10|")
        self.assertEqual(engine8.action(-1.0e-10), "-1.000e-10|")
        self.assertEqual(engine8.action(-1.0e-09), "-1.000e-09|")
        self.assertEqual(engine8.action(-1.0e-08), "-1.000e-08|")
        self.assertEqual(engine8.action(-1.0e-07), "-1.000e-07|")
        self.assertEqual(engine8.action(-1.0e-06), "-1.000e-06|")
        self.assertEqual(engine8.action(-1.0e-05), "-1.000e-05|")
        self.assertEqual(engine8.action(-0.0001), "    -0.000|")
        self.assertEqual(engine8.action(-0.001), "    -0.001|")
        self.assertEqual(engine8.action(-0.01), "    -0.010|")
        self.assertEqual(engine8.action(-0.1), "    -0.100|")
        self.assertEqual(engine8.action(-1.0), "    -1.000|")
        self.assertEqual(engine8.action(-10.0), "   -10.000|")
        self.assertEqual(engine8.action(-100.0), "  -100.000|")
        self.assertEqual(engine8.action(-1000.0), " -1000.000|")
        self.assertEqual(engine8.action(-10000.0), "-10000.000|")
        self.assertEqual(engine8.action(-100000.0), "-100000.000|")
        self.assertEqual(engine8.action(-1.0e+06), "-1.000e+06|")
        self.assertEqual(engine8.action(-1.0e+07), "-1.000e+07|")
        self.assertEqual(engine8.action(-1.0e+08), "-1.000e+08|")
        self.assertEqual(engine8.action(-1.0e+09), "-1.000e+09|")
        self.assertEqual(engine8.action(-1.0e+10), "-1.000e+10|")

        engine9, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, -10, 3]}, {string: "|"}]
''')

        self.assertEqual(engine9.action(float("nan")), "nan       |")
        self.assertEqual(engine9.action(float("inf")), "inf       |")
        self.assertEqual(engine9.action(float("-inf")), "-inf      |")
        self.assertEqual(engine9.action(0.0), "0.000     |")
        self.assertEqual(engine9.action(-0.0), "0.000     |")
        self.assertEqual(engine9.action(math.pi), "3.142     |")
        self.assertEqual(engine9.action(1.0e-10), "1.000e-10 |")
        self.assertEqual(engine9.action(1.0e-09), "1.000e-09 |")
        self.assertEqual(engine9.action(1.0e-08), "1.000e-08 |")
        self.assertEqual(engine9.action(1.0e-07), "1.000e-07 |")
        self.assertEqual(engine9.action(1.0e-06), "1.000e-06 |")
        self.assertEqual(engine9.action(1.0e-05), "1.000e-05 |")
        self.assertEqual(engine9.action(0.0001), "0.000     |")
        self.assertEqual(engine9.action(0.001), "0.001     |")
        self.assertEqual(engine9.action(0.01), "0.010     |")
        self.assertEqual(engine9.action(0.1), "0.100     |")
        self.assertEqual(engine9.action(1.0), "1.000     |")
        self.assertEqual(engine9.action(10.0), "10.000    |")
        self.assertEqual(engine9.action(100.0), "100.000   |")
        self.assertEqual(engine9.action(1000.0), "1000.000  |")
        self.assertEqual(engine9.action(10000.0), "10000.000 |")
        self.assertEqual(engine9.action(100000.0), "100000.000|")
        self.assertEqual(engine9.action(1.0e+06), "1.000e+06 |")
        self.assertEqual(engine9.action(1.0e+07), "1.000e+07 |")
        self.assertEqual(engine9.action(1.0e+08), "1.000e+08 |")
        self.assertEqual(engine9.action(1.0e+09), "1.000e+09 |")
        self.assertEqual(engine9.action(1.0e+10), "1.000e+10 |")
        self.assertEqual(engine9.action(-1.0e-10), "-1.000e-10|")
        self.assertEqual(engine9.action(-1.0e-09), "-1.000e-09|")
        self.assertEqual(engine9.action(-1.0e-08), "-1.000e-08|")
        self.assertEqual(engine9.action(-1.0e-07), "-1.000e-07|")
        self.assertEqual(engine9.action(-1.0e-06), "-1.000e-06|")
        self.assertEqual(engine9.action(-1.0e-05), "-1.000e-05|")
        self.assertEqual(engine9.action(-0.0001), "-0.000    |")
        self.assertEqual(engine9.action(-0.001), "-0.001    |")
        self.assertEqual(engine9.action(-0.01), "-0.010    |")
        self.assertEqual(engine9.action(-0.1), "-0.100    |")
        self.assertEqual(engine9.action(-1.0), "-1.000    |")
        self.assertEqual(engine9.action(-10.0), "-10.000   |")
        self.assertEqual(engine9.action(-100.0), "-100.000  |")
        self.assertEqual(engine9.action(-1000.0), "-1000.000 |")
        self.assertEqual(engine9.action(-10000.0), "-10000.000|")
        self.assertEqual(engine9.action(-100000.0), "-100000.000|")
        self.assertEqual(engine9.action(-1.0e+06), "-1.000e+06|")
        self.assertEqual(engine9.action(-1.0e+07), "-1.000e+07|")
        self.assertEqual(engine9.action(-1.0e+08), "-1.000e+08|")
        self.assertEqual(engine9.action(-1.0e+09), "-1.000e+09|")
        self.assertEqual(engine9.action(-1.0e+10), "-1.000e+10|")

        engine10, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, null, 10]}, {string: "|"}]
''')

        self.assertEqual(engine10.action(float("nan")), "nan|")
        self.assertEqual(engine10.action(float("inf")), "inf|")
        self.assertEqual(engine10.action(float("-inf")), "-inf|")
        self.assertEqual(engine10.action(0.0), "0.0000000000|")
        self.assertEqual(engine10.action(-0.0), "0.0000000000|")
        self.assertEqual(engine10.action(math.pi), "3.1415926536|")
        self.assertEqual(engine10.action(1.0e-10), "1.0000000000e-10|")
        self.assertEqual(engine10.action(1.0e-09), "1.0000000000e-09|")
        self.assertEqual(engine10.action(1.0e-08), "1.0000000000e-08|")
        self.assertEqual(engine10.action(1.0e-07), "1.0000000000e-07|")
        self.assertEqual(engine10.action(1.0e-06), "1.0000000000e-06|")
        self.assertEqual(engine10.action(1.0e-05), "1.0000000000e-05|")
        self.assertEqual(engine10.action(0.0001), "0.0001000000|")
        self.assertEqual(engine10.action(0.001), "0.0010000000|")
        self.assertEqual(engine10.action(0.01), "0.0100000000|")
        self.assertEqual(engine10.action(0.1), "0.1000000000|")
        self.assertEqual(engine10.action(1.0), "1.0000000000|")
        self.assertEqual(engine10.action(10.0), "10.0000000000|")
        self.assertEqual(engine10.action(100.0), "100.0000000000|")
        self.assertEqual(engine10.action(1000.0), "1000.0000000000|")
        self.assertEqual(engine10.action(10000.0), "10000.0000000000|")
        self.assertEqual(engine10.action(100000.0), "100000.0000000000|")
        self.assertEqual(engine10.action(1.0e+06), "1.0000000000e+06|")
        self.assertEqual(engine10.action(1.0e+07), "1.0000000000e+07|")
        self.assertEqual(engine10.action(1.0e+08), "1.0000000000e+08|")
        self.assertEqual(engine10.action(1.0e+09), "1.0000000000e+09|")
        self.assertEqual(engine10.action(1.0e+10), "1.0000000000e+10|")
        self.assertEqual(engine10.action(-1.0e-10), "-1.0000000000e-10|")
        self.assertEqual(engine10.action(-1.0e-09), "-1.0000000000e-09|")
        self.assertEqual(engine10.action(-1.0e-08), "-1.0000000000e-08|")
        self.assertEqual(engine10.action(-1.0e-07), "-1.0000000000e-07|")
        self.assertEqual(engine10.action(-1.0e-06), "-1.0000000000e-06|")
        self.assertEqual(engine10.action(-1.0e-05), "-1.0000000000e-05|")
        self.assertEqual(engine10.action(-0.0001), "-0.0001000000|")
        self.assertEqual(engine10.action(-0.001), "-0.0010000000|")
        self.assertEqual(engine10.action(-0.01), "-0.0100000000|")
        self.assertEqual(engine10.action(-0.1), "-0.1000000000|")
        self.assertEqual(engine10.action(-1.0), "-1.0000000000|")
        self.assertEqual(engine10.action(-10.0), "-10.0000000000|")
        self.assertEqual(engine10.action(-100.0), "-100.0000000000|")
        self.assertEqual(engine10.action(-1000.0), "-1000.0000000000|")
        self.assertEqual(engine10.action(-10000.0), "-10000.0000000000|")
        self.assertEqual(engine10.action(-100000.0), "-100000.0000000000|")
        self.assertEqual(engine10.action(-1.0e+06), "-1.0000000000e+06|")
        self.assertEqual(engine10.action(-1.0e+07), "-1.0000000000e+07|")
        self.assertEqual(engine10.action(-1.0e+08), "-1.0000000000e+08|")
        self.assertEqual(engine10.action(-1.0e+09), "-1.0000000000e+09|")
        self.assertEqual(engine10.action(-1.0e+10), "-1.0000000000e+10|")

        engine11, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, 10, 10]}, {string: "|"}]
''')

        self.assertEqual(engine11.action(float("nan")), "       nan|")
        self.assertEqual(engine11.action(float("inf")), "       inf|")
        self.assertEqual(engine11.action(float("-inf")), "      -inf|")
        self.assertEqual(engine11.action(0.0), "0.0000000000|")
        self.assertEqual(engine11.action(-0.0), "0.0000000000|")
        self.assertEqual(engine11.action(math.pi), "3.1415926536|")
        self.assertEqual(engine11.action(1.0e-10), "1.0000000000e-10|")
        self.assertEqual(engine11.action(1.0e-09), "1.0000000000e-09|")
        self.assertEqual(engine11.action(1.0e-08), "1.0000000000e-08|")
        self.assertEqual(engine11.action(1.0e-07), "1.0000000000e-07|")
        self.assertEqual(engine11.action(1.0e-06), "1.0000000000e-06|")
        self.assertEqual(engine11.action(1.0e-05), "1.0000000000e-05|")
        self.assertEqual(engine11.action(0.0001), "0.0001000000|")
        self.assertEqual(engine11.action(0.001), "0.0010000000|")
        self.assertEqual(engine11.action(0.01), "0.0100000000|")
        self.assertEqual(engine11.action(0.1), "0.1000000000|")
        self.assertEqual(engine11.action(1.0), "1.0000000000|")
        self.assertEqual(engine11.action(10.0), "10.0000000000|")
        self.assertEqual(engine11.action(100.0), "100.0000000000|")
        self.assertEqual(engine11.action(1000.0), "1000.0000000000|")
        self.assertEqual(engine11.action(10000.0), "10000.0000000000|")
        self.assertEqual(engine11.action(100000.0), "100000.0000000000|")
        self.assertEqual(engine11.action(1.0e+06), "1.0000000000e+06|")
        self.assertEqual(engine11.action(1.0e+07), "1.0000000000e+07|")
        self.assertEqual(engine11.action(1.0e+08), "1.0000000000e+08|")
        self.assertEqual(engine11.action(1.0e+09), "1.0000000000e+09|")
        self.assertEqual(engine11.action(1.0e+10), "1.0000000000e+10|")
        self.assertEqual(engine11.action(-1.0e-10), "-1.0000000000e-10|")
        self.assertEqual(engine11.action(-1.0e-09), "-1.0000000000e-09|")
        self.assertEqual(engine11.action(-1.0e-08), "-1.0000000000e-08|")
        self.assertEqual(engine11.action(-1.0e-07), "-1.0000000000e-07|")
        self.assertEqual(engine11.action(-1.0e-06), "-1.0000000000e-06|")
        self.assertEqual(engine11.action(-1.0e-05), "-1.0000000000e-05|")
        self.assertEqual(engine11.action(-0.0001), "-0.0001000000|")
        self.assertEqual(engine11.action(-0.001), "-0.0010000000|")
        self.assertEqual(engine11.action(-0.01), "-0.0100000000|")
        self.assertEqual(engine11.action(-0.1), "-0.1000000000|")
        self.assertEqual(engine11.action(-1.0), "-1.0000000000|")
        self.assertEqual(engine11.action(-10.0), "-10.0000000000|")
        self.assertEqual(engine11.action(-100.0), "-100.0000000000|")
        self.assertEqual(engine11.action(-1000.0), "-1000.0000000000|")
        self.assertEqual(engine11.action(-10000.0), "-10000.0000000000|")
        self.assertEqual(engine11.action(-100000.0), "-100000.0000000000|")
        self.assertEqual(engine11.action(-1.0e+06), "-1.0000000000e+06|")
        self.assertEqual(engine11.action(-1.0e+07), "-1.0000000000e+07|")
        self.assertEqual(engine11.action(-1.0e+08), "-1.0000000000e+08|")
        self.assertEqual(engine11.action(-1.0e+09), "-1.0000000000e+09|")
        self.assertEqual(engine11.action(-1.0e+10), "-1.0000000000e+10|")

        engine12, = PFAEngine.fromYaml('''
input: double
output: string
action:
  s.concat: [{s.number: [input, -10, 10]}, {string: "|"}]
''')

        self.assertEqual(engine12.action(float("nan")), "nan       |")
        self.assertEqual(engine12.action(float("inf")), "inf       |")
        self.assertEqual(engine12.action(float("-inf")), "-inf      |")
        self.assertEqual(engine12.action(0.0), "0.0000000000|")
        self.assertEqual(engine12.action(-0.0), "0.0000000000|")
        self.assertEqual(engine12.action(math.pi), "3.1415926536|")
        self.assertEqual(engine12.action(1.0e-10), "1.0000000000e-10|")
        self.assertEqual(engine12.action(1.0e-09), "1.0000000000e-09|")
        self.assertEqual(engine12.action(1.0e-08), "1.0000000000e-08|")
        self.assertEqual(engine12.action(1.0e-07), "1.0000000000e-07|")
        self.assertEqual(engine12.action(1.0e-06), "1.0000000000e-06|")
        self.assertEqual(engine12.action(1.0e-05), "1.0000000000e-05|")
        self.assertEqual(engine12.action(0.0001), "0.0001000000|")
        self.assertEqual(engine12.action(0.001), "0.0010000000|")
        self.assertEqual(engine12.action(0.01), "0.0100000000|")
        self.assertEqual(engine12.action(0.1), "0.1000000000|")
        self.assertEqual(engine12.action(1.0), "1.0000000000|")
        self.assertEqual(engine12.action(10.0), "10.0000000000|")
        self.assertEqual(engine12.action(100.0), "100.0000000000|")
        self.assertEqual(engine12.action(1000.0), "1000.0000000000|")
        self.assertEqual(engine12.action(10000.0), "10000.0000000000|")
        self.assertEqual(engine12.action(100000.0), "100000.0000000000|")
        self.assertEqual(engine12.action(1.0e+06), "1.0000000000e+06|")
        self.assertEqual(engine12.action(1.0e+07), "1.0000000000e+07|")
        self.assertEqual(engine12.action(1.0e+08), "1.0000000000e+08|")
        self.assertEqual(engine12.action(1.0e+09), "1.0000000000e+09|")
        self.assertEqual(engine12.action(1.0e+10), "1.0000000000e+10|")
        self.assertEqual(engine12.action(-1.0e-10), "-1.0000000000e-10|")
        self.assertEqual(engine12.action(-1.0e-09), "-1.0000000000e-09|")
        self.assertEqual(engine12.action(-1.0e-08), "-1.0000000000e-08|")
        self.assertEqual(engine12.action(-1.0e-07), "-1.0000000000e-07|")
        self.assertEqual(engine12.action(-1.0e-06), "-1.0000000000e-06|")
        self.assertEqual(engine12.action(-1.0e-05), "-1.0000000000e-05|")
        self.assertEqual(engine12.action(-0.0001), "-0.0001000000|")
        self.assertEqual(engine12.action(-0.001), "-0.0010000000|")
        self.assertEqual(engine12.action(-0.01), "-0.0100000000|")
        self.assertEqual(engine12.action(-0.1), "-0.1000000000|")
        self.assertEqual(engine12.action(-1.0), "-1.0000000000|")
        self.assertEqual(engine12.action(-10.0), "-10.0000000000|")
        self.assertEqual(engine12.action(-100.0), "-100.0000000000|")
        self.assertEqual(engine12.action(-1000.0), "-1000.0000000000|")
        self.assertEqual(engine12.action(-10000.0), "-10000.0000000000|")
        self.assertEqual(engine12.action(-100000.0), "-100000.0000000000|")
        self.assertEqual(engine12.action(-1.0e+06), "-1.0000000000e+06|")
        self.assertEqual(engine12.action(-1.0e+07), "-1.0000000000e+07|")
        self.assertEqual(engine12.action(-1.0e+08), "-1.0000000000e+08|")
        self.assertEqual(engine12.action(-1.0e+09), "-1.0000000000e+09|")
        self.assertEqual(engine12.action(-1.0e+10), "-1.0000000000e+10|")

    def testDoJoin2(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.concat: [[one], input]}
''')
        self.assertEqual(engine.action("two"), "onetwo")

    def testRepeat(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.repeat: [input, 5]}
''')
        self.assertEqual(engine.action("hey"), "heyheyheyheyhey")

    def testDoLower(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.lower: [input]}
''')
        self.assertEqual(engine.action("hey"), "hey")
        self.assertEqual(engine.action("Hey"), "hey")
        self.assertEqual(engine.action("HEY"), "hey")
        self.assertEqual(engine.action("hEy"), "hey")
        self.assertEqual(engine.action("heY"), "hey")

    def testDoUpper(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.upper: [input]}
''')
        self.assertEqual(engine.action("hey"), "HEY")
        self.assertEqual(engine.action("Hey"), "HEY")
        self.assertEqual(engine.action("HEY"), "HEY")
        self.assertEqual(engine.action("hEy"), "HEY")
        self.assertEqual(engine.action("heY"), "HEY")

    def testDoLStrip(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.lstrip: [input, ["h "]]}
''')
        self.assertEqual(engine.action("hey"), "ey")
        self.assertEqual(engine.action(" hey"), "ey")
        self.assertEqual(engine.action("  hey"), "ey")
        self.assertEqual(engine.action("hey "), "ey ")
        self.assertEqual(engine.action("Hey"), "Hey")
        self.assertEqual(engine.action(" Hey"), "Hey")
        self.assertEqual(engine.action("  Hey"), "Hey")
        self.assertEqual(engine.action("Hey "), "Hey ")

    def testDoRStrip(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.rstrip: [input, ["y "]]}
''')
        self.assertEqual(engine.action("hey"), "he")
        self.assertEqual(engine.action("hey "), "he")
        self.assertEqual(engine.action("hey  "), "he")
        self.assertEqual(engine.action(" hey"), " he")
        self.assertEqual(engine.action("heY"), "heY")
        self.assertEqual(engine.action("heY "), "heY")
        self.assertEqual(engine.action("heY  "), "heY")
        self.assertEqual(engine.action(" heY"), " heY")

    def testDoStrip(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.strip: [input, ["hy "]]}
''')
        self.assertEqual(engine.action("hey"), "e")
        self.assertEqual(engine.action("hey "), "e")
        self.assertEqual(engine.action("hey  "), "e")
        self.assertEqual(engine.action(" hey"), "e")
        self.assertEqual(engine.action("HEY"), "HEY")
        self.assertEqual(engine.action("HEY "), "HEY")
        self.assertEqual(engine.action("HEY  "), "HEY")
        self.assertEqual(engine.action(" HEY"), "HEY")

    def testDoReplaceAll(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.replaceall: [input, [ey], [EY]]}
''')
        self.assertEqual(engine.action("hey"), "hEY")
        self.assertEqual(engine.action("hey hey hey"), "hEY hEY hEY")
        self.assertEqual(engine.action("abc"), "abc")
        self.assertEqual(engine.action("yeh yeh yeh"), "yeh yeh yeh")

    def testDoReplaceFirst(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.replacefirst: [input, [ey], [EY]]}
''')
        self.assertEqual(engine.action("hey"), "hEY")
        self.assertEqual(engine.action("hey hey hey"), "hEY hey hey")
        self.assertEqual(engine.action("abc"), "abc")
        self.assertEqual(engine.action("yeh yeh yeh"), "yeh yeh yeh")

    def testDoReplaceLast(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.replacelast: [input, [ey], [EY]]}
''')
        self.assertEqual(engine.action("hey"), "hEY")
        self.assertEqual(engine.action("hey hey hey"), "hey hey hEY")
        self.assertEqual(engine.action("abc"), "abc")
        self.assertEqual(engine.action("yeh yeh yeh"), "yeh yeh yeh")

    def testDoTranslate(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.translate: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], [AEIOU], input]}
''')
        self.assertEqual(engine.action("aeiou"), "aBCDeFGHiJKLMNoPQRSTuVWXYZ")
        self.assertEqual(engine.action("aeio"), "aBCDeFGHiJKLMNoPQRSTVWXYZ")
        self.assertEqual(engine.action(""), "BCDFGHJKLMNPQRSTVWXYZ")
        self.assertEqual(engine.action("aeiouuuu"), "aBCDeFGHiJKLMNoPQRSTuVWXYZ")

if __name__ == "__main__":
    unittest.main()
