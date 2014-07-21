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
    
class TestLib1Math(unittest.TestCase):
    def testProvideConstants(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - {m.pi: []}
''')
        self.assertAlmostEqual(engine.action(None), 3.141592653589793, places=14)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - {m.e: []}
''')
        self.assertAlmostEqual(engine.action(None), 2.718281828459045, places=14)

    def testDoAbs(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.abs: input}
''')
        self.assertAlmostEqual(engine.action(-3.14), 3.14, places=2)

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - {m.abs: input}
''')
        self.assertEqual(engine.action(2147483647), 2147483647)
        self.assertEqual(engine.action(-2147483647), 2147483647)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2147483648))

        engine, = PFAEngine.fromYaml('''
input: long
output: long
action:
  - {m.abs: input}
''')
        self.assertEqual(engine.action(9223372036854775807), 9223372036854775807)
        self.assertEqual(engine.action(-9223372036854775807), 9223372036854775807)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-9223372036854775808))

    def testDoAcos(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.acos: input}
''')
        self.assertEqual(str(engine.action(-10)), "nan")
        self.assertAlmostEqual(engine.action(-1), 3.14, places=2)
        self.assertAlmostEqual(engine.action(-0.8), 2.50, places=2)
        self.assertAlmostEqual(engine.action(0), 1.57, places=2)
        self.assertAlmostEqual(engine.action(0.8), 0.64, places=2)
        self.assertAlmostEqual(engine.action(1), 0.00, places=2)

    def testDoAsin(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.asin: input}
''')
        self.assertEqual(str(engine.action(-10)), "nan")
        self.assertAlmostEqual(engine.action(-1), -1.57, places=2)
        self.assertAlmostEqual(engine.action(-0.8), -0.93, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.8), 0.93, places=2)
        self.assertAlmostEqual(engine.action(1), 1.57, places=2)

    def testDoAtan(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.atan: input}
''')
        self.assertAlmostEqual(engine.action(-1), -0.79, places=2)
        self.assertAlmostEqual(engine.action(-0.8), -0.67, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.8), 0.67, places=2)
        self.assertAlmostEqual(engine.action(1), 0.79, places=2)

    def testDoAtan2(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.atan2: [input, 1]}
''')
        self.assertAlmostEqual(engine.action(-1), -0.79, places=2)
        self.assertAlmostEqual(engine.action(-0.8), -0.67, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.8), 0.67, places=2)
        self.assertAlmostEqual(engine.action(1), 0.79, places=2)

    def testCeil(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.ceil: input}
''')
        self.assertEqual(engine.action(-3.2), -3)
        self.assertEqual(engine.action(0), 0)
        self.assertEqual(engine.action(3.2), 4)

    def testCopysign(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.copysign: [5, input]}
''')
        self.assertEqual(engine.action(-3.2), -5)
        self.assertEqual(engine.action(0), 5)
        self.assertEqual(engine.action(3.2), 5)

    def testCos(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.cos: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), -0.87, places=2)
        self.assertAlmostEqual(engine.action(-0.5), 0.88, places=2)
        self.assertAlmostEqual(engine.action(0), 1.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.88, places=2)
        self.assertAlmostEqual(engine.action(22.5), -0.87, places=2)

    def testCosh(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.cosh: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), 2955261031.51, places=2)
        self.assertAlmostEqual(engine.action(-0.5), 1.13, places=2)
        self.assertAlmostEqual(engine.action(0), 1.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 1.13, places=2)
        self.assertAlmostEqual(engine.action(22.5), 2955261031.51, places=2)

    def testExp(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.exp: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), 0.00, places=2)
        self.assertAlmostEqual(engine.action(-0.5), 0.61, places=2)
        self.assertAlmostEqual(engine.action(0), 1.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 1.65, places=2)
        self.assertAlmostEqual(engine.action(22.5), 5910522063.02, places=2)

    def testExpm1(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.expm1: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), -1.00, places=2)
        self.assertAlmostEqual(engine.action(-0.5), -0.39, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.65, places=2)
        self.assertAlmostEqual(engine.action(22.5), 5910522062.02, places=2)

    def testFloor(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.floor: input}
''')
        self.assertEqual(engine.action(-3.2), -4)
        self.assertEqual(engine.action(0), 0)
        self.assertEqual(engine.action(3.2), 3)

    def testHypot(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.hypot: [input, 3.5]}
''')
        self.assertAlmostEqual(engine.action(-22.5), 22.77, places=2)
        self.assertAlmostEqual(engine.action(-0.5), 3.54, places=2)
        self.assertAlmostEqual(engine.action(0), 3.50, places=2)
        self.assertAlmostEqual(engine.action(0.5), 3.54, places=2)
        self.assertAlmostEqual(engine.action(22.5), 22.77, places=2)

    def testLn(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.ln: input}
''')
        self.assertEqual(str(engine.action(-1)), "nan")
        self.assertEqual(str(engine.action(0)), "-inf")
        self.assertAlmostEqual(engine.action(0.00001), -11.51, places=2)
        self.assertAlmostEqual(engine.action(0.5), -0.69, places=2)
        self.assertAlmostEqual(engine.action(22.5), 3.11, places=2)

    def testLog10(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.log10: input}
''')
        self.assertEqual(str(engine.action(-1)), "nan")
        self.assertEqual(str(engine.action(0)), "-inf")
        self.assertAlmostEqual(engine.action(0.00001), -5.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), -0.30, places=2)
        self.assertAlmostEqual(engine.action(22.5), 1.35, places=2)

    def testArbitraryBaseLog(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: double
action:
  - {m.log: [5.5, input]}
''')
        self.assertAlmostEqual(engine.action(2), 2.46, places=2)
        self.assertAlmostEqual(engine.action(5), 1.06, places=2)
        self.assertAlmostEqual(engine.action(10), 0.74, places=2)
        self.assertAlmostEqual(engine.action(16), 0.61, places=2)

        self.assertRaises(PFARuntimeException, lambda: engine.action(0))
        self.assertRaises(PFARuntimeException, lambda: engine.action(-1))

    def testLn(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.ln1p: input}
''')
        self.assertEqual(str(engine.action(-2)), "nan")
        self.assertEqual(str(engine.action(-1)), "-inf")
        self.assertAlmostEqual(engine.action(-0.99999), -11.51, places=2)
        self.assertAlmostEqual(engine.action(-0.99999), -11.51, places=2)
        self.assertAlmostEqual(engine.action(0.0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.00001), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.41, places=2)
        self.assertAlmostEqual(engine.action(22.5), 3.16, places=2)

    def testRound(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: long
action:
  - {m.round: input}
''')
        self.assertEqual(engine.action(-3.8), -4)
        self.assertEqual(engine.action(-3.5), -3)
        self.assertEqual(engine.action(-3.2), -3)
        self.assertEqual(engine.action(0), 0)
        self.assertEqual(engine.action(3.2), 3)
        self.assertEqual(engine.action(3.5), 4)
        self.assertEqual(engine.action(3.8), 4)
        self.assertEqual(engine.action(9.223372036800000e+18), 9223372036800000000)
        self.assertRaises(PFARuntimeException, lambda: engine.action(9.223372036854777e+18))
        self.assertEqual(engine.action(-9.223372036854776e+18), -9223372036854775808)
        self.assertRaises(PFARuntimeException, lambda: engine.action(-9.223372036854777e+18))

    def testRint(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.rint: input}
''')
        self.assertEqual(engine.action(-3.8), -4)
        self.assertEqual(engine.action(-3.5), -4)
        self.assertEqual(engine.action(-3.2), -3)
        self.assertEqual(engine.action(0), 0)
        self.assertEqual(engine.action(3.2), 3)
        self.assertEqual(engine.action(3.5), 4)
        self.assertEqual(engine.action(3.8), 4)

    def testSignum(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.signum: input}
''')
        self.assertEqual(engine.action(-3.2), -1)
        self.assertEqual(engine.action(0), 0)
        self.assertEqual(engine.action(3.2), 1)

    def testSin(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.sin: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), 0.49, places=2)
        self.assertAlmostEqual(engine.action(-0.5), -0.48, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.48, places=2)
        self.assertAlmostEqual(engine.action(22.5), -0.49, places=2)

    def testSinh(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.sinh: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), -2955261031.51, places=2)
        self.assertAlmostEqual(engine.action(-0.5), -0.52, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.52, places=2)
        self.assertAlmostEqual(engine.action(22.5), 2955261031.51, places=2)

    def testSqrt(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.sqrt: input}
''')
        self.assertEqual(str(engine.action(-1)), "nan")
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.71, places=2)
        self.assertAlmostEqual(engine.action(22.5), 4.74, places=2)

    def testTan(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.tan: input}
''')
        self.assertAlmostEqual(engine.action(-10.5), -1.85, places=2)
        self.assertAlmostEqual(engine.action(-0.5), -0.55, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.55, places=2)
        self.assertAlmostEqual(engine.action(10.5), 1.85, places=2)

    def testTanh(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.tanh: input}
''')
        self.assertAlmostEqual(engine.action(-22.5), -1.00, places=2)
        self.assertAlmostEqual(engine.action(-0.5), -0.46, places=2)
        self.assertAlmostEqual(engine.action(0), 0.00, places=2)
        self.assertAlmostEqual(engine.action(0.5), 0.46, places=2)
        self.assertAlmostEqual(engine.action(22.5), 1.00, places=2)

if __name__ == "__main__":
    unittest.main()
