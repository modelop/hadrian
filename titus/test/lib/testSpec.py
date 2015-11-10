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
import math
 
from titus.genpy import PFAEngine
from titus.errors import *

class TestLib1Spec(unittest.TestCase):
    def testLogBetaFcn(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - m.special.lnBeta: [input, 3] #[a, b]
''')
        self.assertAlmostEqual(engine.action(1.0),    -1.0986, places=3)
        self.assertAlmostEqual(engine.action(4.0),    -4.0943, places=3)
        self.assertAlmostEqual(engine.action(.01),     4.5902, places=3)

    ### raise the right exceptions ###
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - m.special.lnBeta: [input, -3] #[input, lambda]
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(0.5))


    def testNChooseKFcn(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - m.special.nChooseK: [input, 2]
''')
        self.assertEqual(engine.action(20),    190)
        self.assertEqual(engine.action(10),     45)
        self.assertEqual(engine.action(3),       3)

    ### raise the right exceptions ###
        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - m.special.nChooseK: [input, 4]
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(1))
        self.assertRaises(PFARuntimeException, lambda: engine.action(0))

        engine, = PFAEngine.fromYaml('''
input: int
output: int
action:
  - m.special.nChooseK: [input, -4] #[input, lambda]
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2))


    def testErfFcn(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.special.erf: input}
''')
        self.assertAlmostEqual(engine.action(-22.5),    -1.00, places=3)
        self.assertAlmostEqual(engine.action(-0.5),     -0.52, places=3)
        self.assertAlmostEqual(engine.action(0),         0.00, places=3)
        self.assertAlmostEqual(engine.action(0.5),       0.52, places=3)
        self.assertAlmostEqual(engine.action(22.5),      1.00, places=3)


    def testErfcFcn(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.special.erfc: input}
''')
        self.assertAlmostEqual(engine.action(-22.5),   2.00, places=3)
        self.assertAlmostEqual(engine.action(-0.5),    1.52, places=3)
        self.assertAlmostEqual(engine.action(0),       1.00, places=3)
        self.assertAlmostEqual(engine.action(0.5),     0.4795, places=3)
        self.assertAlmostEqual(engine.action(22.5),    0.00, places=3)


    def testLnGammaFcn(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.special.lnGamma: input}
''')
        self.assertAlmostEqual(engine.action(0.1),       2.2527, places=3 )
        self.assertAlmostEqual(engine.action(0.5),       0.5724, places=3)
        self.assertAlmostEqual(engine.action(22.5),      46.9199, places=3)

        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {m.special.lnGamma: input}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2))
        self.assertRaises(PFARuntimeException, lambda: engine.action(-2.2))

#    def testRegularizedGammaPFcn(self):
#        engine, = PFAEngine.fromYaml('''
#input: double
#output: double
#action:
#  - m.special.regularizedgammapfcn: [input, 3] #[a, b]
#''')
#        self.assertAlmostEqual(engine.action(1.0),    0.08030, places=3)
#        self.assertAlmostEqual(engine.action(2.0),    0.32332, places=3)
#        self.assertAlmostEqual(engine.action(3.0),    0.57680, places=3)
#
#    ### raise the right exceptions ###
#    # error if a, b <= 0
#        engine, = PFAEngine.fromYaml('''
#input: double
#output: double
#action:
#  - m.special.regularizedgammapfcn: [input, -1.5] #[input, lambda]
#''')
#        self.assertRaises(PFARuntimeException, lambda: engine.action(1.40))
#        self.assertRaises(PFARuntimeException, lambda: engine.action(-1.2))
#
#
#    def testRegularizedGammaQFcn(self):
#        engine, = PFAEngine.fromYaml('''
#input: double
#output: double
#action:
#  - m.special.regularizedgammaqfcn: [input, 3] #[a, b]
#''')
#        self.assertAlmostEqual(engine.action(1.0),    0.91969, places=3)
#        self.assertAlmostEqual(engine.action(2.0),    0.67667, places=3)
#        self.assertAlmostEqual(engine.action(3.0),    0.42319, places=3)
#
#    ### raise the right exceptions ###
#    # error if a, b <= 0
#        engine, = PFAEngine.fromYaml('''
#input: double
#output: double
#action:
#  - m.special.regularizedgammaqfcn: [input, -1.5] #[input, lambda]
#''')
#        self.assertRaises(PFARuntimeException, lambda: engine.action(1.40))
#        self.assertRaises(PFARuntimeException, lambda: engine.action(-1.2))
#
