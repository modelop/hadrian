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
    
class TestLib1Enum(unittest.TestCase):
    def testToString(self):
        engine, = PFAEngine.fromYaml('''
input: {type: enum, name: Test, symbols: ["A", "B", "C"]}
output: string
action:
  enum.toString: input
''')
        self.assertEqual(engine.action("A"), "A")
        self.assertEqual(engine.action("B"), "B")
        self.assertEqual(engine.action("C"), "C")
        self.assertRaises(AvroException, lambda: engine.action("D"))

    def testToInt(self):
        engine, = PFAEngine.fromYaml('''
input: {type: enum, name: Test, symbols: ["A", "B", "C"]}
output: int
action:
  enum.toInt: input
''')
        self.assertEqual(engine.action("A"), 0)
        self.assertEqual(engine.action("B"), 1)
        self.assertEqual(engine.action("C"), 2)

    def testNumSymbols(self):
        engine, = PFAEngine.fromYaml('''
input: {type: enum, name: Test, symbols: ["A", "B", "C"]}
output: int
action:
  enum.numSymbols: input
''')
        self.assertEqual(engine.action("A"), 3)
        self.assertEqual(engine.action("B"), 3)
        self.assertEqual(engine.action("C"), 3)
