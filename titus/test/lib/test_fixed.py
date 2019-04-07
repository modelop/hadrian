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
    
class TestLib1Fixed(unittest.TestCase):
    def testToBytes(self):
        engine, = PFAEngine.fromYaml('''
input: {type: fixed, name: Test, size: 10}
output: bytes
action:
  fixed.toBytes: input
''')
        self.assertEqual(engine.action("0123456789"), "0123456789")

    def testFromBytes(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: {type: fixed, name: Test, size: 10}
action:
  - let:
      original:
        type: Test
        value: "0123456789"
  - fixed.fromBytes: [original, input]
''')
        self.assertEqual(map(ord, engine.action("")), [48, 49, 50, 51, 52, 53, 54, 55, 56, 57])
        self.assertEqual(map(ord, engine.action("".join(map(chr, [0, 1, 2, 3, 4, 5, 6, 7, 8])))), [0, 1, 2, 3, 4, 5, 6, 7, 8, 57])
        self.assertEqual(map(ord, engine.action("".join(map(chr, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])))), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        self.assertEqual(map(ord, engine.action("".join(map(chr, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])))), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
