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

import math
import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1Cast(unittest.TestCase):
    def testToInt(self):
        self.assertEqual(PFAEngine.fromYaml('''
input: int
output: int
action: {cast.int: input}
''')[0].action(5), 5)
        self.assertEqual(PFAEngine.fromYaml('''
input: long
output: int
action: {cast.int: input}
''')[0].action(5), 5)
        self.assertEqual(PFAEngine.fromYaml('''
input: float
output: int
action: {cast.int: input}
''')[0].action(5.0), 5)
        self.assertEqual(PFAEngine.fromYaml('''
input: double
output: int
action: {cast.int: input}
''')[0].action(5.0), 5)

    def testToLong(self):
        self.assertEqual(PFAEngine.fromYaml('''
input: int
output: long
action: {cast.long: input}
''')[0].action(5), 5)
        self.assertEqual(PFAEngine.fromYaml('''
input: long
output: long
action: {cast.long: input}
''')[0].action(5), 5)
        self.assertEqual(PFAEngine.fromYaml('''
input: float
output: long
action: {cast.long: input}
''')[0].action(5.0), 5)
        self.assertEqual(PFAEngine.fromYaml('''
input: double
output: long
action: {cast.long: input}
''')[0].action(5.0), 5)

    def testToFloat(self):
        self.assertEqual(PFAEngine.fromYaml('''
input: int
output: float
action: {cast.float: input}
''')[0].action(5), 5.0)
        self.assertEqual(PFAEngine.fromYaml('''
input: long
output: float
action: {cast.float: input}
''')[0].action(5), 5.0)
        self.assertEqual(PFAEngine.fromYaml('''
input: float
output: float
action: {cast.float: input}
''')[0].action(5.0), 5.0)
        self.assertEqual(PFAEngine.fromYaml('''
input: double
output: float
action: {cast.float: input}
''')[0].action(5.0), 5.0)

    def testToDouble(self):
        self.assertEqual(PFAEngine.fromYaml('''
input: int
output: double
action: {cast.double: input}
''')[0].action(5), 5.0)
        self.assertEqual(PFAEngine.fromYaml('''
input: long
output: double
action: {cast.double: input}
''')[0].action(5), 5.0)
        self.assertEqual(PFAEngine.fromYaml('''
input: float
output: double
action: {cast.double: input}
''')[0].action(5.0), 5.0)
        self.assertEqual(PFAEngine.fromYaml('''
input: double
output: double
action: {cast.double: input}
''')[0].action(5.0), 5.0)

if __name__ == "__main__":
    unittest.main()
