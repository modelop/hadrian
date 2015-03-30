#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
# See: string the License for the specific language governing permissions and
# limitations under the License.


# This is March 4 2015, 4:35:27
# 1425508527.52482

import unittest

from titus.genpy import PFAEngine
from titus.errors import *

class TestLib1Time(unittest.TestCase):
    def testYear(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.year: input} 
""")
        self.assertEqual(engine.action(1425508527),       2015)
        self.assertEqual(engine.action(1.425508527E9),    2015)
        self.assertEqual(engine.action(1425508527.52482), 2015)

    def testMonthOfYear(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.monthOfYear: input} 
""")
        self.assertEqual(engine.action(1425508527),       3)
        self.assertEqual(engine.action(1.425508527E9),    3)
        self.assertEqual(engine.action(1425508527.52482), 3)


    def testDayOfYear(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfYear: input} 
""")
        self.assertEqual(engine.action(1425508527),       63)
        self.assertEqual(engine.action(1.425508527E9),    63)
        self.assertEqual(engine.action(1425508527.52482), 63)


    def testDayOfMonth(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfMonth: input} 
""")
        self.assertEqual(engine.action(1425508527),       4)
        self.assertEqual(engine.action(1.425508527E9),    4)
        self.assertEqual(engine.action(1425508527.52482), 4)


    def testDayOfWeek(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfWeek: input} 
""")
        self.assertEqual(engine.action(1425508527),       2)
        self.assertEqual(engine.action(1.425508527E9),    2)
        self.assertEqual(engine.action(1425508527.52482), 2)


    def testHourOfDay(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.hourOfDay: input}
""")
        self.assertEqual(engine.action(0), 0)
        self.assertEqual(engine.action(1425508527),       22)
        self.assertEqual(engine.action(1.425508527E9),    22)
        self.assertEqual(engine.action(1425508527.52482), 22)



    def testMinuteOfHour(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.minuteOfHour: input}
""")
        self.assertEqual(engine.action(1425508527),       35)
        self.assertEqual(engine.action(1.425508527E9),    35)
        self.assertEqual(engine.action(1425508527.52482), 35)


    def testSecondOfMinute(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.secondOfMinute: input}
""")
        self.assertEqual(engine.action(1425508527),       27)
        self.assertEqual(engine.action(1.425508527E9),    27)
        self.assertEqual(engine.action(1425508527.52482), 27)


    def testMakeTimestamp(self):
        engine, = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {time.makeTimestamp: [input, 3, 11, 14, 50, 6, 245]}
""")
        self.assertEqual(engine.action(2015), 1426085406.245)

        engine, = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {time.makeTimestamp: [input, 1, 1, 0, 0, 0, 0]}
""")
        self.assertEqual(engine.action(1970), 0)


    def testIsSecondOfMinute(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isSecondOfMinute: [input, 25, 29]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)


    def testIsMinuteOfHour(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isMinuteOfHour: [input, 34, 36]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)


    def testIsHourOfDay(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isHourOfDay: [input, 20, 23]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)

        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isHourOfDay: [input, 0, .01]}
""")
        self.assertEqual(engine.action(0), True)


    def testIsDayOfWeek(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfWeek: [input, 2, 3]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)


    def testIsDayOfMonth(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfMonth: [input, 3, 5]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)





    def testIsDayOfYear(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfYear: [input, 1, 300]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)

    def testIsMonthOfYear(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isMonthOfYear: [input, 2, 7]}
""")
        self.assertEqual(engine.action(1425508527.52482), True)

    def testIsWeekend(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isWeekend: input}
""")
        self.assertEqual(engine.action(1425508527.52482), False)

    def testIsWorkHours(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isWorkHours: input}
""")
        self.assertEqual(engine.action(1425508527.52482), False)

    def testIsNonWorkHours(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isNonWorkHours: input}
""")
        self.assertEqual(engine.action(1425508527.52482), True)

    def testFromUTCToLocal(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {time.fromUTCToLocal: [input, -5]}
""")
        self.assertEqual(engine.action(1426018000), 1426000000)
        self.assertEqual(engine.action(1400018000), 1400000000)
        self.assertEqual(engine.action(1000018000), 1000000000)

    def testFromLocalToUTC(self):
        engine, = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {time.fromLocalToUTC: [input, -5]}
""")
        self.assertEqual(engine.action(1426000000), 1426018000)
        self.assertEqual(engine.action(1400000000), 1400018000)
        self.assertEqual(engine.action(1000000000), 1000018000)


