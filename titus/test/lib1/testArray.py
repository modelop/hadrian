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
    
class TestLib1Array(unittest.TestCase):
    def testGetLength(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action: {a.len: {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}}
''')
        self.assertEqual(engine.action(None), 5)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action: {a.len: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}}
''')
        self.assertEqual(engine.action(None), 5)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action: {a.len: {value: [], type: {type: array, items: string}}}
''')
        self.assertEqual(engine.action(None), 0)

    def testGetSubseq(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 4]}
''')
        self.assertEqual(engine.action(None), ["one", "two", "three"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, -2]}
''')
        self.assertEqual(engine.action(None), ["one", "two", "three"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 100]}
''')
        self.assertEqual(engine.action(None), ["one", "two", "three", "four", "five"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 3]}
''')
        self.assertEqual(engine.action(None), [])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 2]}
''')
        self.assertEqual(engine.action(None), [])

    def testDoHeadTailLastInit(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action: {a.head: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
''')
        self.assertEqual(engine.action(None), "zero")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.tail: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
''')
        self.assertEqual(engine.action(None), ["one", "two", "three", "four", "five"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action: {a.last: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
''')
        self.assertEqual(engine.action(None), "five")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.init: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
''')
        self.assertEqual(engine.action(None), ["zero", "one", "two", "three", "four"])

    def testSetSubseqto(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 4, {value: ["ACK!"], type: {type: array, items: string}}]}
''')
        self.assertEqual(engine.action(None), ["zero", "ACK!", "four", "five"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, -2, {value: ["ACK!"], type: {type: array, items: string}}]}
''')
        self.assertEqual(engine.action(None), ["zero", "ACK!", "four", "five"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 100, {value: ["ACK!"], type: {type: array, items: string}}]}
''')
        self.assertEqual(engine.action(None), ["zero", "ACK!"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 3, {value: ["ACK!"], type: {type: array, items: string}}]}
''')
        self.assertEqual(engine.action(None), ["zero", "one", "two", "ACK!", "three", "four", "five"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 2, {value: ["ACK!"], type: {type: array, items: string}}]}
''')
        self.assertEqual(engine.action(None), ["zero", "one", "two", "ACK!", "three", "four", "five"])

   #################################################################### searching

    def testDoContains(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["two", "three", "four"], type: {type: array, items: string}}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["two", "four", "three"], type: {type: array, items: string}}
''')
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "two"}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "two"}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "whatev"}
''')
        self.assertFalse(engine.action(None))

    def testDoCount(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {value: ["two", "one"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), 2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {value: ["one", "two"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), 3)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {value: ["ACK!"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), 0)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {string: "two"}
''')
        self.assertEqual(engine.action(None), 3)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {string: "ACK!"}
''')
        self.assertEqual(engine.action(None), 0)

    def testDoCountPredicate(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [{"%": [x, 2]}, 0]}
''')
        self.assertEqual(engine.action(None), 3)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: [1, 3, 5, 7, 9], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [{"%": [x, 2]}, 0]}
''')
        self.assertEqual(engine.action(None), 0)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.even}
fcns:
  even:
    params: [{x: int}]
    ret: boolean
    do: {"==": [{"%": [x, 2]}, 0]}
''')
        self.assertEqual(engine.action(None), 3)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.count:
    - {value: [1, 3, 5, 7, 9], type: {type: array, items: int}}
    - {fcn: u.even}
fcns:
  even:
    params: [{x: int}]
    ret: boolean
    do: {"==": [{"%": [x, 2]}, 0]}
''')
        self.assertEqual(engine.action(None), 0)

    def testDoIndex(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["two", "three"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), 2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "three"}
''')
        self.assertEqual(engine.action(None), 3)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["ACK!"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), -1)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "ACK!"}
''')
        self.assertEqual(engine.action(None), -1)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["two", "three"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), 6)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "three"}
''')
        self.assertEqual(engine.action(None), 7)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["ACK!"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), -1)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "ACK!"}
''')
        self.assertEqual(engine.action(None), -1)

    def testDoStartsWith(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["one", "two"], type: {type: array, items: string}}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["two", "three"], type: {type: array, items: string}}
''')
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "one"}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "two"}
''')
        self.assertFalse(engine.action(None))

    def testDoEndswith(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["four", "five"], type: {type: array, items: string}}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["three", "four"], type: {type: array, items: string}}
''')
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "five"}
''')
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "four"}
''')
        self.assertFalse(engine.action(None))

   #################################################################### manipulation

    def testConcat(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.concat:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - {value: ["four", "five"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), ["one", "two", "three", "four", "five"])

    def testAppend(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.append:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - {string: "four"}
''')
        self.assertEqual(engine.action(None), ["one", "two", "three", "four"])

    def testCycle(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.cycle:
    - {value: ["one", "two", "three", "four", "five", "six", "seven", "eight"], type: {type: array, items: string}}
    - {string: "nine"}
    - 3
''')
        self.assertEqual(engine.action(None), ["seven", "eight", "nine"])

    def testIndex(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.insert:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
    - {string: "four"}
''')
        self.assertEqual(engine.action(None), ["one", "four", "two", "three"])

    def testReplace(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.replace:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
    - {string: "four"}
''')
        self.assertEqual(engine.action(None), ["one", "four", "three"])

    def testRemove(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.remove:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
''')
        self.assertEqual(engine.action(None), ["one", "three"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.remove:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
    - 2
''')
        self.assertEqual(engine.action(None), ["one", "three"])

    def testRotate(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.rotate:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
''')
        self.assertEqual(engine.action(None), ["two", "three", "one"])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.rotate:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 4
''')
        self.assertEqual(engine.action(None), ["two", "three", "one"])

   #################################################################### reordering

    def testSort(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.sort:
    - {value: [6, 2, 4, 6, 4, 5], type: {type: array, items: int}}
''')
        self.assertEqual(engine.action(None), [2, 4, 4, 5, 6, 6])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.sort:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), ["five", "four", "one", "three", "two"])

    def testSortWithAUserDefinedFunction(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.sortLT:
    - {value: [6, 2, 4, 6, 4, 5], type: {type: array, items: int}}
    - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: int}, {b: int}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 3.4]}}, {m.abs: {"-": [b, 3.4]}}]}
''')
        self.assertEqual(engine.action(None), [4, 4, 2, 5, 6, 6])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.sortLT:
    - {value: [6, 2, 4, 6, 4, 5], type: {type: array, items: int}}
    - params: [{a: int}, {b: int}]
      ret: boolean
      do: {"<": [{m.abs: {"-": [a, 3.4]}}, {m.abs: {"-": [b, 3.4]}}]}
''')
        self.assertEqual(engine.action(None), [4, 4, 2, 5, 6, 6])

#     def testShuffle(self):
#         engine, = PFAEngine.fromYaml('''
# input: "null"
# output: {type: array, items: string}
# action:
#   a.shuffle:
#     - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
# randseed: 12345
# ''')
#         self.assertEqual(engine.action(None), ["two", "four", "five", "one", "three"])

    def testReverse(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  a.reverse:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
''')
        self.assertEqual(engine.action(None), ["five", "four", "three", "two", "one"])

   #################################################################### extreme values

    def testFindNumericalMaxMin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {a.max: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""")
        self.assertEqual(engine.action(None), 7.7)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {a.min: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""")
        self.assertEqual(engine.action(None), 2.2)

    def testFindObjectMaxMin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - {a.max: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""")
        self.assertEqual(engine.action(None), "two")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - {a.min: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""")
        self.assertEqual(engine.action(None), "five")

    def testFindUserDefinedMaxMin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.maxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 2.2)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.maxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 2.2)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.minLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 6.6)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.minLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 6.6)

    def testFindTheTop3NumericalMaxMin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - {a.maxN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""")
        self.assertEqual(engine.action(None), [7.7, 7.6, 6.6])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - {a.minN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""")
        self.assertEqual(engine.action(None), [2.2, 2.2, 4.4])

    def testFindTheTop3ObjectMaxMin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  - {a.maxN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""")
        self.assertEqual(engine.action(None), ["two", "three", "six"])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  - {a.minN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""")
        self.assertEqual(engine.action(None), ["five", "four", "one"])

    def testFindTheTop3UserDefinedMaxMin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.maxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [2.2, 2.2, 4.4])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.maxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [2.2, 2.2, 4.4])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.minNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [6.6, 5.5, 7.6])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.minNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [6.6, 5.5, 7.6])

    def testFindNumericalArgmaxArgmin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmax: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""")
        self.assertEqual(engine.action(None), 2)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmin: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""")
        self.assertEqual(engine.action(None), 1)

    def testFindObjectArgminArgmax(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmax: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""")
        self.assertEqual(engine.action(None), 1)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmin: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""")
        self.assertEqual(engine.action(None), 4)

    def testFindUserDefinedArgmaxArgmin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argmaxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 1)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argmaxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 1)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argminLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 4)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argminLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), 4)

    def testFindTheTop3NumericalArgmaxArgmin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argmaxN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""")
        self.assertEqual(engine.action(None), [2, 6, 4])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argminN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""")
        self.assertEqual(engine.action(None), [1, 5, 3])

    def testFindTheTop3ObjectArgmaxArgmin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argmaxN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""")
        self.assertEqual(engine.action(None), [1, 2, 5])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argminN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""")
        self.assertEqual(engine.action(None), [4, 3, 0])

    def testFindTheTop3UserDefinedArgmaxArgmin(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argmaxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [1, 5, 3])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argmaxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [1, 5, 3])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argminNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [4, 0, 6])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argminNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""")
        self.assertEqual(engine.action(None), [4, 0, 6])

   #################################################################### numerical

    def testSum(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.sum: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}
""")
        self.assertEqual(engine.action(None), 15)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.sum: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}
""")
        self.assertEqual(engine.action(None), 15.0)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.sum: {value: [], type: {type: array, items: double}}
""")
        self.assertEqual(engine.action(None), 0.0)

    def testProduct(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.product: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}
""")
        self.assertEqual(engine.action(None), 120)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.product: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}
""")
        self.assertEqual(engine.action(None), 120.0)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.product: {value: [], type: {type: array, items: double}}
""")
        self.assertEqual(engine.action(None), 1.0)

    def testLnsum(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}
""")
        self.assertAlmostEqual(engine.action(None), 4.79, places=2)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}
""")
        self.assertAlmostEqual(engine.action(None), 4.79, places=2)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [], type: {type: array, items: double}}
""")
        self.assertEqual(engine.action(None), 0.0)

        engine, = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [1, 2, -3, 4, 5], type: {type: array, items: double}}
""")
        self.assertEqual(str(engine.action(None)), "nan")

   #################################################################### set or set-like functions

    def testDistinct(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.distinct:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""")
        self.assertEqual(sorted(engine.action(None)), sorted(["hey", "there", "you", "guys"]))

    def testSetEq(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.seteq:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "hey", "hey", "you", "guys", "there"], type: {type: array, items: string}}
""")
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.seteq:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "hey", "hey", "you", "guys"], type: {type: array, items: string}}
""")
        self.assertFalse(engine.action(None))

    def testUnion(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.union:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""")
        self.assertEqual(set(engine.action(None)), set(["wow", "this", "is", "guys", "different", "you", "there", "hey"]))

    def testIntersection(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.intersection:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""")
        self.assertEqual(set(engine.action(None)), set(["hey", "there"]))

    def testDiff(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.diff:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""")
        self.assertEqual(set(engine.action(None)), set(["you", "guys"]))

    def testDiff(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.symdiff:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""")
        self.assertEqual(set(engine.action(None)), set(["different", "this", "wow", "guys", "is", "you"]))

    def testSubset(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.subset:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""")
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.subset:
    - {value: ["hey", "there", "guys"], type: {type: array, items: string}}
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""")
        self.assertTrue(engine.action(None))

    def testDisjoint(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.disjoint:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""")
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.disjoint:
    - {value: ["hey", "there", "guys"], type: {type: array, items: string}}
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""")
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.disjoint:
    - {value: ["this", "is", "entirely", "different"], type: {type: array, items: string}}
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""")
        self.assertTrue(engine.action(None))

   #################################################################### functional programming

    def testMap(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.map:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: long
      do: {"*": [x, {long: 2}]}
""")
        self.assertEqual(engine.action(None), [0, 2, 4, 6, 8, 10])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.map:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.double}
fcns:
  double:
    params: [{x: int}]
    ret: long
    do: {"*": [x, {long: 2}]}
""")
        self.assertEqual(engine.action(None), [0, 2, 4, 6, 8, 10])

    def testMapWithIndex(self):
      engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: double}
action:
  a.mapWithIndex:
    - {value: [0.0, 1.1, 2.2, 3.3, 4.4, 5.5], type: {type: array, items: double}}
    - params: [{i: int}, {x: double}]
      ret: double
      do: {"-": [x, i]}
''')
      x = engine.action(None)
      self.assertAlmostEqual(x[0], 0.00, places=2)
      self.assertAlmostEqual(x[1], 0.10, places=2)
      self.assertAlmostEqual(x[2], 0.20, places=2)
      self.assertAlmostEqual(x[3], 0.30, places=2)
      self.assertAlmostEqual(x[4], 0.40, places=2)
      self.assertAlmostEqual(x[5], 0.50, places=2)

    def testFilter(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filter:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [{"%": [x, 2]}, 0]}
""")
        self.assertEqual(engine.action(None), [0, 2, 4])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filter:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.even}
fcns:
  even:
    params: [{x: int}]
    ret: boolean
    do: {"==": [{"%": [x, 2]}, 0]}
""")
        self.assertEqual(engine.action(None), [0, 2, 4])

    def testFilterWithIndex(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filterWithIndex:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}]
      ret: boolean
      do: {"&&": [{"==": [{"%": [x, 2]}, 0]}, {"<": [i, 3]}]}
""")
        self.assertEqual(engine.action(None), [0, 2])

    def testFilterMap(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.filterMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: [long, "null"]
      do:
        if: {"==": [{"%": [x, 2]}, 0]}
        then: null
        else: {"*": [x, {long: 10}]}
""")
        self.assertEqual(engine.action(None), [10, 30, 50])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.filterMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.maybeten}
fcns:
  maybeten:
    params: [{x: int}]
    ret: [long, "null"]
    do:
      if: {"==": [{"%": [x, 2]}, 0]}
      then: null
      else: {"*": [x, {long: 10}]}
""")
        self.assertEqual(engine.action(None), [10, 30, 50])

    def testFilterMapWithIndex(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filterWithIndex:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}]
      ret: boolean
      do: {"&&": [{"==": [{"%": [x, 2]}, 0]}, {"<": [i, 3]}]}
""")
        self.assertEqual(engine.action(None), [0, 2])

    def testFlatMap(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.flatMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: {type: array, items: long}
      do: {new: [x, x], type: {type: array, items: long}}
""")
        self.assertEqual(engine.action(None), [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.flatMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.stutter}
fcns:
  stutter:
    params: [{x: int}]
    ret: {type: array, items: long}
    do: {new: [x, x], type: {type: array, items: long}}
""")
        self.assertEqual(engine.action(None), [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5])

    def testFlatMapWithIndex(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.flatMapWithIndex:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}]
      ret: {type: array, items: long}
      do:
        if: {"==": [{"%": [i, 2]}, 0]}
        then: {new: [x, x], type: {type: array, items: long}}
        else: {value: [], type: {type: array, items: long}}
""")
        self.assertEqual(engine.action(None), [0, 0, 2, 2, 4, 4])

    def testReduce(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduce:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - params: [{tally: string}, {x: string}]
      ret: string
      do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "abcde")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduce:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{tally: string}, {x: string}]
    ret: string
    do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "abcde")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduceRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - params: [{x: string}, {tally: string}]
      ret: string
      do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "edcba")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduceRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{x: string}, {tally: string}]
    ret: string
    do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "edcba")

    def testFold(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.fold:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - params: [{tally: string}, {x: string}]
      ret: string
      do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "abcde")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.fold:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{tally: string}, {x: string}]
    ret: string
    do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "abcde")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.foldRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - params: [{x: string}, {tally: string}]
      ret: string
      do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "edcba")

        engine, = PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.foldRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{x: string}, {tally: string}]
    ret: string
    do: {s.concat: [tally, x]}
""")
        self.assertEqual(engine.action(None), "edcba")

    def testTakeWhile(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.takeWhile:
    - {value: [0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"<": [x, 3]}
""")
        self.assertEqual(engine.action(None), [0, 1, 2])

    def testDropWhile(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.dropWhile:
    - {value: [0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"<": [x, 3]}
""")
        self.assertEqual(engine.action(None), [3, 4, 5, 4, 3, 2, 1, 0])

   #################################################################### functional tests

    def testAny(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.any:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"==": [{s.len: x}, 5]}
""")
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.any:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"==": [{s.len: x}, 6]}
""")
        self.assertFalse(engine.action(None))

    def testAll(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.all:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"<": [{s.len: x}, 6]}
""")
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.all:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"<": [{s.len: x}, 5]}
""")
        self.assertFalse(engine.action(None))

    def testCorresponds(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.corresponds:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 4, 6], type: {type: array, items: int}}
    - params: [{x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""")
        self.assertTrue(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.corresponds:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 999, 6], type: {type: array, items: int}}
    - params: [{x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""")
        self.assertFalse(engine.action(None))

        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.corresponds:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 4], type: {type: array, items: int}}
    - params: [{x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""")
        self.assertFalse(engine.action(None))

    def testCorrespondsWithIndex(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.correspondsWithIndex:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 4, 6], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""")
        self.assertTrue(engine.action(None))

   #################################################################### restructuring

    def testSlidingWindow(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [1, 2, 3, 4], type: {type: array, items: int}}
    - 2
    - 1
    - true
""")
        self.assertEqual(engine.action(None), [[1, 2], [2, 3], [3, 4]])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [], type: {type: array, items: int}}
    - 2
    - 1
    - true
""")
        self.assertEqual(engine.action(None), [])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [1, 2, 3, 4, 5, 6], type: {type: array, items: int}}
    - 3
    - 2
    - true
""")
        self.assertEqual(engine.action(None), [[1, 2, 3], [3, 4, 5], [5, 6]])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [1, 2, 3, 4, 5, 6], type: {type: array, items: int}}
    - 3
    - 2
    - false
""")
        self.assertEqual(engine.action(None), [[1, 2, 3], [3, 4, 5]])

    def testSlidingWindow(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  - a.combinations:
      - {value: [1, 2, 3, 4], type: {type: array, items: int}}
      - 2
""")
        self.assertEqual(engine.action(None), [[1, 2], [1, 3], [1, 4], [2, 3], [2, 4], [3, 4]])

    def testPermutations(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.permutations:
    {value: [1, 2, 3, 4], type: {type: array, items: int}}
""")
        self.assertEqual(engine.action(None), [[1, 2, 3, 4], [1, 2, 4, 3], [1, 3, 2, 4], [1, 3, 4, 2], [1, 4, 2, 3], [1, 4, 3, 2], [2, 1, 3, 4], [2, 1, 4, 3], [2, 3, 1, 4], [2, 3, 4, 1], [2, 4, 1, 3], [2, 4, 3, 1], [3, 1, 2, 4], [3, 1, 4, 2], [3, 2, 1, 4], [3, 2, 4, 1], [3, 4, 1, 2], [3, 4, 2, 1], [4, 1, 2, 3], [4, 1, 3, 2], [4, 2, 1, 3], [4, 2, 3, 1], [4, 3, 1, 2], [4, 3, 2, 1]])

        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.permutations:
    {value: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], type: {type: array, items: int}}
options:
  timeout: 1000
""")
        self.assertRaises(PFATimeoutException, lambda: engine.action(None))

    def testFlatten(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.flatten: {value: [[1, 2], [], [3, 4, 5]], type: {type: array, items: {type: array, items: int}}}
""")
        self.assertEqual(engine.action(None), [1, 2, 3, 4, 5])

    def testGroupby(self):
        engine, = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: array, items: int}}
action:
  a.groupby:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: string
      do:
        if: {"==": [{"%": [x, 2]}, 0]}
        then: {string: "even"}
        else: {string: "odd"}
""")
        self.assertEqual(engine.action(None), {"even": [0, 2, 4], "odd": [1, 3, 5]})

if __name__ == "__main__":
    unittest.main()
