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
    def testSigned(self):
        engine2, = PFAEngine.fromYaml('''
input: long
output: long
action: {cast.signed: [input, 2]}
''')

        self.assertEqual(engine2.action(-2), -2)
        self.assertEqual(engine2.action(-1), -1)
        self.assertEqual(engine2.action(0), 0)
        self.assertEqual(engine2.action(1), 1)
        self.assertEqual(engine2.action(2), -2)

        engine8, = PFAEngine.fromYaml('''
input: long
output: long
action: {cast.signed: [input, 8]}
''')

        self.assertEqual(engine8.action(-2 - 256), -2)
        self.assertEqual(engine8.action(-1 - 256), -1)
        self.assertEqual(engine8.action(0 - 256), 0)
        self.assertEqual(engine8.action(1 - 256), 1)
        self.assertEqual(engine8.action(2 - 256), 2)

        self.assertEqual(engine8.action(-2 - 128), 126)
        self.assertEqual(engine8.action(-1 - 128), 127)
        self.assertEqual(engine8.action(0 - 128), -128)
        self.assertEqual(engine8.action(1 - 128), -127)
        self.assertEqual(engine8.action(2 - 128), -126)

        self.assertEqual(engine8.action(-2), -2)
        self.assertEqual(engine8.action(-1), -1)
        self.assertEqual(engine8.action(0), 0)
        self.assertEqual(engine8.action(1), 1)
        self.assertEqual(engine8.action(2), 2)

        self.assertEqual(engine8.action(-2 + 128), 126)
        self.assertEqual(engine8.action(-1 + 128), 127)
        self.assertEqual(engine8.action(0 + 128), -128)
        self.assertEqual(engine8.action(1 + 128), -127)
        self.assertEqual(engine8.action(2 + 128), -126)

        self.assertEqual(engine8.action(-2 + 256), -2)
        self.assertEqual(engine8.action(-1 + 256), -1)
        self.assertEqual(engine8.action(0 + 256), 0)
        self.assertEqual(engine8.action(1 + 256), 1)
        self.assertEqual(engine8.action(2 + 256), 2)

        engine64, = PFAEngine.fromYaml('''
input: long
output: long
action: {cast.signed: [input, 64]}
''')

        self.assertEqual(engine64.action(-9223372036854775808), -9223372036854775808)
        self.assertEqual(engine64.action(-9223372036854775808 + 1), -9223372036854775808 + 1)
        self.assertEqual(engine64.action(-9223372036854775808 + 2), -9223372036854775808 + 2)

        self.assertEqual(engine64.action(-1), -1)
        self.assertEqual(engine64.action(0), 0)
        self.assertEqual(engine64.action(1), 1)

        self.assertEqual(engine64.action(9223372036854775807 - 2), 9223372036854775807 - 2)
        self.assertEqual(engine64.action(9223372036854775807 - 1), 9223372036854775807 - 1)
        self.assertEqual(engine64.action(9223372036854775807), 9223372036854775807)

    def testUnsigned(self):
        engine1, = PFAEngine.fromYaml('''
input: long
output: long
action: {cast.unsigned: [input, 1]}
''')

        self.assertEqual(engine1.action(-2), 0)
        self.assertEqual(engine1.action(-1), 1)
        self.assertEqual(engine1.action(0), 0)
        self.assertEqual(engine1.action(1), 1)
        self.assertEqual(engine1.action(2), 0)

        engine8, = PFAEngine.fromYaml('''
input: long
output: long
action: {cast.unsigned: [input, 8]}
''')

        self.assertEqual(engine8.action(-2 - 2*256), 254)
        self.assertEqual(engine8.action(-1 - 2*256), 255)
        self.assertEqual(engine8.action(0 - 2*256), 0)
        self.assertEqual(engine8.action(1 - 2*256), 1)
        self.assertEqual(engine8.action(2 - 2*256), 2)

        self.assertEqual(engine8.action(-2 - 256), 254)
        self.assertEqual(engine8.action(-1 - 256), 255)
        self.assertEqual(engine8.action(0 - 256), 0)
        self.assertEqual(engine8.action(1 - 256), 1)
        self.assertEqual(engine8.action(2 - 256), 2)

        self.assertEqual(engine8.action(-2), 254)
        self.assertEqual(engine8.action(-1), 255)
        self.assertEqual(engine8.action(0), 0)
        self.assertEqual(engine8.action(1), 1)
        self.assertEqual(engine8.action(2), 2)

        self.assertEqual(engine8.action(-2 + 256), 254)
        self.assertEqual(engine8.action(-1 + 256), 255)
        self.assertEqual(engine8.action(0 + 256), 0)
        self.assertEqual(engine8.action(1 + 256), 1)
        self.assertEqual(engine8.action(2 + 256), 2)

        self.assertEqual(engine8.action(-2 + 2*256), 254)
        self.assertEqual(engine8.action(-1 + 2*256), 255)
        self.assertEqual(engine8.action(0 + 2*256), 0)
        self.assertEqual(engine8.action(1 + 2*256), 1)
        self.assertEqual(engine8.action(2 + 2*256), 2)

        engine63, = PFAEngine.fromYaml('''
input: long
output: long
action: {cast.unsigned: [input, 63]}
''')

        self.assertEqual(engine63.action(-9223372036854775808), 0)
        self.assertEqual(engine63.action(-9223372036854775808 + 1), 1)
        self.assertEqual(engine63.action(-9223372036854775808 + 2), 2)

        self.assertEqual(engine63.action(-1), 9223372036854775807)
        self.assertEqual(engine63.action(0), 0)
        self.assertEqual(engine63.action(1), 1)

        self.assertEqual(engine63.action(9223372036854775807 - 2), 9223372036854775807 - 2)
        self.assertEqual(engine63.action(9223372036854775807 - 1), 9223372036854775807 - 1)
        self.assertEqual(engine63.action(9223372036854775807), 9223372036854775807)

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

    def testFanoutBoolean(self):
        engine1, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: boolean
action:
  cast.fanoutBoolean: input
''')
        self.assertEqual(engine1.action("three"), [False, False, False, True, False, False, False, False, False, False])

        engine2, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: boolean
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutBoolean: [input, {cell: dictionary}, false]
''')
        self.assertEqual(engine2.action("three"), [False, False, False, True, False, False, False, False, False, False])
        self.assertEqual(engine2.action("sdfasdf"), [False, False, False, False, False, False, False, False, False, False])

        engine3, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: boolean
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutBoolean: [input, {cell: dictionary}, true]
''')
        self.assertEqual(engine3.action("three"), [False, False, False, True, False, False, False, False, False, False, False])
        self.assertEqual(engine3.action("adfadfadf"), [False, False, False, False, False, False, False, False, False, False, True])

        engine4, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: boolean
action:
  cast.fanoutBoolean: [input, 10, 20, false]
''')
        self.assertEqual(engine4.action(13), [False, False, False, True, False, False, False, False, False, False])
        self.assertEqual(engine4.action(999), [False, False, False, False, False, False, False, False, False, False])
 
        engine5, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: boolean
action:
  cast.fanoutBoolean: [input, 10, 20, true]
''')
        self.assertEqual(engine5.action(13), [False, False, False, True, False, False, False, False, False, False, False])
        self.assertEqual(engine5.action(999), [False, False, False, False, False, False, False, False, False, False, True])

    def testFanoutInt(self):
        engine1, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: int
action:
  cast.fanoutInt: input
''')
        self.assertEqual(engine1.action("three"), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0])

        engine2, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: int
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutInt: [input, {cell: dictionary}, false]
''')
        self.assertEqual(engine2.action("three"), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine2.action("sdfasdf"), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        engine3, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: int
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutInt: [input, {cell: dictionary}, true]
''')
        self.assertEqual(engine3.action("three"), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine3.action("adfadfadf"), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

        engine4, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: int
action:
  cast.fanoutInt: [input, 10, 20, false]
''')
        self.assertEqual(engine4.action(13), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine4.action(999), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
 
        engine5, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: int
action:
  cast.fanoutInt: [input, 10, 20, true]
''')
        self.assertEqual(engine5.action(13), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine5.action(999), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

    def testFanoutLong(self):
        engine1, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: long
action:
  cast.fanoutLong: input
''')
        self.assertEqual(engine1.action("three"), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0])

        engine2, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: long
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutLong: [input, {cell: dictionary}, false]
''')
        self.assertEqual(engine2.action("three"), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine2.action("sdfasdf"), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        engine3, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: long
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutLong: [input, {cell: dictionary}, true]
''')
        self.assertEqual(engine3.action("three"), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine3.action("adfadfadf"), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

        engine4, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: long
action:
  cast.fanoutLong: [input, 10, 20, false]
''')
        self.assertEqual(engine4.action(13), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine4.action(999), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
 
        engine5, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: long
action:
  cast.fanoutLong: [input, 10, 20, true]
''')
        self.assertEqual(engine5.action(13), [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(engine5.action(999), [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

    def testFanoutFloat(self):
        engine1, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: float
action:
  cast.fanoutFloat: input
''')
        self.assertEqual(engine1.action("three"), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

        engine2, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: float
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutFloat: [input, {cell: dictionary}, false]
''')
        self.assertEqual(engine2.action("three"), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine2.action("sdfasdf"), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

        engine3, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: float
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutFloat: [input, {cell: dictionary}, true]
''')
        self.assertEqual(engine3.action("three"), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine3.action("adfadfadf"), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])

        engine4, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: float
action:
  cast.fanoutFloat: [input, 10, 20, false]
''')
        self.assertEqual(engine4.action(13), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine4.action(999), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
 
        engine5, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: float
action:
  cast.fanoutFloat: [input, 10, 20, true]
''')
        self.assertEqual(engine5.action(13), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine5.action(999), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])

    def testFanoutDouble(self):
        engine1, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: double
action:
  cast.fanoutDouble: input
''')
        self.assertEqual(engine1.action("three"), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

        engine2, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: double
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutDouble: [input, {cell: dictionary}, false]
''')
        self.assertEqual(engine2.action("three"), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine2.action("sdfasdf"), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

        engine3, = PFAEngine.fromYaml('''
input: string
output:
  type: array
  items: double
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutDouble: [input, {cell: dictionary}, true]
''')
        self.assertEqual(engine3.action("three"), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine3.action("adfadfadf"), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])

        engine4, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: double
action:
  cast.fanoutDouble: [input, 10, 20, false]
''')
        self.assertEqual(engine4.action(13), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine4.action(999), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
 
        engine5, = PFAEngine.fromYaml('''
input: int
output:
  type: array
  items: double
action:
  cast.fanoutDouble: [input, 10, 20, true]
''')
        self.assertEqual(engine5.action(13), [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(engine5.action(999), [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])

    def testCastAvro(self):
        self.assertEqual(PFAEngine.fromYaml('''
input: string
output: string
action: {bytes.toBase64: {cast.avro: input}}
''')[0].action("hello"), "CmhlbGxv")

        self.assertEqual(PFAEngine.fromYaml('''
input: int
output: string
action: {bytes.toBase64: {cast.avro: input}}
''')[0].action(12), "GA==")

        self.assertEqual(PFAEngine.fromYaml('''
input: double
output: string
action: {bytes.toBase64: {cast.avro: input}}
''')[0].action(3.14), "H4XrUbgeCUA=")

        self.assertEqual(PFAEngine.fromYaml('''
input: [string, int]
output: string
action: {bytes.toBase64: {cast.avro: input}}
''')[0].action("hello"), "AApoZWxsbw==")

        self.assertEqual(PFAEngine.fromYaml('''
input: [string, int]
output: string
action: {bytes.toBase64: {cast.avro: input}}
''')[0].action(12), "Ahg=")

        self.assertEqual(PFAEngine.fromYaml('''
input:
  type: record
  name: Input
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
output: string
action: {bytes.toBase64: {cast.avro: input}}
''')[0].action({"one": 1, "two": 2.2, "three": "THREE"}), "ApqZmZmZmQFAClRIUkVF")

    def testCastJson(self):
        self.assertEqual(PFAEngine.fromYaml('''
input: string
output: string
action: {cast.json: input}
''')[0].action("hello"), '"hello"')

        self.assertEqual(PFAEngine.fromYaml('''
input: int
output: string
action: {cast.json: input}
''')[0].action(12), "12")

        self.assertEqual(PFAEngine.fromYaml('''
input: double
output: string
action: {cast.json: input}
''')[0].action(3.14), "3.14")

        self.assertEqual(PFAEngine.fromYaml('''
input: [string, int]
output: string
action: {cast.json: input}
''')[0].action("hello"), '''{"string":"hello"}''')

        self.assertEqual(PFAEngine.fromYaml('''
input: [string, int]
output: string
action: {cast.json: input}
''')[0].action(12), '''{"int":12}''')

        self.assertEqual(PFAEngine.fromYaml('''
input:
  type: record
  name: Input
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
output: string
action: {cast.json: input}
''')[0].action({"one": 1, "two": 2.2, "three": "THREE"}), '{"three":"THREE","two":2.2,"one":1}')

if __name__ == "__main__":
    unittest.main()
