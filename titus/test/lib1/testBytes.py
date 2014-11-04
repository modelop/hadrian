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
import struct
import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1Bytes(unittest.TestCase):
#################################################################### testers

    def testCheckAscii(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  bytes.isAscii: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, 108, 111)), True)
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, -127, 111)), False)

    def testCheckLatin1(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  bytes.isLatin1: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, 108, 111)), True)

    def testCheckUtf8(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  bytes.isUtf8: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, 108, 111)), True)

    def testCheckUtf16(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  bytes.isUtf16: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbbbbbbbbb", -1, -2, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0)), True)

    def testCheckUtf16be(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  bytes.isUtf16be: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbbbbbbb", 0, 104, 0, 101, 0, 108, 0, 108, 0, 111)), True)

    def testCheckUtf16le(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  bytes.isUtf16le: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbbbbbbb", 104, 0, 101, 0, 108, 0, 108, 0, 111, 0)), True)

#################################################################### decoders

    def testDecodeAscii(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.decodeAscii: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, 108, 111)), "hello")
        self.assertRaises(PFARuntimeException, lambda: engine.action(struct.pack("bbbbb", 104, 101, 108, -127, 111)))

    def testDecodeLatin1(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.decodeLatin1: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, 108, 111)), "hello")

    def testDecodeUtf8(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.decodeUtf8: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 104, 101, 108, 108, 111)), "hello")

    def testDecodeUtf16(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.decodeUtf16: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbbbbbbbbb", -1, -2, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0)), "hello")

    def testDecodeUtf16be(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.decodeUtf16be: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbbbbbbb", 0, 104, 0, 101, 0, 108, 0, 108, 0, 111)), "hello")

    def testDecodeUtf16le(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.decodeUtf16le: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbbbbbbb", 104, 0, 101, 0, 108, 0, 108, 0, 111, 0)), "hello")

#################################################################### encoders

    def testEncodeAscii(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.encodeAscii: input
''')
        self.assertEqual(engine.action("hello"), struct.pack("bbbbb", 104, 101, 108, 108, 111))
        self.assertRaises(PFARuntimeException, lambda: engine.action("hel\x81o"))

    def testEncodeLatin1(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.encodeLatin1: input
''')
        self.assertEqual(engine.action("hello"), struct.pack("bbbbb", 104, 101, 108, 108, 111))

    def testEncodeUtf8(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.encodeUtf8: input
''')
        self.assertEqual(engine.action("hello"), struct.pack("bbbbb", 104, 101, 108, 108, 111))

    def testEncodeUtf16(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.encodeUtf16: input
''')
        self.assertEqual(engine.action("hello"), struct.pack("bbbbbbbbbbbb", -1, -2, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0))

    def testEncodeUtf16be(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.encodeUtf16be: input
''')
        self.assertEqual(engine.action("hello"), struct.pack("bbbbbbbbbb", 0, 104, 0, 101, 0, 108, 0, 108, 0, 111))

    def testEncodeUtf16le(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.encodeUtf16le: input
''')
        self.assertEqual(engine.action("hello"), struct.pack("bbbbbbbbbb", 104, 0, 101, 0, 108, 0, 108, 0, 111, 0))

#################################################################### encoders

    def testConvertToBase64(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  bytes.toBase64: input
''')
        self.assertEqual(engine.action(struct.pack("bbbbb", 0, 127, 64, 38, 22)), "AH9AJhY=")

    def testConvertFromBase64(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: bytes
action:
  bytes.fromBase64: input
''')
        self.assertEqual(engine.action("AH9AJhY="), struct.pack("bbbbb", 0, 127, 64, 38, 22))
