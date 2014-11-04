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

import base64
import codecs

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "bytes."

#################################################################### testers

class Tester(LibFcn):
    sig = Sigs([Sig([{"x": P.Bytes()}], P.Boolean()),
                Sig([{"x": P.String()}], P.Boolean())])
    codec = None
    def __call__(self, state, scope, paramTypes, x):
        if paramTypes[0] == "bytes" or paramTypes[0] == {"type": "bytes"}:
            try:
                codecs.decode(x, self.codec, "strict")
                return True
            except UnicodeDecodeError:
                return False
        else:
            try:
                codecs.encode(x, self.codec, "strict")
                return True
            except UnicodeDecodeError:
                return False

class IsAscii(Tester):
    name = prefix + "isAscii"
    codec = "ascii"
provide(IsAscii())

class IsLatin1(Tester):
    name = prefix + "isLatin1"
    codec = "latin_1"
provide(IsLatin1())

class isUtf8(Tester):
    name = prefix + "isUtf8"
    codec = "utf_8"
provide(isUtf8())

class isUtf16(Tester):
    name = prefix + "isUtf16"
    codec = "utf_16"
provide(isUtf16())

class isUtf16BE(Tester):
    name = prefix + "isUtf16be"
    codec = "utf_16_be"
provide(isUtf16BE())

class isUtf16LE(Tester):
    name = prefix + "isUtf16le"
    codec = "utf_16_le"
provide(isUtf16LE())

#################################################################### decoders

class Decoder(LibFcn):
    sig = Sig([{"x": P.Bytes()}], P.String())
    codec = None
    def __call__(self, state, scope, paramTypes, x):
        try:
            return codecs.decode(x, self.codec, "strict")
        except UnicodeDecodeError:
            raise PFARuntimeException("invalid bytes")

class DecodeAscii(Decoder):
    name = prefix + "decodeAscii"
    codec = "ascii"
provide(DecodeAscii())

class DecodeLatin1(Decoder):
    name = prefix + "decodeLatin1"
    codec = "latin_1"
provide(DecodeLatin1())

class decodeUtf8(Decoder):
    name = prefix + "decodeUtf8"
    codec = "utf_8"
provide(decodeUtf8())

class decodeUtf16(Decoder):
    name = prefix + "decodeUtf16"
    codec = "utf_16"
provide(decodeUtf16())

class decodeUtf16BE(Decoder):
    name = prefix + "decodeUtf16be"
    codec = "utf_16_be"
provide(decodeUtf16BE())

class decodeUtf16LE(Decoder):
    name = prefix + "decodeUtf16le"
    codec = "utf_16_le"
provide(decodeUtf16LE())

#################################################################### encoders

class Encoder(LibFcn):
    sig = Sig([{"x": P.String()}], P.Bytes())
    codec = None
    def __call__(self, state, scope, paramTypes, x):
        try:
            return codecs.encode(x, self.codec, "strict")
        except UnicodeDecodeError:
            raise PFARuntimeException("invalid string")

class EncodeAscii(Encoder):
    name = prefix + "encodeAscii"
    codec = "ascii"
provide(EncodeAscii())

class EncodeLatin1(Encoder):
    name = prefix + "encodeLatin1"
    codec = "latin_1"
provide(EncodeLatin1())

class encodeUtf8(Encoder):
    name = prefix + "encodeUtf8"
    codec = "utf_8"
provide(encodeUtf8())

class encodeUtf16(Encoder):
    name = prefix + "encodeUtf16"
    codec = "utf_16"
provide(encodeUtf16())

class encodeUtf16BE(Encoder):
    name = prefix + "encodeUtf16be"
    codec = "utf_16_be"
provide(encodeUtf16BE())

class encodeUtf16LE(Encoder):
    name = prefix + "encodeUtf16le"
    codec = "utf_16_le"
provide(encodeUtf16LE())

#################################################################### base64

class ToBase64(LibFcn):
    name = prefix + "toBase64"
    sig = Sig([{"x": P.Bytes()}], P.String())
    def __call__(self, state, scope, paramTypes, x):
        return base64.b64encode(x)
provide(ToBase64())

class FromBase64(LibFcn):
    name = prefix + "fromBase64"
    sig = Sig([{"s": P.String()}], P.Bytes())
    def __call__(self, state, scope, paramTypes, s):
        try:
            return base64.b64decode(s)
        except TypeError:
            raise PFARuntimeException("invalid base64")
provide(FromBase64())
