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

import base64
import codecs
import re

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import startEnd
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "bytes."

#################################################################### basic access

class Len(LibFcn):
    name = prefix + "len"
    sig = Sig([{"x": P.Bytes()}], P.Int())
    errcodeBase = 16000
    def genpy(self, paramTypes, args, pos):
        return "len({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return len(x)
provide(Len())

class Subseq(LibFcn):
    name = prefix + "subseq"
    sig = Sig([{"x": P.Bytes()}, {"start": P.Int()}, {"end": P.Int()}], P.Bytes())
    errcodeBase = 16010
    def __call__(self, state, scope, pos, paramTypes, x, start, end):
        return x[start:end]
provide(Subseq())

class SubseqTo(LibFcn):
    name = prefix + "subseqto"
    sig = Sig([{"x": P.Bytes()}, {"start": P.Int()}, {"end": P.Int()}, {"replacement": P.Bytes()}], P.Bytes())
    errcodeBase = 16020
    def __call__(self, state, scope, pos, paramTypes, x, start, end, replacement):
        normStart, normEnd = startEnd(len(x), start, end)
        before = x[:normStart]
        after = x[normEnd:]
        return before + replacement + after
provide(SubseqTo())

#################################################################### testers

class Tester(LibFcn):
    sig = Sigs([Sig([{"x": P.Bytes()}], P.Boolean()),
                Sig([{"x": P.String()}], P.Boolean())])
    codec = None
    def __call__(self, state, scope, pos, paramTypes, x):
        if paramTypes[0] == "bytes" or paramTypes[0] == {"type": "bytes"}:
            try:
                codecs.decode(x, self.codec, "strict")
            except UnicodeDecodeError:
                return False
            else:
                return True
        else:
            try:
                codecs.encode(x, self.codec, "strict")
            except UnicodeEncodeError:
                return False
            else:
                return True

class IsAscii(Tester):
    name = prefix + "isAscii"
    codec = "ascii"
    errcodeBase = 16030
provide(IsAscii())

class IsLatin1(Tester):
    name = prefix + "isLatin1"
    codec = "latin_1"
    errcodeBase = 16040
provide(IsLatin1())

class isUtf8(Tester):
    name = prefix + "isUtf8"
    codec = "utf_8"
    errcodeBase = 16050
provide(isUtf8())

class isUtf16(Tester):
    name = prefix + "isUtf16"
    codec = "utf_16"
    errcodeBase = 16060
provide(isUtf16())

class isUtf16BE(Tester):
    name = prefix + "isUtf16be"
    codec = "utf_16_be"
    errcodeBase = 16070
provide(isUtf16BE())

class isUtf16LE(Tester):
    name = prefix + "isUtf16le"
    codec = "utf_16_le"
    errcodeBase = 16080
provide(isUtf16LE())

#################################################################### decoders

class Decoder(LibFcn):
    sig = Sig([{"x": P.Bytes()}], P.String())
    codec = None
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            return codecs.decode(x, self.codec, "strict")
        except UnicodeDecodeError as err:
            raise PFARuntimeException("invalid bytes", self.errcodeBase + 0, self.name, pos)

class DecodeAscii(Decoder):
    name = prefix + "decodeAscii"
    codec = "ascii"
    errcodeBase = 16090
provide(DecodeAscii())

class DecodeLatin1(Decoder):
    name = prefix + "decodeLatin1"
    codec = "latin_1"
    errcodeBase = 16100
provide(DecodeLatin1())

class decodeUtf8(Decoder):
    name = prefix + "decodeUtf8"
    codec = "utf_8"
    errcodeBase = 16110
provide(decodeUtf8())

class decodeUtf16(Decoder):
    name = prefix + "decodeUtf16"
    codec = "utf_16"
    errcodeBase = 16120
provide(decodeUtf16())

class decodeUtf16BE(Decoder):
    name = prefix + "decodeUtf16be"
    codec = "utf_16_be"
    errcodeBase = 16130
provide(decodeUtf16BE())

class decodeUtf16LE(Decoder):
    name = prefix + "decodeUtf16le"
    codec = "utf_16_le"
    errcodeBase = 16140
provide(decodeUtf16LE())

#################################################################### encoders

class Encoder(LibFcn):
    sig = Sig([{"x": P.String()}], P.Bytes())
    codec = None
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            return codecs.encode(x, self.codec, "strict")
        except UnicodeEncodeError:
            raise PFARuntimeException("invalid string", self.errcodeBase + 0, self.name, pos)

class EncodeAscii(Encoder):
    name = prefix + "encodeAscii"
    codec = "ascii"
    errcodeBase = 16150
provide(EncodeAscii())

class EncodeLatin1(Encoder):
    name = prefix + "encodeLatin1"
    codec = "latin_1"
    errcodeBase = 16160
provide(EncodeLatin1())

class EncodeUtf8(Encoder):
    name = prefix + "encodeUtf8"
    codec = "utf_8"
    errcodeBase = 16170
provide(EncodeUtf8())

class EncodeUtf16(Encoder):
    name = prefix + "encodeUtf16"
    codec = "utf_16"
    errcodeBase = 16180
provide(EncodeUtf16())

class EncodeUtf16BE(Encoder):
    name = prefix + "encodeUtf16be"
    codec = "utf_16_be"
    errcodeBase = 16190
provide(EncodeUtf16BE())

class EncodeUtf16LE(Encoder):
    name = prefix + "encodeUtf16le"
    codec = "utf_16_le"
    errcodeBase = 16200
provide(EncodeUtf16LE())

#################################################################### base64

class ToBase64(LibFcn):
    name = prefix + "toBase64"
    sig = Sig([{"x": P.Bytes()}], P.String())
    errcodeBase = 16210
    def __call__(self, state, scope, pos, paramTypes, x):
        return base64.b64encode(x)
provide(ToBase64())

class FromBase64(LibFcn):
    name = prefix + "fromBase64"
    sig = Sig([{"s": P.String()}], P.Bytes())
    errcodeBase = 16220
    def __call__(self, state, scope, pos, paramTypes, s):
        try:
            if re.match("^[A-Za-z0-9\+/]*=*$", s) is None:
                raise TypeError
            return base64.b64decode(s)
        except TypeError:
            raise PFARuntimeException("invalid base64", self.errcodeBase + 0, self.name, pos)
provide(FromBase64())
