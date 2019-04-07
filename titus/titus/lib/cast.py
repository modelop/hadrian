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
import io
import json

import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from six.moves import range

from titus.util import untagUnion
from titus.datatype import schemaToAvroType
from titus.datatype import jsonNodeToAvroType
from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.lib.core import INT_MIN_VALUE
from titus.lib.core import INT_MAX_VALUE
from titus.lib.core import LONG_MIN_VALUE
from titus.lib.core import LONG_MAX_VALUE
from titus.lib.core import FLOAT_MIN_VALUE
from titus.lib.core import FLOAT_MAX_VALUE
from titus.lib.core import DOUBLE_MIN_VALUE
from titus.lib.core import DOUBLE_MAX_VALUE
from titus.datatype import jsonEncoder
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "cast."

#################################################################### wrap-around arithmetic

def bitsToMax(x): return 2**x

def doUnsigned(x, bits):
    maximum = bitsToMax(bits)
    if x < 0:
        y = x + maximum * int(math.ceil(-float(x) / maximum))
    else:
        y = x
    return y % maximum

class ToSigned(LibFcn):
    name = prefix + "signed"
    sig = Sig([{"x": P.Long()}, {"bits": P.Int()}], P.Long())
    errcodeBase = 17000
    def __call__(self, state, scope, pos, paramTypes, x, bits):
        if bits < 2 or bits > 64:
            raise PFARuntimeException("unrepresentable unsigned number", self.errcodeBase + 0, self.name, pos)
        y = doUnsigned(x, bits)
        maximum = bitsToMax(bits - 1)
        if y > maximum - 1:
            return y - 2*maximum
        else:
            return y
provide(ToSigned())

class ToUnsigned(LibFcn):
    name = prefix + "unsigned"
    sig = Sig([{"x": P.Long()}, {"bits": P.Int()}], P.Long())
    errcodeBase = 17010
    def __call__(self, state, scope, pos, paramTypes, x, bits):
        if bits < 1 or bits > 63:
            raise PFARuntimeException("unrepresentable unsigned number", self.errcodeBase + 0, self.name, pos)
        return doUnsigned(x, bits)
provide(ToUnsigned())

#################################################################### number precisions

class ToInt(LibFcn):
    name = prefix + "int"
    sig = Sigs([Sig([{"x": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}], P.Int()),
                Sig([{"x": P.Float()}], P.Int()),
                Sig([{"x": P.Double()}], P.Int())])
    errcodeBase = 17020
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            if isinstance(x, float):
                if math.isnan(x):
                    raise OverflowError
                else:
                    out = int(math.floor(x + 0.5))
            else:
                out = x
            if INT_MIN_VALUE <= out <= INT_MAX_VALUE:
                return out
            else:
                raise OverflowError
        except OverflowError:
            raise PFARuntimeException("int overflow", self.errcodeBase + 0, self.name, pos)
provide(ToInt())

class ToLong(LibFcn):
    name = prefix + "long"
    sig = Sigs([Sig([{"x": P.Int()}], P.Long()),
                Sig([{"x": P.Long()}], P.Long()),
                Sig([{"x": P.Float()}], P.Long()),
                Sig([{"x": P.Double()}], P.Long())])
    errcodeBase = 17030
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            if isinstance(x, float):
                if math.isnan(x):
                    raise OverflowError
                else:
                    out = int(math.floor(x + 0.5))
            else:
                out = x
            if LONG_MIN_VALUE <= out <= LONG_MAX_VALUE:
                return out
            else:
                raise OverflowError
        except OverflowError:
            raise PFARuntimeException("long overflow", self.errcodeBase + 0, self.name, pos)
provide(ToLong())

class ToFloat(LibFcn):
    name = prefix + "float"
    sig = Sigs([Sig([{"x": P.Int()}], P.Float()),
                Sig([{"x": P.Long()}], P.Float()),
                Sig([{"x": P.Float()}], P.Float()),
                Sig([{"x": P.Double()}], P.Float())])
    errcodeBase = 17040
    def genpy(self, paramTypes, args, pos):
        return "float({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return float(x)
provide(ToFloat())

class ToDouble(LibFcn):
    name = prefix + "double"
    sig = Sigs([Sig([{"x": P.Int()}], P.Double()),
                Sig([{"x": P.Long()}], P.Double()),
                Sig([{"x": P.Float()}], P.Double()),
                Sig([{"x": P.Double()}], P.Double())])
    errcodeBase = 17050
    def genpy(self, paramTypes, args, pos):
        return "float({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return float(x)
provide(ToDouble())

#################################################################### fanouts

def fanoutEnum(x, symbols):
    return [x == s for s in symbols]

def fanoutString(x, dictionary, outOfRange):
    out = [x == s for s in dictionary]
    if outOfRange:
        return out + [x not in dictionary]
    else:
        return out

def fanoutInt(x, minimum, maximum, outOfRange):
    out = [x == i for i in range(minimum, maximum)]
    if outOfRange:
        return out + [x < minimum or x >= maximum]
    else:
        return out

class FanoutBoolean(LibFcn):
    name = prefix + "fanoutBoolean"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Boolean())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Boolean())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Boolean()))])
    errcodeBase = 17060
    def __call__(self, state, scope, pos, paramTypes, x, *args):
        if len(args) == 0:
            return fanoutEnum(x, paramTypes[0]["symbols"])
        elif len(args) == 2:
            if len(args[0]) != len(set(args[0])):
                raise PFARuntimeException("non-distinct values in dictionary", self.errcodeBase + 0, self.name, pos)
            return fanoutString(x, args[0], args[1])
        elif len(args) == 3:
            return fanoutInt(x, args[0], args[1], args[2])
provide(FanoutBoolean())

class FanoutInt(LibFcn):
    name = prefix + "fanoutInt"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Int())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Int())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Int()))])
    errcodeBase = 17070
    def __call__(self, state, scope, pos, paramTypes, x, *args):
        if len(args) == 0:
            return [1 if y else 0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            if len(args[0]) != len(set(args[0])):
                raise PFARuntimeException("non-distinct values in dictionary", self.errcodeBase + 0, self.name, pos)
            return [1 if y else 0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1 if y else 0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutInt())

class FanoutLong(LibFcn):
    name = prefix + "fanoutLong"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Long())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Long())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Long()))])
    errcodeBase = 17080
    def __call__(self, state, scope, pos, paramTypes, x, *args):
        if len(args) == 0:
            return [1 if y else 0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            if len(args[0]) != len(set(args[0])):
                raise PFARuntimeException("non-distinct values in dictionary", self.errcodeBase + 0, self.name, pos)
            return [1 if y else 0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1 if y else 0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutLong())

class FanoutFloat(LibFcn):
    name = prefix + "fanoutFloat"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Float())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Float())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Float()))])
    errcodeBase = 17090
    def __call__(self, state, scope, pos, paramTypes, x, *args):
        if len(args) == 0:
            return [1.0 if y else 0.0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            if len(args[0]) != len(set(args[0])):
                raise PFARuntimeException("non-distinct values in dictionary", self.errcodeBase + 0, self.name, pos)
            return [1.0 if y else 0.0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1.0 if y else 0.0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutFloat())

class FanoutDouble(LibFcn):
    name = prefix + "fanoutDouble"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Double())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Double())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Double()))])
    errcodeBase = 17100
    def __call__(self, state, scope, pos, paramTypes, x, *args):
        if len(args) == 0:
            return [1.0 if y else 0.0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            if len(args[0]) != len(set(args[0])):
                raise PFARuntimeException("non-distinct values in dictionary", self.errcodeBase + 0, self.name, pos)
            return [1.0 if y else 0.0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1.0 if y else 0.0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutDouble())

#################################################################### serialize

class CastAvro(LibFcn):
    name = prefix + "avro"
    sig = Sig([{"x": P.Wildcard("A")}], P.Bytes())
    errcodeBase = 17110
    def __call__(self, state, scope, pos, paramTypes, x):
        schema = avro.schema.parse(json.dumps(paramTypes[0]))
        x = untagUnion(x, paramTypes[0])
        bytes = io.BytesIO()
        writer = DatumWriter(schema)
        writer.write(x, BinaryEncoder(bytes))
        bytes.flush()
        return bytes.getvalue()
provide(CastAvro())

class CastJson(LibFcn):
    name = prefix + "json"
    sig = Sig([{"x": P.Wildcard("A")}], P.String())
    errcodeBase = 17120
    def __call__(self, state, scope, pos, paramTypes, x):
        return json.dumps(jsonEncoder(jsonNodeToAvroType(paramTypes[0]), x), separators=(",", ":"))
provide(CastJson())
