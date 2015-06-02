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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.lib1.core import INT_MIN_VALUE
from titus.lib1.core import INT_MAX_VALUE
from titus.lib1.core import LONG_MIN_VALUE
from titus.lib1.core import LONG_MAX_VALUE
from titus.lib1.core import FLOAT_MIN_VALUE
from titus.lib1.core import FLOAT_MAX_VALUE
from titus.lib1.core import DOUBLE_MIN_VALUE
from titus.lib1.core import DOUBLE_MAX_VALUE
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
    def __call__(self, state, scope, paramTypes, x, bits):
        if bits < 2 or bits > 64:
            raise PFARuntimeException("unrepresentable unsigned number")
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
    def __call__(self, state, scope, paramTypes, x, bits):
        if bits < 1 or bits > 63:
            raise PFARuntimeException("unrepresentable unsigned number")
        return doUnsigned(x, bits)
provide(ToUnsigned())

#################################################################### number precisions

class ToInt(LibFcn):
    name = prefix + "int"
    sig = Sigs([Sig([{"x": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}], P.Int()),
                Sig([{"x": P.Float()}], P.Int()),
                Sig([{"x": P.Double()}], P.Int())])
    def genpy(self, paramTypes, args):
        return "int({0})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
        return int(x)
provide(ToInt())

class ToLong(LibFcn):
    name = prefix + "long"
    sig = Sigs([Sig([{"x": P.Int()}], P.Long()),
                Sig([{"x": P.Long()}], P.Long()),
                Sig([{"x": P.Float()}], P.Long()),
                Sig([{"x": P.Double()}], P.Long())])
    def genpy(self, paramTypes, args):
        return "int({0})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
        return int(x)
provide(ToLong())

class ToFloat(LibFcn):
    name = prefix + "float"
    sig = Sigs([Sig([{"x": P.Int()}], P.Float()),
                Sig([{"x": P.Long()}], P.Float()),
                Sig([{"x": P.Float()}], P.Float()),
                Sig([{"x": P.Double()}], P.Float())])
    def genpy(self, paramTypes, args):
        return "float({0})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
        return float(x)
provide(ToFloat())

class ToDouble(LibFcn):
    name = prefix + "double"
    sig = Sigs([Sig([{"x": P.Int()}], P.Double()),
                Sig([{"x": P.Long()}], P.Double()),
                Sig([{"x": P.Float()}], P.Double()),
                Sig([{"x": P.Double()}], P.Double())])
    def genpy(self, paramTypes, args):
        return "float({0})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
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
    out = [x == i for i in xrange(minimum, maximum)]
    if outOfRange:
        return out + [x < minimum or x >= maximum]
    else:
        return out

class FanoutBoolean(LibFcn):
    name = prefix + "fanoutBoolean"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Boolean())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Boolean())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Boolean()))])
    def __call__(self, state, scope, paramTypes, x, *args):
        if len(args) == 0:
            return fanoutEnum(x, paramTypes[0]["symbols"])
        elif len(args) == 2:
            return fanoutString(x, args[0], args[1])
        elif len(args) == 3:
            return fanoutInt(x, args[0], args[1], args[2])
provide(FanoutBoolean())

class FanoutInt(LibFcn):
    name = prefix + "fanoutInt"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Int())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Int())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Int()))])
    def __call__(self, state, scope, paramTypes, x, *args):
        if len(args) == 0:
            return [1 if y else 0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            return [1 if y else 0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1 if y else 0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutInt())

class FanoutLong(LibFcn):
    name = prefix + "fanoutLong"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Long())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Long())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Long()))])
    def __call__(self, state, scope, paramTypes, x, *args):
        if len(args) == 0:
            return [1 if y else 0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            return [1 if y else 0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1 if y else 0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutLong())

class FanoutFloat(LibFcn):
    name = prefix + "fanoutFloat"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Float())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Float())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Float()))])
    def __call__(self, state, scope, paramTypes, x, *args):
        if len(args) == 0:
            return [1.0 if y else 0.0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            return [1.0 if y else 0.0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1.0 if y else 0.0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutFloat())

class FanoutDouble(LibFcn):
    name = prefix + "fanoutDouble"
    sig = Sigs([Sig([{"x": P.WildEnum("A")}], P.Array(P.Double())),
                Sig([{"x": P.String()}, {"dictionary": P.Array(P.String())}, {"outOfRange": P.Boolean()}], P.Array(P.Double())),
                Sig([{"x": P.Int()}, {"minimum": P.Int()}, {"maximum": P.Int()}, {"outOfRange": P.Boolean()}], P.Array(P.Double()))])
    def __call__(self, state, scope, paramTypes, x, *args):
        if len(args) == 0:
            return [1.0 if y else 0.0 for y in fanoutEnum(x, paramTypes[0]["symbols"])]
        elif len(args) == 2:
            return [1.0 if y else 0.0 for y in fanoutString(x, args[0], args[1])]
        elif len(args) == 3:
            return [1.0 if y else 0.0 for y in fanoutInt(x, args[0], args[1], args[2])]
provide(FanoutDouble())
