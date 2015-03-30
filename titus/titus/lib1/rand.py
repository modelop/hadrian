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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.lib1.core import INT_MIN_VALUE, INT_MAX_VALUE, LONG_MIN_VALUE, LONG_MAX_VALUE
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "rand."

########################################################### raw numbers of various types

class RandomInt(LibFcn):
    name = prefix + "int"
    sig = Sigs([Sig([], P.Int()),
                Sig([{"low": P.Int()}, {"high": P.Int()}], P.Int())])
    def __call__(self, state, scope, paramTypes, *args):
        if len(args) == 0:
            return state.rand.randint(INT_MIN_VALUE, INT_MAX_VALUE)
        else:
            if args[1] <= args[0]: raise PFARuntimeException("high must be greater than low")
            return state.rand.randint(args[0], args[1] - 1)  # Python's randint has an inclusive upper bound
provide(RandomInt())

class RandomLong(LibFcn):
    name = prefix + "long"
    sig = Sigs([Sig([], P.Long()),
                Sig([{"long": P.Long()}, {"high": P.Long()}], P.Long())])
    def __call__(self, state, scope, paramTypes, *args):
        if len(args) == 0:
            return state.rand.randint(LONG_MIN_VALUE, LONG_MAX_VALUE)
        else:
            if args[1] <= args[0]: raise PFARuntimeException("high must be greater than low")
            return state.rand.randint(args[0], args[1] - 1)  # Python's randint has an inclusive upper bound
provide(RandomLong())

class RandomFloat(LibFcn):
    name = prefix + "float"
    sig = Sig([{"low": P.Float()}, {"high": P.Float()}], P.Float())
    def __call__(self, state, scope, paramTypes, low, high):
        if high <= low: raise PFARuntimeException("high must be greater than low")
        return state.rand.uniform(low, high)
provide(RandomFloat())

class RandomDouble(LibFcn):
    name = prefix + "double"
    sig = Sig([{"low": P.Double()}, {"high": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, low, high):
        if high <= low: raise PFARuntimeException("high must be greater than low")
        return state.rand.uniform(low, high)
provide(RandomDouble())

########################################################### strings and byte arrays

class RandomString(LibFcn):
    name = prefix + "string"
    sig = Sigs([Sig([{"size": P.Int()}], P.String()),
                Sig([{"size": P.Int()}, {"population": P.String()}], P.String()),
                Sig([{"size": P.Int()}, {"low": P.Int()}, {"high": P.Int()}], P.String())])
    def __call__(self, state, scope, paramTypes, size, *args):
        if size <= 0: raise PFARuntimeException("size must be positive")
        if len(args) == 0:
            return "".join(unichr(state.rand.randint(1, 0xD800)) for x in xrange(size))
        elif len(args) == 1:
            return "".join(args[0][state.rand.randint(0, len(args[0]) - 1)] for x in xrange(size))
        else:
            low, high = args
            if high <= low: raise PFARuntimeException("high must be greater than low")
            return "".join(unichr(state.rand.randint(low, high - 1)) for x in xrange(size))
provide(RandomString())

class RandomBytes(LibFcn):
    name = prefix + "bytes"
    sig = Sigs([Sig([{"size": P.Int()}], P.Bytes()),
                Sig([{"size": P.Int()}, {"population": P.Bytes()}], P.Bytes()),
                Sig([{"size": P.Int()}, {"low": P.Int()}, {"high": P.Int()}], P.Bytes())])
    def __call__(self, state, scope, paramTypes, size, *args):
        if size <= 0: raise PFARuntimeException("size must be positive")
        if len(args) == 0:
            return "".join(chr(state.rand.randint(0, 255)) for x in xrange(size))
        elif len(args) == 1:
            return "".join(args[0][state.rand.randint(0, len(args[0]) - 1)] for x in xrange(size))
        else:
            low, high = args
            if high <= low: raise PFARuntimeException("high must be greater than low")
            return "".join(chr(state.rand.randint(low, high - 1)) for x in xrange(size))
provide(RandomBytes())

class RandomUUID(LibFcn):
    name = prefix + "uuid"
    sig = Sig([], P.String())
    def __call__(self, state, scope, paramTypes):
        return "{0:08x}-{1:04x}-4{2:03x}-8{3:03x}-{4:016x}".format(state.rand.getrandbits(32), state.rand.getrandbits(16), state.rand.getrandbits(12), state.rand.getrandbits(12), state.rand.getrandbits(64))
provide(RandomUUID())

########################################################### common probability distributions

class Gaussian(LibFcn):
    name = prefix + "gaussian"
    sig = Sig([{"mu": P.Double()}, {"sigma": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, mu, sigma):
        return state.rand.gauss(mu, sigma)
provide(Gaussian())

