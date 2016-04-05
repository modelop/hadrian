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
import itertools
import json

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, negativeIndex, startEnd
from titus.lib.core import INT_MIN_VALUE
from titus.lib.core import INT_MAX_VALUE
from titus.lib.core import LONG_MIN_VALUE
from titus.lib.core import LONG_MAX_VALUE
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "a."

anyNumber = set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()])

def toCmp(state, scope, lessThan):
    return lambda a, b: -1 if callfcn(state, scope, lessThan, [a, b]) else 1 if callfcn(state, scope, lessThan, [b, a]) else 0

def toLt(state, scope, lessThan):
    return lambda a, b: callfcn(state, scope, lessThan, [a, b])

def checkRange(length, index, code, fcnName, pos):
    if index < 0 or index >= length:
        raise PFARuntimeException("index out of range", code, fcnName, pos)

#################################################################### basic access

class Len(LibFcn):
    name = prefix + "len"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Int())
    errcodeBase = 15000
    def genpy(self, paramTypes, args, pos):
        return "len({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, a):
        return len(a)
provide(Len())

class Subseq(LibFcn):
    name = prefix + "subseq"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"start": P.Int()}, {"end": P.Int()}], P.Array(P.Wildcard("A")))
    errcodeBase = 15010
    def genpy(self, paramTypes, args, pos):
        return "{0}[{1}:{2}]".format(*args)
    def __call__(self, state, scope, pos, paramTypes, a, start, end):
        return a[start:end]
provide(Subseq())

class Head(LibFcn):
    name = prefix + "head"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15020
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return a[0]
provide(Head())

class Tail(LibFcn):
    name = prefix + "tail"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15030
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return a[1:]
provide(Tail())

class Last(LibFcn):
    name = prefix + "last"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15040
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return a[-1]
provide(Last())

class Init(LibFcn):
    name = prefix + "init"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15050
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return a[:-1]
provide(Init())

class SubseqTo(LibFcn):
    name = prefix + "subseqto"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"start": P.Int()}, {"end": P.Int()}, {"replacement": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15060
    def __call__(self, state, scope, pos, paramTypes, a, start, end, replacement):
        normStart, normEnd = startEnd(len(a), start, end)
        before = a[:normStart]
        after = a[normEnd:]
        return before + replacement + after
provide(SubseqTo())

#################################################################### searching

class Contains(LibFcn):
    name = prefix + "contains"
    sig = Sigs([Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Array(P.Wildcard("A"))}], P.Boolean()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Wildcard("A")}], P.Boolean()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Boolean())])
    errcodeBase = 15070
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if isinstance(needle, (list, tuple)):
            for start in xrange(len(haystack) - len(needle) + 1):
                if needle == haystack[start:(start + len(needle))]:
                    return True
            return False
        elif callable(needle):
            for item in haystack:
                if callfcn(state, scope, needle, [item]):
                    return True
            return False
        else:
            try:
                haystack.index(needle)
            except ValueError:
                return False
            else:
                return True
provide(Contains())

class Count(LibFcn):
    name = prefix + "count"
    sig = Sigs([Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Array(P.Wildcard("A"))}], P.Int()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Wildcard("A")}], P.Int()),
                Sig([{"a": P.Array(P.Wildcard("A"))}, {"predicate": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Int())])
    errcodeBase = 15080
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if len(haystack) == 0:
            return 0
        else:
            if isinstance(needle, (list, tuple)):
                if len(needle) == 0:
                    return 0
                else:
                    count = 0
                    for start in xrange(len(haystack) - len(needle) + 1):
                        if needle == haystack[start:(start + len(needle))]:
                            count += 1
                    return count
            elif callable(needle):
                count = 0
                for item in haystack:
                    if callfcn(state, scope, needle, [item]):
                        count += 1
                return count
            else:
                return haystack.count(needle)
provide(Count())

class Index(LibFcn):
    name = prefix + "index"
    sig = Sigs([Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Array(P.Wildcard("A"))}], P.Int()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Wildcard("A")}], P.Int()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Int())])
    errcodeBase = 15090
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if isinstance(needle, (list, tuple)):
            for start in xrange(len(haystack) - len(needle) + 1):
                if needle == haystack[start:(start + len(needle))]:
                    return start
            return -1
        elif callable(needle):
            for index, item in enumerate(haystack):
                if callfcn(state, scope, needle, [item]):
                    return index
            return -1
        else:
            try:
                return haystack.index(needle)
            except ValueError:
                return -1
provide(Index())

class RIndex(LibFcn):
    name = prefix + "rindex"
    sig = Sigs([Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Array(P.Wildcard("A"))}], P.Int()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Wildcard("A")}], P.Int()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Int())])
    errcodeBase = 15100
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if isinstance(needle, (list, tuple)):
            for start in xrange(len(haystack) - len(needle), -1, -1):
                if needle == haystack[start:(start + len(needle))]:
                    return start
            return -1
        elif callable(needle):
            for index, item in enumerate(reversed(haystack)):
                if callfcn(state, scope, needle, [item]):
                    return len(haystack) - 1 - index
            return -1
        else:
            for index in xrange(len(haystack) - 1, -1, -1):
                if needle == haystack[index]:
                    return index
            return -1
provide(RIndex())

class StartsWith(LibFcn):
    name = prefix + "startswith"
    sig = Sigs([Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Array(P.Wildcard("A"))}], P.Boolean()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Wildcard("A")}], P.Boolean())])
    errcodeBase = 15110
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if isinstance(needle, (list, tuple)):
            return needle == haystack[:len(needle)]
        else:
            if len(haystack) == 0:
                return False
            else:
                return needle == haystack[0]
provide(StartsWith())

class EndsWith(LibFcn):
    name = prefix + "endswith"
    sig = Sigs([Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Array(P.Wildcard("A"))}], P.Boolean()),
                Sig([{"haystack": P.Array(P.Wildcard("A"))}, {"needle": P.Wildcard("A")}], P.Boolean())])
    errcodeBase = 15120
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if isinstance(needle, (list, tuple)):
            if len(needle) == 0:
                return True
            else:
                return needle == haystack[-len(needle):]
        else:
            if len(haystack) == 0:
                return False
            else:
                return needle == haystack[-1]
provide(EndsWith())

#################################################################### manipulation

class Concat(LibFcn):
    name = prefix + "concat"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15130
    def genpy(self, paramTypes, args, pos):
        return "({0} + {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return a + b
provide(Concat())

class Append(LibFcn):
    name = prefix + "append"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"item": P.Wildcard("A")}], P.Array(P.Wildcard("A")))
    errcodeBase = 15140
    def genpy(self, paramTypes, args, pos):
        return "({0} + [{1}])".format(*args)
    def __call__(self, state, scope, pos, paramTypes, a, item):
        return a + [item]
provide(Append())

class Cycle(LibFcn):
    name = prefix + "cycle"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"item": P.Wildcard("A")}, {"maxLength": P.Int()}], P.Array(P.Wildcard("A")))
    errcodeBase = 15150
    def __call__(self, state, scope, pos, paramTypes, a, item, maxLength):
        if maxLength < 0:
            raise PFARuntimeException("maxLength out of range", self.errcodeBase + 0, self.name, pos)
        elif maxLength == 0:
            return []
        else:
            out = a + [item]
            return out[-maxLength:]
provide(Cycle())

class Insert(LibFcn):
    name = prefix + "insert"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"index": P.Int()}, {"item": P.Wildcard("A")}], P.Array(P.Wildcard("A")))
    errcodeBase = 15160
    def __call__(self, state, scope, pos, paramTypes, a, index, item):
        normIndex = negativeIndex(len(a), index)
        checkRange(len(a), normIndex, self.errcodeBase + 0, self.name, pos)
        before = a[:normIndex]
        after = a[normIndex:]
        return before + [item] + after
provide(Insert())

class Replace(LibFcn):
    name = prefix + "replace"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"index": P.Int()}, {"item": P.Wildcard("A")}], P.Array(P.Wildcard("A")))
    errcodeBase = 15170
    def __call__(self, state, scope, pos, paramTypes, a, index, item):
        normIndex = negativeIndex(len(a), index)
        checkRange(len(a), normIndex, self.errcodeBase + 0, self.name, pos)
        before = a[:normIndex]
        after = a[(normIndex + 1):]
        return before + [item] + after
provide(Replace())

class Remove(LibFcn):
    name = prefix + "remove"
    sig = Sigs([Sig([{"a": P.Array(P.Wildcard("A"))}, {"start": P.Int()}, {"end": P.Int()}], P.Array(P.Wildcard("A"))),
                Sig([{"a": P.Array(P.Wildcard("A"))}, {"index": P.Int()}], P.Array(P.Wildcard("A")))])
    errcodeBase = 15180
    def __call__(self, state, scope, pos, paramTypes, a, *where):
        if len(where) == 2:
            start, end = where
            normStart, normEnd = startEnd(len(a), start, end)
            before = a[:normStart]
            after = a[normEnd:]
            return before + after
        elif len(where) == 1:
            index, = where
            normIndex = negativeIndex(len(a), index)
            checkRange(len(a), normIndex, self.errcodeBase + 0, self.name, pos)
            before = a[:normIndex]
            after = a[(normIndex + 1):]
            return before + after
provide(Remove())

class Rotate(LibFcn):
    name = prefix + "rotate"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"steps": P.Int()}], P.Array(P.Wildcard("A")))
    errcodeBase = 15190
    def __call__(self, state, scope, pos, paramTypes, a, steps):
        if steps < 0:
            raise PFARuntimeException("steps out of range", self.errcodeBase + 0, self.name, pos)
        elif steps == 0 or len(a) == 0:
            return a
        else:
            index = steps % len(a)
            left, right = a[:index], a[index:]
            return right + left
provide(Rotate())

#################################################################### reordering

class Sort(LibFcn):
    name = prefix + "sort"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15200
    def __call__(self, state, scope, pos, paramTypes, a):
        return sorted(a, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y))
provide(Sort())

class SortLT(LibFcn):
    name = prefix + "sortLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15210
    def __call__(self, state, scope, pos, paramTypes, a, lessThan):
        return sorted(a, toCmp(state, scope, lessThan))
provide(SortLT())

class Shuffle(LibFcn):
    name = prefix + "shuffle"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15220
    def __call__(self, state, scope, pos, paramTypes, a):
        out = list(a)
        state.rand.shuffle(out)
        return out
provide(Shuffle())

class Reverse(LibFcn):
    name = prefix + "reverse"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15230
    def __call__(self, state, scope, pos, paramTypes, a):
        return list(reversed(a))
provide(Reverse())

#################################################################### extreme values

def highestN(a, n, lt):
    out = []
    for x in a:
        found = False
        for index, best in enumerate(out):
            if lt(best, x):
                out.insert(index, x)
                found = True
                break
        if not found:
            out.append(x)
        if len(out) > n:
            out.pop()
    return out

def lowestN(a, n, lt):
    out = []
    for x in a:
        found = False
        for index, best in enumerate(out):
            if lt(x, best):
                out.insert(index, x)
                found = True
                break
        if not found:
            out.append(x)
        if len(out) > n:
            out.pop()
    return out

def argHighestN(a, n, lt):
    out = []
    for i, x in enumerate(a):
        found = False
        for index, (besti, bestx) in enumerate(out):
            if lt(bestx, x):
                ind = index
                while ind <= len(out) and not lt(out[ind][1], x) and not lt(x, out[ind][1]) and out[ind][0] > i:
                    ind += 1
                out.insert(ind, (i, x))
                found = True
                break
        if not found:
            out.append((i, x))
        if len(out) > n:
            out.pop()
    return [i for i, x in out]

def argLowestN(a, n, lt):
    out = []
    for i, x in enumerate(a):
        found = False
        for index, (besti, bestx) in enumerate(out):
            if lt(x, bestx):
                ind = index
                while ind <= len(out) and not lt(out[ind][1], x) and not lt(x, out[ind][1]) and out[ind][0] > i:
                    ind += 1
                out.insert(ind, (i, x))
                found = True
                break
        if not found:
            out.append((i, x))
        if len(out) > n:
            out.pop()
    return [i for i, x in out]

class Max(LibFcn):
    name = prefix + "max"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15240
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return highestN(a, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)[0]
provide(Max())

class Min(LibFcn):
    name = prefix + "min"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15250
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return lowestN(a, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)[0]
provide(Min())

class MaxLT(LibFcn):
    name = prefix + "maxLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Wildcard("A"))
    errcodeBase = 15260
    def __call__(self, state, scope, pos, paramTypes, a, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return highestN(a, 1, toLt(state, scope, lessThan))[0]
provide(MaxLT())

class MinLT(LibFcn):
    name = prefix + "minLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Wildcard("A"))
    errcodeBase = 15270
    def __call__(self, state, scope, pos, paramTypes, a, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return lowestN(a, 1, toLt(state, scope, lessThan))[0]
provide(MinLT())

class MaxN(LibFcn):
    name = prefix + "maxN"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}], P.Array(P.Wildcard("A")))
    errcodeBase = 15280
    def __call__(self, state, scope, pos, paramTypes, a, n):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return highestN(a, n, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)
provide(MaxN())

class MinN(LibFcn):
    name = prefix + "minN"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}], P.Array(P.Wildcard("A")))
    errcodeBase = 15290
    def __call__(self, state, scope, pos, paramTypes, a, n):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return lowestN(a, n, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)
provide(MinN())

class MaxNLT(LibFcn):
    name = prefix + "maxNLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15300
    def __call__(self, state, scope, pos, paramTypes, a, n, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return highestN(a, n, toLt(state, scope, lessThan))
provide(MaxNLT())

class MinNLT(LibFcn):
    name = prefix + "minNLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15310
    def __call__(self, state, scope, pos, paramTypes, a, n, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return lowestN(a, n, toLt(state, scope, lessThan))
provide(MinNLT())

class Argmax(LibFcn):
    name = prefix + "argmax"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Int())
    errcodeBase = 15320
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return argHighestN(a, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)[0]
provide(Argmax())

class Argmin(LibFcn):
    name = prefix + "argmin"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Int())
    errcodeBase = 15330
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return argLowestN(a, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)[0]
provide(Argmin())

class ArgmaxLT(LibFcn):
    name = prefix + "argmaxLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Int())
    errcodeBase = 15340
    def __call__(self, state, scope, pos, paramTypes, a, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return argHighestN(a, 1, toLt(state, scope, lessThan))[0]
provide(ArgmaxLT())

class ArgminLT(LibFcn):
    name = prefix + "argminLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Int())
    errcodeBase = 15350
    def __call__(self, state, scope, pos, paramTypes, a, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return argLowestN(a, 1, toLt(state, scope, lessThan))[0]
provide(ArgminLT())

class ArgmaxN(LibFcn):
    name = prefix + "argmaxN"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}], P.Array(P.Int()))
    errcodeBase = 15360
    def __call__(self, state, scope, pos, paramTypes, a, n):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argHighestN(a, n, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)
provide(ArgmaxN())

class ArgminN(LibFcn):
    name = prefix + "argminN"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}], P.Array(P.Int()))
    errcodeBase = 15370
    def __call__(self, state, scope, pos, paramTypes, a, n):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argLowestN(a, n, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)
provide(ArgminN())

class ArgmaxNLT(LibFcn):
    name = prefix + "argmaxNLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.Int()))
    errcodeBase = 15380
    def __call__(self, state, scope, pos, paramTypes, a, n, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argHighestN(a, n, toLt(state, scope, lessThan))
provide(ArgmaxNLT())

class ArgminNLT(LibFcn):
    name = prefix + "argminNLT"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.Int()))
    errcodeBase = 15390
    def __call__(self, state, scope, pos, paramTypes, a, n, lessThan):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argLowestN(a, n, toLt(state, scope, lessThan))
provide(ArgminNLT())

#################################################################### numerical

class Sum(LibFcn):
    name = prefix + "sum"
    sig = Sig([{"a": P.Array(P.Wildcard("A", oneOf = anyNumber))}], P.Wildcard("A"))
    errcodeBase = 15400
    def __call__(self, state, scope, pos, paramTypes, a):
        out = sum(a)
        if paramTypes[-1] == "int":
            if INT_MIN_VALUE <= out <= INT_MAX_VALUE:
                return out
            else:
                raise PFARuntimeException("int overflow", self.errcodeBase + 0, self.name, pos)
        elif paramTypes[-1] == "long":
            if LONG_MIN_VALUE <= out <= LONG_MAX_VALUE:
                return out
            else:
                raise PFARuntimeException("long overflow", self.errcodeBase + 1, self.name, pos)
        else:
            return out
provide(Sum())

class Product(LibFcn):
    name = prefix + "product"
    sig = Sig([{"a": P.Array(P.Wildcard("A", oneOf = anyNumber))}], P.Wildcard("A"))
    errcodeBase = 15410
    def __call__(self, state, scope, pos, paramTypes, a):
        if paramTypes[-1] == "int":
            out = reduce(lambda a, b: a * b, a, 1)
            if INT_MIN_VALUE <= out <= INT_MAX_VALUE:
                return out
            else:
                raise PFARuntimeException("int overflow", self.errcodeBase + 0, self.name, pos)
        elif paramTypes[-1] == "long":
            out = reduce(lambda a, b: a * b, a, 1)
            if LONG_MIN_VALUE <= out <= LONG_MAX_VALUE:
                return out
            else:
                raise PFARuntimeException("long overflow", self.errcodeBase + 1, self.name, pos)
        else:
            return reduce(lambda a, b: a * b, a, 1.0)
provide(Product())

class Lnsum(LibFcn):
    name = prefix + "lnsum"
    sig = Sig([{"a": P.Array(P.Double())}], P.Double())
    errcodeBase = 15420
    def ln(self, x):
        if x < 0.0:
            return float("nan")
        elif x == 0.0:
            return float("-inf")
        else:
            return math.log(x)
    def __call__(self, state, scope, pos, paramTypes, a):
        return sum([self.ln(x) for x in a])
provide(Lnsum())

class Mean(LibFcn):
    name = prefix + "mean"
    sig = Sig([{"a": P.Array(P.Double())}], P.Double())
    errcodeBase = 15430
    def __call__(self, state, scope, pos, paramTypes, a):
        numer = sum(a)
        denom = float(len(a))
        if denom == 0.0:
            if numer > 0.0:
                return float("inf")
            elif numer < 0.0:
                return float("-inf")
            else:
                return float("nan")
        else:
            return numer / denom
provide(Mean())

class GeoMean(LibFcn):
    name = prefix + "geomean"
    sig = Sig([{"a": P.Array(P.Double())}], P.Double())
    errcodeBase = 15440
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            return float("nan")
        else:
            return reduce(lambda a, b: a * b, a, 1.0)**(1.0/len(a))
provide(GeoMean())

class Median(LibFcn):
    name = prefix + "median"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15450
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        sa = sorted(a, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y))
        half = len(sa) / 2
        dataType = paramTypes[-1]

        if len(sa) % 2:
            return sa[half]
        if (dataType is "float") or (dataType is "double"):
            return (sa[half - 1] + sa[half])/2.0
        else:
            return sa[half - 1]
provide(Median())

class Mode(LibFcn):
    name = prefix + "mode"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15470
    def __call__(self, state, scope, pos, paramTypes, a):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            counter = {}
            lookup = {}
            for x in a:
                xx = json.dumps(x)
                if xx not in counter:
                    counter[xx] = 0
                    lookup[xx] = x
                counter[xx] += 1
            bestn = 0
            for n in counter.values():
                if n > bestn:
                    bestn = n
            besties = [lookup[xx] for xx in counter if counter[xx] == bestn]
            return Median()(state, scope, pos, paramTypes, besties)
provide(Mode())

class NTile(LibFcn):
    name = prefix + "ntile"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"p": P.Double()}], P.Wildcard("A"))
    errcodeBase = 15460
    def __call__(self, state, scope, pos, paramTypes, a, p):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        if math.isnan(p):
            raise PFARuntimeException("p not a number", self.errcodeBase + 1, self.name, pos)
        if p <= 0.0:
            return lowestN(a, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)[0]
        if p >= 1.0:
            return highestN(a, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y) < 0)[0]
        sa = sorted(a, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).items, x, y))
        k = (len(a) - 1.0)*p
        f = math.floor(k)
        dataType = paramTypes[-1]
        if (dataType is "float") or (dataType is "double"):
            c = math.ceil(k)
            if f == c:
                return sa[int(k)]
            d0 = sa[int(f)] * (c - k)
            d1 = sa[int(c)] * (k - f)
            return d0 + d1
        else:
            if len(sa) % 2 == 1:
                return sa[int(f)]
            else:
                return sa[int(k)]
provide(NTile())

#################################################################### set or set-like functions

class Distinct(LibFcn):
    name = prefix + "distinct"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15490
    def __call__(self, state, scope, pos, paramTypes, a):
        out = []
        for ai in a:
            if ai not in out:
                out.append(ai)
        return out
provide(Distinct())

class SetEq(LibFcn):
    name = prefix + "seteq"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Boolean())
    errcodeBase = 15500
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return all(ai in b for ai in a) and all(bi in a for bi in b)
provide(SetEq())

class Union(LibFcn):
    name = prefix + "union"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15510
    def __call__(self, state, scope, pos, paramTypes, a, b):
        out = []
        for ai in a:
            if ai not in out:
                out.append(ai)
        for bi in b:
            if bi not in out:
                out.append(bi)
        return out
provide(Union())

class Intersection(LibFcn):
    name = prefix + "intersection"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15520
    def __call__(self, state, scope, pos, paramTypes, a, b):
        out = []
        for ai in a:
            if ai in b:
                out.append(ai)
        for bi in b:
            if bi in a:
                out.append(bi)
        return out
provide(Intersection())

class Diff(LibFcn):
    name = prefix + "diff"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15530
    def __call__(self, state, scope, pos, paramTypes, a, b):
        out = []
        for ai in a:
            if ai not in b:
                out.append(ai)
        return out
provide(Diff())

class SymDiff(LibFcn):
    name = prefix + "symdiff"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15540
    def __call__(self, state, scope, pos, paramTypes, a, b):
        out = []
        for ai in a:
            if ai not in b:
                out.append(ai)
        for bi in b:
            if bi not in a:
                out.append(bi)
        return out
provide(SymDiff())

class Subset(LibFcn):
    name = prefix + "subset"
    sig = Sig([{"little": P.Array(P.Wildcard("A"))}, {"big": P.Array(P.Wildcard("A"))}], P.Boolean())
    errcodeBase = 15550
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return all(ai in b for ai in a)
provide(Subset())

class Disjoint(LibFcn):
    name = prefix + "disjoint"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("A"))}], P.Boolean())
    errcodeBase = 15560
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return all(ai not in b for ai in a) and all(bi not in a for bi in b)
provide(Disjoint())

#################################################################### functional programming

class MapApply(LibFcn):
    name = prefix + "map"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Wildcard("B"))}], P.Array(P.Wildcard("B")))
    errcodeBase = 15570
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        return [callfcn(state, scope, fcn, [x]) for x in a]
provide(MapApply())

class MapWithIndex(LibFcn):
    name = prefix + "mapWithIndex"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A")], P.Wildcard("B"))}], P.Array(P.Wildcard("B")))
    errcodeBase = 15580
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        return [callfcn(state, scope, fcn, [i, x]) for i, x in enumerate(a)]
provide(MapWithIndex())

class Filter(LibFcn):
    name = prefix + "filter"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15590
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        return [x for x in a if callfcn(state, scope, fcn, [x])]
provide(Filter())

class FilterWithIndex(LibFcn):
    name = prefix + "filterWithIndex"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15600
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        return [x for i, x in enumerate(a) if callfcn(state, scope, fcn, [i, x])]
provide(FilterWithIndex())

class FilterMap(LibFcn):
    name = prefix + "filterMap"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Union([P.Wildcard("B"), P.Null()]))}], P.Array(P.Wildcard("B")))
    errcodeBase = 15610
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        typeNames = [jsonNodeToAvroType(t).name for t in paramTypes[1]["ret"] if t != "null"]
        out = []
        for x in a:
            y = callfcn(state, scope, fcn, [x])
            if y is not None:
                if isinstance(y, dict) and len(y) == 1 and y.keys()[0] in typeNames:
                    tag, value = y.items()[0]
                else:
                    value = y
                out.append(value)
        return out
provide(FilterMap())

class FilterMapWithIndex(LibFcn):
    name = prefix + "filterMapWithIndex"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A")], P.Union([P.Wildcard("B"), P.Null()]))}], P.Array(P.Wildcard("B")))
    errcodeBase = 15620
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        typeNames = [jsonNodeToAvroType(t).name for t in paramTypes[1]["ret"] if t != "null"]
        out = []
        for i, x in enumerate(a):
            y = callfcn(state, scope, fcn, [i, x])
            if y is not None:
                if isinstance(y, dict) and len(y) == 1 and y.keys()[0] in typeNames:
                    tag, value = y.items()[0]
                else:
                    value = y
                out.append(value)
        return out
provide(FilterMapWithIndex())

class FlatMap(LibFcn):
    name = prefix + "flatMap"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Array(P.Wildcard("B")))}], P.Array(P.Wildcard("B")))
    errcodeBase = 15630
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        out = []
        for x in a:
            out.extend(callfcn(state, scope, fcn, [x]))
        return out
provide(FlatMap())

class FlatMapWithIndex(LibFcn):
    name = prefix + "flatMapWithIndex"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A")], P.Array(P.Wildcard("B")))}], P.Array(P.Wildcard("B")))
    errcodeBase = 15640
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        out = []
        for i, x in enumerate(a):
            out.extend(callfcn(state, scope, fcn, [i, x]))
        return out
provide(FlatMapWithIndex())

class ZipMap(LibFcn):
    name = prefix + "zipmap"
    sig = Sigs([Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Wildcard("Z"))}], P.Array(P.Wildcard("Z"))),
                Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"c": P.Array(P.Wildcard("C"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")], P.Wildcard("Z"))}], P.Array(P.Wildcard("Z"))),
                Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"c": P.Array(P.Wildcard("C"))}, {"d": P.Array(P.Wildcard("D"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")], P.Wildcard("Z"))}], P.Array(P.Wildcard("Z")))])
    errcodeBase = 15650
    def __call__(self, state, scope, pos, paramTypes, *args):
        if len(set(map(len, args[:-1]))) != 1:
            raise PFARuntimeException("misaligned arrays", self.errcodeBase + 0, self.name, pos)
        fcn = args[-1]
        out = []
        for abcd in zip(*args[:-1]):
            out.append(callfcn(state, scope, fcn, list(abcd)))
        return out
provide(ZipMap())

class ZipMapWithIndex(LibFcn):
    name = prefix + "zipmapWithIndex"
    sig = Sigs([Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A"), P.Wildcard("B")], P.Wildcard("Z"))}], P.Array(P.Wildcard("Z"))),
                Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"c": P.Array(P.Wildcard("C"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")], P.Wildcard("Z"))}], P.Array(P.Wildcard("Z"))),
                Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"c": P.Array(P.Wildcard("C"))}, {"d": P.Array(P.Wildcard("D"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")], P.Wildcard("Z"))}], P.Array(P.Wildcard("Z")))])
    errcodeBase = 15660
    def __call__(self, state, scope, pos, paramTypes, *args):
        if len(set(map(len, args[:-1]))) != 1:
            raise PFARuntimeException("misaligned arrays", self.errcodeBase + 0, self.name, pos)
        fcn = args[-1]
        out = []
        for i, abcd in enumerate(zip(*args[:-1])):
            out.append(callfcn(state, scope, fcn, [i] + list(abcd)))
        return out
provide(ZipMapWithIndex())

class Reduce(LibFcn):
    name = prefix + "reduce"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15670
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return reduce(lambda x, y: callfcn(state, scope, fcn, [x, y]), a)
provide(Reduce())

class ReduceRight(LibFcn):
    name = prefix + "reduceRight"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Wildcard("A"))}], P.Wildcard("A"))
    errcodeBase = 15680
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        if len(a) == 0:
            raise PFARuntimeException("empty array", self.errcodeBase + 0, self.name, pos)
        else:
            return reduce(lambda x, y: callfcn(state, scope, fcn, [y, x]), reversed(a))
provide(ReduceRight())

class Fold(LibFcn):
    name = prefix + "fold"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"zero": P.Wildcard("B")}, {"fcn": P.Fcn([P.Wildcard("B"), P.Wildcard("A")], P.Wildcard("B"))}], P.Wildcard("B"))
    errcodeBase = 15690
    def __call__(self, state, scope, pos, paramTypes, a, zero, fcn):
        return reduce(lambda x, y: callfcn(state, scope, fcn, [x, y]), a, zero)
provide(Fold())

class FoldRight(LibFcn):
    name = prefix + "foldRight"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"zero": P.Wildcard("B")}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Wildcard("B"))}], P.Wildcard("B"))
    errcodeBase = 15700
    def __call__(self, state, scope, pos, paramTypes, a, zero, fcn):
        return reduce(lambda y, x: callfcn(state, scope, fcn, [x, y]), reversed(a), zero)
provide(FoldRight())

class TakeWhile(LibFcn):
    name = prefix + "takeWhile"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15710
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        out = []
        for x in a:
            if callfcn(state, scope, fcn, [x]):
                out.append(x)
            else:
                break
        return out
provide(TakeWhile())

class DropWhile(LibFcn):
    name = prefix + "dropWhile"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 15720
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        i = 0
        for ai in a:
            if callfcn(state, scope, fcn, [ai]):
                i += 1
            else:
                break
        return a[i:]
provide(DropWhile())

#################################################################### functional tests

class Any(LibFcn):
    name = prefix + "any"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Boolean())
    errcodeBase = 15730
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        return any(callfcn(state, scope, fcn, [x]) for x in a)
provide(Any())

class All(LibFcn):
    name = prefix + "all"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Boolean())
    errcodeBase = 15740
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        return all(callfcn(state, scope, fcn, [x]) for x in a)
provide(All())

class Corresponds(LibFcn):
    name = prefix + "corresponds"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Boolean())}], P.Boolean())
    errcodeBase = 15750
    def __call__(self, state, scope, pos, paramTypes, a, b, fcn):
        return len(a) == len(b) and all(callfcn(state, scope, fcn, [x, y]) for x, y in zip(a, b))
provide(Corresponds())

class CorrespondsWithIndex(LibFcn):
    name = prefix + "correspondsWithIndex"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"b": P.Array(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Int(), P.Wildcard("A"), P.Wildcard("B")], P.Boolean())}], P.Boolean())
    errcodeBase = 15760
    def __call__(self, state, scope, pos, paramTypes, a, b, fcn):
        return len(a) == len(b) and all(callfcn(state, scope, fcn, [i, x, y]) for i, (x, y) in enumerate(zip(a, b)))
provide(CorrespondsWithIndex())

#################################################################### restructuring

class SlidingWindow(LibFcn):
    name = prefix + "slidingWindow"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"size": P.Int()}, {"step": P.Int()}], P.Array(P.Array(P.Wildcard("A"))))
    errcodeBase = 15770
    def __call__(self, state, scope, pos, paramTypes, a, size, step):
        if size < 1:
            raise PFARuntimeException("size < 1", self.errcodeBase + 0, self.name, pos)
        elif step < 1:
            raise PFARuntimeException("step < 1", self.errcodeBase + 1, self.name, pos)
        out = []
        i = 0
        for start in xrange(0, len(a), step):
            chunk = a[start:(start + size)]
            if len(chunk) == size:
                out.append(chunk)
                i += 1
                if i % 1000 == 0:
                    state.checkTime()
        return out
provide(SlidingWindow())

class Combinations(LibFcn):
    name = prefix + "combinations"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"size": P.Int()}], P.Array(P.Array(P.Wildcard("A"))))
    errcodeBase = 15780
    def __call__(self, state, scope, pos, paramTypes, a, size):
        if size < 1:
            raise PFARuntimeException("size < 1", self.errcodeBase + 0, self.name, pos)
        out = []
        i = 0
        for combination in itertools.combinations(a, size):
            i += 1
            if i % 1000 == 0:
                state.checkTime()
            out.append(list(combination))
        return out
provide(Combinations())

class Permutations(LibFcn):
    name = prefix + "permutations"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Array(P.Array(P.Wildcard("A"))))
    errcodeBase = 15790
    def __call__(self, state, scope, pos, paramTypes, a):
        out = []
        i = 0
        for permutation in itertools.permutations(a):
            i += 1
            if i % 1000 == 0:
                state.checkTime()
            out.append(list(permutation))
        return out
provide(Permutations())

class Flatten(LibFcn):
    name = prefix + "flatten"
    sig = Sig([{"a": P.Array(P.Array(P.Wildcard("A")))}], P.Array(P.Wildcard("A")))
    errcodeBase = 15800
    def __call__(self, state, scope, pos, paramTypes, a):
        return titus.util.flatten(a)
provide(Flatten())

class GroupBy(LibFcn):
    name = prefix + "groupby"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.String())}], P.Map(P.Array(P.Wildcard("A"))))
    errcodeBase = 15810
    def __call__(self, state, scope, pos, paramTypes, a, fcn):
        out = {}
        for x in a:
            key = callfcn(state, scope, fcn, [x])
            if key not in out:
                out[key] = []
            out[key].append(x)
        return out
provide(GroupBy())

class Logsumexp(LibFcn):
    name = prefix + "logsumexp"
    sig = Sig([{"a": P.Array(P.Double())}], P.Double())
    errcodeBase = 15480
    def __call__(self, state, scope, pos, paramTypes, datum):
        if len(datum) == 0:
            return float("nan")
        else:
            dmax = max(datum)
            res = 0.0
            for v in datum:
                res += math.exp(v - dmax)
            return math.log(res) + dmax
provide(Logsumexp())
