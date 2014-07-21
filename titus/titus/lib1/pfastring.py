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
from titus.util import callfcn, negativeIndex, checkRange, startEnd
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "s."

#################################################################### basic access

class Len(LibFcn):
    name = prefix + "len"
    sig = Sig([{"s": P.String()}], P.Int())
    def genpy(self, paramTypes, args):
        return "len({})".format(*args)
    def __call__(self, state, scope, paramTypes, s):
        return len(s)
provide(Len())

class Substr(LibFcn):
    name = prefix + "substr"
    sig = Sig([{"s": P.String()}, {"start": P.Int()}, {"end": P.Int()}], P.String())
    def __call__(self, state, scope, paramTypes, s, start, end):
        return s[start:end]
provide(Substr())

class SubstrTo(LibFcn):
    name = prefix + "substrto"
    sig = Sig([{"s": P.String()}, {"start": P.Int()}, {"end": P.Int()}, {"replacement": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, start, end, replacement):
        normStart, normEnd = startEnd(len(s), start, end)
        before = s[:normStart]
        after = s[normEnd:]
        return before + replacement + after
provide(SubstrTo())

#################################################################### searching

class Contains(LibFcn):
    name = prefix + "contains"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Boolean())
    def __call__(self, state, scope, paramTypes, haystack, needle):
        return needle in haystack
provide(Contains())

class Count(LibFcn):
    name = prefix + "count"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Int())
    def __call__(self, state, scope, paramTypes, haystack, needle):
        return haystack.count(needle)
provide(Count())

class Index(LibFcn):
    name = prefix + "index"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Int())
    def __call__(self, state, scope, paramTypes, haystack, needle):
        try:
            return haystack.index(needle)
        except ValueError:
            return -1
provide(Index())

class RIndex(LibFcn):
    name = prefix + "rindex"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Int())
    def __call__(self, state, scope, paramTypes, haystack, needle):
        try:
            return haystack.rindex(needle)
        except ValueError:
            return -1
provide(RIndex())

class StartsWith(LibFcn):
    name = prefix + "startswith"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Boolean())
    def __call__(self, state, scope, paramTypes, haystack, needle):
        return haystack.startswith(needle)
provide(StartsWith())

class EndsWith(LibFcn):
    name = prefix + "endswith"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Boolean())
    def __call__(self, state, scope, paramTypes, haystack, needle):
        return haystack.endswith(needle)
provide(EndsWith())

#################################################################### conversions to/from other types

class Join(LibFcn):
    name = prefix + "join"
    sig = Sig([{"array": P.Array(P.String())}, {"sep": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, array, sep):
        return sep.join(array)
provide(Join())

class Split(LibFcn):
    name = prefix + "split"
    sig = Sig([{"s": P.String()}, {"sep": P.String()}], P.Array(P.String()))
    def __call__(self, state, scope, paramTypes, s, sep):
        return s.split(sep)
provide(Split())

#################################################################### conversions to/from other strings

class Concat(LibFcn):
    name = prefix + "concat"
    sig = Sig([{"x": P.String()}, {"y": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, x, y):
        return x + y
provide(Concat())

class Repeat(LibFcn):
    name = prefix + "repeat"
    sig = Sig([{"s": P.String()}, {"n": P.Int()}], P.String())
    def __call__(self, state, scope, paramTypes, s, n):
        return s * n
provide(Repeat())

class Lower(LibFcn):
    name = prefix + "lower"
    sig = Sig([{"s": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s):
        return s.lower()
provide(Lower())

class Upper(LibFcn):
    name = prefix + "upper"
    sig = Sig([{"s": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s):
        return s.upper()
provide(Upper())

class LStrip(LibFcn):
    name = prefix + "lstrip"
    sig = Sig([{"s": P.String()}, {"chars": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, chars):
        return s.lstrip(chars)
provide(LStrip())

class RStrip(LibFcn):
    name = prefix + "rstrip"
    sig = Sig([{"s": P.String()}, {"chars": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, chars):
        return s.rstrip(chars)
provide(RStrip())

class Strip(LibFcn):
    name = prefix + "strip"
    sig = Sig([{"s": P.String()}, {"chars": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, chars):
        return s.strip(chars)
provide(Strip())

class ReplaceAll(LibFcn):
    name = prefix + "replaceall"
    sig = Sig([{"s": P.String()}, {"original": P.String()}, {"replacement": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, original, replacement):
        return s.replace(original, replacement)
provide(ReplaceAll())

class ReplaceFirst(LibFcn):
    name = prefix + "replacefirst"
    sig = Sig([{"s": P.String()}, {"original": P.String()}, {"replacement": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, original, replacement):
        return s.replace(original, replacement, 1)
provide(ReplaceFirst())

class ReplaceLast(LibFcn):
    name = prefix + "replacelast"
    sig = Sig([{"s": P.String()}, {"original": P.String()}, {"replacement": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, original, replacement):
        backward = "".join(reversed(s))
        replaced = backward.replace("".join(reversed(original)), "".join(reversed(replacement)), 1)
        return "".join(reversed(replaced))
provide(ReplaceLast())

class Translate(LibFcn):
    name = prefix + "translate"
    sig = Sig([{"s": P.String()}, {"oldchars": P.String()}, {"newchars": P.String()}], P.String())
    def __call__(self, state, scope, paramTypes, s, oldchars, newchars):
        out = []
        for c in s:
            try:
                i = oldchars.index(c)
            except ValueError:
                out.append(c)
            else:
                if i < len(newchars):
                    out.append(newchars[i])
        return "".join(out)
provide(Translate())
