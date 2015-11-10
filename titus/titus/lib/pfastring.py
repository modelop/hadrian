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

import re

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.signature import Lifespan
from titus.signature import PFAVersion
from titus.datatype import *
from titus.errors import PFARuntimeException
from titus.util import callfcn, negativeIndex, startEnd
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "s."

#################################################################### basic access

class Len(LibFcn):
    name = prefix + "len"
    sig = Sig([{"s": P.String()}], P.Int())
    errcodeBase = 39000
    def genpy(self, paramTypes, args, pos):
        return "len({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, s):
        return len(s)
provide(Len())

class Substr(LibFcn):
    name = prefix + "substr"
    sig = Sig([{"s": P.String()}, {"start": P.Int()}, {"end": P.Int()}], P.String())
    errcodeBase = 39010
    def __call__(self, state, scope, pos, paramTypes, s, start, end):
        return s[start:end]
provide(Substr())

class SubstrTo(LibFcn):
    name = prefix + "substrto"
    sig = Sig([{"s": P.String()}, {"start": P.Int()}, {"end": P.Int()}, {"replacement": P.String()}], P.String())
    errcodeBase = 39020
    def __call__(self, state, scope, pos, paramTypes, s, start, end, replacement):
        normStart, normEnd = startEnd(len(s), start, end)
        before = s[:normStart]
        after = s[normEnd:]
        return before + replacement + after
provide(SubstrTo())

#################################################################### searching

class Contains(LibFcn):
    name = prefix + "contains"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Boolean())
    errcodeBase = 39030
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        return needle in haystack
provide(Contains())

class Count(LibFcn):
    name = prefix + "count"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Int())
    errcodeBase = 39040
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        if len(haystack) == 0 or len(needle) == 0:
            return 0
        else:
            return haystack.count(needle)
provide(Count())

class Index(LibFcn):
    name = prefix + "index"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Int())
    errcodeBase = 39050
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        try:
            return haystack.index(needle)
        except ValueError:
            return -1
provide(Index())

class RIndex(LibFcn):
    name = prefix + "rindex"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Int())
    errcodeBase = 39060
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        try:
            return haystack.rindex(needle)
        except ValueError:
            return -1
provide(RIndex())

class StartsWith(LibFcn):
    name = prefix + "startswith"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Boolean())
    errcodeBase = 39070
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        return haystack.startswith(needle)
provide(StartsWith())

class EndsWith(LibFcn):
    name = prefix + "endswith"
    sig = Sig([{"haystack": P.String()}, {"needle": P.String()}], P.Boolean())
    errcodeBase = 39080
    def __call__(self, state, scope, pos, paramTypes, haystack, needle):
        return haystack.endswith(needle)
provide(EndsWith())

#################################################################### conversions to/from other types

class Join(LibFcn):
    name = prefix + "join"
    sig = Sig([{"array": P.Array(P.String())}, {"sep": P.String()}], P.String())
    errcodeBase = 39090
    def __call__(self, state, scope, pos, paramTypes, array, sep):
        return sep.join(array)
provide(Join())

class Split(LibFcn):
    name = prefix + "split"
    sig = Sig([{"s": P.String()}, {"sep": P.String()}], P.Array(P.String()))
    errcodeBase = 39100
    def __call__(self, state, scope, pos, paramTypes, s, sep):
        if len(sep) == 0:
            return []
        else:
            return s.split(sep)
provide(Split())

class Hex(LibFcn):
    name = prefix + "hex"
    sig = Sigs([Sig([{"x": P.Long()}], P.String()),
                Sig([{"x": P.Long()}, {"width": P.Int()}, {"zeroPad": P.Boolean()}], P.String())])
    errcodeBase = 39110
    def __call__(self, state, scope, pos, paramTypes, *args):
        if len(args) == 1:
            x, = args
            if x < 0:
                raise PFARuntimeException("negative number", self.errcodeBase + 1, self.name, pos)
            else:
                return "{0:x}".format(x)

        else:
            x, width, zeroPad = args
            if x < 0:
                raise PFARuntimeException("negative number", self.errcodeBase + 1, self.name, pos)
            if not zeroPad:
                if width < 0:
                    formatStr = "{0:<" + str(-width) + "x}"
                elif width == 0:
                    formatStr = ""
                else:
                    formatStr = "{0:" + str(width) + "x}"
            else:
                if width < 0:
                    raise PFARuntimeException("negative width cannot be used with zero-padding", self.errcodeBase + 0, self.name, pos)
                elif width == 0:
                    formatStr = ""
                else:
                    formatStr = "{0:0" + str(width) + "x}"
            return formatStr.format(x)
provide(Hex())
        
class StringInt(LibFcn):
    name = prefix + "int"
    sig = Sigs([Sig([{"x": P.Long()}], P.String()),
                Sig([{"x": P.Long()}, {"width": P.Int()}, {"zeroPad": P.Boolean()}], P.String())])
    errcodeBase = 39240
    def __call__(self, state, scope, pos, paramTypes, *args):
        if len(args) == 1:
            x, = args
            return "{0:d}".format(x)

        elif len(args) == 3:
            x, width, zeroPad = args
            if not zeroPad:
                if width < 0:
                    formatStr = "{0:<" + str(-width) + "d}"
                elif width == 0:
                    formatStr = ""
                else:
                    formatStr = "{0:" + str(width) + "d}"
            else:
                if width < 0:
                    raise PFARuntimeException("negative width cannot be used with zero-padding", self.errcodeBase + 0, self.name, pos)
                elif width == 0:
                    formatStr = ""
                else:
                    formatStr = "{0:0" + str(width) + "d}"
            return formatStr.format(x)
provide(StringInt())

class Number(LibFcn):
    name = prefix + "number"
    sig = Sigs([Sig([{"x": P.Long()}], P.String(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use s.int for integers")),
                Sig([{"x": P.Long()}, {"width": P.Int()}, {"zeroPad": P.Boolean()}], P.String(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use s.int for integers")),
                Sig([{"x": P.Double()}, {"width": P.Union([P.Int(), P.Null()])}, {"precision": P.Union([P.Int(), P.Null()])}], P.String()),
                Sig([{"x": P.Double()}, {"width": P.Union([P.Int(), P.Null()])}, {"precision": P.Union([P.Int(), P.Null()])}, {"minNoExp": P.Double()}, {"maxNoExp": P.Double()}], P.String())])
    errcodeBase = 39120
    def __call__(self, state, scope, pos, paramTypes, *args):
        if len(args) == 1 and paramTypes[0] in ("int", "long"):
            x, = args
            return "{0:d}".format(x)

        elif len(args) == 3 and paramTypes[0] in ("int", "long"):
            x, width, zeroPad = args
            if not zeroPad:
                if width < 0:
                    formatStr = "{0:<" + str(-width) + "d}"
                elif width == 0:
                    formatStr = ""
                else:
                    formatStr = "{0:" + str(width) + "d}"
            else:
                if width < 0:
                    raise PFARuntimeException("negative width cannot be used with zero-padding", self.errcodeBase + 0, self.name, pos)
                elif width == 0:
                    formatStr = ""
                else:
                    formatStr = "{0:0" + str(width) + "d}"
            return formatStr.format(x)

        else:
            if len(args) == 3:
                x, width, precision = args
                minNoExp, maxNoExp = 0.0001, 100000
            else:
                x, width, precision, minNoExp, maxNoExp = args

            if width is None:
                widthStr = ""
            else:
                if isinstance(width, dict):
                    width = width["int"]
                if width < 0:
                    widthStr = "<" + str(-width)
                else:
                    widthStr = str(width)

            if precision is None:
                precisionStr = ".6"
            else:
                if isinstance(precision, dict):
                    precision = precision["int"]
                if precision < 0:
                    raise PFARuntimeException("negative precision", self.errcodeBase + 1, self.name, pos)
                else:
                    precisionStr = "." + str(precision)

            if x == 0.0: x = 0.0   # drop sign bit from zero

            v = abs(x)
            if v == 0.0 or (v >= minNoExp and v <= maxNoExp):
                conv = "f"
            else:
                conv = "e"

            formatStr = "{0:" + widthStr + precisionStr + conv + "}"
            result = formatStr.format(x)

            if precision is None:
                m = re.search("\.[0-9]+?(0+) *$", result)
                if m is None:
                    m = re.search("\.[0-9]+?(0+)e", result)
                if m is not None:
                    start, end = m.regs[1]
                    numChanged = end - start
                    result = result[:start] + result[end:]
                    if width is not None:
                        if width < 0:
                            actual, target = len(result), -width
                            if actual < target:
                                result = result + (" " * (target - actual))
                        elif width == 0:
                            result = ""
                        else:
                            actual, target = len(result), width
                            if actual < target:
                                result = (" " * (target - actual)) + result

            return result

provide(Number())

#################################################################### conversions to/from other strings

class Concat(LibFcn):
    name = prefix + "concat"
    sig = Sig([{"x": P.String()}, {"y": P.String()}], P.String())
    errcodeBase = 39130
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return x + y
provide(Concat())

class Repeat(LibFcn):
    name = prefix + "repeat"
    sig = Sig([{"s": P.String()}, {"n": P.Int()}], P.String())
    errcodeBase = 39140
    def __call__(self, state, scope, pos, paramTypes, s, n):
        return s * n
provide(Repeat())

class Lower(LibFcn):
    name = prefix + "lower"
    sig = Sig([{"s": P.String()}], P.String())
    errcodeBase = 39150
    def __call__(self, state, scope, pos, paramTypes, s):
        return s.lower()
provide(Lower())

class Upper(LibFcn):
    name = prefix + "upper"
    sig = Sig([{"s": P.String()}], P.String())
    errcodeBase = 39160
    def __call__(self, state, scope, pos, paramTypes, s):
        return s.upper()
provide(Upper())

class LStrip(LibFcn):
    name = prefix + "lstrip"
    sig = Sig([{"s": P.String()}, {"chars": P.String()}], P.String())
    errcodeBase = 39170
    def __call__(self, state, scope, pos, paramTypes, s, chars):
        return s.lstrip(chars)
provide(LStrip())

class RStrip(LibFcn):
    name = prefix + "rstrip"
    sig = Sig([{"s": P.String()}, {"chars": P.String()}], P.String())
    errcodeBase = 39180
    def __call__(self, state, scope, pos, paramTypes, s, chars):
        return s.rstrip(chars)
provide(RStrip())

class Strip(LibFcn):
    name = prefix + "strip"
    sig = Sig([{"s": P.String()}, {"chars": P.String()}], P.String())
    errcodeBase = 39190
    def __call__(self, state, scope, pos, paramTypes, s, chars):
        return s.strip(chars)
provide(Strip())

class ReplaceAll(LibFcn):
    name = prefix + "replaceall"
    sig = Sig([{"s": P.String()}, {"original": P.String()}, {"replacement": P.String()}], P.String())
    errcodeBase = 39200
    def __call__(self, state, scope, pos, paramTypes, s, original, replacement):
        return s.replace(original, replacement)
provide(ReplaceAll())

class ReplaceFirst(LibFcn):
    name = prefix + "replacefirst"
    sig = Sig([{"s": P.String()}, {"original": P.String()}, {"replacement": P.String()}], P.String())
    errcodeBase = 39210
    def __call__(self, state, scope, pos, paramTypes, s, original, replacement):
        if len(s) > 0:
            return s.replace(original, replacement, 1)
        else:
            return s.replace(original, replacement)
provide(ReplaceFirst())

class ReplaceLast(LibFcn):
    name = prefix + "replacelast"
    sig = Sig([{"s": P.String()}, {"original": P.String()}, {"replacement": P.String()}], P.String())
    errcodeBase = 39220
    def __call__(self, state, scope, pos, paramTypes, s, original, replacement):
        backward = "".join(reversed(s))
        if len(s) > 0:
            replaced = backward.replace("".join(reversed(original)), "".join(reversed(replacement)), 1)
        else:
            replaced = backward.replace("".join(reversed(original)), "".join(reversed(replacement)))
        return "".join(reversed(replaced))
provide(ReplaceLast())

class Translate(LibFcn):
    name = prefix + "translate"
    sig = Sig([{"s": P.String()}, {"oldchars": P.String()}, {"newchars": P.String()}], P.String())
    errcodeBase = 39230
    def __call__(self, state, scope, pos, paramTypes, s, oldchars, newchars):
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
