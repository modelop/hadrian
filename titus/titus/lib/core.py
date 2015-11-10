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

from titus.errors import PFARuntimeException
from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.util import div
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

anyNumber = set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()])

INT_MIN_VALUE = -2147483648
INT_MAX_VALUE = 2147483647
LONG_MIN_VALUE = -9223372036854775808
LONG_MAX_VALUE = 9223372036854775807
FLOAT_MIN_VALUE = 1.4e-45
FLOAT_MAX_VALUE = 3.4028235e38
DOUBLE_MIN_VALUE = 4.9e-324
DOUBLE_MAX_VALUE = 1.7976931348623157e308

def checkForOverflow(paramType, out, code1, code2, fcnName, pos):
    if paramType == "int":
        if math.isnan(out) or out < INT_MIN_VALUE or out > INT_MAX_VALUE:
            raise PFARuntimeException("int overflow", code1, fcnName, pos)
        else:
            return out
    elif paramType == "long":
        if math.isnan(out) or out < LONG_MIN_VALUE or out > LONG_MAX_VALUE:
            raise PFARuntimeException("long overflow", code2, fcnName, pos)
        else:
            return out
    else:
        return out
    
#################################################################### basic arithmetic

class Plus(LibFcn):
    name = "+"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y" : P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18000
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return checkForOverflow(paramTypes[0], x + y, self.errcodeBase + 0, self.errcodeBase + 1, self.name, pos)
provide(Plus())

class Minus(LibFcn):
    name = "-"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18010
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return checkForOverflow(paramTypes[0], x - y, self.errcodeBase + 0, self.errcodeBase + 1, self.name, pos)
provide(Minus())

class Times(LibFcn):
    name = "*"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18020
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return checkForOverflow(paramTypes[0], x * y, self.errcodeBase + 0, self.errcodeBase + 1, self.name, pos)
provide(Times())

class Divide(LibFcn):
    name = "/"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}], P.Double())
    errcodeBase = 18030
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return div(x, y)
provide(Divide())

class FloorDivide(LibFcn):
    name = "//"
    sig = Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong()]))}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18040
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if y == 0:
            raise PFARuntimeException("integer division by zero", self.errcodeBase + 0, self.name, pos)
        else:
            return int(x / float(y))
provide(FloorDivide())

class Negative(LibFcn):
    name = "u-"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}], P.Wildcard("A"))
    errcodeBase = 18050
    def __call__(self, state, scope, pos, paramTypes, x):
        return checkForOverflow(paramTypes[0], -x, self.errcodeBase + 0, self.errcodeBase + 1, self.name, pos)
provide(Negative())

class Modulo(LibFcn):
    name = "%"
    sig = Sig([{"k": P.Wildcard("A", anyNumber)}, {"n": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18060
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if y == 0:
            if paramTypes[-1] == "int" or paramTypes[-1] == "long":
                raise PFARuntimeException("integer division by zero", self.errcodeBase + 0, self.name, pos)
            else:
                return float("nan")
        else:
            return x % y
provide(Modulo())

class Remainder(LibFcn):
    name = "%%"
    sig = Sig([{"k": P.Wildcard("A", anyNumber)}, {"n": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18070
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if y == 0:
            if paramTypes[-1] == "int" or paramTypes[-1] == "long":
                raise PFARuntimeException("integer division by zero", self.errcodeBase + 0, self.name, pos)
            else:
                return float("nan")
        else:
            if not math.isnan(x) and not math.isinf(x) and math.isinf(y):
                return x
            else:
                out = x % y
                if x < 0 and out > 0:
                    return out - abs(y)
                elif x > 0 and out < 0:
                    return out + abs(y)
                else:
                    return out
provide(Remainder())

def powLikeJava(x, y):
    if math.isnan(x) and y == 0:
        return 1.0
    elif math.isnan(x) or math.isnan(y):
        return float("nan")
    elif x == 0 and y < 0:
        return float("inf")
    elif math.isinf(y):
        if x == 1 or x == -1:
            return float("nan")
        elif abs(x) < 1:
            if y > 0:
                return 0.0
            else:
                return float("inf")
        else:
            if y > 0:
                return float("inf")
            else:
                return 0.0
    elif math.isinf(x):
        if y == 0:
            return 1.0
        elif y < 0:
            return 0.0
        else:
            if x < 0 and round(y) == y and y % 2 == 1:
                return float("-inf")
            else:
                return float("inf")
    elif x < 0 and round(y) != y:
        return float("nan")
    else:
        try:
            return math.pow(x, y)
        except OverflowError:
            if abs(y) < 1:
                if x < 0:
                    return float("nan")
                else:
                    return 1.0
            else:
                if (abs(x) > 1 and y < 0) or (abs(x) < 1 and y > 0):
                    return 0.0
                else:
                    return float("inf")

class Pow(LibFcn):
    name = "**"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18080
    def __call__(self, state, scope, pos, paramTypes, x, y):
        out = powLikeJava(x, y)
        if paramTypes[-1] == "int":
            if math.isnan(out) or out < INT_MIN_VALUE or out > INT_MAX_VALUE:
                raise PFARuntimeException("int overflow", self.errcodeBase + 0, self.name, pos)
            else:
                return int(out)
        elif paramTypes[-1] == "long":
            if math.isnan(out) or out < LONG_MIN_VALUE or out > LONG_MAX_VALUE:
                raise PFARuntimeException("long overflow", self.errcodeBase + 1, self.name, pos)
            else:
                return int(out)
        else:
            return out
provide(Pow())

#################################################################### generic comparison operators

class Comparison(LibFcn):
    name = "cmp"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Int())
    errcodeBase = 18090
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y)
provide(Comparison())

class Equal(LibFcn):
    name = "=="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 18100
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y) == 0
provide(Equal())

class GreaterOrEqual(LibFcn):
    name = ">="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 18110
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y) >= 0
provide(GreaterOrEqual())

class GreaterThan(LibFcn):
    name = ">"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 18120
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y) > 0
provide(GreaterThan())

class NotEqual(LibFcn):
    name = "!="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 18130
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y) != 0
provide(NotEqual())

class LessThan(LibFcn):
    name = "<"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 18140
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y) < 0
provide(LessThan())

class LessOrEqual(LibFcn):
    name = "<="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 18150
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return compare(jsonNodeToAvroType(paramTypes[0]), x, y) <= 0
provide(LessOrEqual())

#################################################################### max and min

class Max(LibFcn):
    name = "max"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18160
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if compare(jsonNodeToAvroType(paramTypes[0]), x, y) >= 0:
            return x
        else:
            return y
provide(Max())

class Min(LibFcn):
    name = "min"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 18170
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if compare(jsonNodeToAvroType(paramTypes[0]), x, y) < 0:
            return x
        else:
            return y
provide(Min())

#################################################################### logical operators

class LogicalAnd(LibFcn):
    name = "&&"
    sig = Sig([{"x": P.Boolean()}, {"y": P.Boolean()}], P.Boolean())
    errcodeBase = 18180
    def genpy(self, paramTypes, args, pos):
        return "({0} and {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return x and y
provide(LogicalAnd())

class LogicalOr(LibFcn):
    name = "||"
    sig = Sig([{"x": P.Boolean()}, {"y": P.Boolean()}], P.Boolean())
    errcodeBase = 18190
    def genpy(self, paramTypes, args, pos):
        return "({0} or {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return x or y
provide(LogicalOr())

class LogicalXOr(LibFcn):
    name = "^^"
    sig = Sig([{"x": P.Boolean()}, {"y": P.Boolean()}], P.Boolean())
    errcodeBase = 18200
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return (x or y) and not (x and y)
provide(LogicalXOr())

class LogicalNot(LibFcn):
    name = "!"
    sig = Sig([{"x": P.Boolean()}], P.Boolean())
    errcodeBase = 18210
    def genpy(self, paramTypes, args, pos):
        return "(not {0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return not x
provide(LogicalNot())

#################################################################### three-valued arithmetic

class KleeneAnd(LibFcn):
    name = "&&&"
    sig = Sig([{"x": P.Union([P.Boolean(), P.Null()])}, {"y": P.Union([P.Boolean(), P.Null()])}], P.Union([P.Boolean(), P.Null()]))
    errcodeBase = 18220
    def genpy(self, paramTypes, args, pos):
        return """self.f["&&&"](state, scope, {0}, {1}, lambda: ({2}), lambda: ({3}))""".format(*([repr(pos), repr(paramTypes)] + args))
    def __call__(self, state, scope, pos, paramTypes, x, y):
        xval = x()
        if xval is False or xval == {"boolean": False}:
            return {"boolean": False}
        yval = y()
        if yval is False or yval == {"boolean": False}:
            return {"boolean": False}
        elif (xval is True or xval == {"boolean": True}) and (yval is True or yval == {"boolean": True}):
            return {"boolean": True}
        else:
            return None
provide(KleeneAnd())

class KleeneOr(LibFcn):
    name = "|||"
    sig = Sig([{"x": P.Union([P.Boolean(), P.Null()])}, {"y": P.Union([P.Boolean(), P.Null()])}], P.Union([P.Boolean(), P.Null()]))
    errcodeBase = 18230
    def genpy(self, paramTypes, args, pos):
        return """self.f["|||"](state, scope, {0}, {1}, lambda: ({2}), lambda: ({3}))""".format(*([repr(pos), repr(paramTypes)] + args))
    def __call__(self, state, scope, pos, paramTypes, x, y):
        xval = x()
        if xval is True or xval == {"boolean": True}:
            return {"boolean": True}
        yval = y()
        if yval is True or yval == {"boolean": True}:
            return {"boolean": True}
        elif (xval is False or xval == {"boolean": False}) and (yval is False or yval == {"boolean": False}):
            return {"boolean": False}
        else:
            return None
provide(KleeneOr())

class KleeneNot(LibFcn):
    name = "!!!"
    sig = Sig([{"x": P.Union([P.Boolean(), P.Null()])}], P.Union([P.Boolean(), P.Null()]))
    errcodeBase = 18240
    def __call__(self, state, scope, pos, paramTypes, x):
        if x is True or x == {"boolean": True}:
            return {"boolean": False}
        elif x is False or x == {"boolean": False}:
            return {"boolean": True}
        else:
            return None
provide(KleeneNot())

#################################################################### bitwise arithmetic

class BitwiseAnd(LibFcn):
    name = "&"
    sig = Sigs([Sig([{"x": P.Int()}, {"y": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}, {"y": P.Long()}], P.Long())])
    errcodeBase = 18250
    def genpy(self, paramTypes, args, pos):
        return "({0} & {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return x & y
provide(BitwiseAnd())

class BitwiseOr(LibFcn):
    name = "|"
    sig = Sigs([Sig([{"x": P.Int()}, {"y": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}, {"y": P.Long()}], P.Long())])

    errcodeBase = 18260
    def genpy(self, paramTypes, args, pos):
        return "({0} | {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return x | y
provide(BitwiseOr())

class BitwiseXOr(LibFcn):
    name = "^"
    sig = Sigs([Sig([{"x": P.Int()}, {"y": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}, {"y": P.Long()}], P.Long())])
    errcodeBase = 18270
    def genpy(self, paramTypes, args, pos):
        return "({0} ^ {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return x ^ y
provide(BitwiseXOr())

class BitwiseNot(LibFcn):
    name = "~"
    sig = Sigs([Sig([{"x": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}], P.Long())])
    errcodeBase = 18280
    def genpy(self, paramTypes, args, pos):
        return "(~{0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return ~x
provide(BitwiseNot())
