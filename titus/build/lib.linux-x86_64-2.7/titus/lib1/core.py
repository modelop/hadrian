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
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

anyNumber = set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()])

INT_MIN_VALUE = -2147483648
INT_MAX_VALUE = 2147483647
LONG_MIN_VALUE = -9223372036854775808
LONG_MAX_VALUE = 9223372036854775807

def checkForOverflow(paramTypes, out):
    if paramTypes == ["int", "int"] or paramTypes == ["int"]:
        if out < INT_MIN_VALUE or out > INT_MAX_VALUE:
            raise PFARuntimeException("int overflow")
        else:
            return out
    elif paramTypes == ["long", "long"] or paramTypes == ["long"]:
        if out < LONG_MIN_VALUE or out > LONG_MAX_VALUE:
            raise PFARuntimeException("long overflow")
        else:
            return out
    else:
        return out
    
#################################################################### basic arithmetic

class Plus(LibFcn):
    name = "+"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y" : P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, y):
        return checkForOverflow(paramTypes, x + y)
provide(Plus())

class Minus(LibFcn):
    name = "-"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, y):
        return checkForOverflow(paramTypes, x - y)
provide(Minus())

class Times(LibFcn):
    name = "*"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, y):
        return checkForOverflow(paramTypes, x * y)
provide(Times())

class Divide(LibFcn):
    name = "/"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}], P.Double())
    def genpy(self, paramTypes, args):
        return "({} / float({}))".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x / float(y)
provide(Divide())

class FloorDivide(LibFcn):
    name = "//"
    sig = Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong()]))}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    def genpy(self, paramTypes, args):
        return "({} // {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x // y
provide(FloorDivide())

class Negative(LibFcn):
    name = "u-"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x):
        return checkForOverflow(paramTypes, -x)
provide(Negative())

class Modulo(LibFcn):
    name = "%"
    sig = Sig([{"k": P.Wildcard("A", anyNumber)}, {"n": P.Wildcard("A")}], P.Wildcard("A"))
    def genpy(self, paramTypes, args):
        return "({} % {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x % y
provide(Modulo())

class Remainder(LibFcn):
    name = "%%"
    sig = Sig([{"k": P.Wildcard("A", anyNumber)}, {"n": P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, y):
        q = math.trunc(x / y)
        return math.copysign(x - y * q, x)
provide(Remainder())

class Pow(LibFcn):
    name = "**"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, y):
        return checkForOverflow(paramTypes, x**y)
provide(Pow())

#################################################################### generic comparison operators

class Comparison(LibFcn):
    name = "cmp"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Int())
    def genpy(self, paramTypes, args):
        return "cmp({}, {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return cmp(x, y)
provide(Comparison())

class Equal(LibFcn):
    name = "=="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} == {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x == y
provide(Equal())

class GreaterOrEqual(LibFcn):
    name = ">="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} >= {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x >= y
provide(GreaterOrEqual())

class GreaterThan(LibFcn):
    name = ">"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} > {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x > y
provide(GreaterThan())

class NotEqual(LibFcn):
    name = "!="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} != {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x != y
provide(NotEqual())

class LessThan(LibFcn):
    name = "<"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} < {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x < y
provide(LessThan())

class LessOrEqual(LibFcn):
    name = "<="
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} <= {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x <= y
provide(LessOrEqual())

#################################################################### max and min

class Max(LibFcn):
    name = "max"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    def genpy(self, paramTypes, args):
        return "max({}, {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return max(x, y)
provide(Max())

class Min(LibFcn):
    name = "min"
    sig = Sig([{"x": P.Wildcard("A")}, {"y": P.Wildcard("A")}], P.Wildcard("A"))
    def genpy(self, paramTypes, args):
        return "min({}, {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return min(x, y)
provide(Min())

#################################################################### logical operators

class LogicalAnd(LibFcn):
    name = "&&"
    sig = Sig([{"x": P.Boolean()}, {"y": P.Boolean()}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} and {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x and y
provide(LogicalAnd())

class LogicalOr(LibFcn):
    name = "||"
    sig = Sig([{"x": P.Boolean()}, {"y": P.Boolean()}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "({} or {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x or y
provide(LogicalOr())

class LogicalXOr(LibFcn):
    name = "^^"
    sig = Sig([{"x": P.Boolean()}, {"y": P.Boolean()}], P.Boolean())
    def __call__(self, state, scope, paramTypes, x, y):
        return (x or y) and not (x and y)
provide(LogicalXOr())

class LogicalNot(LibFcn):
    name = "!"
    sig = Sig([{"x": P.Boolean()}], P.Boolean())
    def genpy(self, paramTypes, args):
        return "(not {})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
        return not x
provide(LogicalNot())

#################################################################### bitwise arithmetic

class BitwiseAnd(LibFcn):
    name = "&"
    sig = Sigs([Sig([{"x": P.Int()}, {"y": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}, {"y": P.Long()}], P.Long())])
    def genpy(self, paramTypes, args):
        return "({} & {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x & y
provide(BitwiseAnd())

class BitwiseOr(LibFcn):
    name = "|"
    sig = Sigs([Sig([{"x": P.Int()}, {"y": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}, {"y": P.Long()}], P.Long())])

    def genpy(self, paramTypes, args):
        return "({} | {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x | y
provide(BitwiseOr())

class BitwiseXOr(LibFcn):
    name = "^"
    sig = Sigs([Sig([{"x": P.Int()}, {"y": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}, {"y": P.Long()}], P.Long())])
    def genpy(self, paramTypes, args):
        return "({} ^ {})".format(*args)
    def __call__(self, state, scope, paramTypes, x, y):
        return x ^ y
provide(BitwiseXOr())

class BitwiseNot(LibFcn):
    name = "~"
    sig = Sigs([Sig([{"x": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}], P.Long())])
    def genpy(self, paramTypes, args):
        return "(~{})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
        return ~x
provide(BitwiseNot())
