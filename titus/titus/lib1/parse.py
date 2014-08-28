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

prefix = "parse."

#################################################################### functions

class ParseInt(LibFcn):
    name = prefix + "int"
    sig = Sig([{"str": P.String()}, {"base": P.Int()}], P.Int())
    def __call__(self, state, scope, paramTypes, str, base):
        if base < 2 or base > 36:
            raise PFARuntimeException("base out of range")
        out = int(str)
        if out < INT_MIN_VALUE or out > INT_MAX_VALUE:
            raise PFARuntimeException("not an integer")
        else:
            return out
provide(ParseInt())

class ParseLong(LibFcn):
    name = prefix + "long"
    sig = Sig([{"str": P.String()}, {"base": P.Int()}], P.Long())
    def __call__(self, state, scope, paramTypes, str, base):
        if base < 2 or base > 36:
            raise PFARuntimeException("base out of range")
        out = int(str)
        if out < LONG_MIN_VALUE or out > LONG_MAX_VALUE:
            raise PFARuntimeException("not a long integer")
        else:
            return out
provide(ParseLong())

class ParseFloat(LibFcn):
    name = prefix + "float"
    sig = Sig([{"str": P.String()}], P.Float())
    def __call__(self, state, scope, paramTypes, str):
        out = float(str)
        if math.isnan(out):
            return out
        elif math.isinf(out):
            return out
        elif out > FLOAT_MAX_VALUE:
            return float("inf")
        elif -out > FLOAT_MAX_VALUE:
            return float("-inf")
        elif abs(out) < FLOAT_MIN_VALUE:
            return 0.0
        else:
            return out
provide(ParseFloat())

class ParseDouble(LibFcn):
    name = prefix + "double"
    sig = Sig([{"str": P.String()}], P.Double())
    def __call__(self, state, scope, paramTypes, str):
        out = float(str)
        if math.isnan(out):
            return out
        elif math.isinf(out):
            return out
        elif out > DOUBLE_MAX_VALUE:
            return float("inf")
        elif -out > DOUBLE_MAX_VALUE:
            return float("-inf")
        elif abs(out) < DOUBLE_MIN_VALUE:
            return 0.0
        else:
            return out
provide(ParseDouble())
