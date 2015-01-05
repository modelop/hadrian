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

#################################################################### functions

class ToInt(LibFcn):
    name = prefix + "int"
    sig = Sigs([Sig([{"x": P.Int()}], P.Int()),
                Sig([{"x": P.Long()}], P.Int()),
                Sig([{"x": P.Float()}], P.Int()),
                Sig([{"x": P.Double()}], P.Int())])
    def genpy(self, paramTypes, args):
        return "int({})".format(*args)
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
        return "int({})".format(*args)
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
        return "float({})".format(*args)
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
        return "float({})".format(*args)
    def __call__(self, state, scope, paramTypes, x):
        return float(x)
provide(ToDouble())

