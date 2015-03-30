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
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "impute."

class ErrorOnNull(LibFcn):
    name = prefix + "errorOnNull"
    sig = Sig([{"x": P.Union([P.Wildcard("A"), P.Null()])}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x):
        if x is None:
            raise PFARuntimeException("encountered null")
        else:
            if isinstance(paramTypes[-1], (list, tuple)):
                return x
            else:
                return x.values()[0]
provide(ErrorOnNull())

class DefaultOnNull(LibFcn):
    name = prefix + "defaultOnNull"
    sig = Sig([{"x": P.Union([P.Wildcard("A"), P.Null()])}, {"default": P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, default):
        if x is None:
            return default
        else:
            if isinstance(paramTypes[-1], (list, tuple)):
                return x
            else:
                return x.values()[0]
provide(DefaultOnNull())

class IsNan(LibFcn):
    name = prefix + "isnan"
    sig = Sigs([Sig([{"x": P.Float()}], P.Boolean()),
                Sig([{"x": P.Double()}], P.Boolean())])
    def __call__(self, state, scope, paramTypes, x):
        return math.isnan(x)
provide(IsNan())

class IsInf(LibFcn):
    name = prefix + "isinf"
    sig = Sigs([Sig([{"x": P.Float()}], P.Boolean()),
                Sig([{"x": P.Double()}], P.Boolean())])
    def __call__(self, state, scope, paramTypes, x):
        return math.isinf(x)
provide(IsInf())

class IsNum(LibFcn):
    name = prefix + "isnum"
    sig = Sigs([Sig([{"x": P.Float()}], P.Boolean()),
                Sig([{"x": P.Double()}], P.Boolean())])
    def __call__(self, state, scope, paramTypes, x):
        return not math.isnan(x) and not math.isinf(x)
provide(IsNum())

class ErrorOnNonNum(LibFcn):
    name = prefix + "errorOnNonNum"
    sig = Sigs([Sig([{"x": P.Float()}], P.Float()),
                Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        if math.isnan(x):
            raise PFARuntimeException("encountered nan")
        elif math.isinf(x):
            if x > 0.0:
                raise PFARuntimeException("encountered +inf")
            else:
                raise PFARuntimeException("encountered -inf")
        else:
            return x
provide(ErrorOnNonNum())

class DefaultOnNonNum(LibFcn):
    name = prefix + "defaultOnNonNum"
    sig = Sigs([Sig([{"x": P.Float()}, {"default": P.Float()}], P.Float()),
                Sig([{"x": P.Double()}, {"default": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x, default):
        if math.isnan(x) or math.isinf(x):
            return default
        else:
            return x
provide(DefaultOnNonNum())
