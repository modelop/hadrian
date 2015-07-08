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
from titus.util import callfcn, div
import titus.P as P
from titus.lib1.array import argLowestN
from titus.lib1.prob.dist import Chi2Distribution

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "m.link."

#################################################################### 

class SoftMax(LibFcn):
    name = prefix + "softmax"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double()))])
    def __call__(self, state, scope, paramTypes, x):
        if paramTypes[0]["type"] == "map":
            xx = x.copy()
            tmp = map(abs, xx.values())
            if xx.values()[tmp.index(max(tmp))] >= 0:
                m = max(xx.values())
            else:
                m = min(xx.values())
            denom = sum([math.exp(v - m) for v in x.values()])
            for key in x.keys():
                xx[key] = float(math.exp(xx[key] - m)/denom)
            return xx
        else:
            tmp = map(abs, x)
            if x[tmp.index(max(tmp))] >= 0:
                m = max(x)
            else:
                m = min(x)
            denom = sum([math.exp(v - m) for v in x])
            return [float(math.exp(val - m)/denom) for val in x]
provide(SoftMax())

def unwrapForNorm(x, func):
    if isinstance(x, dict):
        xx = x.copy()
        for key, val in zip(x.keys(), x.values()):
            xx[key] = float(func(val))
        return xx
    elif isinstance(x, (tuple, list)):
        xx = x[:]
        for i, val in enumerate(x):
            xx[i] = float(func(val))
        return xx
    else:
        return float(func(x))

class Logit(LibFcn):
    name = prefix + "logit"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: 1./(1. + math.exp(-y)))
provide(Logit())

class Probit(LibFcn):
    name = prefix + "probit"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: (math.erf(y/math.sqrt(2.)) + 1.)/2.)
provide(Probit())

class CLoglog(LibFcn):
    name = prefix + "cloglog"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: 1. - math.exp(-math.exp(y)))
provide(CLoglog())

class LogLog(LibFcn):
    name = prefix + "loglog"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: math.exp(-math.exp(y)))
provide(LogLog())

class Cauchit(LibFcn):
    name = prefix + "cauchit"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: 0.5 + (1./math.pi)*math.atan(y))
provide(Cauchit())

class Softplus(LibFcn):
    name = prefix + "softplus"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: math.log(1.0 + math.exp(y)))
provide(Softplus())

class ReLu(LibFcn):
    name = prefix + "relu"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: max(0,y))
provide(ReLu())

class Tanh(LibFcn):
    name = prefix + "tanh"
    sig  = Sigs([Sig([{"x": P.Array(P.Double())}], P.Array(P.Double())),
                 Sig([{"x": P.Map(P.Double())}], P.Map(P.Double())),
                 Sig([{"x": P.Double()}], P.Double())])
    def __call__(self, state, scope, paramTypes, x):
        return unwrapForNorm(x, lambda y: math.tanh(y))
provide(Tanh())








