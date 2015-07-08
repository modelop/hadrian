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
from titus.lib1.core import INT_MIN_VALUE, INT_MAX_VALUE, LONG_MIN_VALUE, LONG_MAX_VALUE
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "test."

class KSTwoSample(LibFcn):
    name = prefix + "kolmogorov"
    sig = Sig([{"x": P.Array(P.Double())}, {"y": P.Array(P.Double())}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y):
        n1, n2 = len(x), len(y)
        x = sorted(x)
        y = sorted(y)
        if (x == y):
            return 1.0
        elif ((len(x) == 0) or (len(y) == 0)):
            return 0.0
        else:
            en1 = n1
            en2 = n2
            j1 = j2 = 0
            fn1 = fn2 = d = 0.0
            while ((j1 < n1) and (j2 < n2)):
                d1 = x[j1]
                d2 = y[j2]
                if d1 <= d2:
                    j1 += 1
                    fn1 = float(j1)/n1
                if d2 <= d1:
                    j2 += 1
                    fn2 = float(j2)/n2
                dt = abs(fn2 - fn1)
                if dt > d:
                    d = dt
            en = math.sqrt((n1 * n2)/float(n1 + n2))
            stat = (en + 0.12 + 0.11/en)*d
            return 1.0 - kolomogorov_cdf(stat)
provide(KSTwoSample())

def kolomogorov_cdf(z):
    """
    Function to compute the cdf of the Kolomogorov distribution.  Used by other
        variants of KS test
    """
    L = math.sqrt(2.0 * math.pi) * (1.0/z)
    sumpart = 0.0
    for v in range(1, 150):
        sumpart += math.exp(-((2.0*v - 1.0)**2)*((math.pi**2)/(8.0*(z**2))))
    return L*sumpart
