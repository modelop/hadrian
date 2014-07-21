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
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "prob.dist."

class GaussianLL(LibFcn):
    name = prefix + "gaussianLL"
    sig = Sigs([Sig([{"x": P.Double()}, {"mu": P.Double()}, {"sigma": P.Double()}], P.Double()),
                Sig([{"x": P.Double()}, {"params": P.WildRecord("A", {"mean": P.Double(), "variance": P.Double()})}], P.Double())])
    def __call__(self, state, scope, paramTypes, x, *others):
        if len(others) == 2:
            mu, sigma = others
        else:
            mu = others[0]["mean"]
            sigma = math.sqrt(others[0]["variance"])

        return -(x - mu)**2/(2.0 * sigma**2) - math.log(sigma * math.sqrt(2.0 * math.pi))

provide(GaussianLL())
