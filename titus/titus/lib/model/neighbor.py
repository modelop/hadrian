#!/usr/bin/env python

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math

from six.moves import range

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, div
import titus.P as P
from titus.lib.array import argLowestN

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "model.neighbor."

#################################################################### 

class Mean(LibFcn):
    name = prefix + "mean"
    sig = Sigs([Sig([{"points": P.Array(P.Array(P.Double()))}], P.Array(P.Double())),
                Sig([{"points": P.Array(P.Array(P.Double()))}, {"weight": P.Fcn([P.Array(P.Double())], P.Double())}], P.Array(P.Double()))])
    errcodeBase = 30000
    def __call__(self, state, scope, pos, paramTypes, points, *args):
        if len(args) == 1:
            weight, = args
        else:
            weight = None

        if len(points) == 0:
            raise PFARuntimeException("not enough points", self.errcodeBase + 0, self.name, pos)

        dimensions = len(points[0])
        numer = [0.0] * dimensions
        denom = [0.0] * dimensions
        for point in points:
            if len(point) != dimensions:
                raise PFARuntimeException("inconsistent dimensionality", self.errcodeBase + 1, self.name, pos)

            if weight is None:
                w = 1.0
            else:
                w = callfcn(state, scope, weight, [point])

            for i in range(dimensions):
                numer[i] += w * point[i]
                denom[i] += w

        for i in range(dimensions):
            numer[i] /= denom[i]
        return numer
provide(Mean())

class NearestK(LibFcn):
    name = prefix + "nearestK"
    sig = Sigs([
        Sig([{"k": P.Int()}, {"datum": P.Array(P.Double())}, {"codebook": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
        Sig([{"k": P.Int()}, {"datum": P.Wildcard("A")}, {"codebook": P.Array(P.Wildcard("B"))}, {"metric": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}], P.Array(P.Wildcard("B")))])

    errcodeBase = 30010
    def __call__(self, state, scope, pos, paramTypes, k, datum, codebook, *args):
        if k < 0:
            raise PFARuntimeException("k must be nonnegative", self.errcodeBase + 0, self.name, pos)

        if len(args) == 1:
            metric, = args
            distances = [callfcn(state, scope, metric, [datum, x]) for x in codebook]
        else:
            if len(codebook) == 0:
                return []
            else:
                dimensions = len(datum)
                for x in codebook:
                    if len(x) != dimensions:
                        raise PFARuntimeException("inconsistent dimensionality", self.errcodeBase + 1, self.name, pos)
            distances = [sum((di - xi)**2 for di, xi in zip(datum, x)) for x in codebook]

        indexes = argLowestN(distances, k, lambda a, b: a < b)
        return [codebook[i] for i in indexes]

provide(NearestK())

class BallR(LibFcn):
    name = prefix + "ballR"
    sig = Sigs([
        Sig([{"r": P.Double()}, {"datum": P.Array(P.Double())}, {"codebook": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
        Sig([{"r": P.Double()}, {"datum": P.Wildcard("A")}, {"codebook": P.Array(P.Wildcard("B"))}, {"metric": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}], P.Array(P.Wildcard("B")))])
    errcodeBase = 30020
    def __call__(self, state, scope, pos, paramTypes, r, datum, codebook, *args):
        if len(args) == 1:
            metric, = args
            distances = [callfcn(state, scope, metric, [datum, x]) for x in codebook]
        else:
            distances = [math.sqrt(sum((di - xi)**2 for di, xi in zip(datum, x))) for x in codebook]
        return [x for x, d in zip(codebook, distances) if d < r]
provide(BallR())
