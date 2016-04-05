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
import itertools

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn
from titus.util import div
from titus.lib.core import powLikeJava
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "metric."

class SimpleEuclidean(LibFcn):
    name = prefix + "simpleEuclidean"
    sig = Sig([{"x": P.Array(P.Double())}, {"y": P.Array(P.Double())}], P.Double())
    errcodeBase = 28000
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if len(x) != len(y):
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        return math.sqrt(sum(((xi - yi)**2 for xi, yi in zip(x, y))))
provide(SimpleEuclidean())

class AbsDiff(LibFcn):
    name = prefix + "absDiff"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}], P.Double())
    errcodeBase = 28010
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return abs(x - y)
provide(AbsDiff())

class GaussianSimilarity(LibFcn):
    name = prefix + "gaussianSimilarity"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}, {"sigma": P.Double()}], P.Double())
    errcodeBase = 28020
    def __call__(self, state, scope, pos, paramTypes, x, y, sigma):
        return math.exp(div(-math.log(2) * (x - y)**2, sigma**2))
provide(GaussianSimilarity())

class MetricWithMissingValues(LibFcn):
    def __call__(self, state, scope, pos, paramTypes, similarity, x, y, missingWeight=None):
        length = len(x)
        if missingWeight is None:
            missingWeight = [1.0] * length
        if len(y) != length or len(missingWeight) != length:
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        tally = 0.0
        numer = 0.0
        denom = 0.0
        for i in xrange(length):
            xi = x[i]
            yi = y[i]
            if xi is not None and yi is not None:
                if isinstance(paramTypes[1]["items"], (tuple, list)):
                    xi, = xi.values()
                if isinstance(paramTypes[2]["items"], (tuple, list)):
                    yi, = yi.values()
                tally = self.increment(tally, callfcn(state, scope, similarity, [xi, yi]))
                denom += missingWeight[i]
            numer += missingWeight[i]
        if denom == 0.0:
            return self.finalize(float("nan"))
        else:
            return self.finalize(tally * numer / denom)

class Euclidean(MetricWithMissingValues):
    name = prefix + "euclidean"
    sig = Sigs([
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}], P.Double()),
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"missingWeight": P.Array(P.Double())}], P.Double())
        ])
    errcodeBase = 28030
    def increment(self, tally, x):
        return tally + x**2
    def finalize(self, x):
        return math.sqrt(x)
provide(Euclidean())

class SquaredEuclidean(MetricWithMissingValues):
    name = prefix + "squaredEuclidean"
    sig = Sigs([
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}], P.Double()),
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"missingWeight": P.Array(P.Double())}], P.Double())
        ])
    errcodeBase = 28040
    def increment(self, tally, x):
        return tally + x**2
    def finalize(self, x):
        return x
provide(SquaredEuclidean())

class Chebyshev(MetricWithMissingValues):
    name = prefix + "chebyshev"
    sig = Sigs([
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}], P.Double()),
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"missingWeight": P.Array(P.Double())}], P.Double())
        ])
    errcodeBase = 28050
    def increment(self, tally, x):
        return max(tally, x)
    def finalize(self, x):
        return x
provide(Chebyshev())

class Taxicab(MetricWithMissingValues):
    name = prefix + "taxicab"
    sig = Sigs([
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}], P.Double()),
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"missingWeight": P.Array(P.Double())}], P.Double())
        ])
    errcodeBase = 28060
    def increment(self, tally, x):
        return tally + x
    def finalize(self, x):
        return x
provide(Taxicab())

class Minkowski(LibFcn):
    name = prefix + "minkowski"
    sig = Sigs([
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"p": P.Double()}], P.Double()),
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"p": P.Double()}, {"missingWeight": P.Array(P.Double())}], P.Double())
        ])
    errcodeBase = 28070
    def __call__(self, state, scope, pos, paramTypes, similarity, x, y, p, missingWeight=None):
        length = len(x)
        if missingWeight is None:
            missingWeight = [1.0] * length
        if len(y) != length or len(missingWeight) != length:
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        if math.isnan(p) or p <= 0:
            raise PFARuntimeException("Minkowski parameter p must be positive", self.errcodeBase + 1, self.name, pos)
        tally = 0.0
        numer = 0.0
        denom = 0.0
        if math.isinf(p):
            for i in xrange(length):
                xi = x[i]
                yi = y[i]
                if xi is not None and yi is not None:
                    if isinstance(paramTypes[1]["items"], (tuple, list)):
                        xi, = xi.values()
                    if isinstance(paramTypes[2]["items"], (tuple, list)):
                        yi, = yi.values()
                    z = callfcn(state, scope, similarity, [xi, yi])
                    if z > tally:
                        tally = z
                    denom += missingWeight[i]
                numer += missingWeight[i]
            if denom == 0.0:
                return float("nan")
            else:
                return tally * numer / denom
        else:
            for i in xrange(length):
                xi = x[i]
                yi = y[i]
                if xi is not None and yi is not None:
                    if isinstance(paramTypes[1]["items"], (tuple, list)):
                        xi, = xi.values()
                    if isinstance(paramTypes[2]["items"], (tuple, list)):
                        yi, = yi.values()
                    tally += powLikeJava(callfcn(state, scope, similarity, [xi, yi]), p)
                    denom += missingWeight[i]
                numer += missingWeight[i]
            if denom == 0.0:
                return float("nan")
            else:
                return powLikeJava(tally * numer / denom, 1.0/p)
provide(Minkowski())

class BinaryMetric(LibFcn):
    def countPairs(self, x, y):
        a00 = 0
        a01 = 0
        a10 = 0
        a11 = 0
        for xi, yi in zip(x, y):
            if xi and yi: a11 += 1
            if xi and not yi: a10 += 1
            if not xi and yi: a01 += 1
            if not xi and not yi: a00 += 1
        return a00, a01, a10, a11

class SimpleMatching(BinaryMetric):
    name = prefix + "simpleMatching"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}], P.Double())
    errcodeBase = 28080
    def __call__(self, state, scope, pos, paramTypes, x, y):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        a00, a01, a10, a11 = self.countPairs(x, y)
        return div((a11 + a00), (a11 + a10 + a01 + a00))
provide(SimpleMatching())

class Jaccard(BinaryMetric):
    name = prefix + "jaccard"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}], P.Double())
    errcodeBase = 28090
    def __call__(self, state, scope, pos, paramTypes, x, y):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        a00, a01, a10, a11 = self.countPairs(x, y)
        return div(a11, (a11 + a10 + a01))
provide(Jaccard())

class Tanimoto(BinaryMetric):
    name = prefix + "tanimoto"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}], P.Double())
    errcodeBase = 28100
    def __call__(self, state, scope, pos, paramTypes, x, y):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        a00, a01, a10, a11 = self.countPairs(x, y)
        return div((a11 + a00), (a11 + 2*(a10 + a01) + a00))
provide(Tanimoto())

class BinarySimilarity(BinaryMetric):
    name = prefix + "binarySimilarity"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}, {"c00": P.Double()}, {"c01": P.Double()}, {"c10": P.Double()}, {"c11": P.Double()}, {"d00": P.Double()}, {"d01": P.Double()}, {"d10": P.Double()}, {"d11": P.Double()}], P.Double())
    errcodeBase = 28110
    def __call__(self, state, scope, pos, paramTypes, x, y, c00, c01, c10, c11, d00, d01, d10, d11):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match", self.errcodeBase + 0, self.name, pos)
        a00, a01, a10, a11 = self.countPairs(x, y)
        return div((c11*a11 + c10*a10 + c01*a01 + c00*a00), (d11*a11 + d10*a10 + d01*a01 + d00*a00))
provide(BinarySimilarity())
