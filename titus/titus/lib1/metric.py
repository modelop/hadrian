#!/usr/bin/env python

import math
import itertools

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "metric."

class SimpleEuclidean(LibFcn):
    name = prefix + "simpleEuclidean"
    sig = Sig([{"x": P.Array(P.Double())}, {"y": P.Array(P.Double())}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y):
        if len(x) != len(y):
            raise PFARuntimeException("dimensions of vectors do not match")
        return math.sqrt(sum(((xi - yi)**2 for xi, yi in zip(x, y))))
provide(SimpleEuclidean())

class AbsDiff(LibFcn):
    name = prefix + "absDiff"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y):
        return abs(x - y)
provide(AbsDiff())

class GaussianSimilarity(LibFcn):
    name = prefix + "gaussianSimilarity"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}, {"sigma": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y, sigma):
        return math.exp(-math.log(2) * (x - y)**2 / sigma**2)
provide(GaussianSimilarity())

class MetricWithMissingValues(LibFcn):
    def __call__(self, state, scope, paramTypes, similarity, x, y, missingWeight=None):
        length = len(x)
        if missingWeight is None:
            missingWeight = [1.0] * length
        if len(y) != length or len(missingWeight) != length:
            raise PFARuntimeException("dimensions of vectors do not match")
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
        return self.finalize(tally * numer / denom)

class Euclidean(MetricWithMissingValues):
    name = prefix + "euclidean"
    sig = Sigs([
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}], P.Double()),
        Sig([{"similarity": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Double())}, {"x": P.Array(P.Union([P.Null(), P.Wildcard("A")]))}, {"y": P.Array(P.Union([P.Null(), P.Wildcard("B")]))}, {"missingWeight": P.Array(P.Double())}], P.Double())
        ])
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
    def __call__(self, state, scope, paramTypes, similarity, x, y, p, missingWeight=None):
        length = len(x)
        if missingWeight is None:
            missingWeight = [1.0] * length
        if len(y) != length or len(missingWeight) != length:
            raise PFARuntimeException("dimensions of vectors do not match")
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
                tally += callfcn(state, scope, similarity, [xi, yi])**p
                denom += missingWeight[i]
            numer += missingWeight[i]
        return (tally * numer / denom)**(1.0/p)
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
    def __call__(self, state, scope, paramTypes, x, y):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match")
        a00, a01, a10, a11 = self.countPairs(x, y)
        return (a11 + a00)/float(a11 + a10 + a01 + a00)
provide(SimpleMatching())

class Jaccard(BinaryMetric):
    name = prefix + "jaccard"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match")
        a00, a01, a10, a11 = self.countPairs(x, y)
        return a11/float(a11 + a10 + a01)
provide(Jaccard())

class Tanimoto(BinaryMetric):
    name = prefix + "tanimoto"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match")
        a00, a01, a10, a11 = self.countPairs(x, y)
        return (a11 + a00)/float(a11 + 2*(a10 + a01) + a00)
provide(Tanimoto())

class BinarySimilarity(BinaryMetric):
    name = prefix + "binarySimilarity"
    sig = Sig([{"x": P.Array(P.Boolean())}, {"y": P.Array(P.Boolean())}, {"c00": P.Double()}, {"c01": P.Double()}, {"c10": P.Double()}, {"c11": P.Double()}, {"d00": P.Double()}, {"d01": P.Double()}, {"d10": P.Double()}, {"d11": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, x, y, c00, c01, c10, c11, d00, d01, d10, d11):
        length = len(x)
        if len(y) != length:
            raise PFARuntimeException("dimensions of vectors do not match")
        a00, a01, a10, a11 = self.countPairs(x, y)
        return (c11*a11 + c10*a10 + c01*a01 + c00*a00)/float(d11*a11 + d10*a10 + d01*a01 + d00*a00)
provide(BinarySimilarity())
