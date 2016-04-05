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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn
from titus.util import div
import titus.P as P

def np():
    import numpy
    return numpy

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "interp."

class Bin(LibFcn):
    name = prefix + "bin"
    sig = Sigs([Sig([{"x": P.Double()}, {"numbins": P.Int()}, {"low": P.Double()}, {"high": P.Double()}], P.Int()),
                Sig([{"x": P.Double()}, {"origin": P.Double()}, {"width": P.Double()}], P.Int())])
    errcodeBase = 22000
    def __call__(self, state, scope, pos, paramTypes, x, *args):
        if len(args) == 3:
            numbins, low, high = args
            if low >= high or math.isnan(low) or math.isnan(high):
                raise PFARuntimeException("bad histogram range", self.errcodeBase + 0, self.name, pos)
            if numbins < 1:
                raise PFARuntimeException("bad histogram scale", self.errcodeBase + 1, self.name, pos)
            if math.isnan(x) or x < low or x >= high:
                raise PFARuntimeException("x out of range", self.errcodeBase + 2, self.name, pos)

            out = int(math.floor(numbins * div((x - low), (high - low))))

            if out < 0 or out >= numbins:
                raise PFARuntimeException("x out of range", self.errcodeBase + 2, self.name, pos)
            return out
        else:
            origin, width = args
            if math.isnan(origin) or math.isinf(origin):
                raise PFARuntimeException("bad histogram range", self.errcodeBase + 0, self.name, pos)
            if width <= 0.0 or math.isnan(width):
                raise PFARuntimeException("bad histogram scale", self.errcodeBase + 1, self.name, pos)
            if math.isnan(x) or math.isinf(x):
                raise PFARuntimeException("x out of range", self.errcodeBase + 2, self.name, pos)
            else:
                return int(math.floor(div((x - origin), width)))
provide(Bin())
    
class Nearest(LibFcn):
    name = prefix + "nearest"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Wildcard("T")}))}], P.Wildcard("T")),
                Sig([{"x": P.Array(P.Double())}, {"table": P.Array(P.WildRecord("R", {"x": P.Array(P.Double()), "to": P.Wildcard("T")}))}], P.Wildcard("T")),
                Sig([{"x": P.Wildcard("X1")}, {"table": P.Array(P.WildRecord("R", {"x": P.Wildcard("X2"), "to": P.Wildcard("T")}))}, {"metric": P.Fcn([P.Wildcard("X1"), P.Wildcard("X2")], P.Double())}], P.Wildcard("T"))])
    errcodeBase = 22010
    def __call__(self, state, scope, pos, paramTypes, datum, table, *metric):
        if len(table) == 0:
            raise PFARuntimeException("table must have at least one entry", self.errcodeBase + 0, self.name, pos)
        if len(metric) == 1:
            metric, = metric
            # do signature 3
            one = None
            oned = None
            for item in table:
                d = callfcn(state, scope, metric, [datum, item["x"]])
                if one is None or d < oned:
                    one = item
                    oned = d
            return one["to"] 
        elif isinstance(paramTypes[0], dict) and paramTypes[0].get("type") == "array":
            # do signature 2
            one = None
            oned = None
            for item in table:
                x = item["x"]
                if len(x) != len(datum):
                    raise PFARuntimeException("inconsistent dimensionality", self.errcodeBase + 1, self.name, pos)
                d = sum((x0i - xi)**2 for x0i, xi in zip(datum, x))
                if one is None or d < oned:
                    one = item
                    oned = d
            return one["to"]
        else:
            # do signature 1
            one = None
            oned = None
            for item in table:
                d = abs(datum - item["x"])
                if one is None or d < oned:
                    one = item
                    oned = d
            return one["to"]
provide(Nearest())

class Linear(LibFcn):
    name = prefix + "linear"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Double()}))}], P.Double()),
                Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Array(P.Double())}))}], P.Array(P.Double()))])
    errcodeBase = 22020
    @staticmethod
    def closest(datum, table, code, fcnName, pos):
        below = None
        above = None
        belowd = None
        aboved = None
        for item in table:
            if item["x"] <= datum and (below is None or datum - item["x"] < belowd):
                below = item
                belowd = datum - item["x"]
            if datum < item["x"] and (above is None or item["x"] - datum < aboved):
                above = item
                aboved = item["x"] - datum
        if below is not None and above is not None:
            return below, above, True
        else:
            one = None
            two = None
            oned = None
            twod = None
            for item in table:
                d = abs(datum - item["x"])
                if one is None or d < oned:
                    two = one
                    twod = oned
                    one = item
                    oned = d
                elif (two is None or d < twod) and d != oned:
                    two = item
                    twod = d
            if two is None:
                raise PFARuntimeException("table must have at least two distinct x values", code, fcnName, pos)
            return one, two, False
    @staticmethod
    def interpolateSingle(datum, one, two):
        onex = one["x"]
        twox = two["x"]
        oney = one["to"]
        twoy = two["to"]
        unitless = div((datum - onex), (twox - onex))
        return (1.0 - unitless)*oney + unitless*twoy
    @staticmethod
    def interpolateMulti(datum, one, two, code, fcnName, pos):
        onex = one["x"]
        twox = two["x"]
        oney = one["to"]
        twoy = two["to"]
        unitless = div((datum - onex), (twox - onex))
        if len(oney) != len(twoy):
            raise PFARuntimeException("inconsistent dimensionality", code, fcnName, pos)
        return [(1.0 - unitless)*oney[i] + unitless*twoy[i] for i in xrange(len(oney))]
    def __call__(self, state, scope, pos, paramTypes, datum, table):
        one, two, between = Linear.closest(datum, table, self.errcodeBase + 0, self.name, pos)
        if isinstance(paramTypes[-1], dict) and paramTypes[-1].get("type") == "array":
            return Linear.interpolateMulti(datum, one, two, self.errcodeBase + 1, self.name, pos)
        else:
            return Linear.interpolateSingle(datum, one, two)
provide(Linear())

class LinearFlat(LibFcn):
    name = prefix + "linearFlat"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Double()}))}], P.Double()),
                Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Array(P.Double())}))}], P.Array(P.Double()))])
    errcodeBase = 22030
    def __call__(self, state, scope, pos, paramTypes, datum, table):
        one, two, between = Linear.closest(datum, table, self.errcodeBase + 0, self.name, pos)
        if not between:
            return one["to"]
        elif isinstance(paramTypes[-1], dict) and paramTypes[-1].get("type") == "array":
            return Linear.interpolateMulti(datum, one, two, self.errcodeBase + 1, self.name, pos)
        else:
            return Linear.interpolateSingle(datum, one, two)
provide(LinearFlat())

class LinearMissing(LibFcn):
    name = prefix + "linearMissing"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Double()}))}], P.Union([P.Null(), P.Double()])),
                Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Array(P.Double())}))}], P.Union([P.Null(), P.Array(P.Double())]))])
    errcodeBase = 22040
    def __call__(self, state, scope, pos, paramTypes, datum, table):
        one, two, between = Linear.closest(datum, table, self.errcodeBase + 0, self.name, pos)
        if not between and one["x"] != datum:
            return None
        elif isinstance([x for x in paramTypes[-1] if x != "null"][0], dict) and [x for x in paramTypes[-1] if x != "null"][0].get("type") == "array":
            return Linear.interpolateMulti(datum, one, two, self.errcodeBase + 1, self.name, pos)
        else:
            return Linear.interpolateSingle(datum, one, two)
provide(LinearMissing())

# barycentric (could be a 2-d version with triangles or an arbitrary-dimension one with hypervolumes)
# bilinear (requires a 2-d grid)
# trilinear (requires a 3-d grid)
# spline (also 1-d)
# cubic (also 1-d)
# loess (also 1-d)
