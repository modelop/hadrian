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
from titus.util import callfcn
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "interp."

class Nearest(LibFcn):
    name = prefix + "nearest"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Wildcard("T")}))}], P.Wildcard("T")),
                Sig([{"x": P.Array(P.Double())}, {"table": P.Array(P.WildRecord("R", {"x": P.Array(P.Double()), "to": P.Wildcard("T")}))}], P.Wildcard("T")),
                Sig([{"x": P.Wildcard("X1")}, {"table": P.Array(P.WildRecord("R", {"x": P.Wildcard("X2"), "to": P.Wildcard("T")}))}, {"metric": P.Fcn([P.Wildcard("X1"), P.Wildcard("X2")], P.Double())}], P.Wildcard("T"))])
    def __call__(self, state, scope, paramTypes, datum, table, *metric):
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
                    raise PFARuntimeException("inconsistent dimensionality")
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
    @staticmethod
    def closest(datum, table):
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
                elif two is None or (d < twod and d != oned):
                    two = item
                    twod = d
            if two is None:
                raise PFARuntimeException("table must have at least two distinct x values")
            return one, two, False
    @staticmethod
    def interpolateSingle(datum, one, two):
        onex = one["x"]
        twox = two["x"]
        oney = one["to"]
        twoy = two["to"]
        unitless = (datum - onex) / (twox - onex)
        return (1.0 - unitless)*oney + unitless*twoy
    @staticmethod
    def interpolateMulti(datum, one, two):
        onex = one["x"]
        twox = two["x"]
        oney = one["to"]
        twoy = two["to"]
        unitless = (datum - onex) / (twox - onex)
        if len(oney) != len(twoy):
            raise PFARuntimeException("inconsistent dimensionality")
        return [(1.0 - unitless)*oney[i] + unitless*twoy[i] for i in xrange(len(oney))]
    def __call__(self, state, scope, paramTypes, datum, table):
        one, two, between = Linear.closest(datum, table)
        if isinstance(paramTypes[-1], dict) and paramTypes[-1].get("type") == "array":
            return Linear.interpolateMulti(datum, one, two)
        else:
            return Linear.interpolateSingle(datum, one, two)
provide(Linear())

class LinearFlat(LibFcn):
    name = prefix + "linearFlat"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Double()}))}], P.Double()),
                Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Array(P.Double())}))}], P.Array(P.Double()))])
    def __call__(self, state, scope, paramTypes, datum, table):
        one, two, between = Linear.closest(datum, table)
        if not between:
            return one["to"]
        elif isinstance(paramTypes[-1], dict) and paramTypes[-1].get("type") == "array":
            return Linear.interpolateMulti(datum, one, two)
        else:
            return Linear.interpolateSingle(datum, one, two)
provide(LinearFlat())

class LinearMissing(LibFcn):
    name = prefix + "linearMissing"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Double()}))}], P.Union([P.Null(), P.Double()])),
                Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Array(P.Double())}))}], P.Union([P.Null(), P.Array(P.Double())]))])
    def __call__(self, state, scope, paramTypes, datum, table):
        one, two, between = Linear.closest(datum, table)
        if not between and one["x"] != datum:
            return None
        elif isinstance(paramTypes[-1], dict) and paramTypes[-1].get("type") == "array":
            return Linear.interpolateMulti(datum, one, two)
        else:
            return Linear.interpolateSingle(datum, one, two)
provide(LinearMissing())

# barycentric (could be a 2-d version with triangles or an arbitrary-dimension one with hypervolumes)
# bilinear (requires a 2-d grid)
# trilinear (requires a 3-d grid)
# spline (also 1-d)
# cubic (also 1-d)
# loess (also 1-d)
