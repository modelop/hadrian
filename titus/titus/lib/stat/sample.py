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
from titus.util import div, callfcn
from titus.lib.core import INT_MIN_VALUE
from titus.lib.core import INT_MAX_VALUE
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "stat.sample."

#################################################################### 

class UpdateMeanVariance(LibFcn):
    def genpy(self, paramTypes, args, pos):
        hasMean = False
        hasVariance = False
        for x in self._getRecord(paramTypes[2]).fields:
            if x.name == "mean":
                if not x.avroType.accepts(AvroDouble()):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"mean\" field is not a double: " + str(x.avroType), None)
                hasMean = True
            elif x.name == "variance":
                if not x.avroType.accepts(AvroDouble()):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"variance\" field is not a double: " + str(x.avroType), None)
                hasVariance = True
        if (hasMean, hasVariance) == (False, False):
            level = 0
        elif (hasMean, hasVariance) == (True, False):
            level = 1
        elif (hasMean, hasVariance) == (True, True):
            level = 2
        elif (hasMean, hasVariance) == (False, True):
            raise PFASemanticException(prefix + "update with \"variance\" must also have \"mean\" in the state record type", None)
        return "self.f[{0}]({1}, {2})".format(repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args), level)

class Update(UpdateMeanVariance):
    name = prefix + "update"
    sig = Sig([{"x": P.Double()}, {"w": P.Double()}, {"state": P.WildRecord("A", {"count": P.Double()})}], P.Wildcard("A"))
    errcodeBase = 14000
    def _getRecord(self, paramType):
        if isinstance(paramType, AvroUnion):
            for t in paramType.types:
                if not isinstance(t, AvroNull):
                    return t
        else:
            return paramType

    def __call__(self, state, scope, pos, paramTypes, x, w, theState, level):
        originalCount = theState["count"]
        count = originalCount + w
        if level == 0:
            return dict(theState, count=count)
        else:
            mean = theState["mean"]
            delta = x - mean
            shift = div(delta * w, count)
            mean += shift
            if level == 1:
                return dict(theState, count=count, mean=mean)
            else:
                varianceTimesCount = theState["variance"] * originalCount
                varianceTimesCount += originalCount * delta * shift
                return dict(theState, count=count, mean=mean, variance=div(varianceTimesCount, count))

provide(Update())

class UpdateCovariance(LibFcn):
    name = prefix + "updateCovariance"
    sig = Sigs([Sig([{"x": P.Array(P.Double())}, {"w": P.Double()}, {"state": P.WildRecord("A", {"count": P.Double(), "mean": P.Array(P.Double()), "covariance": P.Array(P.Array(P.Double()))})}], P.Wildcard("A")),
                Sig([{"x": P.Map(P.Double())}, {"w": P.Double()}, {"state": P.WildRecord("A", {"count": P.Map(P.Map(P.Double())), "mean": P.Map(P.Double()), "covariance": P.Map(P.Map(P.Double()))})}], P.Wildcard("A"))])

    errcodeBase = 14010
    def __call__(self, state, scope, pos, paramTypes, x, w, theState):
        size = len(x)
        if size < 2:
            raise PFARuntimeException("too few components", self.errcodeBase + 1, self.name, pos)

        if paramTypes[0]["type"] == "map":
            oldCount = theState["count"]
            oldMean = theState["mean"]
            oldCovariance = theState["covariance"]

            countKeys = set(oldCount.keys()).union(reduce(lambda a, b: a.union(b), (set(v.keys()) for v in oldCount.values()), set()))
            covarKeys = set(oldCovariance.keys()).union(reduce(lambda a, b: a.union(b), (set(v.keys()) for v in oldCovariance.values()), set()))
            keys = set(x.keys()).union(countKeys).union(covarKeys)

            newCount = {}
            for i in keys:
                row = {}
                for j in keys:
                    old = oldCount.get(i, {}).get(j, 0.0)
                    if (i in x) and (j in x):
                        row[j] = old + w
                    else:
                        row[j] = old
                newCount[i] = row

            newMean = {}
            for i in keys:
                old = oldMean.get(i, 0.0)
                if i in x:
                    newMean[i] = old + div((x[i] - old) * w, newCount[i][i])
                else:
                    newMean[i] = old

            newCovariance = {}
            for i in keys:
                row = {}
                for j in keys:
                    oldCov = oldCovariance.get(i, {}).get(j, 0.0)
                    if (i in x) and (j in x):
                        oldC = oldCount.get(i, {}).get(j, 0.0)
                        oldMi = oldMean.get(i, 0.0)
                        oldMj = oldMean.get(j, 0.0)
                        row[j] = div(((oldCov*oldC) + div((x[i] - oldMi) * (x[j] - oldMj) * w*oldC, newCount[i][j])), newCount[i][j])
                    else:
                        row[j] = oldCov
                newCovariance[i] = row

            return dict(theState, count=newCount, mean=newMean, covariance=newCovariance)

        else:
            oldCount = theState["count"]
            oldMean = theState["mean"]
            oldCovariance = theState["covariance"]

            if (size != len(oldMean) or size != len(oldCovariance) or any(size != len(xi) for xi in oldCovariance)):
                raise PFARuntimeException("unequal length arrays", self.errcodeBase + 2, self.name, pos)

            newCount = oldCount + w
            newMean = [oldm + div((x[i] - oldm) * w, newCount) for i, oldm in enumerate(oldMean)]
            newCovariance = []
            for i in xrange(size):
                row = []
                for j in xrange(size):
                    row.append(div((oldCovariance[i][j]*oldCount) + div((x[i] - oldMean[i]) * (x[j] - oldMean[j]) * w*oldCount, newCount), newCount))
                newCovariance.append(row)

            return dict(theState, count=newCount, mean=newMean, covariance=newCovariance)

provide(UpdateCovariance())

class UpdateWindow(UpdateMeanVariance):
    name = prefix + "updateWindow"
    sig = Sig([{"x": P.Double()}, {"w": P.Double()}, {"state": P.Array(P.WildRecord("A", {"x": P.Double(), "w": P.Double(), "count": P.Double()}))}, {"windowSize": P.Int()}], P.Array(P.Wildcard("A")))
    errcodeBase = 14020
    def _getRecord(self, paramType):
        return paramType.items

    def __call__(self, state, scope, pos, paramTypes, x, w, theState, windowSize, level):
        if windowSize < 2:
            raise PFARuntimeException("windowSize must be at least 2", self.errcodeBase + 0, self.name, pos)

        if len(theState) == 0:
            out = {}
            paramType = state.parser.getAvroType(paramTypes[2])
            for field in self._getRecord(paramType).fields:
                if field.name == "x":
                    out["x"] = x
                elif field.name == "w":
                    out["w"] = w
                elif field.name == "count":
                    out["count"] = w
                elif field.name == "mean":
                    out["mean"] = x
                elif field.name == "variance":
                    out["variance"] = 0.0
                else:
                    raise PFARuntimeException("cannot initialize unrecognized fields", self.errcodeBase + 1, self.name, pos)
            return [out]
        
        elif len(theState) >= windowSize:
            record = theState[-1]

            splitAt = len(theState) - windowSize + 1
            remove, keep = theState[:splitAt], theState[splitAt:]
            oldx = [xi["x"] for xi in remove]
            oldw = [-xi["w"] for xi in remove]

            originalCount = record["count"]
            count = originalCount + w

            count2 = count + sum(oldw)

            if level == 0:
                return keep + [dict(record, x=x, w=w, count=count2)]
            else:
                mean = record["mean"]
                delta = x - mean
                shift = div(delta * w, count)

                mean += shift

                accumulatedCount = count
                varianceCorrection = 0.0
                for ox, ow in zip(oldx, oldw):
                    accumulatedCount += ow
                    delta2 = ox - mean
                    shift2 = div(delta2 * ow, accumulatedCount)

                    mean += shift2
                    varianceCorrection += (accumulatedCount - ow) * delta2 * shift2

                if level == 1:
                    return keep + [dict(record, x=x, w=w, count=count2, mean=mean)]
                else:
                    varianceTimesCount = record["variance"] * originalCount
                    varianceTimesCount += originalCount * delta * shift

                    varianceTimesCount += varianceCorrection

                    return keep + [dict(record, x=x, w=w, count=count2, mean=mean, variance=div(varianceTimesCount, count2))]

        else:
            record = theState[-1]

            originalCount = record["count"]
            count = originalCount + w

            if level == 0:
                return theState + [dict(record, x=x, w=w, count=count)]
            else:
                mean = record["mean"]
                delta = x - mean
                shift = div(delta * w, count)
                mean += shift

                if level == 1:
                    return theState + [dict(record, x=x, w=w, count=count, mean=mean)]
                else:
                    varianceTimesCount = record["variance"] * originalCount
                    varianceTimesCount += originalCount * delta * shift

                    return theState + [dict(record, x=x, w=w, count=count, mean=mean, variance=div(varianceTimesCount, count))]

provide(UpdateWindow())

class UpdateEWMA(LibFcn):
    name = prefix + "updateEWMA"
    sig = Sig([{"x": P.Double()}, {"alpha": P.Double()}, {"state": P.WildRecord("A", {"mean": P.Double()})}], P.Wildcard("A"))
    errcodeBase = 14030
    def _getRecord(self, paramType):
        if isinstance(paramType, AvroUnion):
            for t in paramType.types:
                if not isinstance(t, AvroNull):
                    return t
        else:
            return paramType

    def genpy(self, paramTypes, args, pos):
        hasVariance = False
        for x in self._getRecord(paramTypes[2]).fields:
            if x.name == "variance":
                if not x.avroType.accepts(AvroDouble()):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"variance\" field is not a double: " + str(x.avroType), None)
                hasVariance = True
        return "self.f[{0}]({1}, {2})".format(repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args), hasVariance)

    def __call__(self, state, scope, pos, paramTypes, x, alpha, theState, hasVariance):
        if alpha < 0.0 or alpha > 1.0:
            raise PFARuntimeException("alpha out of range", self.errcodeBase + 0, self.name, pos)

        mean = theState["mean"]
        diff = x - mean
        incr = alpha * diff

        if hasVariance:
            variance = theState["variance"]
            return dict(theState, mean=(mean + incr), variance=((1.0 - alpha) * (variance + diff * incr)))
        else:
            return dict(theState, mean=(mean + incr))

provide(UpdateEWMA())

class UpdateHoltWinters(LibFcn):
    name = prefix + "updateHoltWinters"
    sig = Sig([{"x": P.Double()}, {"alpha": P.Double()}, {"beta": P.Double()}, {"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double()})}], P.Wildcard("A"))
    errcodeBase = 14040
    def __call__(self, state, scope, pos, paramTypes, x, alpha, beta, theState):
        if alpha < 0.0 or alpha > 1.0:
            raise PFARuntimeException("alpha out of range", self.errcodeBase + 0, self.name, pos)
        if beta < 0.0 or beta > 1.0:
            raise PFARuntimeException("beta out of range", self.errcodeBase + 1, self.name, pos)
        level_prev = theState["level"]
        trend_prev = theState["trend"]
        level = alpha * x + (1.0 - alpha) * (level_prev + trend_prev)
        trend = beta * (level - level_prev) + (1.0 - beta) * trend_prev
        return dict(theState, level=level, trend=trend)
provide(UpdateHoltWinters())

class UpdateHoltWintersPeriodic(LibFcn):
    name = prefix + "updateHoltWintersPeriodic"
    sig = Sig([{"x": P.Double()}, {"alpha": P.Double()}, {"beta": P.Double()}, {"gamma": P.Double()}, {"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double(), "cycle": P.Array(P.Double()), "multiplicative": P.Boolean()})}], P.Wildcard("A"))
    errcodeBase = 14050
    def __call__(self, state, scope, pos, paramTypes, x, alpha, beta, gamma, theState):
        if alpha < 0.0 or alpha > 1.0:
            raise PFARuntimeException("alpha out of range", self.errcodeBase + 0, self.name, pos)
        if beta < 0.0 or beta > 1.0:
            raise PFARuntimeException("beta out of range", self.errcodeBase + 1, self.name, pos)
        if gamma < 0.0 or gamma > 1.0:
            raise PFARuntimeException("gamma out of range", self.errcodeBase + 2, self.name, pos)

        level_prev = theState["level"]
        trend_prev = theState["trend"]
        cycle_unrotated = theState["cycle"]
        if len(cycle_unrotated) == 0:
            raise PFARuntimeException("empty cycle", self.errcodeBase + 3, self.name, pos)
        cycle_rotated = cycle_unrotated[1:] + [cycle_unrotated[0]]
        cycle_prev = cycle_rotated[0]

        if theState["multiplicative"]:
            level = div(alpha * x, cycle_prev) + (1.0 - alpha) * (level_prev + trend_prev)
            trend = beta * (level - level_prev) + (1.0 - beta) * trend_prev
            cycle = div(gamma * x, level) + (1.0 - gamma) * cycle_prev
        else:
            level = alpha * (x - cycle_prev) + (1.0 - alpha) * (level_prev + trend_prev)
            trend = beta * (level - level_prev) + (1.0 - beta) * trend_prev
            cycle = gamma * (x - level) + (1.0 - gamma) * cycle_prev

        return dict(theState, level=level, trend=trend, cycle=([cycle] + cycle_rotated[1:]))

provide(UpdateHoltWintersPeriodic())

class Forecast1HoltWinters(LibFcn):
    name = prefix + "forecast1HoltWinters"
    sig = Sig([{"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double()})}], P.Double())

    errcodeBase = 14060
    def genpy(self, paramTypes, args, pos):
        hasCycle = False
        hasMultiplicative = False
        for x in paramTypes[0].fields:
            if x.name == "cycle":
                if not x.avroType.accepts(AvroArray(AvroDouble())):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"cycle\" field is not an array of double: " + str(x.avroType), None)
                hasCycle = True
            elif x.name == "multiplicative":
                if not x.avroType.accepts(AvroBoolean()):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"multiplicative\" field is not a boolean: " + str(x.avroType), None)
                hasMultiplicative = True

        if hasCycle ^ hasMultiplicative:
            raise PFASemanticException(self.name + " is being given a state record type with a \"cycle\" but no \"multiplicative\" or vice-versa", None)

        return "self.f[{0}]({1}, {2})".format(repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args), hasCycle)

    def __call__(self, state, scope, pos, paramTypes, theState, hasPeriodic):
        level = theState["level"]
        trend = theState["trend"]

        if not hasPeriodic:
            return level + trend
        else:
            cycle = theState["cycle"]
            L = len(cycle)
            if L == 0:
                raise PFARuntimeException("empty cycle", self.errcodeBase + 0, self.name, pos)

            if theState["multiplicative"]:
                return (level + trend) * cycle[1 % L]
            else:
                return level + trend + cycle[1 % L]

provide(Forecast1HoltWinters())

class ForecastHoltWinters(LibFcn):
    name = prefix + "forecastHoltWinters"
    sig = Sig([{"n": P.Int()}, {"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double()})}], P.Array(P.Double()))

    errcodeBase = 14070
    def genpy(self, paramTypes, args, pos):
        hasCycle = False
        hasMultiplicative = False
        for x in paramTypes[1].fields:
            if x.name == "cycle":
                if not x.avroType.accepts(AvroArray(AvroDouble())):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"cycle\" field is not an array of double: " + str(x.avroType), None)
                hasCycle = True
            elif x.name == "multiplicative":
                if not x.avroType.accepts(AvroBoolean()):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"multiplicative\" field is not a boolean: " + str(x.avroType), None)
                hasMultiplicative = True

        if hasCycle ^ hasMultiplicative:
            raise PFASemanticException(self.name + " is being given a state record type with a \"cycle\" but no \"multiplicative\" or vice-versa", None)

        return "self.f[{0}]({1}, {2})".format(repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args), hasCycle)

    def __call__(self, state, scope, pos, paramTypes, n, theState, hasPeriodic):
        level = theState["level"]
        trend = theState["trend"]

        if not hasPeriodic:
            return [level + i*trend for i in xrange(1, n + 1)]
        else:
            cycle = theState["cycle"]
            L = len(cycle)
            if L == 0:
                raise PFARuntimeException("empty cycle", self.errcodeBase + 0, self.name, pos)

            if theState["multiplicative"]:
                return [(level + i*trend) * cycle[i % L] for i in xrange(1, n + 1)]
            else:
                return [level + i*trend + cycle[i % L] for i in xrange(1, n + 1)]

provide(ForecastHoltWinters())

class FillHistogram(LibFcn):
    name = prefix + "fillHistogram"
    sig = Sigs([Sig([{"x": P.Double()}, {"w": P.Double()}, {"histogram": P.WildRecord("A", {"numbins": P.Int(), "low": P.Double(), "high": P.Double(), "values": P.Array(P.Double())})}], P.Wildcard("A")),
                Sig([{"x": P.Double()}, {"w": P.Double()}, {"histogram": P.WildRecord("A", {"low": P.Double(), "binsize": P.Double(), "values": P.Array(P.Double())})}], P.Wildcard("A")),
                Sig([{"x": P.Double()}, {"w": P.Double()}, {"histogram": P.WildRecord("A", {"ranges": P.Array(P.Array(P.Double())), "values": P.Array(P.Double())})}], P.Wildcard("A"))])

    errcodeBase = 14080
    def genpy(self, paramTypes, args, pos):
        def has(name, avroType):
            for x in paramTypes[2].fields:
                if x.name == name:
                    if not x.avroType.accepts(avroType):
                        raise PFASemanticException("{0} is being given a record type in which the \"{1}\" field is not {2}: {3}".format(self.name, name, avroType, x.avroType), None)
                    return True
            return False

        method0 = has("numbins", AvroInt()) and has("low", AvroDouble()) and has("high", AvroDouble())
        method1 = has("low", AvroDouble()) and has("binsize", AvroDouble())
        method2 = has("ranges", AvroArray(AvroArray(AvroDouble())))

        if       method0 and not method1 and not method2:
            method = 0
        elif not method0 and     method1 and not method2:
            method = 1
        elif not method0 and not method1 and     method2:
            method = 2
        else:
            raise PFASemanticException(prefix + "fillHistogram must have \"numbins\", \"low\", \"high\" xor it must have \"low\", \"binsize\" xor it must have \"ranges\", but not any other combination of these fields.", None)

        hasUnderflow = has("underflow", AvroDouble())
        hasOverflow = has("overflow", AvroDouble())
        hasNanflow = has("nanflow", AvroDouble())
        hasInfflow = has("infflow", AvroDouble())

        return "self.f[{0}]({1}, {2}, {3}, {4}, {5}, {6})".format(repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args), method, hasUnderflow, hasOverflow, hasNanflow, hasInfflow)

    def updateHistogram(self, w, histogram, newValues, hasUnderflow, hasOverflow, hasNanflow, hasInfflow, underflow, overflow, nanflow, infflow):
        updator = {"values": newValues}
        if (hasUnderflow):
            updator["underflow"] = histogram["underflow"] + (w if underflow else 0.0)
        if (hasOverflow):
            updator["overflow"] = histogram["overflow"] + (w if overflow else 0.0)
        if (hasNanflow):
            updator["nanflow"] = histogram["nanflow"] + (w if nanflow else 0.0)
        if (hasInfflow):
            updator["infflow"] = histogram["infflow"] + (w if infflow else 0.0)
        return dict(histogram, **updator)

    def __call__(self, state, scope, pos, paramTypes, x, w, histogram, method, hasUnderflow, hasOverflow, hasNanflow, hasInfflow):
        values = histogram["values"]

        if method == 0:
            numbins, low, high = histogram["numbins"], histogram["low"], histogram["high"]

            if numbins < 1:
                raise PFARuntimeException("bad histogram scale", self.errcodeBase + 2, self.name, pos)
            if len(values) != numbins:
                raise PFARuntimeException("wrong histogram size", self.errcodeBase + 0, self.name, pos)
            if low >= high or math.isnan(low) or math.isinf(low) or math.isnan(high) or math.isinf(high):
                raise PFARuntimeException("bad histogram range", self.errcodeBase + 1, self.name, pos)

            if hasInfflow and math.isinf(x):
                underflow, overflow, nanflow, infflow = False, False, False, True
            elif math.isnan(x):
                underflow, overflow, nanflow, infflow = False, False, True, False
            elif x >= high:
                underflow, overflow, nanflow, infflow = False, True, False, False
            elif x < low:
                underflow, overflow, nanflow, infflow = True, False, False, False
            else:
                underflow, overflow, nanflow, infflow = False, False, False, False

            if not underflow and not overflow and not nanflow and not infflow:
                try:
                    indexFloat = math.floor((x - low) / (high - low) * numbins)
                except ZeroDivisionError:
                    index = 0
                else:
                    if math.isinf(indexFloat):
                        if indexFloat > 0:
                            index = INT_MAX_VALUE
                        else:
                            index = INT_MIN_VALUE
                    else:
                        index = int(indexFloat)
                newValues = list(values)
                newValues[index] = newValues[index] + w
            else:
                newValues = values

            return self.updateHistogram(w, histogram, newValues, hasUnderflow, hasOverflow, hasNanflow, hasInfflow, underflow, overflow, nanflow, infflow)

        elif method == 1:
            low, binsize = histogram["low"], histogram["binsize"]

            if binsize <= 0.0 or math.isnan(binsize) or math.isinf(binsize):
                raise PFARuntimeException("bad histogram scale", self.errcodeBase + 2, self.name, pos)

            if hasInfflow and math.isinf(x):
                underflow, overflow, nanflow, infflow = False, False, False, True
            elif math.isnan(x):
                underflow, overflow, nanflow, infflow = False, False, True, False
            elif math.isinf(x) and x > 0.0:
                underflow, overflow, nanflow, infflow = False, True, False, False
            elif x < low:
                underflow, overflow, nanflow, infflow = True, False, False, False
            else:
                underflow, overflow, nanflow, infflow = False, False, False, False
                
            if not underflow and not overflow and not nanflow and not infflow:
                currentHigh = low + binsize * len(values)
                try:
                    index = int(math.floor((x - low) / (currentHigh - low) * len(values)))
                except ZeroDivisionError:
                    index = 0
                if index < len(values):
                    newValues = list(values)
                    newValues[index] = newValues[index] + w
                else:
                    newValues = values + [0.0] * (index - len(values)) + [w]

            else:
                newValues = values

            return self.updateHistogram(w, histogram, newValues, hasUnderflow, hasOverflow, hasNanflow, hasInfflow, underflow, overflow, nanflow, infflow)

        elif method == 2:
            ranges = histogram["ranges"]

            if len(values) != len(ranges):
                raise PFARuntimeException("wrong histogram size", self.errcodeBase + 0, self.name, pos)

            if any(len(x) != 2 or x[0] >= x[1] or math.isnan(x[0]) or math.isinf(x[0]) or math.isnan(x[1]) or math.isinf(x[1]) for x in ranges):
                raise PFARuntimeException("bad histogram ranges", self.errcodeBase + 3, self.name, pos)

            isInfinite = math.isinf(x)
            isNan = math.isnan(x)

            newValues = list(values)
            hitOne = False

            if not isInfinite and not isNan:
                for index, rang in enumerate(ranges):
                    low = rang[0]
                    high = rang[1]

                    if low == high and x == low:
                        newValues[index] = newValues[index] + w
                        hitOne = True

                    elif x >= low and x < high:
                        newValues[index] = newValues[index] + w
                        hitOne = True

            if hasInfflow and isInfinite:
                underflow, overflow, nanflow, infflow = False, False, False, True
            elif isNan:
                underflow, overflow, nanflow, infflow = False, False, True, False
            elif not hitOne:
                underflow, overflow, nanflow, infflow = True, False, False, False
            else:
                underflow, overflow, nanflow, infflow = False, False, False, False

            return self.updateHistogram(w, histogram, newValues, hasUnderflow, hasOverflow, hasNanflow, hasInfflow, underflow, overflow, nanflow, infflow)

provide(FillHistogram())

class FillHistogram2d(LibFcn):
    name = prefix + "fillHistogram2d"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}, {"w": P.Double()}, {"histogram": P.WildRecord("A", {"xnumbins": P.Int(), "xlow": P.Double(), "xhigh": P.Double(), "ynumbins": P.Int(), "ylow": P.Double(), "yhigh": P.Double(), "values": P.Array(P.Array(P.Double()))})}], P.Wildcard("A"))

    errcodeBase = 14090
    def genpy(self, paramTypes, args, pos):
        def has(name):
            for x in paramTypes[3].fields:
                if x.name == name:
                    if not x.avroType.accepts(AvroDouble()):
                        raise PFASemanticException("{0} is being given a record type in which the \"{1}\" field is not a double: {2}".format(self.name, name, x.avroType), None)
                    return True
            return False

        return "self.f[{0}]({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11})".format(
            repr(self.name), ", ".join(["state", "scope", repr(pos), repr(paramTypes)] + args),
            has("underunderflow"),
            has("undermidflow"),
            has("underoverflow"),
            has("midunderflow"),
            has("midoverflow"),
            has("overunderflow"),
            has("overmidflow"),
            has("overoverflow"),
            has("nanflow"),
            has("infflow"))

    def __call__(self, state, scope, pos, paramTypes, x, y, w, histogram,
                 hasUnderunderflow, hasUndermidflow, hasUnderoverflow,
                 hasMidunderflow,                    hasMidoverflow,
                 hasOverunderflow,  hasOvermidflow,  hasOveroverflow,
                 hasNanflow, hasInfflow):

        values = histogram["values"]
        xnumbins = histogram["xnumbins"]
        xlow = histogram["xlow"]
        xhigh = histogram["xhigh"]
        ynumbins = histogram["ynumbins"]
        ylow = histogram["ylow"]
        yhigh = histogram["yhigh"]

        if xnumbins < 1 or ynumbins < 1:
            raise PFARuntimeException("bad histogram scale", self.errcodeBase + 2, self.name, pos)
        if len(values) != xnumbins or any(len(x) != ynumbins for x in values):
            raise PFARuntimeException("wrong histogram size", self.errcodeBase + 0, self.name, pos)
        if xlow >= xhigh or ylow >= yhigh or math.isnan(xlow) or math.isinf(xlow) or math.isnan(xhigh) or math.isinf(xhigh) or math.isnan(ylow) or math.isinf(ylow) or math.isnan(yhigh) or math.isinf(yhigh):
            raise PFARuntimeException("bad histogram range", self.errcodeBase + 1, self.name, pos)

        if math.isnan(x) or math.isnan(y):  # do nan check first: nan wins over inf
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, False, False, False, False, True, False
        elif hasInfflow and (math.isinf(x) or math.isinf(y)):
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, False, False, False, False, False, True
        elif x >= xhigh and y >= yhigh:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, False, False, False, True, False, False
        elif x >= xhigh and y >= ylow and y < yhigh:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, False, False, True, False, False, False
        elif x >= xhigh and y < ylow:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, False, True, False, False, False, False
        elif x >= xlow and x < xhigh and y >= yhigh:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, True, False, False, False, False, False
        elif x >= xlow and x < xhigh and y < ylow:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, True, False, False, False, False, False, False
        elif x < xlow and y >= yhigh:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, True, False, False, False, False, False, False, False
        elif x < xlow and y >= ylow and y < yhigh:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, True, False, False, False, False, False, False, False, False
        elif x < xlow and y < ylow:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = True, False, False, False, False, False, False, False, False, False
        else:
            underunderflow, undermidflow, underoverflow, midunderflow, midoverflow, overunderflow, overmidflow, overoverflow, nanflow, infflow = False, False, False, False, False, False, False, False, False, False

        if not underunderflow and not undermidflow and not underoverflow and not midunderflow and not midoverflow and not overunderflow and not overmidflow and not overoverflow and not nanflow and not infflow:
            try:
                xindex = int(math.floor((x - xlow) / (xhigh - xlow) * xnumbins))
            except ZeroDivisionError:
                xindex = 0
            try:
                yindex = int(math.floor((y - ylow) / (yhigh - ylow) * ynumbins))
            except ZeroDivisionError:
                yindex = 0
            newValues = [list(x) for x in values]
            newValues[xindex][yindex] = newValues[xindex][yindex] + w
        else:
            newValues = values

        updator = {"values": newValues}
        if hasUnderunderflow:
            updator["underunderflow"] = histogram["underunderflow"] + (w if underunderflow else 0.0)
        if hasUndermidflow:
            updator["undermidflow"] = histogram["undermidflow"] + (w if undermidflow else 0.0)
        if hasUnderoverflow:
            updator["underoverflow"] = histogram["underoverflow"] + (w if underoverflow else 0.0)
        if hasMidunderflow:
            updator["midunderflow"] = histogram["midunderflow"] + (w if midunderflow else 0.0)
        if hasMidoverflow:
            updator["midoverflow"] = histogram["midoverflow"] + (w if midoverflow else 0.0)
        if hasOverunderflow:
            updator["overunderflow"] = histogram["overunderflow"] + (w if overunderflow else 0.0)
        if hasOvermidflow:
            updator["overmidflow"] = histogram["overmidflow"] + (w if overmidflow else 0.0)
        if hasOveroverflow:
            updator["overoverflow"] = histogram["overoverflow"] + (w if overoverflow else 0.0)
        if hasNanflow:
            updator["nanflow"] = histogram["nanflow"] + (w if nanflow else 0.0)
        if hasInfflow:
            updator["infflow"] = histogram["infflow"] + (w if infflow else 0.0)

        return dict(histogram, **updator)

provide(FillHistogram2d())

class FillCounter(LibFcn):
    name = prefix + "fillCounter"
    sig = Sig([{"x": P.String()}, {"w": P.Double()}, {"counter": P.WildRecord("A", {"values": P.Map(P.Double())})}], P.Wildcard("A"))
    errcodeBase = 14100
    def __call__(self, state, scope, pos, paramTypes, x, w, counter):
        oldmap = counter["values"]
        newmap = dict(oldmap, **{x: oldmap.get(x, 0.0) + w})
        return dict(counter, values=newmap)

provide(FillCounter())

class TopN(LibFcn):
    name = prefix + "topN"
    sig = Sig([{"x": P.Wildcard("A")}, {"top": P.Array(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.Wildcard("A")))
    errcodeBase = 14110
    def __call__(self, state, scope, pos, paramTypes, x, top, n, lessThan):
        if n <= 0:
            return []
        else:
            index = 0
            for best in top:
                if callfcn(state, scope, lessThan, [best, x]):
                    break
                index += 1
            if index == len(top):
                out = top + [x]
            else:
                above, below = top[:index], top[index:]
                out = above + [x] + below
            return out[:n]

provide(TopN())
