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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import div
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "stat.sample."

#################################################################### 

class UpdateMeanVariance(LibFcn):
    def genpy(self, paramTypes, args):
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
        return "self.f[{}]({}, {})".format(repr(self.name), ", ".join(["state", "scope", repr(paramTypes)] + args), level)

class Update(UpdateMeanVariance):
    name = prefix + "update"
    sig = Sig([{"x": P.Double()}, {"w": P.Double()}, {"state": P.WildRecord("A", {"count": P.Double()})}], P.Wildcard("A"))
    def _getRecord(self, paramType):
        if isinstance(paramType, AvroUnion):
            for t in paramType.types:
                if not isinstance(t, AvroNull):
                    return t
        else:
            return paramType

    def __call__(self, state, scope, paramTypes, x, w, theState, level):
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

    def __call__(self, state, scope, paramTypes, x, w, theState):
        size = len(x)
        if size < 2:
            raise PFARuntimeException("too few components")

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
                    newMean[i] = old + ((x[i] - old) * w / newCount[i][i])
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
                        row[j] = ((oldCov*oldC) + ((x[i] - oldMi) * (x[j] - oldMj) * w*oldC/newCount[i][j])) / newCount[i][j]
                    else:
                        row[j] = oldCov
                newCovariance[i] = row

            return dict(theState, count=newCount, mean=newMean, covariance=newCovariance)

        else:
            oldCount = theState["count"]
            oldMean = theState["mean"]
            oldCovariance = theState["covariance"]

            if (size != len(oldMean) or size != len(oldCovariance) or any(size != len(xi) for xi in oldCovariance)):
                raise PFARuntimeException("unequal length arrays")

            newCount = oldCount + w
            newMean = [oldm + ((x[i] - oldm) * w / newCount) for i, oldm in enumerate(oldMean)]
            newCovariance = []
            for i in xrange(size):
                row = []
                for j in xrange(size):
                    row.append(((oldCovariance[i][j]*oldCount) + ((x[i] - oldMean[i]) * (x[j] - oldMean[j]) * w*oldCount/newCount)) / newCount)
                newCovariance.append(row)

            return dict(theState, count=newCount, mean=newMean, covariance=newCovariance)

provide(UpdateCovariance())

class UpdateWindow(UpdateMeanVariance):
    name = prefix + "updateWindow"
    sig = Sig([{"x": P.Double()}, {"w": P.Double()}, {"state": P.Array(P.WildRecord("A", {"x": P.Double(), "w": P.Double(), "count": P.Double()}))}, {"windowSize": P.Int()}], P.Array(P.Wildcard("A")))
    def _getRecord(self, paramType):
        return paramType.items

    def __call__(self, state, scope, paramTypes, x, w, theState, windowSize, level):
        if windowSize < 2:
            raise PFARuntimeException("windowSize must be at least 2")

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
                    raise PFARuntimeException("cannot initialize unrecognized fields")
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

                    return theState + [dict(record, x=x, w=w, count=count, mean=mean, variance=(varianceTimesCount / count))]

provide(UpdateWindow())

class UpdateEWMA(LibFcn):
    name = prefix + "updateEWMA"
    sig = Sig([{"x": P.Double()}, {"alpha": P.Double()}, {"state": P.Union([P.Null(), P.WildRecord("A", {"mean": P.Double()})])}], P.Wildcard("A"))
    def _getRecord(self, paramType):
        if isinstance(paramType, AvroUnion):
            for t in paramType.types:
                if not isinstance(t, AvroNull):
                    return t
        else:
            return paramType

    def genpy(self, paramTypes, args):
        hasVariance = False
        for x in self._getRecord(paramTypes[2]).fields:
            if x.name == "variance":
                if not x.avroType.accepts(AvroDouble()):
                    raise PFASemanticException(self.name + " is being given a state record type in which the \"variance\" field is not a double: " + str(x.avroType), None)
                hasVariance = True
        return "self.f[{}]({}, {})".format(repr(self.name), ", ".join(["state", "scope", repr(paramTypes)] + args), hasVariance)

    def __call__(self, state, scope, paramTypes, x, alpha, theState, hasVariance):
        if alpha < 0.0 or alpha > 1.0:
            raise PFARuntimeException("alpha is out of range")

        if theState is None:
            out = {}
            paramType = state.parser.getAvroType(paramTypes[2])
            for field in self._getRecord(paramType).fields:
                if field.name == "mean":
                    out["mean"] = x
                elif field.name == "variance":
                    out["variance"] = 0.0
                else:
                    raise PFARuntimeException("cannot initialize unrecognized fields")
            return out

        mean = theState["mean"]
        variance = theState["variance"]
        diff = x - mean
        incr = alpha * diff

        if hasVariance:
            return dict(theState, mean=(mean + incr), variance=((1.0 - alpha) * (variance + diff * incr)))
        else:
            return dict(theState, mean=(mean + incr))

provide(UpdateEWMA())

class UpdateHoltWinters(LibFcn):
    name = prefix + "updateHoltWinters"
    sig = Sig([{"x": P.Double()}, {"alpha": P.Double()}, {"beta": P.Double()}, {"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double()})}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, alpha, beta, theState):
        if alpha < 0.0 or alpha > 1.0:
            raise PFARuntimeException("alpha is out of range")
        if beta < 0.0 or beta > 1.0:
            raise PFARuntimeException("beta is out of range")
        level_prev = theState["level"]
        trend_prev = theState["trend"]
        level = alpha * x + (1.0 - alpha) * (level_prev + trend_prev)
        trend = beta * (level - level_prev) + (1.0 - beta) * trend_prev
        return dict(theState, level=level, trend=trend)
provide(UpdateHoltWinters())

class UpdateHoltWintersPeriodic(LibFcn):
    name = prefix + "updateHoltWintersPeriodic"
    sig = Sig([{"x": P.Double()}, {"alpha": P.Double()}, {"beta": P.Double()}, {"gamma": P.Double()}, {"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double(), "cycle": P.Array(P.Double()), "multiplicative": P.Boolean()})}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, alpha, beta, gamma, theState):
        if alpha < 0.0 or alpha > 1.0:
            raise PFARuntimeException("alpha is out of range")
        if beta < 0.0 or beta > 1.0:
            raise PFARuntimeException("beta is out of range")
        if gamma < 0.0 or gamma > 1.0:
            raise PFARuntimeException("gamma is out of range")

        level_prev = theState["level"]
        trend_prev = theState["trend"]
        cycle_unrotated = theState["cycle"]
        if len(cycle_unrotated) == 0:
            raise PFARuntimeException("empty cycle")
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

    def genpy(self, paramTypes, args):
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

        return "self.f[{}]({}, {})".format(repr(self.name), ", ".join(["state", "scope", repr(paramTypes)] + args), hasCycle)

    def __call__(self, state, scope, paramTypes, theState, hasPeriodic):
        level = theState["level"]
        trend = theState["trend"]

        if not hasPeriodic:
            return level + trend
        else:
            cycle = theState["cycle"]
            L = len(cycle)

            if theState["multiplicative"]:
                return (level + trend) * cycle[1 % L]
            else:
                return level + trend + cycle[1 % L]

provide(Forecast1HoltWinters())

class ForecastHoltWinters(LibFcn):
    name = prefix + "forecastHoltWinters"
    sig = Sig([{"n": P.Int()}, {"state": P.WildRecord("A", {"level": P.Double(), "trend": P.Double()})}], P.Array(P.Double()))

    def genpy(self, paramTypes, args):
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

        return "self.f[{}]({}, {})".format(repr(self.name), ", ".join(["state", "scope", repr(paramTypes)] + args), hasCycle)

    def __call__(self, state, scope, paramTypes, n, theState, hasPeriodic):
        level = theState["level"]
        trend = theState["trend"]

        if not hasPeriodic:
            return [level + i*trend for i in xrange(1, n + 1)]
        else:
            cycle = theState["cycle"]
            L = len(cycle)

            if theState["multiplicative"]:
                return [(level + i*trend) * cycle[1 % L] for i in xrange(1, n + 1)]
            else:
                return [level + i*trend + cycle[1 % L] for i in xrange(1, n + 1)]

provide(ForecastHoltWinters())

