#!/usr/bin/env python

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

prefix = "stat.change."

class UpdateTrigger(LibFcn):
    name = prefix + "updateTrigger"
    sig = Sig([{"predicate": P.Boolean()}, {"history": P.Union([P.Null(), P.WildRecord("A", {"numEvents": P.Int(), "numRuns": P.Int(), "currentRun": P.Int(), "longestRun": P.Int()})])}], P.Wildcard("A"))
    def _getRecord(self, paramType):
        for t in paramType.types:
            if not isinstance(t, AvroNull):
                return t

    def __call__(self, state, scope, paramTypes, predicate, history):
        if history is None:
            paramType = state.parser.getAvroType(paramTypes[2])
            for field in self._getRecord(paramType)["fields"]:
                if field["name"] not in ("numEvents", "numRuns", "currentRun", "longestRun"):
                    raise PFARuntimeException("cannot initialize unrecognized fields")
            if predicate:
                return {"numEvents": 1, "numRuns": 1, "currentRun": 1, "longestRun": 1}
            else:
                return {"numEvents": 0, "numRuns": 0, "currentRun": 0, "longestRun": 0}

        else:
            numEvents = history["numEvents"]
            numRuns = history["numRuns"]
            currentRun = history["currentRun"]
            longestRun = history["longestRun"]

            if numEvents < 0 or numRuns < 0 or currentRun < 0 or longestRun < 0:
                raise PFARuntimeException("counter out of range")

            if predicate:
                numEvents += 1
                if currentRun == 0:
                    numRuns += 1
                currentRun += 1
                if currentRun > longestRun:
                    longestRun = currentRun
            else:
                currentRun = 0

            return dict(history, numEvents=numEvents, numRuns=numRuns, currentRun=currentRun, longestRun=longestRun)
provide(UpdateTrigger())

class ZValue(LibFcn):
    name = prefix + "zValue"
    sig = Sig([{"x": P.Double()}, {"meanVariance": P.WildRecord("A", {"count": P.Double(), "mean": P.Double(), "variance": P.Double()})}, {"unbiased": P.Boolean()}], P.Double())
    def __call__(self, state, scope, paramTypes, x, meanVariance, unbiased):
        count = meanVariance["count"]
        mean = meanVariance["mean"]
        variance = meanVariance["variance"]
        if unbiased:
            return ((x - mean)/math.sqrt(variance)) * math.sqrt((count) / (count - 1.0))
        else:
            return ((x - mean)/math.sqrt(variance))
provide(ZValue())

class UpdateCUSUM(LibFcn):
    name = prefix + "updateCUSUM"
    sig = Sig([{"logLikelihoodRatio": P.Double()}, {"last": P.Double()}, {"reset": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, logLikelihoodRatio, last, reset):
        out = logLikelihoodRatio + last
        if out > reset:
            return out
        else:
            return reset
provide(UpdateCUSUM())
