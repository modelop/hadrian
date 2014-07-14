#!/usr/bin/env python

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn
import titus.P as P
from titus.lib1.array import argLowestN

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "model.cluster."

#################################################################### 

class Closest(LibFcn):
    name = prefix + "closest"
    sig = Sig([{"datum": P.Array(P.Wildcard("A"))}, {"clusters": P.Array(P.WildRecord("C", {"center": P.Array(P.Wildcard("B"))}))}, {"metric": P.Fcn([P.Array(P.Wildcard("A")), P.Array(P.Wildcard("B"))], P.Double())}], P.Wildcard("C"))
    def __call__(self, state, scope, paramTypes, datum, clusters, metric):
        if len(clusters) == 0:
            raise PFARuntimeException("no clusters")
        distances = [callfcn(state, scope, metric, [datum, x["center"]]) for x in clusters]
        index, = argLowestN(distances, 1, lambda a, b: a < b)
        return clusters[index]
provide(Closest())

class ClosestN(LibFcn):
    name = prefix + "closestN"
    sig = Sig([{"datum": P.Array(P.Wildcard("A"))}, {"clusters": P.Array(P.WildRecord("C", {"center": P.Array(P.Wildcard("B"))}))}, {"metric": P.Fcn([P.Array(P.Wildcard("A")), P.Array(P.Wildcard("B"))], P.Double())}, {"n": P.Int()}], P.Array(P.Wildcard("C")))
    def __call__(self, state, scope, paramTypes, datum, clusters, metric, n):
        distances = [callfcn(state, scope, metric, [datum, x["center"]]) for x in clusters]
        indexes = argLowestN(distances, n, lambda a, b: a < b)
        return [clusters[i] for i in indexes]
provide(ClosestN())
