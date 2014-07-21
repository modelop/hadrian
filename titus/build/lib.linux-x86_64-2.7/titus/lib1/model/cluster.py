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
