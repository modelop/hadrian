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
from titus.util import callfcn, div
import titus.P as P
import math
tiny = 2.2250738585072014e-308

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "model.naive."

####################################################################
class Gaussian(LibFcn):
    name = prefix + "gaussian"
    sig = Sigs([Sig([{"datum": P.Array(P.Double())}, {"classModel": P.Array(P.WildRecord("C", {"mean": P.Double(), "variance": P.Double()}))}], P.Double()),
                Sig([{"datum": P.Map(P.Double())}, {"classModel": P.Map(P.WildRecord("C", {"mean": P.Double(), "variance": P.Double()}))}], P.Double())])
    errcodeBase = 10000
    def __call__(self, state, scope, pos, paramTypes, datum, classModel):
        ll = 0.0
        if isinstance(datum, list) or isinstance(datum, tuple):
            if len(datum) != len(classModel):
                raise PFARuntimeException("datum and classModel misaligned", self.errcodeBase + 0, self.name, pos)
            for i, x in enumerate(datum):
                mu   = classModel[i]["mean"]
                vari = classModel[i]["variance"]
                if vari <= 0.0:
                    raise PFARuntimeException("variance less than or equal to zero", self.errcodeBase + 1, self.name, pos)
                ll += -0.5*math.log(2.*math.pi * vari)
                ll += -0.5*((x - mu)**2 / vari)
            return ll
        else:
            datumkeys = datum.keys()
            modelkeys = classModel.keys()
            if set(datumkeys) != set(modelkeys):
                raise PFARuntimeException("datum and classModel misaligned", self.errcodeBase + 0, self.name, pos)
            for feature in datumkeys:
                x    = datum[feature]
                mu   = classModel[feature]["mean"]
                vari = classModel[feature]["variance"]
                if vari <= 0.0:
                    raise PFARuntimeException("variance less than or equal to zero", self.errcodeBase + 1, self.name, pos)
                ll += -0.5*math.log(2.*math.pi * vari)
                ll += -0.5*((x - mu)**2 / vari)
            return ll
provide(Gaussian())


####################################################################
class Multinomial(LibFcn):
    name = prefix + "multinomial"
    sig = Sigs([Sig([{"datum": P.Array(P.Double())}, {"classModel": P.Array(P.Double())}], P.Double()),
                Sig([{"datum": P.Map(P.Double())}, {"classModel": P.Map(P.Double())}], P.Double()),
                Sig([{"datum": P.Array(P.Double())}, {"classModel": P.WildRecord("C", {"values": P.Array(P.Double())})}], P.Double()),
                Sig([{"datum": P.Map(P.Double())}, {"classModel": P.WildRecord("C", {"values": P.Map(P.Double())})}], P.Double())])

    errcodeBase = 10010

    def doarray(self, datum, classModel, pos):
        ll = 0.0
        normalizing = sum(classModel)
        if len(classModel) == 0 or not all(x > 0.0 for x in classModel):
            raise PFARuntimeException("classModel must be non-empty and strictly positive", self.errcodeBase + 1, self.name, pos)
        if len(datum) != len(classModel):
            raise PFARuntimeException("datum and classModel misaligned", self.errcodeBase + 0, self.name, pos)
        for d, p in zip(datum, classModel):
            p = p/normalizing + tiny
            ll += d * math.log(p)
        return ll

    def domap(self, datum, classModel, pos):
        ll = 0.0
        datumkeys = datum.keys()
        modelkeys = classModel.keys()
        normalizing = sum(classModel.values())
        if len(classModel) == 0 or not all(x > 0.0 for x in classModel.values()):
            raise PFARuntimeException("classModel must be non-empty and strictly positive", self.errcodeBase + 1, self.name, pos)
        if set(datumkeys) != set(modelkeys):
            raise PFARuntimeException("datum and classModel misaligned", self.errcodeBase + 0, self.name, pos)
        for d in datumkeys:
            p = classModel[d]/normalizing + tiny
            ll += datum[d]*math.log(classModel[d])
        return ll

    def __call__(self, state, scope, pos, paramTypes, datum, classModel):
        if paramTypes[1]["type"] == "record":
            classModel = classModel["values"]
        if paramTypes[0]["type"] == "array":
            return self.doarray(datum, classModel, pos)
        elif paramTypes[0]["type"] == "map":
            return self.domap(datum, classModel, pos)

provide(Multinomial())


####################################################################
class Bernoulli(LibFcn):
    name = prefix + "bernoulli"
    sig = Sigs([Sig([{"datum": P.Array(P.String())}, {"classModel": P.Map(P.Double())}], P.Double()),
                Sig([{"datum": P.Array(P.String())}, {"classModel": P.WildRecord("C", {"values": P.Map(P.Double())})}], P.Double())])
          
    errcodeBase = 10020
    def __call__(self, state, scope, pos, paramTypes, datum, classModel):
        if paramTypes[1]["type"] == "record":
            classModel = classModel["values"]

        ll = 0.0
        for v in classModel.values():
            if (v <= 0.0) or (v >= 1.0):
                raise PFARuntimeException("probability in classModel cannot be less than 0 or greater than 1", self.errcodeBase + 0, self.name, pos)
            ll += math.log(1.0 - v)
        for item in datum:
            p = classModel.get(item, None)
            if p is not None:
                ll += math.log(p) - math.log(1.0 - p)
        return ll

provide(Bernoulli())
