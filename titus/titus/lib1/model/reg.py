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

def np():
    import numpy
    return numpy

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, div
import titus.P as P
from titus.lib1.array import argLowestN
from titus.lib1.prob.dist import Chi2Distribution

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "model.reg."

#################################################################### 

class Linear(LibFcn):
    name = prefix + "linear"
    sig  = Sigs([Sig([{"datum": P.Array(P.Double())}, {"model": P.WildRecord("M", {"coeff": P.Array(P.Double()), "const": P.Double()})}], P.Double()),
                 Sig([{"datum": P.Array(P.Double())}, {"model": P.WildRecord("M", {"coeff": P.Array(P.Array(P.Double())), "const": P.Array(P.Double())})}], P.Array(P.Double())),
                 Sig([{"datum": P.Map(P.Double())}, {"model": P.WildRecord("M", {"coeff": P.Map(P.Double()), "const": P.Double()})}], P.Double()),
                 Sig([{"datum": P.Map(P.Double())}, {"model": P.WildRecord("M", {"coeff": P.Map(P.Map(P.Double())), "const": P.Map(P.Double())})}], P.Map(P.Double()))])
    def __call__(self, state, scope, paramTypes, datum, model):
        coeffType = [x["type"] for x in paramTypes[1]["fields"] if x["name"] == "coeff"][0]

        if coeffType == {'items': 'double', 'type': 'array'}: #sig1
            coeff = model["coeff"] + [model["const"]]
            datum = np().array(datum + [1.0])
            if len(datum) != len(coeff):
                raise PFARuntimeException("misaligned coeff")
            return float(np().dot(coeff, datum))

        elif coeffType == {'items': {'items': 'double', 'type': 'array'}, 'type': 'array'}: #sig2
            coeff = np().array(model["coeff"])
            const = np().array(model["const"])
            datum = np().array(datum + [1.0])
            try:
                coeff = np().vstack((coeff.T, const))
            except:
                raise PFARuntimeException('misaligned coeff')
            return map(float, np().dot(coeff.T, datum))

        elif coeffType == {'values': 'double', 'type': 'map'}: #sig3
            coeff = model["coeff"]
            const = model["const"]
            if len(datum) != len(coeff):
                raise PFARuntimeException("misaligned coeff")
            out = 0.0
            try:
                for key in datum.keys():
                    out += datum[key] * coeff[key]
            except:
                raise PFARuntimeException("misaligned const")
            return float(out + const)

        else:
            coeff = model["coeff"]
            const = model["const"]
            outMap = {}
            for ckey in coeff.keys():
                out = 0.0
                for rkey in coeff[ckey].keys():
                    try:
                        out += datum[rkey] * coeff[ckey][rkey]
                    except:
                        raise PFARuntimeException("misaligned coeff")
                try:
                    outMap[ckey] = float(out + const[ckey])
                except:
                    raise PFARuntimeException("misaligned const")
            return outMap
provide(Linear())

class LinearVariance(LibFcn):
    name = prefix + "linearVariance"
    sig = Sigs([Sig([{"datum": P.Array(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Array(P.Array(P.Double()))})}], P.Double()),
                Sig([{"datum": P.Array(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Array(P.Array(P.Array(P.Double())))})}], P.Array(P.Double())),
                Sig([{"datum": P.Map(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Map(P.Map(P.Double()))})}], P.Double()),
                Sig([{"datum": P.Map(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Map(P.Map(P.Map(P.Double())))})}], P.Map(P.Double()))])
    def __call__(self, state, scope, paramTypes, datum, model):
        covarType = [x["type"] for x in paramTypes[1]["fields"] if x["name"] == "covar"][0]

        if covarType == {"type": "array", "items": {"type": "array", "items": "double"}}:  # sig1
            datum = datum + [1.0]
            covar = model["covar"]
            if len(datum) != len(covar) or any(len(datum) != len(x) for x in covar):
                raise PFARuntimeException("misaligned covariance")
            x = np().matrix([datum])
            C = np().matrix(covar)
            return float(x.dot(C.dot(x.T))[0][0])

        elif covarType == {"type": "array", "items": {"type": "array", "items": {"type": "array", "items": "double"}}}:  # sig2
            datum = datum + [1.0]
            x = np().matrix([datum])
            out = []
            for covar in model["covar"]:
                if len(datum) != len(covar) or any(len(datum) != len(x) for x in covar):
                    raise PFARuntimeException("misaligned covariance")
                C = np().matrix(covar)
                out.append(float(x.dot(C.dot(x.T))[0][0]))
            return out

        elif covarType == {"type": "map", "values": {"type": "map", "values": "double"}}:  # sig3
            datum = dict(list(datum.items()) + [("", 1.0)])
            covar = model["covar"]
            keys = list(datum.keys())
            x = np().matrix([[datum[k] for k in keys]])
            try:
                C = np().matrix([[covar[i][j] for j in keys] for i in keys])
            except KeyError:
                raise PFARuntimeException("misaligned covariance")
            return float(x.dot(C.dot(x.T))[0][0])

        else:
            datum = dict(list(datum.items()) + [("", 1.0)])
            keys = list(datum.keys())
            x = np().matrix([[datum[k] for k in keys]])
            out = {}
            for depkey, covar in model["covar"].items():
                try:
                    C = np().matrix([[covar[i][j] for j in keys] for i in keys])
                except KeyError:
                    raise PFARuntimeException("misaligned covariance")
                out[depkey] = float(x.dot(C.dot(x.T))[0][0])
            return out

provide(LinearVariance())


class Residual(LibFcn):
    name = prefix + "residual"
    sig = Sigs([Sig([{"observation": P.Double()}, {"prediction": P.Double()}], P.Double()),
                Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}], P.Map(P.Double()))])
    def __call__(self, state, scope, paramTypes, observation, prediction):
        if isinstance(observation, dict):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction")
            result = {}
            for k, o in observation.items():
                try:
                    result[k] = o - prediction[k]
                except KeyError:
                    raise PFARuntimeException("misaligned prediction")
            return result

        elif isinstance(observation, (tuple, list)):
            try:
                result = [float(o - p) for o, p in zip(observation, prediction)]
            except:
                raise PFARuntimeException("misaligned prediction")
            return result

        else:
            return float(observation - prediction)

provide(Residual())

class Pull(LibFcn):
    name = prefix + "pull"
    sig = Sigs([Sig([{"observation": P.Double()}, {"prediction": P.Double()}, {"uncertainty": P.Double()}], P.Double()),
                Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}, {"uncertainty": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}, {"uncertainty": P.Map(P.Double())}], P.Map(P.Double()))])
    def __call__(self, state, scope, paramTypes, observation, prediction, uncertainty):
        if isinstance(observation, dict):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction")
            if len(observation) != len(uncertainty):
                raise PFARuntimeException("misaligned uncertainty")
            result = {}
            for k, o in observation.items():
                try:
                    p = prediction[k]
                except KeyError:
                    raise PFARuntimeException("misaligned prediction")
                try:
                    u = uncertainty[k]
                except KeyError:
                    raise PFARuntimeException("misaligned uncertainty")
                try:
                    result[k] = (o - p) / u
                except ZeroDivisionError:
                    result[k] = float("nan")
            return result

        elif isinstance(observation, (tuple, list)):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction")
            if len(observation) != len(uncertainty):
                raise PFARuntimeException("misaligned prediction")
            return [float((o - p)/u) for o, p, u in zip(observation, prediction, uncertainty)]

        else:
            return float((observation - prediction)/uncertainty)

provide(Pull())

class Mahalanobis(LibFcn):
    name = prefix + "mahalanobis"
    sig = Sigs([Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())},
                     {"covariance": P.Array(P.Array(P.Double()))}], P.Double()),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())},
                     {"covariance": P.Map(P.Map(P.Double()))}], P.Double())])
    def __call__(self, state, scope, paramTypes, observation, prediction, covariance):
        if isinstance(observation, (tuple, list)):
            if (len(observation) < 1):
                raise PFARuntimeException("too few rows/cols")
            if (len(observation) != len(prediction)):
                raise PFARuntimeException("misaligned prediction")
            if (not all(len(i)==len(covariance[0]) for i in covariance)) and (len(covariance) != len(covariance[0])):
                raise PFARuntimeException("misaligned covariance")
            x = np().array([(o - p) for o, p in zip(observation, prediction)])
            C = np().array(covariance)
        else:
            if (len(observation) < 1):
                raise PFARuntimeException("too few rows/cols")
            if (len(observation) != len(prediction)):
                raise PFARuntimeException("misaligned prediction")
            # use observation keys throughout
            keys = observation.keys()
            try:
                x = np().array([observation[key] - prediction[key] for key in keys])
            except:
                raise PFARuntimeException("misaligned prediction")
            C = np().empty((len(keys), len(keys)))
            try:
                for i,k1 in enumerate(keys):
                    for j,k2 in enumerate(keys):
                        C[i,j] = float(covariance[k1][k2])
            except:
                raise PFARuntimeException("misaligned covariance")
        return float(np().sqrt(np().linalg.solve(C, x).T.dot(x)))
provide(Mahalanobis())


class UpdateChi2(LibFcn):
    name = prefix + "updateChi2"
    sig = Sigs([Sig([{"pull": P.Double()}, {"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Wildcard("A")),
                Sig([{"pull": P.Array(P.Double())}, {"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Wildcard("A")),
                Sig([{"pull": P.Map(P.Double())}, {"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Wildcard("A"))])
    def __call__(self, state, scope, paramTypes, pull, state_):
        if isinstance(pull, float):
            return update(pull*pull, state_)
        elif isinstance(pull, (tuple, list)):
            return update(sum([y**2 for y in pull]), state_)
        else:
            return update(sum([y**2 for y in pull.values()]), state_)
provide(UpdateChi2())
def update(x, state_):
    state_["chi2"] = float(state_["chi2"] + x)
    state_["DOF"]  = float(state_["DOF"] + 1)
    return state_

class ReducedChi2(LibFcn):
    name = prefix + "reducedChi2"
    sig = Sig([{"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Double())
    def __call__(self, state, scope, paramTypes, state_):
        return float(state_["chi2"]/state_["DOF"])
provide(ReducedChi2())

class Chi2Prob(LibFcn):
    name = prefix + "chi2Prob"
    sig = Sig([{"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Double())
    def __call__(self, state, scope, paramTypes, state_):
        return float(Chi2Distribution(state_["DOF"]).CDF(state_["chi2"]))
provide(Chi2Prob())


