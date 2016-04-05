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

def np():
    import numpy
    return numpy

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import div
from titus.lib.core import INT_MIN_VALUE, INT_MAX_VALUE, LONG_MIN_VALUE, LONG_MAX_VALUE
import titus.P as P
from titus.lib.prob.dist import Chi2Distribution

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "stat.test."

class KSTwoSample(LibFcn):
    name = prefix + "kolmogorov"
    sig = Sig([{"x": P.Array(P.Double())}, {"y": P.Array(P.Double())}], P.Double())
    errcodeBase = 38000
    def __call__(self, state, scope, pos, paramTypes, x, y):
        x = sorted([xi for xi in x if not math.isnan(xi)])
        y = sorted([yi for yi in y if not math.isnan(yi)])
        n1, n2 = len(x), len(y)
        if (x == y):
            return 1.0
        elif ((len(x) == 0) or (len(y) == 0)):
            return 0.0
        else:
            j1 = j2 = 0
            fn1 = fn2 = d = 0.0
            while ((j1 < n1) and (j2 < n2)):
                d1 = x[j1]
                d2 = y[j2]
                if d1 <= d2:
                    j1 += 1
                    fn1 = float(j1)/n1
                if d2 <= d1:
                    j2 += 1
                    fn2 = float(j2)/n2
                dt = abs(fn2 - fn1)
                if dt > d:
                    d = dt
            en = math.sqrt((n1 * n2)/float(n1 + n2))
            stat = (en + 0.12 + 0.11/en)*d
            return 1.0 - kolomogorov_cdf(stat)
provide(KSTwoSample())

def kolomogorov_cdf(z):
    """
    Function to compute the cdf of the Kolomogorov distribution.  Used by other
        variants of KS test
    """
    L = math.sqrt(2.0 * math.pi) * (1.0/z)
    sumpart = 0.0
    for v in range(1, 150):
        sumpart += math.exp(-((2.0*v - 1.0)**2)*((math.pi**2)/(8.0*(z**2))))
    return L*sumpart

class Residual(LibFcn):
    name = prefix + "residual"
    sig = Sigs([Sig([{"observation": P.Double()}, {"prediction": P.Double()}], P.Double()),
                Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}], P.Map(P.Double()))])
    errcodeBase = 38010
    def __call__(self, state, scope, pos, paramTypes, observation, prediction):
        if isinstance(observation, dict):
            if set(observation.keys()) != set(prediction.keys()):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            return dict((k, observation[k] - prediction[k]) for k in observation)

        elif isinstance(observation, (tuple, list)):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            return [float(o - p) for o, p in zip(observation, prediction)]

        else:
            return float(observation - prediction)

provide(Residual())

class Pull(LibFcn):
    name = prefix + "pull"
    sig = Sigs([Sig([{"observation": P.Double()}, {"prediction": P.Double()}, {"uncertainty": P.Double()}], P.Double()),
                Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}, {"uncertainty": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}, {"uncertainty": P.Map(P.Double())}], P.Map(P.Double()))])
    errcodeBase = 38020
    def __call__(self, state, scope, pos, paramTypes, observation, prediction, uncertainty):
        if isinstance(observation, dict):
            if set(observation.keys()) != set(prediction.keys()):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            if set(observation.keys()) != set(uncertainty.keys()):
                raise PFARuntimeException("misaligned uncertainty", self.errcodeBase + 1, self.name, pos)
            return dict((k, div(observation[k] - prediction[k], uncertainty[k])) for k in observation)

        elif isinstance(observation, (tuple, list)):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            if len(observation) != len(uncertainty):
                raise PFARuntimeException("misaligned uncertainty", self.errcodeBase + 1, self.name, pos)
            return [float(div(o - p, u)) for o, p, u in zip(observation, prediction, uncertainty)]

        else:
            return div(observation - prediction, uncertainty)

provide(Pull())

class Mahalanobis(LibFcn):
    name = prefix + "mahalanobis"
    sig = Sigs([Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())},
                     {"covariance": P.Array(P.Array(P.Double()))}], P.Double()),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())},
                     {"covariance": P.Map(P.Map(P.Double()))}], P.Double())])
    errcodeBase = 38030
    def __call__(self, state, scope, pos, paramTypes, observation, prediction, covariance):
        if isinstance(observation, (tuple, list)):
            if len(observation) < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 1, self.name, pos)
            if len(covariance) != len(observation) or any(len(x) != len(observation) for x in covariance):
                raise PFARuntimeException("misaligned covariance", self.errcodeBase + 2, self.name, pos)
            x = np().array([(o - p) for o, p in zip(observation, prediction)])
            C = np().array(covariance)
        else:
            if len(observation) < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if (set(observation.keys()) != set(prediction.keys())):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 1, self.name, pos)
            # use observation keys throughout
            keys = observation.keys()
            try:
                x = np().array([observation[key] - prediction[key] for key in keys])
            except:
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 1, self.name, pos)
            C = np().empty((len(keys), len(keys)))
            try:
                for i,k1 in enumerate(keys):
                    for j,k2 in enumerate(keys):
                        C[i,j] = float(covariance[k1][k2])
            except:
                raise PFARuntimeException("misaligned covariance", self.errcodeBase + 2, self.name, pos)
        resultSquared = float(np().linalg.solve(C, x).T.dot(x))
        if resultSquared < 0:
            return float("nan")
        else:
            return math.sqrt(resultSquared)
provide(Mahalanobis())


class UpdateChi2(LibFcn):
    name = prefix + "updateChi2"
    sig = Sigs([Sig([{"pull": P.Double()}, {"state": P.WildRecord("A", {"chi2": P.Double(), "dof": P.Int()})}], P.Wildcard("A")),
                Sig([{"pull": P.Array(P.Double())}, {"state": P.WildRecord("A", {"chi2": P.Double(), "dof": P.Int()})}], P.Wildcard("A")),
                Sig([{"pull": P.Map(P.Double())}, {"state": P.WildRecord("A", {"chi2": P.Double(), "dof": P.Int()})}], P.Wildcard("A"))])
    errcodeBase = 38040
    def __call__(self, state, scope, pos, paramTypes, pull, state_):
        if isinstance(pull, float):
            return dict(state_, chi2=(state_["chi2"] + pull*pull), dof=(state_["dof"] + 1))
        elif isinstance(pull, (tuple, list)):
            return dict(state_, chi2=(state_["chi2"] + sum([y**2 for y in pull])), dof=(state_["dof"] + 1))
        else:
            return dict(state_, chi2=(state_["chi2"] + sum([y**2 for y in pull.values()])), dof=(state_["dof"] + 1))
provide(UpdateChi2())

class ReducedChi2(LibFcn):
    name = prefix + "reducedChi2"
    sig = Sig([{"state": P.WildRecord("A", {"chi2": P.Double(), "dof": P.Int()})}], P.Double())
    errcodeBase = 38050
    def __call__(self, state, scope, pos, paramTypes, state_):
        return div(state_["chi2"], state_["dof"])
provide(ReducedChi2())

class Chi2Prob(LibFcn):
    name = prefix + "chi2Prob"
    sig = Sig([{"state": P.WildRecord("A", {"chi2": P.Double(), "dof": P.Int()})}], P.Double())
    errcodeBase = 38060
    def __call__(self, state, scope, pos, paramTypes, state_):
        chi2 = state_["chi2"]
        dof = state_["dof"]
        if dof < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif dof == 0:
            if chi2 > 0:
                return 1.0
            else:
                return 0.0
        elif math.isnan(chi2):
            return float("nan")
        elif math.isinf(chi2):
            if chi2 > 0:
                return 1.0
            else:
                return 0.0
        else:
            return float(Chi2Distribution(dof, self.errcodeBase + 0, self.name, pos).CDF(chi2))
provide(Chi2Prob())
