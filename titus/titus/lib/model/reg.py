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
from titus.signature import Lifespan
from titus.signature import PFAVersion
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, div, flatten
import titus.P as P
from titus.lib.array import argLowestN
from titus.lib.prob.dist import Chi2Distribution

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
    errcodeBase = 31000
    def __call__(self, state, scope, pos, paramTypes, datum, model):
        coeffType = [x["type"] for x in paramTypes[1]["fields"] if x["name"] == "coeff"][0]

        if coeffType == {'items': 'double', 'type': 'array'}: #sig1
            coeff = model["coeff"] + [model["const"]]
            datum = np().array(datum + [1.0])
            if len(datum) != len(coeff):
                raise PFARuntimeException("misaligned coeff", self.errcodeBase + 0, self.name, pos)
            return float(np().dot(coeff, datum))

        elif coeffType == {'items': {'items': 'double', 'type': 'array'}, 'type': 'array'}: #sig2
            if len(model["const"]) != len(model["coeff"]):
                raise PFARuntimeException("misaligned const", self.errcodeBase + 1, self.name, pos)
            elif any(len(row) != len(datum) for row in model["coeff"]):
                raise PFARuntimeException("misaligned coeff", self.errcodeBase + 0, self.name, pos)
            elif len(datum) == 0:
                return model["const"]
            elif len(model["const"]) == 0:
                return []
            else:
                coeff = np().array(model["coeff"])
                const = np().array(model["const"])
                datum = np().array(datum + [1.0])
                coeff = np().vstack((coeff.T, const))
                return map(float, np().dot(coeff.T, datum))

        elif coeffType == {'values': 'double', 'type': 'map'}: #sig3
            coeff = model["coeff"]
            const = model["const"]
            out = 0.0
            for key in set(datum.keys() + coeff.keys()):
                out += datum.get(key, 0.0) * coeff.get(key, 0.0)
            return float(out + const)

        else:
            coeff = model["coeff"]
            const = model["const"]
            outMap = {}

            innerKeys = set(datum.keys() + sum([x.keys() for x in coeff.values()], []))
            outerKeys = set(const.keys() + coeff.keys())

            for outerKey in outerKeys:
                out = 0.0
                for innerKey in innerKeys:
                    out += datum.get(innerKey, 0.0) * coeff.get(outerKey, {}).get(innerKey, 0.0)
                outMap[outerKey] = float(out + const.get(outerKey, 0.0))
            return outMap

provide(Linear())

class LinearVariance(LibFcn):
    name = prefix + "linearVariance"
    sig = Sigs([Sig([{"datum": P.Array(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Array(P.Array(P.Double()))})}], P.Double()),
                Sig([{"datum": P.Array(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Array(P.Array(P.Array(P.Double())))})}], P.Array(P.Double())),
                Sig([{"datum": P.Map(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Map(P.Map(P.Double()))})}], P.Double()),
                Sig([{"datum": P.Map(P.Double())}, {"model": P.WildRecord("M", {"covar": P.Map(P.Map(P.Map(P.Double())))})}], P.Map(P.Double()))])
    errcodeBase = 31010
    def __call__(self, state, scope, pos, paramTypes, datum, model):
        covarType = [x["type"] for x in paramTypes[1]["fields"] if x["name"] == "covar"][0]

        if covarType == {"type": "array", "items": {"type": "array", "items": "double"}}:  # sig1
            datum = datum + [1.0]
            covar = model["covar"]
            if len(datum) != len(covar) or any(len(datum) != len(x) for x in covar):
                raise PFARuntimeException("misaligned covariance", self.errcodeBase + 0, self.name, pos)
            x = np().matrix([datum])
            C = np().matrix(covar)
            return float(x.dot(C.dot(x.T))[0][0])

        elif covarType == {"type": "array", "items": {"type": "array", "items": {"type": "array", "items": "double"}}}:  # sig2
            datum = datum + [1.0]
            x = np().matrix([datum])
            out = []
            for covar in model["covar"]:
                if len(datum) != len(covar) or any(len(datum) != len(x) for x in covar):
                    raise PFARuntimeException("misaligned covariance", self.errcodeBase + 0, self.name, pos)
                C = np().matrix(covar)
                out.append(float(x.dot(C.dot(x.T))[0][0]))
            return out

        elif covarType == {"type": "map", "values": {"type": "map", "values": "double"}}:  # sig3
            datum = dict(list(datum.items()) + [("", 1.0)])
            covar = model["covar"]
            keys = list(set(datum.keys() + sum([x.keys() for x in covar.values()], [])))
            x = np().matrix([[datum.get(k, 0.0) for k in keys]])
            C = np().matrix([[covar.get(i, {}).get(j, 0.0) for j in keys] for i in keys])
            return float(x.dot(C.dot(x.T))[0][0])

        else:
            datum = dict(list(datum.items()) + [("", 1.0)])
            covar = model["covar"]
            keys = list(set(datum.keys() + sum(flatten([[x.keys() for x in row.values()] for row in covar.values()]), [])))
            x = np().matrix([[datum.get(k, 0.0) for k in keys]])
            out = {}
            for depkey, row in covar.items():
                C = np().matrix([[row.get(i, {}).get(j, 0.0) for j in keys] for i in keys])
                out[depkey] = float(x.dot(C.dot(x.T))[0][0])
            return out

provide(LinearVariance())

class GaussianProcess(LibFcn):
    name = prefix + "gaussianProcess"
    sig = Sigs([Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Double()}))},
                     {"krigingWeight": P.Union([P.Null(), P.Double()])},
                     {"kernel": P.Fcn([P.Array(P.Double()), P.Array(P.Double())], P.Double())}], P.Double()),

                Sig([{"x": P.Double()}, {"table": P.Array(P.WildRecord("R", {"x": P.Double(), "to": P.Array(P.Double())}))},
                     {"krigingWeight": P.Union([P.Null(), P.Double()])},
                     {"kernel": P.Fcn([P.Array(P.Double()), P.Array(P.Double())], P.Double())}], P.Array(P.Double())),

                Sig([{"x": P.Array(P.Double())}, {"table": P.Array(P.WildRecord("R", {"x": P.Array(P.Double()), "to": P.Double()}))},
                     {"krigingWeight": P.Union([P.Null(), P.Double()])},
                     {"kernel": P.Fcn([P.Array(P.Double()), P.Array(P.Double())], P.Double())}], P.Double()),

                Sig([{"x": P.Array(P.Double())}, {"table": P.Array(P.WildRecord("R", {"x": P.Array(P.Double()), "to": P.Array(P.Double())}))},
                     {"krigingWeight": P.Union([P.Null(), P.Double()])},
                     {"kernel": P.Fcn([P.Array(P.Double()), P.Array(P.Double())], P.Double())}], P.Array(P.Double()))])

    errcodeBase = 31080

    def genpy(self, paramTypes, args, pos):
        toType = None
        for x in paramTypes[1].items.fields:
            if x.name == "to":
                toType = x.avroType
        for x in paramTypes[1].items.fields:
            if x.name == "sigma":
                if not toType.accepts(x.avroType) or not x.avroType.accepts(toType):
                    raise PFASemanticException(self.name + " is being given a table record in which the \"sigma\" field does not have the same type as the \"to\" field: " + str(x.avroType), None)
        return LibFcn.genpy(self, paramTypes, args, pos)

    def getbeta(self, krigingWeight, pos):
        if isinstance(krigingWeight, dict):
            beta = krigingWeight["double"]
            if math.isnan(beta) or math.isinf(beta):
                raise PFARuntimeException("krigingWeight is not finite", self.errcodeBase + 7, self.name, pos)
        else:
            beta = krigingWeight

    def getnoutputs(self, x, table, paramTypes, pos):
        if isinstance(paramTypes[-1], dict) and paramTypes[-1].get("type") == "array":
            n_outputs = len(table[0]["to"])
            if n_outputs < 1:
                raise PFARuntimeException("table outputs must have at least 1 dimension", self.errcodeBase + 3, self.name, pos)

            if any(len(t["to"]) != n_outputs for t in table):
                raise PFARuntimeException("table outputs must all have the same number of dimensions", self.errcodeBase + 4, self.name, pos)

            if isinstance(x, (list, tuple)):
                if any(math.isnan(xi) or math.isinf(xi) for xi in x):
                    raise PFARuntimeException("x is not finite", self.errcodeBase + 5, self.name, pos)
            else:
                if math.isnan(x) or math.isinf(x):
                    raise PFARuntimeException("x is not finite", self.errcodeBase + 5, self.name, pos)

            if any(any(math.isnan(ti) or math.isinf(ti) for ti in t["to"]) for t in table):
                raise PFARuntimeException("table value is not finite", self.errcodeBase + 6, self.name, pos)

        else:
            if isinstance(x, (list, tuple)):
                if any(math.isnan(xi) or math.isinf(xi) for xi in x):
                    raise PFARuntimeException("x is not finite", self.errcodeBase + 5, self.name, pos)
            else:
                if math.isnan(x) or math.isinf(x):
                    raise PFARuntimeException("x is not finite", self.errcodeBase + 5, self.name, pos)

            if any(math.isnan(t["to"]) or math.isinf(t["to"]) for t in table):
                raise PFARuntimeException("table value is not finite", self.errcodeBase + 6, self.name, pos)
            n_outputs = None

        return n_outputs

    def __call__(self, state, scope, pos, paramTypes, x, table, krigingWeight, kernel):
        def kern(xvector, yvector):
            return callfcn(state, scope, kernel, [xvector, yvector])

        n_samples = len(table)
        if n_samples < 1:
            raise PFARuntimeException("table must have at least 1 entry", self.errcodeBase + 0, self.name, pos)
                
        if isinstance(x, (list, tuple)):
            n_features = len(x)
            if n_features < 1:
                raise PFARuntimeException("x must have at least 1 feature", self.errcodeBase + 1, self.name, pos)
            if any(len(t["x"]) != n_features for t in table):
                raise PFARuntimeException("table must have the same number of features as x", self.errcodeBase + 2, self.name, pos)

            n_outputs = self.getnoutputs(x, table, paramTypes, pos)

            if any(any(math.isnan(xi) or math.isinf(xi) for xi in t["x"]) for t in table):
                raise PFARuntimeException("table value is not finite", self.errcodeBase + 6, self.name, pos)

            beta = self.getbeta(krigingWeight, pos)

            X = np().array([t["x"] for t in table])
            
            if n_outputs is None:
                y = np().array([t["to"] for t in table])
                if "sigma" in table[0]:
                    nugget = np().array([(t["sigma"]/t["to"])**2 if t["to"] != 0.0 else float("inf") for t in table])
                else:
                    nugget = 10.0 * np().finfo(np().double).eps

                beta, gamma = self.fit(X, y, beta, nugget, kern, pos)
                return self.predict(x, X, y, beta, gamma, kern)

            else:
                out = [None] * n_outputs
                for i in xrange(n_outputs):
                    y = np().array([t["to"][i] for t in table])
                    if "sigma" in table[0]:
                        nugget = np().array([(t["sigma"][i]/t["to"][i])**2 if t["to"][i] != 0.0 else float("inf") for t in table])
                    else:
                        nugget = 10.0 * np().finfo(np().double).eps

                    beta, gamma = self.fit(X, y, beta, nugget, kern, pos)
                    out[i] = self.predict(x, X, y, beta, gamma, kern)

                return out

        else:
            n_outputs = self.getnoutputs(x, table, paramTypes, pos)

            if any(math.isnan(t["x"]) or math.isinf(t["x"]) for t in table):
                raise PFARuntimeException("table value is not finite", self.errcodeBase + 6, self.name, pos)

            beta = self.getbeta(krigingWeight, pos)

            X = np().array([[t["x"]] for t in table])

            if n_outputs is None:
                y = np().array([t["to"] for t in table])
                if "sigma" in table[0]:
                    nugget = np().array([(t["sigma"]/t["to"])**2 if t["to"] != 0.0 else float("inf") for t in table])
                else:
                    nugget = 10.0 * np().finfo(np().double).eps

                beta, gamma = self.fit(X, y, beta, nugget, kern, pos)
                return self.predict(x, X, y, beta, gamma, kern)

            else:
                out = [None] * n_outputs
                for i in xrange(n_outputs):
                    y = np().array([t["to"][i] for t in table])
                    if "sigma" in table[0]:
                        nugget = np().array([(t["sigma"][i]/t["to"][i])**2 if t["to"][i] != 0.0 else float("inf") for t in table])
                    else:
                        nugget = 10.0 * np().finfo(np().double).eps

                    beta, gamma = self.fit(X, y, beta, nugget, kern, pos)
                    out[i] = self.predict(x, X, y, beta, gamma, kern)
                return out

    def fit(self, X, y, beta, nugget, kern, pos):
        n_samples, n_features = X.shape

        Xnorm = (X - X.mean(axis=0))/X.std(axis=0)
        ynorm = (y - y.mean(axis=0))/y.std(axis=0)

        n_nonzero_cross_dist = n_samples * (n_samples - 1) / 2
        ij = np().zeros((n_nonzero_cross_dist, 2), dtype=np().int)
        r = np().zeros(n_nonzero_cross_dist)
        ll_1 = 0
        for k in range(n_samples - 1):
            ll_0 = ll_1
            ll_1 = ll_0 + n_samples - k - 1
            ij[ll_0:ll_1, 0] = k
            ij[ll_0:ll_1, 1] = np().arange(k + 1, n_samples)
            r[ll_0:ll_1] = [kern(Xnorm[k].tolist(), XnormOther.tolist()) for XnormOther in Xnorm[(k + 1):n_samples]]

        ij = ij.astype(np().int)

        R = np().eye(n_samples) * (1. + nugget)
        R[ij[:, 0], ij[:, 1]] = r
        R[ij[:, 1], ij[:, 0]] = r

        try:
            C = np().linalg.cholesky(R)
        except numpy.linalg.linalg.LinAlgError:
            raise PFARuntimeException("matrix of kernel results is not positive definite", self.errcodeBase + 8, self.name, pos)

        F = np().array([[1.0]] * len(X))
        Ft = np().linalg.solve(C, F)

        Q, G = np().linalg.qr(Ft)
        Yt = np().linalg.solve(C, ynorm)

        # beta can be computed from universal Kriging or a given
        if beta is None:
            beta = np().linalg.solve(G, np().dot(Q.T, Yt))
        else:
            beta = np().array([beta])

        rho = Yt - np().dot(Ft, beta)

        gamma = np().linalg.solve(C.T, rho)

        return float(beta[0]), gamma.ravel()

    def predict(self, x_pred, X, y, beta, gamma, kern):
        Xmean = X.mean(axis=0)
        Xstd = X.std(axis=0)
        Xnorm = (X - Xmean) / Xstd

        x_pred_norm = (np().array(x_pred) - Xmean) / Xstd

        y_norm = beta
        for sampleX, g in zip(Xnorm, gamma):
            r = kern(x_pred_norm.tolist(), sampleX.tolist())
            y_norm += r * g

        return y.mean() + y.std() * y_norm

provide(GaussianProcess())







############################################################# deprecated stuff (moved to stat.test.*)

class Residual(LibFcn):
    name = prefix + "residual"
    sig = Sigs([Sig([{"observation": P.Double()}, {"prediction": P.Double()}], P.Double(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.residual instead")),
                Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}], P.Array(P.Double()), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.residual instead")),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}], P.Map(P.Double()), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.residual instead"))])
    errcodeBase = 31020
    def __call__(self, state, scope, pos, paramTypes, observation, prediction):
        if isinstance(observation, dict):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            result = {}
            for k, o in observation.items():
                try:
                    result[k] = o - prediction[k]
                except KeyError:
                    raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            return result

        elif isinstance(observation, (tuple, list)):
            try:
                result = [float(o - p) for o, p in zip(observation, prediction)]
            except:
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            return result

        else:
            return float(observation - prediction)

provide(Residual())

class Pull(LibFcn):
    name = prefix + "pull"
    sig = Sigs([Sig([{"observation": P.Double()}, {"prediction": P.Double()}, {"uncertainty": P.Double()}], P.Double(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.pull instead")),
                Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}, {"uncertainty": P.Array(P.Double())}], P.Array(P.Double()), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.pull instead")),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}, {"uncertainty": P.Map(P.Double())}], P.Map(P.Double()), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.pull instead"))])
    errcodeBase = 31030
    def __call__(self, state, scope, pos, paramTypes, observation, prediction, uncertainty):
        if isinstance(observation, dict):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            if len(observation) != len(uncertainty):
                raise PFARuntimeException("misaligned uncertainty", self.errcodeBase + 1, self.name, pos)
            result = {}
            for k, o in observation.items():
                try:
                    p = prediction[k]
                except KeyError:
                    raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
                try:
                    u = uncertainty[k]
                except KeyError:
                    raise PFARuntimeException("misaligned uncertainty", self.errcodeBase + 1, self.name, pos)
                try:
                    result[k] = (o - p) / u
                except ZeroDivisionError:
                    result[k] = float("nan")
            return result

        elif isinstance(observation, (tuple, list)):
            if len(observation) != len(prediction):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            if len(observation) != len(uncertainty):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 0, self.name, pos)
            return [float((o - p)/u) for o, p, u in zip(observation, prediction, uncertainty)]

        else:
            return float((observation - prediction)/uncertainty)

provide(Pull())

class Mahalanobis(LibFcn):
    name = prefix + "mahalanobis"
    sig = Sigs([Sig([{"observation": P.Array(P.Double())}, {"prediction": P.Array(P.Double())}, {"covariance": P.Array(P.Array(P.Double()))}], P.Double(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.mahalanobis instead")),
                Sig([{"observation": P.Map(P.Double())}, {"prediction": P.Map(P.Double())}, {"covariance": P.Map(P.Map(P.Double()))}], P.Double(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.mahalanobis instead"))])
    errcodeBase = 31040
    def __call__(self, state, scope, pos, paramTypes, observation, prediction, covariance):
        if isinstance(observation, (tuple, list)):
            if (len(observation) < 1):
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if (len(observation) != len(prediction)):
                raise PFARuntimeException("misaligned prediction", self.errcodeBase + 1, self.name, pos)
            if (not all(len(i)==len(covariance[0]) for i in covariance)) and (len(covariance) != len(covariance[0])):
                raise PFARuntimeException("misaligned covariance", self.errcodeBase + 2, self.name, pos)
            x = np().array([(o - p) for o, p in zip(observation, prediction)])
            C = np().array(covariance)
        else:
            if (len(observation) < 1):
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if (len(observation) != len(prediction)):
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
        return float(np().sqrt(np().linalg.solve(C, x).T.dot(x)))
provide(Mahalanobis())


class UpdateChi2(LibFcn):
    name = prefix + "updateChi2"
    sig = Sigs([Sig([{"pull": P.Double()}, {"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Wildcard("A"), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.updateChi2 instead")),
                Sig([{"pull": P.Array(P.Double())}, {"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Wildcard("A"), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.updateChi2 instead")),
                Sig([{"pull": P.Map(P.Double())}, {"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Wildcard("A"), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.updateChi2 instead"))])
    errcodeBase = 31050
    def __call__(self, state, scope, pos, paramTypes, pull, state_):
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
    sig = Sig([{"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Double(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.reducedChi2 instead"))
    errcodeBase = 31060
    def __call__(self, state, scope, pos, paramTypes, state_):
        return float(state_["chi2"]/state_["DOF"])
provide(ReducedChi2())

class Chi2Prob(LibFcn):
    name = prefix + "chi2Prob"
    sig = Sig([{"state_": P.WildRecord("A", {"chi2": P.Double(), "DOF": P.Int()})}], P.Double(), Lifespan(None, PFAVersion(0, 7, 2), PFAVersion(0, 9, 0), "use test.chi2Prob instead"))
    errcodeBase = 31070
    def __call__(self, state, scope, pos, paramTypes, state_):
        return float(Chi2Distribution(state_["DOF"], self.name, self.errcodeBase, pos).CDF(state_["chi2"]))
provide(Chi2Prob())


