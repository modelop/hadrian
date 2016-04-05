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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
import titus.P as P

# import special functions requred to compute distributions
from titus.lib.spec import logBetaFunction
from titus.lib.spec import incompleteBetaFunction
from titus.lib.spec import inverseIncompleteBetaFunction
from titus.lib.spec import regularizedGammaQ
from titus.lib.spec import regularizedGammaP
from titus.lib.spec import nChooseK

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "prob.dist."

class GaussianLL(LibFcn):
    name = prefix + "gaussianLL"
    sig = Sigs([Sig([{"x": P.Double()}, {"mu": P.Double()}, {"sigma": P.Double()}], P.Double()),
                Sig([{"x": P.Double()}, {"params": P.WildRecord("A", {"mean": P.Double(), "variance": P.Double()})}], P.Double())])
    errcodeBase = 13000
    def __call__(self, state, scope, pos, paramTypes, x, *others):
        if len(others) == 2:
            mu, sigma = others
        else:
            mu = others[0]["mean"]
            if math.isnan(others[0]["variance"]) or others[0]["variance"] < 0.0:
                raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
            else:
                sigma = math.sqrt(others[0]["variance"])
        if math.isinf(mu) or math.isnan(mu) or math.isinf(sigma) or math.isnan(sigma) or sigma < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif sigma == 0.0:
            if x != mu:
                return float("-inf")
            else:
                return float("inf")
        else:
            return GaussianDistribution(mu, sigma, self.errcodeBase + 0, self.name, pos).LL(x)
provide(GaussianLL())

class GaussianCDF(LibFcn):
    name = prefix + "gaussianCDF"
    sig = Sigs([Sig([{"x": P.Double()}, {"mu": P.Double()}, {"sigma": P.Double()}], P.Double()),
                Sig([{"x": P.Double()}, {"params": P.WildRecord("A", {"mean": P.Double(), "variance": P.Double()})}], P.Double())])
    errcodeBase = 13010
    def __call__(self, state, scope, pos, paramTypes, x, *others):
        if len(others) == 2:
            mu, sigma = others
        else:
            mu = others[0]["mean"]
            if math.isnan(others[0]["variance"]) or others[0]["variance"] < 0.0:
                raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
            else:
                sigma = math.sqrt(others[0]["variance"])
        if math.isinf(mu) or math.isnan(mu) or math.isinf(sigma) or math.isnan(sigma) or sigma < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif sigma == 0.0:
            if x < mu:
                return 0.0
            else:
                return 1.0
        else:
            return GaussianDistribution(mu, sigma, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(GaussianCDF())

# written using http://www.johndcook.com/normal_cdf_inverse.html
class GaussianQF(LibFcn):
    name = prefix + "gaussianQF"
    sig = Sigs([Sig([{"p": P.Double()}, {"mu": P.Double()}, {"sigma": P.Double()}], P.Double()),
                Sig([{"p": P.Double()}, {"params": P.WildRecord("A", {"mean": P.Double(), "variance": P.Double()})}], P.Double())])
    errcodeBase = 13020
    def __call__(self, state, scope, pos, paramTypes, p, *others):
        if len(others) == 2:
            mu, sigma = others
        else:
            mu = others[0]["mean"]
            if math.isnan(others[0]["variance"]) or others[0]["variance"] < 0.0:
                raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
            else:
                sigma = math.sqrt(others[0]["variance"])
        if math.isinf(mu) or math.isnan(mu) or math.isinf(sigma) or math.isnan(sigma) or sigma < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1.0:
            return float("inf")
        elif p == 0.0:
            return float("-inf")
        elif sigma == 0.0:
            return mu
        else:
            return GaussianDistribution(mu, sigma, self.errcodeBase + 0, self.name, pos).QF(p)
provide(GaussianQF())

################ Exponential
class ExponentialPDF(LibFcn):
    name = prefix + "exponentialPDF"
    sig = Sig([{"x": P.Double()}, {"lambda": P.Double()}], P.Double())
    errcodeBase = 13030
    def __call__(self, state, scope, pos, paramTypes, x, rate):
        if math.isinf(rate) or math.isnan(rate) or rate < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif rate == 0.0:
            return 0.0
        elif x < 0.0:
            return 0.0
        elif x == 0.0:
            return rate
        else:
            return ExponentialDistribution(rate, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(ExponentialPDF())

class ExponentialCDF(LibFcn):
    name = prefix + "exponentialCDF"
    sig = Sig([{"x": P.Double()}, {"lambda": P.Double()}], P.Double())
    errcodeBase = 13040
    def __call__(self, state, scope, pos, paramTypes, x, rate):
        if math.isinf(rate) or math.isnan(rate) or rate < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif rate == 0.0 or x == 0.0:
            return 0.0
        elif x <= 0.0:
            return 0.0
        else:
            return ExponentialDistribution(rate, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(ExponentialCDF())

class ExponentialQF(LibFcn):
    name = prefix + "exponentialQF"
    sig = Sig([{"p": P.Double()}, {"lambda": P.Double()}], P.Double())
    errcodeBase = 13050
    def __call__(self, state, scope, pos, paramTypes, p, rate):
        if math.isinf(rate) or math.isnan(rate) or rate < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif rate == 0.0 and p == 0.0:
            return 0.0
        elif rate == 0.0 and p > 0:
            return float("inf")
        elif p == 1.0:
            return float("inf")
        elif p == 0.0:
            return 0.0
        else:
            return ExponentialDistribution(rate, self.errcodeBase + 0, self.name, pos).QF(p)
provide(ExponentialQF())

################ Chi2
class Chi2PDF(LibFcn):
    name = prefix + "chi2PDF"
    sig = Sig([{"x": P.Double()}, {"dof": P.Int()}], P.Double())
    errcodeBase = 13060
    def __call__(self, state, scope, pos, paramTypes, x, df):
        if df < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif df == 0:
            if x != 0:
                return 0.0
            else:
                return float("inf")
        elif x <= 0.0:
            return 0.0
        else:
            return Chi2Distribution(df, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(Chi2PDF())

class Chi2CDF(LibFcn):
    name = prefix + "chi2CDF"
    sig = Sig([{"x": P.Double()}, {"dof": P.Int()}], P.Double())
    errcodeBase = 13070
    def __call__(self, state, scope, pos, paramTypes, x, df):
        if df < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif df == 0:
            if x > 0:
                return 1.0
            else:
                return 0.0
        else:
            return Chi2Distribution(df, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(Chi2CDF())

class Chi2QF(LibFcn):
    name = prefix + "chi2QF"
    sig = Sig([{"p": P.Double()}, {"dof": P.Int()}], P.Double())
    errcodeBase = 13080
    def __call__(self, state, scope, pos, paramTypes, p, df):
        if df < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1.0:
            return float("inf")
        elif df == 0:
            return 0.0
        elif p == 0.0:
            return 0.0
        else:
            return Chi2Distribution(df, self.errcodeBase + 0, self.name, pos).QF(p)
provide(Chi2QF())

################ Poisson #######################################
class PoissonPDF(LibFcn):
    name = prefix + "poissonPDF"
    sig = Sig([{"x": P.Int()}, {"lambda": P.Double()}], P.Double())
    errcodeBase = 13090
    def __call__(self, state, scope, pos, paramTypes, x, lamda):
        if math.isinf(lamda) or math.isnan(lamda) or lamda < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif lamda == 0:
            if x != 0:
                return 0.0
            else:
                return 1.0
        elif x < 0:
            return 0.0
        else:
            return PoissonDistribution(lamda, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(PoissonPDF())

class PoissonCDF(LibFcn):
    name = prefix + "poissonCDF"
    sig = Sig([{"x": P.Int()}, {"lambda": P.Double()}], P.Double())
    errcodeBase = 13100
    def __call__(self, state, scope, pos, paramTypes, x, lamda):
        if math.isinf(lamda) or math.isnan(lamda) or lamda < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif lamda == 0:
            if x >= 0:
                return 1.0
            else:
                return 0.0
        else:
            return PoissonDistribution(lamda, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(PoissonCDF())

class PoissonQF(LibFcn):
    name = prefix + "poissonQF"
    sig = Sig([{"p": P.Double()}, {"lambda": P.Double()}], P.Double())
    errcodeBase = 13110
    def __call__(self, state, scope, pos, paramTypes, p, lamda):
        if math.isinf(lamda) or math.isnan(lamda) or lamda < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif lamda == 0:
            return 0.0
        elif p == 1:
            return float("inf")
        elif p == 0:
            return 0.0
        else:
            return PoissonDistribution(lamda, self.errcodeBase + 0, self.name, pos).QF(p)
provide(PoissonQF())

################ Gamma
class GammaPDF(LibFcn):
    name = prefix + "gammaPDF"
    sig = Sig([{"x": P.Double()}, {"shape": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13120
    def __call__(self, state, scope, pos, paramTypes, x, shape, scale):
        if math.isinf(shape) or math.isnan(shape) or math.isinf(scale) or math.isnan(scale) or shape < 0 or scale < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif shape == 0 or scale == 0:
            if x != 0:
                return 0.0
            else:
                return float("inf")
        elif x < 0:
            return 0.0
        elif x == 0:
            if shape < 1:
                return float("inf")
            elif shape == 1:
                return 1.0/scale
            else:
                return 0.0
        else:
            return GammaDistribution(shape, scale, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(GammaPDF())

class GammaCDF(LibFcn):
    name = prefix + "gammaCDF"
    sig = Sig([{"x": P.Double()}, {"shape": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13130
    def __call__(self, state, scope, pos, paramTypes, x, shape, scale):
        if math.isinf(shape) or math.isnan(shape) or math.isinf(scale) or math.isnan(scale) or shape < 0 or scale < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif shape == 0 or scale == 0:
            if x != 0:
                return 1.0
            else:
                return 0.0
        elif x < 0:
            return 0.0
        else:
            return GammaDistribution(shape, scale, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(GammaCDF())

class GammaQF(LibFcn):
    name = prefix + "gammaQF"
    sig = Sig([{"p": P.Double()}, {"shape": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13140
    def __call__(self, state, scope, pos, paramTypes, p, shape, scale):
        if math.isinf(shape) or math.isnan(shape) or math.isinf(scale) or math.isnan(scale) or shape <= 0 or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1.0:
            return float("inf")
        elif p == 0.0:
            return 0.0
        else:
            return GammaDistribution(shape, scale, self.errcodeBase + 0, self.name, pos).QF(p)
provide(GammaQF())

################ Beta
class BetaPDF(LibFcn):
    name = prefix + "betaPDF"
    sig = Sig([{"x": P.Double()}, {"a": P.Double()}, {"b": P.Double()}], P.Double())
    errcodeBase = 13150
    def __call__(self, state, scope, pos, paramTypes, x, shape1, shape2):
        if math.isinf(shape1) or math.isnan(shape1) or math.isinf(shape2) or math.isnan(shape2) or shape1 <= 0 or shape2 <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x <= 0 or x >= 1:
            return 0.0
        else:
            return BetaDistribution(shape1, shape2, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(BetaPDF())

class BetaCDF(LibFcn):
    name = prefix + "betaCDF"
    sig = Sig([{"x": P.Double()}, {"a": P.Double()}, {"b": P.Double()}], P.Double())
    errcodeBase = 13160
    def __call__(self, state, scope, pos, paramTypes, x, shape1, shape2):
        if math.isinf(shape1) or math.isnan(shape1) or math.isinf(shape2) or math.isnan(shape2) or shape1 <= 0 or shape2 <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x <= 0:
            return 0.0
        elif x >= 1:
            return 1.0
        else:
            return BetaDistribution(shape1, shape2, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(BetaCDF())

class BetaQF(LibFcn):
    name = prefix + "betaQF"
    sig = Sig([{"p": P.Double()}, {"a": P.Double()}, {"b": P.Double()}], P.Double())
    errcodeBase = 13170
    def __call__(self, state, scope, pos, paramTypes, p, shape1, shape2):
        if math.isinf(shape1) or math.isnan(shape1) or math.isinf(shape2) or math.isnan(shape2) or shape1 <= 0 or shape2 <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return 1.0
        elif p == 0:
            return 0.0
        else:
            return BetaDistribution(shape1, shape2, self.errcodeBase + 0, self.name, pos).QF(p)
provide(BetaQF())

################ Cauchy
class CauchyPDF(LibFcn):
    name = prefix + "cauchyPDF"
    sig = Sig([{"x": P.Double()}, {"location": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13180
    def __call__(self, state, scope, pos, paramTypes, x, location, scale):
        if math.isinf(location) or math.isnan(location) or math.isinf(scale) or math.isnan(scale) or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return CauchyDistribution(location, scale, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(CauchyPDF())

class CauchyCDF(LibFcn):
    name = prefix + "cauchyCDF"
    sig = Sig([{"x": P.Double()}, {"location": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13190
    def __call__(self, state, scope, pos, paramTypes, x, location, scale):
        if math.isinf(location) or math.isnan(location) or math.isinf(scale) or math.isnan(scale) or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return CauchyDistribution(location, scale, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(CauchyCDF())

class CauchyQF(LibFcn):
    name = prefix + "cauchyQF"
    sig = Sig([{"p": P.Double()}, {"location": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13200
    def __call__(self, state, scope, pos, paramTypes, p, location, scale):
        if math.isinf(location) or math.isnan(location) or math.isinf(scale) or math.isnan(scale) or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return float("inf")
        elif p == 0:
            return float("-inf")
        else:
            return CauchyDistribution(location, scale, self.errcodeBase + 0, self.name, pos).QF(p)
provide(CauchyQF())

################ F
class FPDF(LibFcn):
    name = prefix + "fPDF"
    sig = Sig([{"x": P.Double()}, {"d1": P.Int()}, {"d2": P.Int()}], P.Double())
    errcodeBase = 13210
    def __call__(self, state, scope, pos, paramTypes, x, d1, d2):
        if d2 <= 0 or d1 <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x <= 0:
            return 0.0
        else:
            return FDistribution(d1, d2, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(FPDF())

class FCDF(LibFcn):
    name = prefix + "fCDF"
    sig = Sig([{"x": P.Double()}, {"d1": P.Int()}, {"d2": P.Int()}], P.Double())
    errcodeBase = 13220
    def __call__(self, state, scope, pos, paramTypes, x, d1, d2):
        if d2 <= 0 or d1 <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return FDistribution(d1, d2, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(FCDF())

class FQF(LibFcn):
    name = prefix + "fQF"
    sig = Sig([{"p": P.Double()}, {"d1": P.Int()}, {"d2": P.Int()}], P.Double())
    errcodeBase = 13230
    def __call__(self, state, scope, pos, paramTypes, p, d1, d2):
        if d1 <= 0 or d2 <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return float("inf")
        else:
            return FDistribution(d1, d2, self.errcodeBase + 0, self.name, pos).QF(p)
provide(FQF())

################ Lognormal
class LognormalPDF(LibFcn):
    name = prefix + "lognormalPDF"
    sig = Sig([{"x": P.Double()}, {"meanlog": P.Double()}, {"sdlog": P.Double()}], P.Double())
    errcodeBase = 13240
    def __call__(self, state, scope, pos, paramTypes, x, meanlog, sdlog):
        if math.isinf(meanlog) or math.isnan(meanlog) or math.isinf(sdlog) or math.isnan(sdlog) or sdlog <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return LognormalDistribution(meanlog, sdlog, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(LognormalPDF())

class LognormalCDF(LibFcn):
    name = prefix + "lognormalCDF"
    sig = Sig([{"x": P.Double()}, {"meanlog": P.Double()}, {"sdlog": P.Double()}], P.Double())
    errcodeBase = 13250
    def __call__(self, state, scope, pos, paramTypes, x, meanlog, sdlog):
        if math.isinf(meanlog) or math.isnan(meanlog) or math.isinf(sdlog) or math.isnan(sdlog) or sdlog <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return LognormalDistribution(meanlog, sdlog, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(LognormalCDF())

class LognormalQF(LibFcn):
    name = prefix + "lognormalQF"
    sig = Sig([{"p": P.Double()}, {"meanlog": P.Double()}, {"sdlog": P.Double()}], P.Double())
    errcodeBase = 13260
    def __call__(self, state, scope, pos, paramTypes, p, meanlog, sdlog):
        if math.isinf(meanlog) or math.isnan(meanlog) or math.isinf(sdlog) or math.isnan(sdlog) or sdlog <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return float("inf")
        else:
            return LognormalDistribution(meanlog, sdlog, self.errcodeBase + 0, self.name, pos).QF(p)
provide(LognormalQF())

################ T
class TPDF(LibFcn):
    name = prefix + "tPDF"
    sig = Sig([{"x": P.Double()}, {"dof": P.Int()}], P.Double())
    errcodeBase = 13270
    def __call__(self, state, scope, pos, paramTypes, x, df):
        if df <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return TDistribution(df, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(TPDF())

class TCDF(LibFcn):
    name = prefix + "tCDF"
    sig = Sig([{"x": P.Double()}, {"dof": P.Int()}], P.Double())
    errcodeBase = 13280
    def __call__(self, state, scope, pos, paramTypes, x, df):
        if df <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return TDistribution(df, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(TCDF())

class TQF(LibFcn):
    name = prefix + "tQF"
    sig = Sig([{"p": P.Double()}, {"dof": P.Int()}], P.Double())
    errcodeBase = 13290
    def __call__(self, state, scope, pos, paramTypes, p, df):
        if df <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return float("inf")
        elif p == 0:
            return float("-inf")
        else:
            return TDistribution(df, self.errcodeBase + 0, self.name, pos).QF(p)
provide(TQF())

################ Binomial
class BinomialPDF(LibFcn):
    name = prefix + "binomialPDF"
    sig = Sig([{"x": P.Int()}, {"size": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13300
    def __call__(self, state, scope, pos, paramTypes, x, size, prob):
        if math.isinf(prob) or math.isnan(prob) or size <= 0 or prob < 0 or prob > 1:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x < 0:
            return 0.0
        elif x >= size:
            return 0.0
        else:
            return BinomialDistribution(size, prob, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(BinomialPDF())

class BinomialCDF(LibFcn):
    name = prefix + "binomialCDF"
    sig = Sig([{"x": P.Double()}, {"size": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13310
    def __call__(self, state, scope, pos, paramTypes, x, size, prob):
        if math.isinf(prob) or math.isnan(prob) or size <= 0 or prob < 0 or prob > 1:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x < 0:
            return 0.0
        elif x >= size:
            return 1.0
        elif prob == 1:
            if x < size:
                return 0.0
            else:
                return 1.0
        elif prob == 0:
            return 1.0
        else:
            return BinomialDistribution(size, prob, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(BinomialCDF())

class BinomialQF(LibFcn):
    name = prefix + "binomialQF"
    sig = Sig([{"p": P.Double()}, {"size": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13320
    def __call__(self, state, scope, pos, paramTypes, p, size, prob):
        if math.isinf(prob) or math.isnan(prob) or size <= 0 or prob < 0 or prob > 1:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return float(size)
        elif p == 0:
            return 0.0
        else:
            return BinomialDistribution(size, prob, self.errcodeBase + 0, self.name, pos).QF(p)
provide(BinomialQF())

################ Uniform
class UniformPDF(LibFcn):
    name = prefix + "uniformPDF"
    sig = Sig([{"x": P.Double()}, {"min": P.Double()}, {"max": P.Double()}], P.Double())
    errcodeBase = 13330
    def __call__(self, state, scope, pos, paramTypes, x, min, max):
        if math.isinf(min) or math.isnan(min) or math.isinf(max) or math.isnan(max) or min >= max:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        return UniformDistribution(min, max, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(UniformPDF())

class UniformCDF(LibFcn):
    name = prefix + "uniformCDF"
    sig = Sig([{"x": P.Double()}, {"min": P.Double()}, {"max": P.Double()}], P.Double())
    errcodeBase = 13340
    def __call__(self, state, scope, pos, paramTypes, x, min, max):
        if math.isinf(min) or math.isnan(min) or math.isinf(max) or math.isnan(max) or min >= max:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        return UniformDistribution(min, max, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(UniformCDF())

class UniformQF(LibFcn):
    name = prefix + "uniformQF"
    sig = Sig([{"p": P.Double()}, {"min": P.Double()}, {"max": P.Double()}], P.Double())
    errcodeBase = 13350
    def __call__(self, state, scope, pos, paramTypes, p, min, max):
        if math.isinf(min) or math.isnan(min) or math.isinf(max) or math.isnan(max) or min >= max:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return UniformDistribution(min, max, self.errcodeBase + 0, self.name, pos).QF(p)
provide(UniformQF())

################ Geometric
class GeometricPDF(LibFcn):
    name = prefix + "geometricPDF"
    sig = Sig([{"x": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13360
    def __call__(self, state, scope, pos, paramTypes, x, prob):
        if math.isinf(prob) or math.isnan(prob) or prob <= 0 or prob > 1:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return GeometricDistribution(prob, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(GeometricPDF())

class GeometricCDF(LibFcn):
    name = prefix + "geometricCDF"
    sig = Sig([{"x": P.Double()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13370
    def __call__(self, state, scope, pos, paramTypes, x, prob):
        if math.isinf(prob) or math.isnan(prob) or prob <= 0 or prob > 1:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return GeometricDistribution(prob, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(GeometricCDF())

class GeometricQF(LibFcn):
    name = prefix + "geometricQF"
    sig = Sig([{"p": P.Double()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13380
    def __call__(self, state, scope, pos, paramTypes, p, prob):
        if math.isinf(prob) or math.isnan(prob) or prob <= 0 or prob > 1:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 1:
            return float("inf")
        else:
            return GeometricDistribution(prob, self.errcodeBase + 0, self.name, pos).QF(p)
provide(GeometricQF())

################ Hypergeometric
class HypergeometricPDF(LibFcn):
    name = prefix + "hypergeometricPDF"
    sig = Sig([{"x": P.Int()}, {"m": P.Int()}, {"n": P.Int()}, {"k": P.Int()}], P.Double())
    errcodeBase = 13390
    def __call__(self, state, scope, pos, paramTypes, x, m, n, k):
        if m + n < k or m < 0 or n <= 0 or m + n == 0 or k < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x > m:
            return 0.0
        else:
            return HypergeometricDistribution(m, n, k, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(HypergeometricPDF())

class HypergeometricCDF(LibFcn):
    name = prefix + "hypergeometricCDF"
    sig = Sig([{"x": P.Int()}, {"m": P.Int()}, {"n": P.Int()}, {"k": P.Int()}], P.Double())
    errcodeBase = 13400
    def __call__(self, state, scope, pos, paramTypes, x, m, n, k):
        if m + n < k or m < 0 or n <= 0 or m + n == 0 or k < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x > m:
            return 0.0
        else:
            return HypergeometricDistribution(m, n, k, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(HypergeometricCDF())

class HypergeometricQF(LibFcn):
    name = prefix + "hypergeometricQF"
    sig = Sig([{"p": P.Double()}, {"m": P.Int()}, {"n": P.Int()}, {"k": P.Int()}], P.Double())
    errcodeBase = 13410
    def __call__(self, state, scope, pos, paramTypes, p, m, n, k):
        if m + n < k or m < 0 or n <= 0 or m + n == 0 or k < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return HypergeometricDistribution(m, n, k, self.errcodeBase + 0, self.name, pos).QF(p)
provide(HypergeometricQF())

################ Weibull
class WeibullPDF(LibFcn):
    name = prefix + "weibullPDF"
    sig = Sig([{"x": P.Double()}, {"shape": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13420
    def __call__(self, state, scope, pos, paramTypes, x, shape, scale):
        if math.isinf(shape) or math.isnan(shape) or math.isinf(scale) or math.isnan(scale) or shape <= 0 or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x >= 0:
            return WeibullDistribution(shape, scale, self.errcodeBase + 0, self.name, pos).PDF(x)
        else:
            return 0.0
provide(WeibullPDF())

class WeibullCDF(LibFcn):
    name = prefix + "weibullCDF"
    sig = Sig([{"x": P.Double()}, {"shape": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13430
    def __call__(self, state, scope, pos, paramTypes, x, shape, scale):
        if math.isinf(shape) or math.isnan(shape) or math.isinf(scale) or math.isnan(scale) or shape <= 0 or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x < 0:
            return 0.0
        else:
            return WeibullDistribution(shape, scale, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(WeibullCDF())

class WeibullQF(LibFcn):
    name = prefix + "weibullQF"
    sig = Sig([{"p": P.Double()}, {"shape": P.Double()}, {"scale": P.Double()}], P.Double())
    errcodeBase = 13440
    def __call__(self, state, scope, pos, paramTypes, p, shape, scale):
        if math.isinf(shape) or math.isnan(shape) or math.isinf(scale) or math.isnan(scale) or shape <= 0 or scale <= 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        else:
            return WeibullDistribution(shape, scale, self.errcodeBase + 0, self.name, pos).QF(p)
provide(WeibullQF())

################ NegativeBinomial
class NegativeBinomialPDF(LibFcn):
    name = prefix + "negativeBinomialPDF"
    sig = Sig([{"x": P.Int()}, {"size": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13450
    def __call__(self, state, scope, pos, paramTypes, x, size, prob):
        if math.isinf(prob) or math.isnan(prob) or prob > 1 or prob <= 0 or size < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x == 0 and size == 0:
            return 1.0
        elif size == 0:
            return 0.0
        else:
            return NegativeBinomialDistribution(size, prob, self.errcodeBase + 0, self.name, pos).PDF(x)
provide(NegativeBinomialPDF())

class NegativeBinomialCDF(LibFcn):
    name = prefix + "negativeBinomialCDF"
    sig = Sig([{"x": P.Double()}, {"size": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13460
    def __call__(self, state, scope, pos, paramTypes, x, size, prob):
        if math.isinf(prob) or math.isnan(prob) or prob > 1 or prob <= 0 or size < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif math.isinf(x) or math.isnan(x):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif x == 0 and size == 0:
            return 1.0
        elif size == 0:
            return 0.0
        elif x < 0:
            return 0.0
        else:
            return NegativeBinomialDistribution(size, prob, self.errcodeBase + 0, self.name, pos).CDF(x)
provide(NegativeBinomialCDF())

class NegativeBinomialQF(LibFcn):
    name = prefix + "negativeBinomialQF"
    sig = Sig([{"p": P.Double()}, {"size": P.Int()}, {"prob": P.Double()}], P.Double())
    errcodeBase = 13470
    def __call__(self, state, scope, pos, paramTypes, p, size, prob):
        if math.isinf(prob) or math.isnan(prob) or prob <= 0 or prob > 1 or size < 0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, pos)
        elif not (0.0 <= p <= 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, pos)
        elif p == 0 and size == 0:
            return 0.0
        elif size == 0:
            return 0.0
        elif p == 1:
            return float("inf")
        else:
            return NegativeBinomialDistribution(size, prob, self.errcodeBase + 0, self.name, pos).QF(p)
provide(NegativeBinomialQF())

#########################################################################################
##### The actual distribution functions #################################################
#########################################################################################

################### Gaussian
class GaussianDistribution(object):
    def __init__(self, mu, sigma, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        if sigma < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)
        self.mu = mu
        self.sigma = sigma

    def LL(self, x):
        if (self.sigma == 0.0) and (x == self.mu):
            return float("inf")
        elif (self.sigma == 0.0) and (x != self.mu):
            return float("-inf")
        else:
            term1 = -(x - self.mu)**2/(2.0 * self.sigma**2)
            term2 = math.log(self.sigma * math.sqrt(2.0 * math.pi))
            return term1 - term2

    def CDF(self, x):
        if (self.sigma == 0.0) and (x < self.mu):
            return 0.0
        elif (self.sigma == 0.0) and (x >= self.mu):
            return 1.0
        else:
            return 0.5 * (1.0 + math.erf((x - self.mu)/(self.sigma * math.sqrt(2.0))))

    def QF(self, p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return float("-inf")
        elif (self.sigma == 0.0):
            return self.mu
        else:
            # http://www.johnkerl.org/python/sp_funcs_m.py.txt
            c0 = 2.515517
            c1 = 0.802853
            c2 = 0.010328
            d1 = 1.432788
            d2 = 0.189269
            d3 = 0.001308

            sign = -1.0
            if (p > 0.5):
                sign = 1.0
                p = 1.0 - p

            arg = -2.0*math.log(p)
            t = math.sqrt(arg)
            g = t - (c0 + t*(c1 + t*c2)) / (1.0 + t*(d1 + t*(d2 + t*d3)))
            standard_normal_qf = sign*g
            return self.mu + self.sigma*standard_normal_qf

################### Exponential
class ExponentialDistribution(object):
    def __init__(self, rate, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.rate = rate
        if (self.rate < 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if (self.rate == 0.0):
            return 0.0
        elif (x < 0.0):
            return 0.0
        elif (x == 0.0):
            return self.rate
        else:
            return self.rate*math.exp(-self.rate*x)

    def CDF(self, x):
        if (self.rate == 0.0) or (x == 0.0):
            return 0.0
        elif (x <= 0.0):
            return 0.0
        else:
            return 1.0 - math.exp(-self.rate*x)

    def QF(self, p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (self.rate == 0.0) and (p == 0.0):
            return 0.0
        elif (self.rate == 0.0) and (p > 0):
            return float("inf")
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return 0.0
        else:
            return -math.log(1.0 - p)/self.rate

################### Chi2
# from: http://www.stat.tamu.edu/~jnewton/604/chap3.pdf
class Chi2Distribution(object):
    def __init__(self, DOF, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.maxIter = 200
        self.epsilon = 1e-15
        self.DOF     = DOF
        if (self.DOF < 0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self,x):
        if (self.DOF == 0) and (x != 0.0):
            return 0.0
        elif (self.DOF == 0) and (x == 0.0):
            return float("inf")
        elif (x == 0.0) and (self.DOF < 2):
            return float("inf")
        elif (x < 0.0):
            return 0.0
        else:
            return GammaDistribution(self.DOF/2.0, 2.0, self.name, self.errcodeBase, self.pos).PDF(x)

    def CDF(self,x):
        if math.isnan(x):
            return float("nan")
        elif (self.DOF == 0) and (x > 0.0):
            return 1.0
        elif (self.DOF == 0) and (x <= 0.0):
            return 0.0
        elif (x <= 0.0):
            return 0.0
        else:
            return GammaDistribution(self.DOF/2.0, 2.0, self.name, self.errcodeBase, self.pos).CDF(x)

    def QF(self,p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (self.DOF == 0):
            return 0.0
        elif (p == 0.0):
            return 0.0
        else:
            return GammaDistribution(self.DOF/2.0, 2.0, self.name, self.errcodeBase, self.pos).QF(p)

################### Poisson
class PoissonDistribution(object):
    def __init__(self, lamda, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.lamda = float(lamda)
        if (self.lamda < 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if (self.lamda == 0.0):
            if (x != 0.0):
                return 0.0
            else:
                return 1.0
        elif (x < 0.0):
            return 0.0
        else:
            return math.exp(x * math.log(self.lamda) - self.lamda - math.lgamma(x + 1.0))

    def CDF(self, x):
        if math.isnan(x):
            return float("nan")
        elif (self.lamda == 0.0):
            if (x >= 0.0):
                return 1.0
            else:
                return 0.0
        elif (x >= 0.0):
            return regularizedGammaQ(math.floor(x + 1.0), self.lamda)
        else:
            return 0.0

    def QF(self, p):
        if math.isnan(p):
            return float("nan")
        elif (self.lamda == 0.0):
            return 0.0
        elif (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return 0.0
        else:
            # step through CDFs until we find the right one
            x = 0
            p0 = 0.0
            while (p0 <= p):
                p0 = self.CDF(x)
                x += 1
            return x - 1

################### Gamma
class GammaDistribution(object):
    def __init__(self, shape, scale, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        # alpha: shape parameter
        self.alpha   = shape
        # beta:  scale parameter
        self.beta    = scale
        self.epsilon = 1e-15
        self.maxIter = 800

        if (self.alpha < 0.0) or (self.beta < 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if (self.alpha == 0.0) or (self.beta == 0.0):
            if (x != 0.0):
                return 0.0
            else:
                return float("inf")
        elif (x < 0.0):
            return 0.0
        elif (self.alpha == 1.0) and (x == 0.0):
            return self.beta
        else:
            try:
                term1a = math.log(x/self.beta) * (self.alpha - 1.0)
            except ValueError:
                term1a = float("-inf") * (self.alpha - 1.0)
            term1 = term1a - math.log(self.beta)
            term2 = -x/self.beta
            term3 = math.lgamma(self.alpha)
            return math.exp(term1 + (term2 - term3))

    def CDF(self, x):
        if (self.alpha == 0.0) or (self.beta == 0.0):
            if (x != 0.0):
                return 1.0
            else:
                return 0.0
        elif (x <= 0.0):
            return 0.0
        else:
            return regularizedGammaP(self.alpha, x/self.beta)

    def QF(self,p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return 0.0
        else:
            y = self.alpha*self.beta
            y_old = y
            for i in range(0,100):
                h = (self.CDF(y_old) - p)/self.PDF(y_old)
                if y_old - h <= 0.0:
                    y_new = y_old / 2.0
                else:
                    y_new = y_old - h
                if abs(y_new) <= self.epsilon:
                    y_new = y_old/10.0
                    h = y_old - y_new
                if abs(h) < math.sqrt(self.epsilon):
                    break
                y_old = y_new
            return y_new

################### Beta
class BetaDistribution(object):
    def __init__(self, alpha, beta, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        # first shape parameter
        self.alpha = alpha
        # second shape parameter
        self.beta  = beta
        if (self.alpha <= 0.0) or (self.beta <= 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)
        # normalization factor
        self.Z = math.lgamma(self.alpha) + math.lgamma(self.beta) \
                 - math.lgamma(self.alpha + self.beta)
        # tolerance
        self.epsilon = 1e-15
        # max Iterations
        self.maxIter = 1000

    def PDF(self,x):
        if (x <= 0.0) or (x >= 1.0):
            return 0.0
        else:
            logX = math.log(x)
            if (x < 0.0) and (x > 0.0):
                return 0.0
            log1mX = math.log1p(-x)
            ret = math.exp((self.alpha - 1.0) * logX + (self.beta - 1.0) \
                  * log1mX - self.Z)
            return ret

    def CDF(self,x):
        if math.isnan(x):
            return float("nan")
        elif (x <= 0.0):
            return 0.0
        elif (x >= 1.0):
            return 1.0
        else:
            return incompleteBetaFunction(x,self.alpha,self.beta)

    def QF(self,p):
        if math.isnan(p):
            return float("nan")
        elif (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return 1.0
        elif (p == 0.0):
            return 0.0
        else:
            return inverseIncompleteBetaFunction(p,self.alpha,self.beta)

################### Cauchy
class CauchyDistribution(object):
    def __init__(self, location, scale, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.loc = location
        self.s = scale
        if self.s <= 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        term1 = 1.0/(math.pi*self.s)
        term2 = 1.0/(1 + pow((x - self.loc)/self.s,2))
        return term1 * term2

    def CDF(self, x):
        return 0.5 + math.atan2(x-self.loc, self.s)*(1.0/math.pi)

    def QF(self, p):
        if (p < 0.0) or (p > 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return float("-inf")
        else:
            return self.loc + self.s*math.tan(math.pi*(p - 0.5))

################### F
# from: http://www.stat.tamu.edu/~jnewton/604/chap3.pdf
class FDistribution(object):
    def __init__(self, upperDOF, lowerDOF, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.maxIter = 1000
        self.epsilon = 1e-8
        self.d1 = float(upperDOF)
        self.d2 = float(lowerDOF)
        if (self.d1 <= 0.0) or (self.d2 <= 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self,x):
        if (x <= 0.0):
            return 0.0
        elif (x == 0) and (self.d1 < 2.0):
            return float("inf")
        elif (x == 0) and (self.d1 == 2.0):
            return 1.0
        else:
            num_arg1 = pow(self.d1/self.d2, self.d1/2.0)
            num_arg2 = pow(x, (self.d1/2.0)-1.0)
            den_arg1 = math.exp(logBetaFunction(self.d1/2.0, self.d2/2.0))
            den_arg2 = pow((1.0 + (self.d1*x)/self.d2), (self.d1 + self.d2)/2.0)
            return (num_arg1*num_arg2)/(den_arg1*den_arg2)

    def CDF(self,x):
        if math.isnan(x):
            return float("nan")
        elif x <= 0.0:
            return 0.0
        else:
            arg1 = (self.d1*x)/(self.d1*x + self.d2)
            arg2 = self.d1/2.0
            arg3 = self.d2/2.0
            return incompleteBetaFunction(arg1, arg2, arg3)

    def QF(self, p):
        if (p < 0.0) or (p > 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return 0.0
        else:
            low = 0.0
            high = 1.0
            while self.CDF(high) < p:
                high *= 2.0
            diff = None
            while diff is None or abs(diff) > self.epsilon:
                mid = (low + high) / 2.0
                diff = self.CDF(mid) - p
                if diff > 0:
                    high = mid
                else:
                    low = mid
            return mid

################### Lognormal
class LognormalDistribution(object):
    def __init__(self, meanlog, sdlog, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.mu = meanlog
        self.sigma = sdlog
        self.epsilon = 1e-8
        self.maxIter = 100
        if self.sigma <= 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if x <= 0.0:
            return 0.0
        else:
            term1 = 1.0/(x*self.sigma*math.sqrt(2.0*math.pi))
            term2 = pow(math.log(x) - self.mu, 2.0)/(2.0*pow(self.sigma, 2.0))
            return term1 * math.exp(-term2)

    def CDF(self, x):
        if x <= 0.0:
            return 0.0
        else:
            return GaussianDistribution(0.0, 1.0, self.name, self.errcodeBase, self.pos).CDF((math.log(x) - self.mu)/self.sigma)

    def QF(self, p):
        if math.isnan(p):
            return float("nan")
        if (p < 0.0) or (p > 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 0.0):
            return 0.0
        elif (p == 1.0):
            return float("inf")
        else:
            low = 0.0
            high = 1.0
            while self.CDF(high) < p:
                high *= 2.0
            diff = None
            while diff is None or abs(diff) > self.epsilon:
                mid = (low + high) / 2.0
                diff = self.CDF(mid) - p
                if diff > 0:
                    high = mid
                else:
                    low = mid
            return mid

            # # Using Newton-Raphson algorithm
            # if p <= .001:
            #     self.epsilon = 1e-5
            # p1 = p
            # if (p1 > 0.8) and (p1 < 0.9):
            #     p2 = .5
            # else:
            #     p2 = 0.85
            # counter = 0
            # while (abs(p1 - p2) > self.epsilon) and (counter < self.maxIter):
            #     q2 = (self.CDF(p2) - p)
            #     p1 = p2
            #     p2 = p1 - (q2/self.PDF(p1))
            #     counter += 1
            # return p2

################### Student's T
class TDistribution(object):
    def __init__(self, DOF, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.df = float(DOF)
        self.epsilon = 1e-8
        self.maxIter = 800
        if self.df <= 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        term1 = 1.0/(math.sqrt(self.df) * math.exp(logBetaFunction(0.5, self.df/2.0)))
        term2 = pow(1.0 + (x*x/self.df), -(self.df + 1.0)/2.0)
        return term1 * term2

    def CDF(self, x):
        if math.isnan(x):
            return float("nan")
        arg1 = self.df/(self.df + x*x)
        arg2 = self.df/2.0
        arg3 = 0.5
        if (x > 0):
            return 1.0 - 0.5*incompleteBetaFunction(arg1, arg2, arg3)
        elif (x == 0.0):
            return 0.5
        else:
            return 0.5*incompleteBetaFunction(arg1, arg2, arg3)

    def QF(self, p):
        if (p < 0.0) or (p > 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        elif (p == 0.0):
            return float("-inf")
        else:
            low = -1.0
            high = 1.0
            while self.CDF(low) > p:
                low *= 2.0
            while self.CDF(high) < p:
                high *= 2.0
            diff = None
            while diff is None or abs(diff) > self.epsilon:
                mid = (low + high) / 2.0
                diff = self.CDF(mid) - p
                if diff > 0:
                    high = mid
                else:
                    low = mid
            return mid

            # # Using Newton-Raphson algorithm
            # if p <= .001:
            #     self.epsilon = 1e-5
            # p1 = p
            # if (p1 > 0.8) and (p1 < 0.9):
            #     p2 = .5
            # else:
            #     p2 = 0.85
            # counter = 0
            # while (abs(p1 - p2) > self.epsilon) or (counter < self.maxIter):
            #     q2 = (self.CDF(p2) - p)
            #     p1 = p2
            #     p2 = p1 - (q2/self.PDF(p1))
            #     counter += 1
            # return p2

################### Binomial
class BinomialDistribution(object):
    def __init__(self, size, p_success, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.prob = p_success
        self.n = float(size)
        if self.n < 0.0:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)
        elif (self.prob < 0.0) or (self.prob > 1.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if x < 0.0:
            return 0.0
        else:
            if x == 0:
                nchoosek = 1.0
            elif (x < 0) or (x > self.n):
                nchoosek = 0.0
            else:
                nchoosek = nChooseK(self.n, x)
            return nchoosek * pow(self.prob, x) * pow(1.0 - self.prob, self.n - x)

    def CDF(self, x):
        if math.isnan(x):
            return float("nan")
        elif x < 0.0:
            return 0.0
        else:
            if (self.n - x <= 0.0) or (self.prob == 0.0):
                return 1.0
            else:
                x = math.floor(x)
                return incompleteBetaFunction(1.0 - self.prob, self.n - x, 1.0 + x)

    def QF(self, p):
        if (p < 0.0) or (p > 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif math.isnan(p):
            return float("nan")
        elif (p == 1.0):
            return self.n
        elif (p == 0.0):
            return 0.0
        elif (p > 0.0) and (p < 1.0):
            # step through CDFs until we find the right one
            x = 0
            p0 = 0.0
            while (p0 < p):
                p0 = p0 + self.PDF(x)
                x += 1
            return x - 1
        else:
            return 0.0

################### Uniform
class UniformDistribution(object):
    def __init__(self, minimum, maximum, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.mi = minimum
        self.ma = maximum
        if self.mi >= self.ma:
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if (x < self.mi) or (x > self.ma):
            return 0.0
        elif (math.isnan(x)):
            return float("nan")
        else:
            return 1.0/(self.ma - self.mi)

    def CDF(self, x):
        if (x < self.mi):
            return 0.0
        elif (x > self.ma):
            return 1.0
        else:
            return (x - self.mi)/(self.ma - self.mi)

    def QF(self, p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p > 0.0) or (p < 1.0):
            return self.mi + p*(self.ma - self.mi)
        elif (math.isnan(p)):
            return float("nan")
        else:
            return 0.0

################### Geometric
class GeometricDistribution(object):
    def __init__(self, p_success, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.prob = p_success
        if (self.prob < 0.0) or (self.prob > 1.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if (x < 0.0):
            return 0.0
        else:
            return self.prob*pow(1.0 - self.prob, x)

    def CDF(self, x):
        if (x < 0.0):
            return 0.0
        else:
            x = math.floor(x)
            return 1.0 - pow(1.0 - self.prob, x + 1.0)

    def QF(self, p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (math.isnan(p)):
            return float("nan")
        elif (p == 1.0):
            return float("inf")
        elif (p > 0.0) and (p < 1.0):
            if self.prob == 1.0:
                return 0.0
            else:
                return math.floor(math.log(1.0 - p)/math.log(1.0 - self.prob))
        else:
            return 0.0

################### Hypergeometric
class HypergeometricDistribution(object):
    def __init__(self, n_white, n_black, n_drawn, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.n_white = n_white
        self.n_black = n_black
        self.n_drawn = n_drawn
        if (n_white + n_black < n_drawn):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x): # x is number of white balls drawn
        # compute nchoosek(n_white,x)
        if x == 0:
            nchoosek1 = 1.0
        elif (x < 0) or (x >= self.n_white):
            nchoosek1 = 0.0
        else:
            nchoosek1 = nChooseK(self.n_white, x)
        # compute nchoosek(n_black, n_drawn - x)
        if (self.n_drawn - x == 0):
            nchoosek2 = 1.0
        elif (self.n_drawn - x < 0) or (self.n_drawn - x > self.n_black):
            nchoosek2 = 0.0
        else:
            nchoosek2 = nChooseK(self.n_black, self.n_drawn - x)
        # compute nchoosek(n_white + n_black, n_drawn)
        if self.n_drawn == 0:
            nchoosek3 = 1.0
        elif (self.n_drawn < 0) or (self.n_drawn > self.n_white + self.n_black):
            nchoosek3 = 0.0
        else:
            nchoosek3 = nChooseK(self.n_white + self.n_black, self.n_drawn)
        # compute
        return nchoosek1 * nchoosek2 / nchoosek3

    def CDF(self, x):
        if (x > self.n_white):
            return 0.0
        else:
            val = 0.0
            for i in range(0, int(math.floor(x + 1.0))):
                val = val + self.PDF(float(i))
            return val

    def QF(self, p):
        if (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif math.isnan(p):
            return float("nan")
        elif (p == 1):
            return self.n_drawn
        else:
            # step through CDFs until we find the right one
            x = 0
            p0 = 0.0
            while (p0 <= p):
                p0 = p0 + self.PDF(x)
                x += 1
            return x - 1

################### Weibull
class WeibullDistribution(object):
    def __init__(self, shape, scale, name, errcodeBase, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.a = float(shape)
        self.b = float(scale)
        if (self.a <= 0.0) or (self.b <= 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if math.isnan(x):
            return float("nan")
        elif x == 0.0:
            if self.a < 1.0:
                return float("nan")
            elif self.a == 1.0:
                return 1.0/self.b
            else:
                return 0.0
        elif x >= 0.0:
            term1 = (self.a/self.b)
            term2 = pow(x/self.b, self.a - 1.0)
            term3 = math.exp(-pow(x/self.b, self.a))
            return term1 * term2 * term3

        else:
            return 0.0

    def CDF(self, x):
        if math.isnan(x):
            return float("nan")
        elif x >= 0.0:
            return 1.0 - math.exp(-pow(x/self.b, self.a))
        else:
            return 0.0

    def QF(self, p):
        if (p < 0.0) or (p > 1.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif (p == 1.0):
            return float("inf")
        else:
            try:
                return self.b * pow(-math.log(1.0 - p), 1.0/self.a)
            except OverflowError:
                return float("inf")

################### NegativeBinomial
class NegativeBinomialDistribution(object):
    def __init__(self, n, p, errcodeBase, name, pos):
        self.name = name
        self.errcodeBase = errcodeBase
        self.pos = pos
        self.n = n
        self.prob = p
        if (p > 1.0) or (p <= 0.0) or (self.n < 0.0):
            raise PFARuntimeException("invalid parameterization", self.errcodeBase + 0, self.name, self.pos)

    def PDF(self, x):
        if (math.isnan(x)):
            return float("nan")
        elif (self.n == 0.0) and (x == 0.0):
            return 1.0
        elif (self.n == 0.0) and (x != 0.0):
            return 0.0
        elif (self.prob == 1.0) and (x != 0.0):
            return 0.0
        elif (self.prob == 1.0) and (x == 0.0):
            return 1.0
        elif (x >= 0.0):
            if x == 0:
                val = 1.0
            elif x < 0:
                val = 0.0
            else:
                val = nChooseK(self.n + x - 1.0, x)
            return val * pow(1.0 - self.prob, x) * pow(self.prob, self.n)
        else:
            return 0.0

    def CDF(self, x):
        if math.isnan(x):
            return float("nan")
        elif (self.n == 0.0) and (x == 0.0):
            return 1.0
        val = 0.0
        for i in range(0, int(math.floor(x + 1.0))):
            val = val + self.PDF(float(i))
        return val

    def QF(self, p):
        # CDF SEEMS MORE ACCURATE NOW, REVISIT
        if math.isnan(p):
            return float("nan")
        elif (self.n == 0.0) and (x == 0.0):
            return 0.0
        elif (self.n == 0.0):
            return 0.0
        elif (p == 0.0):
            return 0.0
        elif (p == 1.0):
            return float("inf")
        elif (p > 1.0) or (p < 0.0):
            raise PFARuntimeException("invalid input", self.errcodeBase + 1, self.name, self.pos)
        elif self.prob == 1.0:
            return 0.0
        elif (p > 0.0) or (p < 1.0):
            # using Cornish-Fisher expansion
            Q = 1.0/self.prob
            P = (1.0 - self.prob)*Q
            mu = self.n * (1.0 - self.prob)/self.prob
            sigma = math.sqrt(self.n * P * Q)
            gamma = (Q + P)/sigma
            z = GaussianDistribution(0.0, 1.0, self.name, self.errcodeBase, self.pos).QF(p)

            # CF approximation gets us close
            w = math.ceil(mu + sigma * (z + gamma * (z*z - 1.0)/6.0))
            # use math.ceil (next term has a minus sign)

            # CDF seems mildly unstable for extreme values
            # cant use newton raphson. The way this computes CDF doesnt
            # seem to be accurate enough for these extreme cases

            # CDF
            # only use CD for very extreme values
            if w > 100000:
                return w
            else:  # do the step-through-CDF method
                x = 0.0
                s = 0.0
                while (s <= p):
                    s = s + self.PDF(x)
                    x += 1
                return x - 1
