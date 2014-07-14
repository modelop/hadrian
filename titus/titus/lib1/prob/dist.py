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

prefix = "prob.dist."

class GaussianLL(LibFcn):
    name = prefix + "gaussianLL"
    sig = Sigs([Sig([{"x": P.Double()}, {"mu": P.Double()}, {"sigma": P.Double()}], P.Double()),
                Sig([{"x": P.Double()}, {"params": P.WildRecord("A", {"mean": P.Double(), "variance": P.Double()})}], P.Double())])
    def __call__(self, state, scope, paramTypes, x, *others):
        if len(others) == 2:
            mu, sigma = others
        else:
            mu = others[0]["mean"]
            sigma = math.sqrt(others[0]["variance"])

        return -(x - mu)**2/(2.0 * sigma**2) - math.log(sigma * math.sqrt(2.0 * math.pi))

provide(GaussianLL())
