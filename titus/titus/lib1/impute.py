#!/usr/bin/env python

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "impute."

class ErrorOnNull(LibFcn):
    name = prefix + "errorOnNull"
    sig = Sig([{"x": P.Union([P.Wildcard("A"), P.Null()])}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x):
        if x is None:
            raise PFARuntimeException("encountered null")
        else:
            return x
provide(ErrorOnNull())

class DefaultOnNull(LibFcn):
    name = prefix + "defaultOnNull"
    sig = Sig([{"x": P.Union([P.Wildcard("A"), P.Null()])}, {"default": P.Wildcard("A")}], P.Wildcard("A"))
    def __call__(self, state, scope, paramTypes, x, default):
        if x is None:
            return default
        else:
            return x
provide(DefaultOnNull())
