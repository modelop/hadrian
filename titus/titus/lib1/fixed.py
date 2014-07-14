#!/usr/bin/env python

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "fixed."

#################################################################### basic access
