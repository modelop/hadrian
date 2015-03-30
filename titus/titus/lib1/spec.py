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
prefix = "m.special."

class NChooseK(LibFcn):
    name = prefix + "nChooseK"
    sig = Sig([{"n": P.Int()}, {"k": P.Int()}], P.Int())
    def __call__(self, state, scope, paramTypes, n, k):
        try:
            return nChooseK(n,k)
        except:
            raise PFARuntimeException("domain error")
provide(NChooseK())

class LnBetaFcn(LibFcn):
    name = prefix + "lnBeta"
    sig = Sig([{"a": P.Double()}, {"b": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, a, b):
        try:
            return logBetaFunction(a,b)
        except:
            raise PFARuntimeException("domain error")
provide(LnBetaFcn())

class ErfFcn(LibFcn):
    name = prefix + "erf"
    sig = Sig([{"a": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, a):
        try:
            return math.erf(a)
        except:
            raise PFARuntimeException("domain error")
provide(ErfFcn())

class ErfcFcn(LibFcn):
    name = prefix + "erfc"
    sig = Sig([{"a": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, a):
        try:
            return math.erfc(a)
        except:
            raise PFARuntimeException("domain error")
provide(ErfcFcn())

class LnGammaFcn(LibFcn):
    name = prefix + "lnGamma"
    sig = Sig([{"a": P.Double()}], P.Double())
    def __call__(self, state, scope, paramTypes, a):
        if (a > 0):
            try:
                return math.lgamma(a)
            except:
                raise PFARuntimeException("domain error")
        else:
            raise PFARuntimeException("domain error")
provide(LnGammaFcn())

#class IncompleteBetaFcn(LibFcn):
#    name = prefix + "incompletebetafcn"
#    sig = Sig([{"x": P.Double()}, {"a": P.Double()}, {"b": P.Double()}], P.Double())
#    def __call__(self, state, scope, paramTypes, x, a, b):
#        return incompleteBetaFunction(x, a, b)
#provide(IncompleteBetaFcn())

#class IncompleteBetaFcnInv(LibFcn):
#    name = prefix + "incompletebetafcninv"
#    sig = Sig([{"p": P.Double()}, {"a": P.Double()}, {"b": P.Double()}], P.Double())
#    def __call__(self, state, scope, paramTypes, p, a, b):
#        return inverseIncompleteBetaFunction(p, a, b)
#provide(IncompleteBetaFcnInv())

# http://mathworld.wolfram.com/RegularizedGammaFunction.html
#class RegularizedGammaPFcn(LibFcn):
#    name = prefix + "regularizedgammapfcn"
#    sig = Sig([{"x": P.Double()}, {"a": P.Double()}], P.Double())
#    def __call__(self, state, scope, paramTypes, x, a):
#        return regularizedGammaP(a, x)
#provide(RegularizedGammaPFcn())

# http://mathworld.wolfram.com/RegularizedGammaFunction.html
#class RegularizedGammaQFcn(LibFcn):
#    name = prefix + "regularizedgammaqfcn"
#    sig = Sig([{"x": P.Double()}, {"a": P.Double()}], P.Double())
#    def __call__(self, state, scope, paramTypes, x, a):
#        return regularizedGammaQ(a, x)
#provide(RegularizedGammaQFcn())

#########################################################################################
### The actual functions ################################################################
#########################################################################################

def logBetaFunction(a, b):
    if (a <= 0.0) or (b <= 0.0):
        raise PFARuntimeException("domain error")
    else:
        return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)

# numerical recipes in C
def incompleteBetaFunction(x,a,b):
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) \
            + a * math.log(x) + b * math.log(1.0 - x)
    if (x < (a + 1)/(a + b + 2)):
        return math.exp(lbeta) * contFractionBeta(a,b,x)/a
    else:
        return 1 - math.exp(lbeta) * contFractionBeta(b,a,1.-x)/b

# numerical recipes in C
def inverseIncompleteBetaFunction(p, a, b):
    a1 = a - 1.0
    b1 = b - 1.0
    ERROR = 1.0e-10

    if (p <= 0.0):
        return 0.0
    elif (p >= 1.0):
        return 1.0;
    elif (a >= 1.0) and (b >= 1.0):
        if (p < 0.5):
            pp = p
        else:
            pp = 1.0 - p;
        t = math.sqrt(-2.0*math.log(pp))
        x = (2.30753+t*0.27061)/(1.0+t*(0.99229+t*0.04481)) - t
        if (p < 0.5):
            x = -x
        al = ((x*x)-3.0)/6.0;
        h = 2.0/(1.0/(2.0*a-1.0)+1.0/(2.0*b-1.0));
        w = (x*math.sqrt(al+h)/h)-(1.0/(2.0*b-1)-1.0/(2.0*a-1.0))*(al+5.0/6.0-2.0/(3.0*h))
        x = a/(a+b*math.exp(2.0*w))
    else:
        lna = math.log(a/(a+b))
        lnb = math.log(b/(a+b))
        t = math.exp(a*lna)/a
        u = math.exp(b*lnb)/b
        w = t + u
        if (p < t/w):
            x = math.pow(a*w*p,1.0/a)
        else:
            x = 1.0 - math.pow(b*w*(1.0-p), 1.0/b)
    afac = -math.lgamma(a)-math.lgamma(b)+math.lgamma(a+b)
    j = 0
    for i in range(10):
        if (0.0) or ( x == 1.0):
            return x;
        err = incompleteBetaFunction(x,a,b) - p
        t = math.exp(a1*math.log(x)+b1*math.log(1.0-x) + afac)
        u = err/t
        t = u/(1.0-0.5*min(1.0,u*(a1/x - b1/(1.0-x))))
        x -= t
        if (x <= 0.0):
            x = 0.5*(x + t)
        if (x >= 1.0):
            x = 0.5*(x + t + 1.0)
        if (math.fabs(t) < ERROR*x) and (j > 0):
            break
    return x

# numerical recipes in C
# used by incompleteBetaFcn and incompleteBetaFcnComp
def contFractionBeta(a,b,x, maxIter = 1000, epsilon=1e-15):
    bm = 1.0
    az = 1.0
    am = 1.0
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    bz = 1.0 - qab*x/qap
    for i in range(0,maxIter):
        em = i + 1
        tem = em + em
        d = em*(b-em)*x/((qam+tem)*(a+tem))
        ap = az + d*am
        bp = bz+d*bm
        d = -(a+em)*(qab+em)*x/((qap+tem)*(a+tem))
        app = ap+d*az
        bpp = bp+d*bz
        aold = az
        am = ap/bpp
        bm = bp/bpp
        az = app/bpp
        bz = 1.0
        if (abs(az-aold) < (epsilon*abs(az))):
            return az
    raise PFARuntimeException("domain error")

# http://grepcode.com/file/repo1.maven.org/maven2/org.apache.commons/commons-math3/3.0/org/apache/commons/math3/special/Gamma.java#Gamma.regularizedGammaP%28double%2Cdouble%29
def regularizedGammaP(a, x):
    if (x < 0.0) or (a <= 0.0):
        raise PFARuntimeException("domain error")
    maxIter = 1000
    epsilon = 1e-15
    n  = 0.0
    an = 1.0/a
    total = an
    while (abs(an/total) > epsilon) and \
          (n < maxIter) and \
          (total < float("inf")):
        n = n + 1.0
        an = an * (x / (a + n))
        total = total + an
    if n >= maxIter:
        raise PFARuntimeException("domain error")
    elif total == float("inf"):
        ans = 1.0
    else:
        ans = math.exp(-x + (a*math.log(x)) - math.lgamma(a)) * total
    return ans

# http://www.johnkerl.org/python/sp_funcs_m.py.txt
def regularizedGammaQ(a, x):
    """Incomplete gamma function."""
    if (x < 0.0 or a <= 0.0):
        raise PFARuntimeException("domain error")
    if (x < a+1.0):
        return 1.0 - gser(a,x)[0]
    else:
        return gcf(a,x)[0]

# http://www.johnkerl.org/python/sp_funcs_m.py.txt
# used by incompleteGammaFcn
def gser(a, x, itmax=700, eps=3.0e-7):
    """Series approximation to the incomplete gamma function."""
    gln = math.lgamma(a)
    if (x < 0.0):
        raise PFARuntimeException("domain error")
    if (x == 0.0):
        return [0.0]
    ap = a
    total = 1.0 / a
    delta = total
    n = 1
    while n <= itmax:
        ap = ap + 1.0
        delta = delta * x / ap
        total = total + delta
        if (abs(delta) < abs(total)*eps):
            return [total * math.exp(-x + a*math.log(x) - gln), gln]
        n = n + 1
    raise PFARuntimeException("domain error")

# http://www.johnkerl.org/python/sp_funcs_m.py.txt
# used by incompleteGammaFcn
def gcf(a, x, itmax=200, eps=3.0e-7):
    """Continued fraction approximation of the incomplete gamma function."""
    gln = math.lgamma(a)
    gold = 0.0
    a0 = 1.0
    a1 = x
    b0 = 0.0
    b1 = 1.0
    fac = 1.0
    n = 1
    while n <= itmax:
        an = n
        ana = an - a
        a0 = (a1 + a0*ana)*fac
        b0 = (b1 + b0*ana)*fac
        anf = an*fac
        a1 = x*a0 + anf*a1
        b1 = x*b0 + anf*b1
        if (a1 != 0.0):
            fac = 1.0 / a1
            g = b1*fac
            if (abs((g-gold)/g) < eps):
                return (g*math.exp(-x+a*math.log(x)-gln), gln)
            gold = g
        n = n + 1
    raise PFARuntimeException("domain error")

def nChooseK(n,k):
    # is n an integer?
    nInt = (math.floor(n) == n)
    if (n <= k) or (k <= 0):
        raise PFARuntimeException("domain error")

    if (nInt) and (n < 0.0):
        b = pow(-1.0, k) * math.exp(math.lgamma(abs(n + k)) \
                                  - math.lgamma(k + 1.0)    \
                                  - math.lgamma(abs(n)))
        return round(b)
    if (n >= k):
        b = math.exp(math.lgamma(n + 1.0) - math.lgamma(k + 1.0) \
                   - math.lgamma(n - k + 1.0))
        return round(b)
    if not (nInt) and (n < k):
        b = (1.0/math.pi) * math.exp(math.lgamma(n + 1.0) \
                                   - math.lgamma(k + 1)   \
                                   + math.lgamma(k - n)   \
                   + math.log(math.sin(math.pi * (n - k + 1.0))))
        return round(b)
    return 0.0
