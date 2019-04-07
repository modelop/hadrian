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

from six.moves import range
from six.moves import reduce

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn, div
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "la."

def np():
    import numpy
    return numpy

def rowKeys(x):
    return set(x.keys())

def colKeys(x):
    if len(x) == 0:
        return set()
    else:
        return reduce(lambda a, b: a.union(b), [set(xi.keys()) for xi in x.values()])

def arraysToMatrix(x):
    return np().matrix(x, dtype=np().double)

def arrayToRowVector(x):
    return np().matrix(x, dtype=np().double).T

def rowVectorToArray(x):
    return x.T.tolist()[0]

def matrixToArrays(x):
    return x.tolist()

def mapsToMatrix(x, rows, cols):
    return np().matrix([[x.get(i, {}).get(j, 0.0) for j in cols] for i in rows], dtype=np().double)

def mapToRowVector(x, keys):
    return np().matrix([x.get(k, 0.0) for k in keys], dtype=np().double).T

def rowVectorToMap(x, keys):
    return dict(zip(keys, x.T.tolist()[0]))

def matrixToMaps(x, rows, cols):
    return dict((row, dict(zip(cols, xi))) for row, xi in zip(rows, x.tolist()))

def raggedArray(x):
    collens = map(len, x)
    return max(collens) != min(collens)

def raggedMap(x):
    return len(set(len(xi) for xi in x.values())) != 1

class MapApply(LibFcn):
    name = prefix + "map"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}, {"fcn": P.Fcn([P.Double()], P.Double())}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"fcn": P.Fcn([P.Double()], P.Double())}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24000
    def __call__(self, state, scope, pos, paramTypes, x, fcn):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            return [[callfcn(state, scope, fcn, [xj]) for xj in xi] for xi in x]

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            return dict((i, dict((j, callfcn(state, scope, fcn, [xj])) for j, xj in xi.items())) for i, xi in x.items())

provide(MapApply())

class Scale(LibFcn):
    name = prefix + "scale"
    sig = Sigs([Sig([{"x": P.Array(P.Double())}, {"alpha": P.Double()}], P.Array(P.Double())),
                Sig([{"x": P.Array(P.Array(P.Double()))}, {"alpha": P.Double()}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Double())}, {"alpha": P.Double()}], P.Map(P.Double())),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"alpha": P.Double()}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24010
    def __call__(self, state, scope, pos, paramTypes, x, alpha):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            return [[xj * alpha for xj in xi] for xi in x]
        elif isinstance(x, (list, tuple)):
            return [xi * alpha for xi in x]
        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x):
            return dict((i, dict((j, xj * alpha) for j, xj in xi.items())) for i, xi in x.items())
        else:
            return dict((i, xi * alpha) for i, xi in x.items())

provide(Scale())

class ZipMap(LibFcn):
    name = prefix + "zipmap"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}, {"y": P.Array(P.Array(P.Double()))}, {"fcn": P.Fcn([P.Double(), P.Double()], P.Double())}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"y": P.Map(P.Map(P.Double()))}, {"fcn": P.Fcn([P.Double(), P.Double()], P.Double())}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24020
    def __call__(self, state, scope, pos, paramTypes, x, y, fcn):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x) and \
           isinstance(y, (list, tuple)) and all(isinstance(yi, (list, tuple)) for yi in y):
            if len(x) != len(y) or any(len(xi) != len(yi) for xi, yi in zip(x, y)):
                raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)
            return [[callfcn(state, scope, fcn, [xj, yj]) for xj, yj in zip(xi, yi)] for xi, yi in zip(x, y)]

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()) and \
             isinstance(y, dict) and all(isinstance(y[i], dict) for i in y.keys()):
            rows = rowKeys(x).union(rowKeys(y))
            cols = colKeys(x).union(colKeys(y))
            return dict((i, dict((j, callfcn(state, scope, fcn, [x.get(i, {}).get(j, 0.0), y.get(i, {}).get(j, 0.0)])) for j in cols)) for i in rows)

provide(ZipMap())

class Add(LibFcn):
    name = prefix + "add"
    sig = Sigs([Sig([{"x": P.Array(P.Double())}, {"y": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"x": P.Array(P.Array(P.Double()))}, {"y": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Double())}, {"y": P.Map(P.Double())}], P.Map(P.Double())),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"y": P.Map(P.Map(P.Double()))}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24030
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x) and \
           isinstance(y, (list, tuple)) and all(isinstance(yi, (list, tuple)) for yi in y):
            if len(x) != len(y) or any(len(xi) != len(yi) for xi, yi in zip(x, y)):
                raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)
            return [[xj + yj for xj, yj in zip(xi, yi)] for xi, yi in zip(x, y)]

        elif isinstance(x, (list, tuple)) and isinstance(y, (list, tuple)):
            if len(x) != len(y):
                raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)
            return [xi + yi for xi, yi in zip(x, y)]

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()) and \
             isinstance(y, dict) and all(isinstance(y[i], dict) for i in y.keys()):
            rows = rowKeys(x).union(rowKeys(y))
            cols = colKeys(x).union(colKeys(y))
            return dict((i, dict((j, x.get(i, {}).get(j, 0.0) + y.get(i, {}).get(j, 0.0)) for j in cols)) for i in rows)

        else:
            rows = rowKeys(x).union(rowKeys(y))
            return dict((i, x.get(i, 0.0) + y.get(i, 0.0)) for i in rows)

provide(Add())

class Sub(LibFcn):
    name = prefix + "sub"
    sig = Sigs([Sig([{"x": P.Array(P.Double())}, {"y": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"x": P.Array(P.Array(P.Double()))}, {"y": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Double())}, {"y": P.Map(P.Double())}], P.Map(P.Double())),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"y": P.Map(P.Map(P.Double()))}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24040
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x) and \
           isinstance(y, (list, tuple)) and all(isinstance(yi, (list, tuple)) for yi in y):
            if len(x) != len(y) or any(len(xi) != len(yi) for xi, yi in zip(x, y)):
                raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)
            return [[xj - yj for xj, yj in zip(xi, yi)] for xi, yi in zip(x, y)]

        elif isinstance(x, (list, tuple)) and isinstance(y, (list, tuple)):
            if len(x) != len(y):
                raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)
            return [xi - yi for xi, yi in zip(x, y)]

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()) and \
             isinstance(y, dict) and all(isinstance(y[i], dict) for i in y.keys()):
            rows = rowKeys(x).union(rowKeys(y))
            cols = colKeys(x).union(colKeys(y))
            return dict((i, dict((j, x.get(i, {}).get(j, 0.0) - y.get(i, {}).get(j, 0.0)) for j in cols)) for i in rows)

        else:
            rows = rowKeys(x).union(rowKeys(y))
            return dict((i, x.get(i, 0.0) - y.get(i, 0.0)) for i in rows)

provide(Sub())

class Dot(LibFcn):
    name = prefix + "dot"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}, {"y": P.Array(P.Double())}], P.Array(P.Double())),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"y": P.Map(P.Double())}], P.Map(P.Double())),
                Sig([{"x": P.Array(P.Array(P.Double()))}, {"y": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"y": P.Map(P.Map(P.Double()))}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24050
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if paramTypes[1]["type"] == "array":
            if isinstance(paramTypes[1]["items"], dict) and paramTypes[1]["items"]["type"] == "array":
                # array matrix-matrix case
                bad = any(any(math.isnan(z) or math.isinf(z) for z in row) for row in x) or \
                      any(any(math.isnan(z) or math.isinf(z) for z in row) for row in y)
                xmat = arraysToMatrix(x)
                ymat = arraysToMatrix(y)
                if xmat.shape[0] == 0 or xmat.shape[1] == 0 or ymat.shape[0] == 0 or ymat.shape[1] == 0:
                    raise PFARuntimeException("too few rows/cols", self.errcodeBase + 1, self.name, pos)
                try:
                    if bad: raise PFARuntimeException("contains non-finite value", self.errcodeBase + 2, self.name, pos)
                    return matrixToArrays(np().dot(xmat, ymat))
                except ValueError:
                    raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)

            else:
                # array matrix-vector case
                bad = any(any(math.isnan(z) or math.isinf(z) for z in row) for row in x) or \
                      any(math.isnan(z) or math.isinf(z) for z in y)
                xmat = arraysToMatrix(x)
                ymat = arrayToRowVector(y)
                if xmat.shape[0] == 0 or xmat.shape[1] == 0 or ymat.shape[0] == 0 or ymat.shape[1] == 0:
                    raise PFARuntimeException("too few rows/cols", self.errcodeBase + 1, self.name, pos)
                try:
                    if bad: raise PFARuntimeException("contains non-finite value", self.errcodeBase + 2, self.name, pos)
                    return rowVectorToArray(np().dot(xmat, ymat))
                except ValueError:
                    raise PFARuntimeException("misaligned matrices", self.errcodeBase + 0, self.name, pos)

        elif paramTypes[1]["type"] == "map":
            if isinstance(paramTypes[1]["values"], dict) and paramTypes[1]["values"]["type"] == "map":
                # map matrix-matrix case
                bad = any(any(math.isnan(z) or math.isinf(z) for z in row.values()) for row in x.values()) or \
                      any(any(math.isnan(z) or math.isinf(z) for z in row.values()) for row in y.values())
                rows = list(rowKeys(x))
                inter = list(colKeys(x).union(rowKeys(y)))
                cols = list(colKeys(y))
                xmat = mapsToMatrix(x, rows, inter)
                ymat = mapsToMatrix(y, inter, cols)
                if xmat.shape[0] == 0 or xmat.shape[1] == 0 or ymat.shape[0] == 0 or ymat.shape[1] == 0:
                    raise PFARuntimeException("too few rows/cols", self.errcodeBase + 1, self.name, pos)
                if bad: raise PFARuntimeException("contains non-finite value", self.errcodeBase + 2, self.name, pos)
                return matrixToMaps(np().dot(xmat, ymat), rows, cols)

            else:
                # map matrix-vector case
                bad = any(any(math.isnan(z) or math.isinf(z) for z in row.values()) for row in x.values()) or \
                      any(math.isnan(z) or math.isinf(z) for z in y.values())
                rows = list(rowKeys(x))
                cols = list(colKeys(x).union(rowKeys(y)))
                xmat = mapsToMatrix(x, rows, cols)
                ymat = mapToRowVector(y, cols)
                if xmat.shape[0] == 0 or xmat.shape[1] == 0 or ymat.shape[0] == 0 or ymat.shape[1] == 0:
                    raise PFARuntimeException("too few rows/cols", self.errcodeBase + 1, self.name, pos)
                if bad: raise PFARuntimeException("contains non-finite value", self.errcodeBase + 2, self.name, pos)
                return rowVectorToMap(np().dot(xmat, ymat), rows)

provide(Dot())
    
class Transpose(LibFcn):
    name = prefix + "transpose"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24060
    def __call__(self, state, scope, pos, paramTypes, x):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            cols = len(x[0])
            if cols < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedArray(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            return [[x[r][c] for r in range(rows)] for c in range(cols)]

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            rows = rowKeys(x)
            cols = colKeys(x)
            if len(rows) < 1 or len(cols) < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedMap(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            return dict((c, dict((r, x[r][c]) for r in rows)) for c in cols)

provide(Transpose())

class Inverse(LibFcn):
    name = prefix + "inverse"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24070
    def __call__(self, state, scope, pos, paramTypes, x):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            cols = len(x[0])
            if cols < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedArray(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            return matrixToArrays(arraysToMatrix(x).I)

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            rows = list(rowKeys(x))
            cols = list(colKeys(x))
            if len(rows) < 1 or len(cols) < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            xmat = mapsToMatrix(x, rows, cols)
            return matrixToMaps(xmat.I, cols, rows)

provide(Inverse())

class Trace(LibFcn):
    name = prefix + "trace"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}], P.Double()),
                Sig([{"x": P.Map(P.Map(P.Double()))}], P.Double())])
    errcodeBase = 24080
    def __call__(self, state, scope, pos, paramTypes, x):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows == 0:
                return 0.0
            else:
                cols = len(x[0])
                if raggedArray(x):
                    raise PFARuntimeException("ragged columns", self.errcodeBase + 0, self.name, pos)
                return sum(x[i][i] for i in range(min(rows, cols)))

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            keys = rowKeys(x).intersection(colKeys(x))
            return sum(x[i][i] for i in keys)

provide(Trace())

class Det(LibFcn):
    name = prefix + "det"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}], P.Double()),
                Sig([{"x": P.Map(P.Map(P.Double()))}], P.Double())])
    errcodeBase = 24090
    def __call__(self, state, scope, pos, paramTypes, x):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            cols = len(x[0])
            if cols < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedArray(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            if rows != cols:
                raise PFARuntimeException("non-square matrix", self.errcodeBase + 2, self.name, pos)
            if any(any(math.isnan(z) or math.isinf(z) for z in row) for row in x):
                return float("nan")
            else:
                return float(np().linalg.det(arraysToMatrix(x)))

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            keys = list(rowKeys(x).union(colKeys(x)))
            if len(keys) < 1 or all(len(row) == 0 for row in x.values()):
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if any(any(math.isnan(z) or math.isinf(z) for z in row.values()) for row in x.values()):
                return float("nan")
            else:
                return float(np().linalg.det(mapsToMatrix(x, keys, keys)))

provide(Det())

class Symmetric(LibFcn):
    name = prefix + "symmetric"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}, {"tol": P.Double()}], P.Boolean()),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"tol": P.Double()}], P.Boolean())])
    errcodeBase = 24100
    @staticmethod
    def same(x, y, tol):
        if math.isinf(x) and math.isinf(y) and ((x > 0.0 and y > 0.0) or (x < 0.0 and y < 0.0)):
            return True
        elif math.isnan(x) and math.isnan(y):
            return True
        elif not math.isinf(x) and not math.isnan(x) and not math.isinf(y) and not math.isnan(y):
            return abs(x - y) < tol
        else:
            return False
    def __call__(self, state, scope, pos, paramTypes, x, tol):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            cols = len(x[0])
            if cols < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedArray(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            if rows != cols:
                raise PFARuntimeException("non-square matrix", self.errcodeBase + 2, self.name, pos)
            return all(all(self.same(x[i][j], x[j][i], tol) for j in range(cols)) for i in range(rows))

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            keys = list(rowKeys(x).union(colKeys(x)))
            if len(keys) < 1 or all(len(row) == 0 for row in x.values()):
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            return all(all(self.same(x.get(i, {}).get(j, 0.0), x.get(j, {}).get(i, 0.0), tol) for j in keys) for i in keys)

provide(Symmetric())

class EigenBasis(LibFcn):
    name = prefix + "eigenBasis"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}], P.Map(P.Map(P.Double())))])

    errcodeBase = 24110
    def calculate(self, x, size):
        symm = (x + x.T) * 0.5

        evals, evects = np().linalg.eig(symm)
        evects = np().array(evects)
        evects2 = [evects[:,i] * (-1.0 if evects[0,i] < 0.0 else 1.0) for i in range(size)]

        eigvalm2 = [div(1.0, math.sqrt(abs(ei))) for ei in evals]
        order = np().argsort(eigvalm2)

        out = np().empty((size, size), dtype=np().double)
        for i in range(size):
            for j in range(size):
                out[i,j] = evects2[order[i]][j] * eigvalm2[order[i]]
        return out

    def __call__(self, state, scope, pos, paramTypes, x):
        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            cols = len(x[0])
            if cols < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedArray(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            if rows != cols:
                raise PFARuntimeException("non-square matrix", self.errcodeBase + 2, self.name, pos)
            if any(any(math.isnan(z) or math.isinf(z) for z in row) for row in x):
                raise PFARuntimeException("non-finite matrix", self.errcodeBase + 3, self.name, pos)
            return matrixToArrays(self.calculate(arraysToMatrix(x), rows))

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            keys = list(rowKeys(x).union(colKeys(x)))
            if len(keys) < 1 or all(len(z) == 0 for z in x.values()):
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if any(any(math.isnan(z) or math.isinf(z) for z in row.values()) for row in x.values()):
                raise PFARuntimeException("non-finite matrix", self.errcodeBase + 3, self.name, pos)
            return matrixToMaps(self.calculate(mapsToMatrix(x, keys, keys), len(keys)), map(str, range(len(keys))), keys)

provide(EigenBasis())

class Truncate(LibFcn):
    name = prefix + "truncate"
    sig = Sigs([Sig([{"x": P.Array(P.Array(P.Double()))}, {"keep": P.Int()}], P.Array(P.Array(P.Double()))),
                Sig([{"x": P.Map(P.Map(P.Double()))}, {"keep": P.Array(P.String())}], P.Map(P.Map(P.Double())))])
    errcodeBase = 24120
    def __call__(self, state, scope, pos, paramTypes, x, keep):
        if keep < 0:
            keep = 0

        if isinstance(x, (list, tuple)) and all(isinstance(xi, (list, tuple)) for xi in x):
            rows = len(x)
            if rows < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            cols = len(x[0])
            if cols < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            if raggedArray(x):
                raise PFARuntimeException("ragged columns", self.errcodeBase + 1, self.name, pos)
            return x[:keep]

        elif isinstance(x, dict) and all(isinstance(x[i], dict) for i in x.keys()):
            rows = rowKeys(x)
            cols = colKeys(x)
            if len(rows) < 1 or len(cols) < 1:
                raise PFARuntimeException("too few rows/cols", self.errcodeBase + 0, self.name, pos)
            return dict((k, x[k]) for k in rows if k in keep)

provide(Truncate())
