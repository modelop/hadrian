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

import base64
import io
import json

import avro.schema
from avro.io import BinaryEncoder, BinaryDecoder, DatumReader, DatumWriter

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.util import callfcn
from titus.errors import PFARuntimeException
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "map."

class ObjKey(object):
    def toKey(self, x, avroType):
        x = jsonEncoder(avroType, x, False)
        bytes = io.BytesIO()
        writer = DatumWriter(avroType.schema)
        writer.write(x, BinaryEncoder(bytes))
        bytes.flush()
        return base64.b64encode(bytes.getvalue())

    def fromKey(self, key, avroType):
        bytes = io.BytesIO(base64.b64decode(key))
        reader = DatumReader(avroType.schema)
        return reader.read(BinaryDecoder(bytes))

def toLt(state, scope, lessThan):
    return lambda a, b: callfcn(state, scope, lessThan, [a, b])

#################################################################### basic access

class Len(LibFcn):
    name = prefix + "len"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Int())
    errcodeBase = 26000
    def __call__(self, state, scope, pos, paramTypes, m):
        return len(m)
provide(Len())

class Keys(LibFcn):
    name = prefix + "keys"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Array(P.String()))
    errcodeBase = 26010
    def __call__(self, state, scope, pos, paramTypes, m):
        return m.keys()
provide(Keys())

class Values(LibFcn):
    name = prefix + "values"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 26020
    def __call__(self, state, scope, pos, paramTypes, m):
        return m.values()
provide(Values())

#################################################################### searching

class ContainsKey(LibFcn):
    name = prefix + "containsKey"
    sig = Sigs([Sig([{"m": P.Map(P.Wildcard("A"))}, {"key": P.String()}], P.Boolean()),
                Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String()], P.Boolean())}], P.Boolean())])
    errcodeBase = 26030
    def __call__(self, state, scope, pos, paramTypes, m, key):
        if callable(key):
            for k in m:
                if callfcn(state, scope, key, [k]):
                    return True
            return False
        else:
            return key in m
provide(ContainsKey())

class ContainsValue(LibFcn):
    name = prefix + "containsValue"
    sig = Sigs([Sig([{"m": P.Map(P.Wildcard("A"))}, {"value": P.Wildcard("A")}], P.Boolean()),
                Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Boolean())])
    errcodeBase = 26040
    def __call__(self, state, scope, pos, paramTypes, m, value):
        if callable(value):
            for v in m.values():
                if callfcn(state, scope, value, [v]):
                    return True
            return False
        else:
            for v in m.values():
                if v == value:
                    return True
            return False
provide(ContainsValue())

#################################################################### manipulation

class Add(LibFcn, ObjKey):
    name = prefix + "add"
    sig = Sigs([Sig([{"m": P.Map(P.Wildcard("A"))}, {"key": P.String()}, {"value": P.Wildcard("A")}], P.Map(P.Wildcard("A"))),
                Sig([{"m": P.Map(P.Wildcard("A"))}, {"item": P.Wildcard("A")}], P.Map(P.Wildcard("A")))])
    errcodeBase = 26050
    def __call__(self, state, scope, pos, paramTypes, m, *args):
        if len(args) == 2:
            key, value = args
            return dict(m, **{key: value})
        else:
            item, = args
            key = self.toKey(item, jsonNodeToAvroType(paramTypes[1]))
            return dict(m, **{key: item})
provide(Add())

class Remove(LibFcn):
    name = prefix + "remove"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"key": P.String()}], P.Map(P.Wildcard("A")))
    errcodeBase = 26060
    def __call__(self, state, scope, pos, paramTypes, m, key):
        out = dict(m)
        try:
            del out[key]
        except KeyError:
            pass
        return out
provide(Remove())

class Only(LibFcn):
    name = prefix + "only"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"keys": P.Array(P.String())}], P.Map(P.Wildcard("A")))
    errcodeBase = 26070
    def __call__(self, state, scope, pos, paramTypes, m, keys):
        return dict((k, v) for k, v in m.items() if k in keys)
provide(Only())

class Except(LibFcn):
    name = prefix + "except"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"keys": P.Array(P.String())}], P.Map(P.Wildcard("A")))
    errcodeBase = 26080
    def __call__(self, state, scope, pos, paramTypes, m, keys):
        return dict((k, v) for k, v in m.items() if k not in keys)
provide(Except())

class Update(LibFcn):
    name = prefix + "update"
    sig = Sig([{"base": P.Map(P.Wildcard("A"))}, {"overlay": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26090
    def __call__(self, state, scope, pos, paramTypes, base, overlay):
        return dict(base.items() + overlay.items())
provide(Update())

class Split(LibFcn):
    name = prefix + "split"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Array(P.Map(P.Wildcard("A"))))
    errcodeBase = 26100
    def __call__(self, state, scope, pos, paramTypes, m):
        return [{k: v} for k, v in m.items()]
provide(Split())

class Join(LibFcn):
    name = prefix + "join"
    sig = Sig([{"a": P.Array(P.Map(P.Wildcard("A")))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26110
    def __call__(self, state, scope, pos, paramTypes, a):
        out = []
        for m in a:
            out.extend(m.items())
        return dict(out)
provide(Join())    

#################################################################### min/max functions

def argHighestN(m, n, lt):
    out = []
    for k, x in m.items():
        found = False
        for index, (bestk, bestx) in enumerate(out):
            if lt(bestx, x) or (not lt(x, bestx) and bestk > k):
                ind = index
                while ind < len(out) and not lt(out[ind][1], x) and not lt(x, out[ind][1]) and out[ind][0] < k:
                    ind += 1
                out.insert(ind, (k, x))
                found = True
                break
        if not found:
            out.append((k, x))
        if len(out) > n:
            out.pop()
    return [k for k, x in out]

def argLowestN(m, n, lt):
    out = []
    for k, x in m.items():
        found = False
        for index, (bestk, bestx) in enumerate(out):
            if lt(x, bestx) or (not lt(bestx, x) and bestk > k):
                ind = index
                while ind < len(out) and not lt(out[ind][1], x) and not lt(x, out[ind][1]) and out[ind][0] < k:
                    ind += 1
                out.insert(ind, (k, x))
                found = True
                break
        if not found:
            out.append((k, x))
        if len(out) > n:
            out.pop()
    return [k for k, x in out]

class Argmax(LibFcn):
    name = prefix + "argmax"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.String())
    errcodeBase = 26120
    def __call__(self, state, scope, pos, paramTypes, m):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        else:
            return argHighestN(m, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).values, x, y) < 0)[0]
provide(Argmax())

class Argmin(LibFcn):
    name = prefix + "argmin"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.String())
    errcodeBase = 26130
    def __call__(self, state, scope, pos, paramTypes, m):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        else:
            return argLowestN(m, 1, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).values, x, y) < 0)[0]
provide(Argmin())

class ArgmaxLT(LibFcn):
    name = prefix + "argmaxLT"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.String())
    errcodeBase = 26140
    def __call__(self, state, scope, pos, paramTypes, m, lessThan):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        else:
            return argHighestN(m, 1, toLt(state, scope, lessThan))[0]
provide(ArgmaxLT())

class ArgminLT(LibFcn):
    name = prefix + "argminLT"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.String())
    errcodeBase = 26150
    def __call__(self, state, scope, pos, paramTypes, m, lessThan):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        else:
            return argLowestN(m, 1, toLt(state, scope, lessThan))[0]
provide(ArgminLT())

class ArgmaxN(LibFcn):
    name = prefix + "argmaxN"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"n": P.Int()}], P.Array(P.String()))
    errcodeBase = 26160
    def __call__(self, state, scope, pos, paramTypes, m, n):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argHighestN(m, n, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).values, x, y) < 0)
provide(ArgmaxN())

class ArgminN(LibFcn):
    name = prefix + "argminN"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"n": P.Int()}], P.Array(P.String()))
    errcodeBase = 26170
    def __call__(self, state, scope, pos, paramTypes, m, n):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argLowestN(m, n, lambda x, y: compare(jsonNodeToAvroType(paramTypes[0]).values, x, y) < 0)
provide(ArgminN())

class ArgmaxNLT(LibFcn):
    name = prefix + "argmaxNLT"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.String()))
    errcodeBase = 26180
    def __call__(self, state, scope, pos, paramTypes, m, n, lessThan):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argHighestN(m, n, toLt(state, scope, lessThan))
provide(ArgmaxNLT())

class ArgminNLT(LibFcn):
    name = prefix + "argminNLT"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"n": P.Int()}, {"lessThan": P.Fcn([P.Wildcard("A"), P.Wildcard("A")], P.Boolean())}], P.Array(P.String()))
    errcodeBase = 26190
    def __call__(self, state, scope, pos, paramTypes, m, n, lessThan):
        if len(m) == 0:
            raise PFARuntimeException("empty map", self.errcodeBase + 0, self.name, pos)
        elif n < 0:
            raise PFARuntimeException("n < 0", self.errcodeBase + 1, self.name, pos)
        else:
            return argLowestN(m, n, toLt(state, scope, lessThan))
provide(ArgminNLT())

#################################################################### set or set-like functions

class ToSet(LibFcn, ObjKey):
    name = prefix + "toset"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26200
    def __call__(self, state, scope, pos, paramTypes, a):
        return dict((self.toKey(x, jsonNodeToAvroType(paramTypes[0]["items"])), x) for x in a)
provide(ToSet())

class FromSet(LibFcn, ObjKey):
    name = prefix + "fromset"
    sig = Sig([{"s": P.Map(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    errcodeBase = 26210
    def __call__(self, state, scope, pos, paramTypes, s):
        return s.values()
provide(FromSet())

class In(LibFcn, ObjKey):
    name = prefix + "in"
    sig = Sig([{"s": P.Map(P.Wildcard("A"))}, {"x": P.Wildcard("A")}], P.Boolean())
    errcodeBase = 26220
    def __call__(self, state, scope, pos, paramTypes, s, x):
        return self.toKey(x, jsonNodeToAvroType(paramTypes[0]["values"])) in s
provide(In())

class Union(LibFcn):
    name = prefix + "union"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26230
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return dict(a.items() + b.items())
provide(Union())

class Intersection(LibFcn):
    name = prefix + "intersection"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26240
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return dict((k, v) for k, v in a.items() if k in b)
provide(Intersection())

class Diff(LibFcn):
    name = prefix + "diff"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26250
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return dict((k, v) for k, v in a.items() if k not in b)
provide(Diff())

class SymDiff(LibFcn):
    name = prefix + "symdiff"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    errcodeBase = 26260
    def __call__(self, state, scope, pos, paramTypes, a, b):
        cc = set(a.keys()).symmetric_difference(set(b.keys()))
        return dict((k, v) for k, v in a.items() + b.items() if k in cc)
provide(SymDiff())

class Subset(LibFcn):
    name = prefix + "subset"
    sig = Sig([{"little": P.Map(P.Wildcard("A"))}, {"big": P.Map(P.Wildcard("A"))}], P.Boolean())
    errcodeBase = 26270
    def __call__(self, state, scope, pos, paramTypes, little, big):
        return all(k in big for k in little.keys())
provide(Subset())

class Disjoint(LibFcn):
    name = prefix + "disjoint"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Boolean())
    errcodeBase = 26280
    def __call__(self, state, scope, pos, paramTypes, a, b):
        return len(set(a.keys()).intersection(set(b.keys()))) == 0
provide(Disjoint())

#################################################################### functional programming

class MapApply(LibFcn):
    name = prefix + "map"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Wildcard("B"))}], P.Map(P.Wildcard("B")))
    errcodeBase = 26290
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        return dict((k, callfcn(state, scope, fcn, [v])) for k, v in m.items())
provide(MapApply())

class MapWithKey(LibFcn):
    name = prefix + "mapWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Wildcard("B"))}], P.Map(P.Wildcard("B")))
    errcodeBase = 26300
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        return dict((k, callfcn(state, scope, fcn, [k, v])) for k, v in m.items())
provide(MapWithKey())

class Filter(LibFcn):
    name = prefix + "filter"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Map(P.Wildcard("A")))
    errcodeBase = 26310
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        return dict((k, v) for k, v in m.items() if callfcn(state, scope, fcn, [v]))
provide(Filter())

class FilterWithKey(LibFcn):
    name = prefix + "filterWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Boolean())}], P.Map(P.Wildcard("A")))
    errcodeBase = 26320
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        return dict((k, v) for k, v in m.items() if callfcn(state, scope, fcn, [k, v]))
provide(FilterWithKey())

class FilterMap(LibFcn):
    name = prefix + "filterMap"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Union([P.Wildcard("B"), P.Null()]))}], P.Map(P.Wildcard("B")))
    errcodeBase = 26330
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        typeNames = [jsonNodeToAvroType(t).name for t in paramTypes[1]["ret"] if t != "null"]
        out = {}
        for k, v in m.items():
            vv = callfcn(state, scope, fcn, [v])
            if vv is not None:
                if isinstance(vv, dict) and len(vv) == 1 and vv.keys()[0] in typeNames:
                    tag, value = vv.items()[0]
                else:
                    value = vv
                out[k] = value
        return out
provide(FilterMap())

class FilterMapWithKey(LibFcn):
    name = prefix + "filterMapWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Union([P.Wildcard("B"), P.Null()]))}], P.Map(P.Wildcard("B")))
    errcodeBase = 26340
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        typeNames = [jsonNodeToAvroType(t).name for t in paramTypes[1]["ret"] if t != "null"]
        out = {}
        for k, v in m.items():
            vv = callfcn(state, scope, fcn, [k, v])
            if vv is not None:
                if isinstance(vv, dict) and len(vv) == 1 and vv.keys()[0] in typeNames:
                    tag, value = vv.items()[0]
                else:
                    value = vv
                out[k] = value
        return out
provide(FilterMapWithKey())

class FlatMap(LibFcn):
    name = prefix + "flatMap"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Map(P.Wildcard("B")))}], P.Map(P.Wildcard("B")))
    errcodeBase = 26350
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        out = {}
        for key, value in m.items():
            for k, v in callfcn(state, scope, fcn, [value]).items():
                out[k] = v
        return out
provide(FlatMap())

class FlatMapWithKey(LibFcn):
    name = prefix + "flatMapWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Map(P.Wildcard("B")))}], P.Map(P.Wildcard("B")))
    errcodeBase = 26360
    def __call__(self, state, scope, pos, paramTypes, m, fcn):
        out = {}
        for key, value in m.items():
            for k, v in callfcn(state, scope, fcn, [key, value]).items():
                out[k] = v
        return out
provide(FlatMapWithKey())

class ZipMap(LibFcn):
    name = prefix + "zipmap"
    sig = Sigs([Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Wildcard("Z"))}], P.Map(P.Wildcard("Z"))),
                Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"c": P.Map(P.Wildcard("C"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")], P.Wildcard("Z"))}], P.Map(P.Wildcard("Z"))),
                Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"c": P.Map(P.Wildcard("C"))}, {"d": P.Map(P.Wildcard("D"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")], P.Wildcard("Z"))}], P.Map(P.Wildcard("Z")))])
    errcodeBase = 26370
    def __call__(self, state, scope, pos, paramTypes, *args):
        fcn = args[-1]
        maps = args[:-1]
        keys = maps[0].keys()
        for m in maps:
            if keys != m.keys():
                raise PFARuntimeException("misaligned maps", self.errcodeBase + 0, self.name, pos)
        out = {}
        for k in keys:
            out[k] = callfcn(state, scope, fcn, [x[k] for x in maps])
        return out
provide(ZipMap())

class ZipMapWithKey(LibFcn):
    name = prefix + "zipmapWithKey"
    sig = Sigs([Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A"), P.Wildcard("B")], P.Wildcard("Z"))}], P.Map(P.Wildcard("Z"))),
                Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"c": P.Map(P.Wildcard("C"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")], P.Wildcard("Z"))}], P.Map(P.Wildcard("Z"))),
                Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"c": P.Map(P.Wildcard("C"))}, {"d": P.Map(P.Wildcard("D"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")], P.Wildcard("Z"))}], P.Map(P.Wildcard("Z")))])
    errcodeBase = 26380
    def __call__(self, state, scope, pos, paramTypes, *args):
        fcn = args[-1]
        maps = args[:-1]
        keys = maps[0].keys()
        for m in maps:
            if keys != m.keys():
                raise PFARuntimeException("misaligned maps", self.errcodeBase + 0, self.name, pos)
        out = {}
        for k in keys:
            out[k] = callfcn(state, scope, fcn, [k] + [x[k] for x in maps])
        return out
provide(ZipMapWithKey())

#################################################################### functional tests

class Corresponds(LibFcn):
    name = prefix + "corresponds"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Boolean())}], P.Boolean())
    errcodeBase = 26390
    def __call__(self, state, scope, pos, paramTypes, a, b, fcn):
        aset = set(a.keys())
        bset = set(b.keys())
        if aset != bset:
            return False
        else:
            return all(callfcn(state, scope, fcn, [a[k], b[k]]) for k in aset)
provide(Corresponds())

class CorrespondsWithKey(LibFcn):
    name = prefix + "correspondsWithKey"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A"), P.Wildcard("B")], P.Boolean())}], P.Boolean())
    errcodeBase = 26400
    def __call__(self, state, scope, pos, paramTypes, a, b, fcn):
        aset = set(a.keys())
        bset = set(b.keys())
        if aset != bset:
            return False
        else:
            return all(callfcn(state, scope, fcn, [k, a[k], b[k]]) for k in aset)
provide(CorrespondsWithKey())
