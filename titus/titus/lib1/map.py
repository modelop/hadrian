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
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "map."

class ObjKey(object):
    def toKey(self, x, schema):
        bytes = io.BytesIO()
        writer = DatumWriter(schema)
        writer.write(x, BinaryEncoder(bytes))
        bytes.flush()
        return base64.b64encode(bytes.getvalue())

    def fromKey(self, key, schema):
        bytes = io.BytesIO(base64.b64decode(key))
        reader = DatumReader(schema)
        return reader.read(BinaryDecoder(bytes))

#################################################################### basic access

class Len(LibFcn):
    name = prefix + "len"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Int())
    def __call__(self, state, scope, paramTypes, m):
        return len(m)
provide(Len())

class Keys(LibFcn):
    name = prefix + "keys"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Array(P.String()))
    def __call__(self, state, scope, paramTypes, m):
        return m.keys()
provide(Keys())

class Values(LibFcn):
    name = prefix + "values"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, m):
        return m.values()
provide(Values())

#################################################################### searching

class ContainsKey(LibFcn):
    name = prefix + "containsKey"
    sig = Sigs([Sig([{"m": P.Map(P.Wildcard("A"))}, {"key": P.String()}], P.Boolean()),
                Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String()], P.Boolean())}], P.Boolean())])
    def __call__(self, state, scope, paramTypes, m, key):
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
    def __call__(self, state, scope, paramTypes, m, value):
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
    def __call__(self, state, scope, paramTypes, m, *args):
        if len(args) == 2:
            key, value = args
            return dict(m, **{key: value})
        else:
            item, = args
            schema = avro.schema.parse(json.dumps(paramTypes[1]))
            key = self.toKey(item, schema)
            return dict(m, **{key: item})
provide(Add())

class Remove(LibFcn):
    name = prefix + "remove"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"key": P.String()}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, m, key):
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
    def __call__(self, state, scope, paramTypes, m, keys):
        return dict((k, v) for k, v in m.items() if k in keys)
provide(Only())

class Except(LibFcn):
    name = prefix + "except"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"keys": P.Array(P.String())}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, m, keys):
        return dict((k, v) for k, v in m.items() if k not in keys)
provide(Except())

class Update(LibFcn):
    name = prefix + "update"
    sig = Sig([{"base": P.Map(P.Wildcard("A"))}, {"overlay": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, base, overlay):
        return dict(base.items() + overlay.items())
provide(Update())

class Split(LibFcn):
    name = prefix + "split"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}], P.Array(P.Map(P.Wildcard("A"))))
    def __call__(self, state, scope, paramTypes, m):
        return [{k: v} for k, v in m.items()]
provide(Split())

class Join(LibFcn):
    name = prefix + "join"
    sig = Sig([{"a": P.Array(P.Map(P.Wildcard("A")))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, a):
        out = []
        for m in a:
            out.extend(m.items())
        return dict(out)
provide(Join())    

#################################################################### set or set-like functions

class ToSet(LibFcn, ObjKey):
    name = prefix + "toset"
    sig = Sig([{"a": P.Array(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, a):
        schema = avro.schema.parse(json.dumps(paramTypes[0]["items"]))
        return dict((self.toKey(x, schema), x) for x in a)
provide(ToSet())

class FromSet(LibFcn, ObjKey):
    name = prefix + "fromset"
    sig = Sig([{"s": P.Map(P.Wildcard("A"))}], P.Array(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, s):
        return s.values()
provide(FromSet())

class In(LibFcn, ObjKey):
    name = prefix + "in"
    sig = Sig([{"s": P.Map(P.Wildcard("A"))}, {"x": P.Wildcard("A")}], P.Boolean())
    def __call__(self, state, scope, paramTypes, s, x):
        return self.toKey(x, avro.schema.parse(json.dumps(paramTypes[0]["values"]))) in s
provide(In())

class Union(LibFcn):
    name = prefix + "union"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, a, b):
        return dict(a.items() + b.items())
provide(Union())

class Intersection(LibFcn):
    name = prefix + "intersection"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, a, b):
        return dict((k, v) for k, v in a.items() if k in b)
provide(Intersection())

class Diff(LibFcn):
    name = prefix + "diff"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, a, b):
        return dict((k, v) for k, v in a.items() if k not in b)
provide(Diff())

class SymDiff(LibFcn):
    name = prefix + "symdiff"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, a, b):
        cc = set(a.keys()).symmetric_difference(set(b.keys()))
        return dict((k, v) for k, v in a.items() + b.items() if k in cc)
provide(SymDiff())

class Subset(LibFcn):
    name = prefix + "subset"
    sig = Sig([{"little": P.Map(P.Wildcard("A"))}, {"big": P.Map(P.Wildcard("A"))}], P.Boolean())
    def __call__(self, state, scope, paramTypes, little, big):
        return all(k in big for k in little.keys())
provide(Subset())

class Disjoint(LibFcn):
    name = prefix + "disjoint"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("A"))}], P.Boolean())
    def __call__(self, state, scope, paramTypes, a, b):
        return len(set(a.keys()).intersection(set(b.keys()))) == 0
provide(Disjoint())

#################################################################### functional programming

class MapApply(LibFcn):
    name = prefix + "map"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Wildcard("B"))}], P.Map(P.Wildcard("B")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        return dict((k, callfcn(state, scope, fcn, [v])) for k, v in m.items())
provide(MapApply())

class MapWithKey(LibFcn):
    name = prefix + "mapWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Wildcard("B"))}], P.Map(P.Wildcard("B")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        return dict((k, callfcn(state, scope, fcn, [k, v])) for k, v in m.items())
provide(MapWithKey())

class Filter(LibFcn):
    name = prefix + "filter"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Boolean())}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        return dict((k, v) for k, v in m.items() if callfcn(state, scope, fcn, [v]))
provide(Filter())

class FilterWithKey(LibFcn):
    name = prefix + "filterWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Boolean())}], P.Map(P.Wildcard("A")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        return dict((k, v) for k, v in m.items() if callfcn(state, scope, fcn, [k, v]))
provide(FilterWithKey())

class FilterMap(LibFcn):
    name = prefix + "filterMap"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.Wildcard("A")], P.Union([P.Wildcard("B"), P.Null()]))}], P.Map(P.Wildcard("B")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        out = {}
        for k, v in m.items():
            vv = callfcn(state, scope, fcn, [v])
            if vv is not None:
                out[k] = vv
        return out
provide(FilterMap())

class FilterMapWithKey(LibFcn):
    name = prefix + "filterMapWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Union([P.Wildcard("B"), P.Null()]))}], P.Map(P.Wildcard("B")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        out = {}
        for k, v in m.items():
            vv = callfcn(state, scope, fcn, [k, v])
            if vv is not None:
                out[k] = vv
        return out
provide(FilterMapWithKey())

class FlatMapWithKey(LibFcn):
    name = prefix + "flatMapWithKey"
    sig = Sig([{"m": P.Map(P.Wildcard("A"))}, {"fcn": P.Fcn([P.String(), P.Wildcard("A")], P.Map(P.Wildcard("B")))}], P.Map(P.Wildcard("B")))
    def __call__(self, state, scope, paramTypes, m, fcn):
        out = {}
        for key, value in m.items():
            for k, v in callfcn(state, scope, fcn, [key, value]).items():
                out[k] = v
        return out
provide(FlatMapWithKey())

#################################################################### functional tests

class Corresponds(LibFcn):
    name = prefix + "corresponds"
    sig = Sig([{"a": P.Map(P.Wildcard("A"))}, {"b": P.Map(P.Wildcard("B"))}, {"fcn": P.Fcn([P.Wildcard("A"), P.Wildcard("B")], P.Boolean())}], P.Boolean())
    def __call__(self, state, scope, paramTypes, a, b, fcn):
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
    def __call__(self, state, scope, paramTypes, a, b, fcn):
        aset = set(a.keys())
        bset = set(b.keys())
        if aset != bset:
            return False
        else:
            return all(callfcn(state, scope, fcn, [k, a[k], b[k]]) for k in aset)
provide(CorrespondsWithKey())
