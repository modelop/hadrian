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

from com.opendatagroup.antinous.producer import Dataset

def normalizeType(spec):
    from collections import OrderedDict
    from antinous import null, boolean, double, bytes, string, array, map, fixed, enum, record, union
    if spec is null or spec is None or spec == "null" or spec == {"type": "null"}:
        return "null"
    elif spec is boolean or spec is bool or spec == "boolean" or spec == {"type": "boolean"}:
        return "boolean"
    elif spec is int or spec == "int" or spec == {"type": "int"}:
        return "int"
    elif spec is long or spec == "long" or spec == {"type": "long"}:
        return "long"
    elif spec is float or spec == "float" or spec == {"type": "float"}:
        return "float"
    elif spec is double or spec == "double" or spec == {"type": "double"}:
        return "double"
    elif spec is bytes or spec == "bytes" or spec == {"type": "bytes"}:
        return "bytes"
    elif spec is string or spec is unicode or spec == "string" or spec == {"type": "string"}:
        return "string"
    elif isinstance(spec, array):
        return OrderedDict([("type", "array"), ("items", normalizeType(spec.items))])
    elif isinstance(spec, dict) and spec.get("type") == "array":
        return OrderedDict([("type", "array"), ("items", normalizeType(spec["items"]))])
    elif isinstance(spec, map):
        return OrderedDict([("type", "map"), ("values", normalizeType(spec.values))])
    elif isinstance(spec, dict) and spec.get("type") == "map":
        return OrderedDict([("type", "map"), ("values", normalizeType(spec["values"]))])
    elif isinstance(spec, fixed):
        out = [("type", "fixed"), ("name", spec.name)]
        if spec.namespace is not None:
            out.append(("namespace", spec.namespace))
        out.append(("size", spec.size))
        return OrderedDict(out)
    elif isinstance(spec, dict) and spec.get("type") == "fixed":
        out = [("type", "fixed"), ("name", spec["name"])]
        if spec.get("namespace") is not None:
            out.append(("namespace", spec["namespace"]))
        out.append(("size", spec["size"]))
        return OrderedDict(out)
    elif isinstance(spec, enum):
        out = [("type", "enum"), ("name", spec.name)]
        if spec.namespace is not None:
            out.append(("namespace", spec.namespace))
        out.append(("symbols", spec.symbols))
        return OrderedDict(out)
    elif isinstance(spec, dict) and spec.get("type") == "enum":
        out = [("type", "enum"), ("name", spec["name"])]
        if spec.get("namespace") is not None:
            out.append(("namespace", spec["namespace"]))
        out.append(("symbols", spec["symbols"]))
        return OrderedDict(out)
    elif isinstance(spec, record):
        out = [("type", "record"), ("name", spec.name)]
        if spec.namespace is not None:
            out.append(("namespace", spec.namespace))
        out.append(("fields", []))
        for fieldName in sorted(spec.fields.keys()):
            out[-1][1].append(OrderedDict([("name", fieldName), ("type", normalizeType(spec.fields[fieldName]))]))
        return OrderedDict(out)
    elif isinstance(spec, dict) and spec.get("type") == "record":
        out = [("type", "record"), ("name", spec["name"])]
        if spec.get("namespace") is not None:
            out.append(("namespace", spec["namespace"]))
        out.append(("fields", []))
        for field in spec["fields"]:
            out[-1][1].append(OrderedDict([(key, normalizeType(field[key]) if key == "type" else field[key]) for key in sorted(field.keys())]))
        return OrderedDict(out)
    elif isinstance(spec, union):
        return [normalizeType(x) for x in spec.types]
    elif isinstance(spec, (tuple, list)):
        return [normalizeType(x) for x in spec]
    elif isinstance(spec, basestring):
        return spec
    else:
        raise TypeError("unrecognized type specification: {}".format(spec))

def normalizeEngine(spec):
    def passThrough():
        pass

    import json
    out = {}
    out["input"] = json.dumps(normalizeType(spec["input"]))
    out["output"] = json.dumps(normalizeType(spec["output"]))
    out["begin"] = spec.get("begin", passThrough)
    out["action"] = spec["action"]
    out["end"] = spec.get("end", passThrough)
    out["fcns"] = {k: v for k, v in spec.items() if callable(v)}

    if not callable(out["begin"]):
        raise TypeError("\"begin\" must be a callable function")
    if not callable(out["action"]):
        raise TypeError("\"action\" must be a callable function")
    if not callable(out["end"]):
        raise TypeError("\"end\" must be a callable function")

    return out

def saveState(spec):
    import pickle
    import types

    class State(object):
        def __init__(self, obj):
            if isinstance(obj, Dataset):
                self.data = obj
                self.pickled = False
            else:
                try:
                    self.data = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
                    self.pickled = True
                except Exception as err:
                    self.data = obj
                    self.pickled = False
        def revert(self):
            if isinstance(self.data, Dataset):
                self.data.revert()
                return self.data
            elif self.pickled:
                return pickle.loads(self.data)
            else:
                return self.data

    return {k: State(v) for k, v in spec.items() if k not in ["emit", "log"] and not isinstance(v, types.ModuleType)}
