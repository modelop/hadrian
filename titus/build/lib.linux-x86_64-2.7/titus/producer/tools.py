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

import json
from collections import OrderedDict

def look(expr, maxDepth=8, inlineDepth=2, indexWidth=30, dropAt=True):
    if dropAt:
        expr = _dropAt(expr, maxDepth + inlineDepth)
    print ("%%-%ds %%s" % indexWidth) % ("index", "data")
    print "-" * (indexWidth * 2)
    for reprindex, reprdata in _look(expr, maxDepth, inlineDepth, [], indexWidth, ""):
        print ("%%-%ds %%s" % indexWidth) % (reprindex, reprdata)

def _dropAt(expr, depth):
    if isinstance(expr, dict):
        return expr.__class__([(k, _dropAt(v, depth - 1)) for k, v in expr.items() if k != "@"])
    elif isinstance(expr, (list, tuple)):
        return [_dropAt(x, depth - 1) for x in expr]
    else:
        return expr

def _acceptableDepth(expr, limit):
    if limit < 0:
        return False
    elif isinstance(expr, dict):
        return all(_acceptableDepth(x, limit - 1) for x in expr.values())
    elif isinstance(expr, (list, tuple)):
        return all(_acceptableDepth(x, limit - 1) for x in expr)
    else:
        return True

class _Pair(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

class _Item(object):
    def __init__(self, value):
        self.value = value

def _look(expr, maxDepth, inlineDepth, index, indexWidth, indent):
    reprindex = ",".join(map(str, index))
    if len(reprindex) > indexWidth:
        reprindex = reprindex[:(indexWidth - 3)] + "..."

    if isinstance(expr, _Pair):
        block = _look(expr.value, maxDepth, inlineDepth, index, indexWidth, indent)
        block[0][0] = reprindex
        block[0][1] = indent + json.dumps(expr.key) + ": " + block[0][1].lstrip()
        block[-1][1] = block[-1][1] + ","
        return block

    elif isinstance(expr, _Item):
        block = _look(expr.value, maxDepth, inlineDepth, index, indexWidth, indent)
        block[0][0] = reprindex
        block[0][1] = indent + block[0][1].lstrip()
        block[-1][1] = block[-1][1] + ","
        return block

    elif _acceptableDepth(expr, inlineDepth):
        return [[reprindex, indent + json.dumps(expr)]]

    elif isinstance(expr, dict):
        if maxDepth > 0:
            block = []
            for key, value in expr.items():
                block.extend(_look(_Pair(key, value), maxDepth - 1, inlineDepth, index + [key], indexWidth, indent + "  "))
            block[-1][1] = block[-1][1].rstrip(",")
            return [[reprindex, indent + "{"]] + block + [["", indent + "}"]]
        else:
            return [[reprindex, indent + "{...}"]]

    elif isinstance(expr, (list, tuple)):
        if maxDepth > 0:
            block = []
            for i, value in enumerate(expr):
                block.extend(_look(_Item(value), maxDepth - 1, inlineDepth, index + [i], indexWidth, indent + "  "))
            block[-1][1] = block[-1][1].rstrip(",")
            return [[reprindex, indent + "["]] + block + [["", indent + "]"]]
        else:
            return [[reprindex, indent + "[...]"]]

def uniqueSymbols(expr, toavoid, tr=None):
    if tr is None:
        tr = {}

    if isinstance(expr, basestring):
        return tr.get(expr, expr)

    if isinstance(expr, (int, long, float)):
        return expr

    elif isinstance(expr, list) and len(expr) == 1:
        return expr

    elif isinstance(expr, dict):
        keys = expr.keys()
        if "@" in keys:
            del keys["@"]
        if keys in (["int"], ["long"], ["float"], ["double"], ["string"], ["base64"], ["type", "value"], ["doc"], ["error"], ["error", "code"], ["fcnref"]):
            return expr

        if keys == ["new", "type"] and isinstance(expr["new"], dict):
            out = {"new": dict((k, uniqueSymbols(v, toavoid, tr)) if k != "@" else (k, v) for k, v in expr["new"].items()), "type": expr["type"]}

        elif keys == ["new", "type"]:
            out = {"new": [uniqueSymbols(v, toavoid, tr) for v in expr["new"]], "type": expr["type"]}

        elif keys == ["do"]:
            out = {"do": _uniqueMaybeList(expr["do"], toavoid, dict(tr))}

        elif keys == ["let"]:
            out = {"let": _uniqueDeclare(expr["let"], toavoid, tr)}

        elif keys == ["set"]:
            out = {"set": _uniqueReassign(expr["set"], toavoid, tr)}

        elif "attr" in keys:
            out = {"attr": uniqueSymbols(expr["attr"], toavoid, dict(tr)), "path": [uniqueSymbols(x, toavoid, dict(tr)) for x in expr["path"]]}
            if "to" in keys:
                out["to"] = uniqueSymbols(expr["to"], toavoid, tr)

        elif "cell" in keys:
            out = {"cell": expr["cell"]}
            if "path" in keys:
                out["path"] = [uniqueSymbols(x, toavoid, dict(tr)) for x in expr["path"]]
            if "to" in keys:
                out["to"] = uniqueSymbols(expr["to"], toavoid, tr)

        elif "pool" in keys:
            out = {"pool": expr["pool"], "path": [uniqueSymbols(x, toavoid, dict(tr)) for x in expr["path"]]}
            if "to" in keys:
                out["to"] = uniqueSymbols(expr["to"], toavoid, tr)
            if "init" in keys:
                out["init"] = uniqueSymbols(expr["init"], toavoid, tr)

        elif "if" in keys:
            out = {"if": uniqueSymbols(expr["if"], toavoid, dict(tr)), "then": _uniqueMaybeList(expr["then"], toavoid, dict(tr))}
            if "else" in keys:
                out["else"] = _uniqueMaybeList(expr["else"], toavoid, dict(tr))

        elif "cond" in keys:
            out = {"cond": [uniqueSymbols(x, toavoid, tr) for x in expr["cond"]]}
            if "else" in keys:
                out["else"] = _uniqueMaybeList(expr["else"], toavoid, dict(tr))

        elif keys == ["while", "do"]:
            out = {"while": uniqueSymbols(expr["while"], toavoid, dict(tr)), "do": _uniqueMaybeList(expr["do"], toavoid, dict(tr))}

        elif keys == ["do", "until"]:
            out = {"do": _uniqueMaybeList(expr["do"], toavoid, dict(tr)), "until": uniqueSymbols(expr["until"], toavoid, dict(tr))}

        elif keys == ["for", "while", "step", "do"]:
            out = {"for": _uniqueDeclare(expr["for"], toavoid, tr),
                   "while": uniqueSymbols(expr["while"], toavoid, dict(tr)),
                   "step": _uniqueReassign(expr["step"], toavoid, tr),
                   "do": _uniqueMaybeList(expr["do"], toavoid, dict(tr))}

        elif "foreach" in keys:
            out = {"foreach": _newSymbol(expr["foreach"], toavoid, tr),
                   "in": uniqueSymbols(expr["in"], toavoid, dict(tr)),
                   "do": _uniqueMaybeList(expr["do"], toavoid, dict(tr))}
            if "seq" in keys:
                out["seq"] = expr["seq"]

        elif keys == ["forkey", "forval", "in", "do"]:
            out = {"forkey": _newSymbol(expr["forkey"], toavoid, tr),
                   "forval": _newSymbol(expr["forval"], toavoid, tr),
                   "in": uniqueSymbols(expr["in"], toavoid, dict(tr)),
                   "do": _uniqueMaybeList(expr["do"], toavoid, dict(tr))}

        elif "cast" in keys:
            out = {"cast": uniqueSymbols(expr["cast"], toavoid, dict(tr)), "cases": []}
            for x in expr["cases"]:
                scope = dict(tr)
                out["cases"].append({"as": x["as"], "named": _newSymbol(x["named"], toavoid, scope), "do": _uniqueMaybeList(x["do"], toavoid, scope)})
            if "partial" in keys:
                out["partial"] = expr["partial"]

        elif keys == ["upcast", "as"]:
            out = {"upcast": uniqueSymbols(expr["upcast"], toavoid, dict(tr)), "as": expr["as"]}

        elif "ifnotnull" in keys:
            scope = dict(tr)
            out = {"ifnotnull": _uniqueDeclare(expr["ifnotnull"], toavoid, scope),
                   "then": _uniqueMaybeList(expr["then"], toavoid, scope)}
            if "else" in keys:
                out["else"] = _uniqueMaybeList(expr["else"], toavoid, dict(tr))

        elif "log" in keys:
            out = {"log": _uniqueMaybeList(expr["log"], toavoid, dict(tr))}
            if "namespace" in keys:
                out["namespace"] = expr["namespace"]

        elif keys == ["params", "ret", "do"]:
            scope = dict(tr)
            out = {"params": [dict((_newSymbol(k, toavoid, scope), v) if k != "@" else (k, v) for k, v in x) for x in expr["params"]],
                   "ret": expr["ret"],
                   "do": _uniqueMaybeList(expr["do"], toavoid, scope)}

        else:
            key, = expr.keys()
            value, = expr.values()
            out = {key: uniqueSymbols(value, toavoid, dict(tr))}

        if "@" in expr:
            out = OrderedDict([("@", expr["@"])] + out.items())
        return out

    else:
        raise RuntimeError

class _NewSymbol(object):
    def __init__(self):
        self._highestSeen = 0
    def __call__(self, original, toavoid, tr):
        if original not in toavoid:
            return original
        trial = "var" + str(self._highestSeen)
        while trial in toavoid:
            self._highestSeen += 1
            trial = "var" + str(self._highestSeen)
        toavoid.add(trial)
        tr[original] = trial
        return trial
_newSymbol = _NewSymbol()

def _uniqueDeclare(declare, toavoid, tr):
    scope = dict(tr)
    return OrderedDict([(_newSymbol(k, toavoid, tr), uniqueSymbols(v, toavoid, scope)) if k != "@" else (k, v) for k, v in declare.items()])

def _uniqueReassign(reassign, toavoid, tr):
    scope = dict(tr)
    return OrderedDict([(tr.get(k, k), uniqueSymbols(v, toavoid, scope)) if k != "@" else (k, v) for k, v in declare.items()])

def _uniqueMaybeList(x, toavoid, tr):
    if isinstance(x, list):
        return [uniqueSymbols(y, toavoid, dict(tr)) for y in x]
    else:
        return uniqueSymbols(x, toavoid, dict(tr))

