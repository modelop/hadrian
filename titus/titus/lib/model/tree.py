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

from titus.fcn import Fcn
from titus.fcn import LibFcn
from titus.signature import Sig
from titus.signature import Sigs
from titus.datatype import *
from titus.errors import *
from titus.util import callfcn
import titus.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "model.tree."

#################################################################### 

def simpleComparison(paramTypes, datum, comparison, missingOperators, parser, code1, code2, fcnName, pos):
    field = comparison["field"]
    fieldValue = datum[field]
    operator = comparison["operator"]
    value = comparison["value"]

    fieldValueType = [x for x in paramTypes[0]["fields"] if x["name"] == field][0]["type"]
    valueType = [x for x in paramTypes[1]["fields"] if x["name"] == "value"][0]["type"]

    if not missingOperators:
        if isinstance(fieldValueType, (list, tuple)):
            withoutNull = [x for x in fieldValueType if x != "null" and x != {"type": "null"}]
            if len(withoutNull) == 1:
                fieldValueType = withoutNull[0]
            else:
                fieldValueType = withoutNull

            if isinstance(fieldValue, dict) and len(fieldValue) == 1 and any(parser.getAvroType(x).name == fieldValue.keys()[0] for x in withoutNull):
                fieldValue, = fieldValue.values()
        
    fieldValueType = parser.getAvroType(fieldValueType)
    valueType = parser.getAvroType(valueType)

    if operator == "alwaysTrue":
        return True
    elif operator == "alwaysFalse":
        return False
    elif missingOperators and operator == "isMissing":
        return (fieldValue is None)
    elif missingOperators and operator == "notMissing":
        return (fieldValue is not None)

    elif operator == "in" or operator == "notIn":
        if isinstance(valueType, AvroArray) and valueType.items.accepts(fieldValueType):
            pass
        elif isinstance(valueType, AvroUnion) and any(isinstance(x, AvroArray) and x.items.accepts(fieldValueType) for x in valueType.types):
            pass
        else:
            raise PFARuntimeException("bad value type", code1, fcnName, pos)

        if isinstance(value, dict) and len(value) == 1 and value.keys() == ["array"]:
            value, = value.values()

        if not missingOperators:
            if fieldValue is None:
                return None

        containedInList = (fieldValue in value)
        if operator == "in":
            return containedInList
        else:
            return not containedInList

    else:
        if not valueType.accepts(fieldValueType):
            if isinstance(valueType, (AvroInt, AvroLong, AvroFloat, AvroDouble)) and \
               isinstance(fieldValueType, (AvroInt, AvroLong, AvroFloat, AvroDouble)):
                pass
            else:
                raise PFARuntimeException("bad value type", code1, fcnName, pos)

        if not missingOperators and fieldValue is None:
            return None

        if isinstance(fieldValueType, (AvroInt, AvroLong, AvroFloat, AvroDouble)):
            if isinstance(value, dict) and (value.keys() == ["int"] or value.keys() == ["long"] or value.keys() == ["float"] or value.keys() == ["double"]):
                value, = value.values()
            if not isinstance(value, (int, long, float)):
                raise PFARuntimeException("bad value type", code1, fcnName, pos)

            if operator == "<=":
                return fieldValue <= value
            elif operator == "<":
                return fieldValue < value
            elif operator == ">=":
                return fieldValue >= value
            elif operator == ">":
                return fieldValue > value
            elif operator == "==":
                return fieldValue == value
            elif operator == "!=":
                return fieldValue != value
            else:
                raise PFARuntimeException("invalid comparison operator", code2, fcnName, pos)

        else:
            if operator == "<=":
                return compare(valueType, fieldValue, value) <= 0
            elif operator == "<":
                return compare(valueType, fieldValue, value) < 0
            elif operator == ">=":
                return compare(valueType, fieldValue, value) >= 0
            elif operator == ">":
                return compare(valueType, fieldValue, value) > 0
            elif operator == "==":
                return compare(valueType, fieldValue, value) == 0
            elif operator == "!=":
                return compare(valueType, fieldValue, value) != 0
            else:
                raise PFARuntimeException("invalid comparison operator", code2, fcnName, pos)

class SimpleTest(LibFcn):
    name = prefix + "simpleTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"comparison": P.WildRecord("T", {"field": P.EnumFields("F", "D"), "operator": P.String(), "value": P.Wildcard("V")})}], P.Boolean())
    errcodeBase = 32000
    def __call__(self, state, scope, pos, paramTypes, datum, comparison):
        return simpleComparison(paramTypes, datum, comparison, True, state.parser, self.errcodeBase + 1, self.errcodeBase + 0, self.name, pos)
provide(SimpleTest())

class CompoundTest(LibFcn):
    name = prefix + "compoundTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"operator": P.String()}, {"comparisons": P.Array(P.WildRecord("T", {}))}, {"test": P.Fcn([P.Wildcard("D"), P.Wildcard("T")], P.Boolean())}], P.Boolean())
    errcodeBase = 32020
    def __call__(self, state, scope, pos, paramTypes, datum, operator, comparisons, test):
        if operator == "and":
            for comparison in comparisons:
                if callfcn(state, scope, test, [datum, comparison]) is False:
                    return False
            return True
        elif operator == "or":
            for comparison in comparisons:
                if callfcn(state, scope, test, [datum, comparison]) is True:
                    return True
            return False
        elif operator == "xor":
            numTrue = 0
            for comparison in comparisons:
                if callfcn(state, scope, test, [datum, comparison]) is True:
                    numTrue += 1
            return numTrue % 2 == 1
        else:
            raise PFARuntimeException("unrecognized logical operator", self.errcodeBase + 0, self.name, pos)
provide(CompoundTest())

class MissingTest(LibFcn):
    name = prefix + "missingTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"comparison": P.WildRecord("T", {"field": P.EnumFields("F", "D"), "operator": P.String(), "value": P.Wildcard("V")})}], P.Union([P.Null(), P.Boolean()]))
    errcodeBase = 32010
    def __call__(self, state, scope, pos, paramTypes, datum, comparison):
#         newDatumTypeFields = [{"name": x["name"], "type": removeNull(x["type"])} if x["name"] == comparison["field"] else x for x in paramTypes[0]["fields"]]
#         newParamTypes = [dict(paramTypes[0], fields=newDatumTypeFields)] + paramTypes[1:]
#         return simpleComparison(newParamTypes, datum, comparison, False, state.parser)
        out = simpleComparison(paramTypes, datum, comparison, False, state.parser, self.errcodeBase + 1, self.errcodeBase + 0, self.name, pos)
        if out is True:
            return {"boolean": True}
        elif out is False:
            return {"boolean": False}
        else:
            return out
provide(MissingTest())

class SurrogateTest(LibFcn):
    name = prefix + "surrogateTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"comparisons": P.Array(P.WildRecord("T", {}))}, {"missingTest": P.Fcn([P.Wildcard("D"), P.Wildcard("T")], P.Union([P.Null(), P.Boolean()]))}], P.Boolean())
    errcodeBase = 32030
    def __call__(self, state, scope, pos, paramTypes, datum, comparisons, missingTest):
        for comparison in comparisons:
            result = callfcn(state, scope, missingTest, [datum, comparison])
            if result is not None:
                if isinstance(result, dict) and result.keys() == ["boolean"]:
                    return result.values()[0]
                else:
                    return result
        raise PFARuntimeException("no successful surrogate", self.errcodeBase + 0, self.name, pos)
provide(SurrogateTest())

class SimpleWalk(LibFcn):
    name = prefix + "simpleWalk"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"treeNode": P.WildRecord("T", {"pass": P.Union([P.WildRecord("T", {}), P.Wildcard("S")]), "fail": P.Union([P.WildRecord("T", {}), P.Wildcard("S")])})}, {"test": P.Fcn([P.WildRecord("D", {}), P.WildRecord("T", {})], P.Boolean())}], P.Wildcard("S"))
    errcodeBase = 32040
    def __call__(self, state, scope, pos, paramTypes, datum, treeNode, test):
        treeNodeTypeName = paramTypes[1]["name"]
        node = treeNode
        while True:
            if callfcn(state, scope, test, [datum, node]):
                union = node["pass"]
            else:
                union = node["fail"]
            if union is None:
                node = None
                break
            else:
                (utype, node), = union.items()
                if utype != treeNodeTypeName:
                    break
        return node
provide(SimpleWalk())

class MissingWalk(LibFcn):
    name = prefix + "missingWalk"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"treeNode": P.WildRecord("T", {"pass": P.Union([P.WildRecord("T", {}), P.Wildcard("S")]), "fail": P.Union([P.WildRecord("T", {}), P.Wildcard("S")]), "missing": P.Union([P.WildRecord("T", {}), P.Wildcard("S")])})}, {"test": P.Fcn([P.WildRecord("D", {}), P.WildRecord("T", {})], P.Union([P.Null(), P.Boolean()]))}], P.Wildcard("S"))
    errcodeBase = 32050
    def __call__(self, state, scope, pos, paramTypes, datum, treeNode, test):
        treeNodeTypeName = paramTypes[1]["name"]
        node = treeNode
        while True:
            result = callfcn(state, scope, test, [datum, node])
            if result is True or result == {"boolean": True}:
                union = node["pass"]
            elif result is False or result == {"boolean": False}:
                union = node["fail"]
            elif result is None:
                union = node["missing"]
            if union is None:
                node = None
                break
            else:
                (utype, node), = union.items()
                if utype != treeNodeTypeName:
                    break
        return node
provide(MissingWalk())

class SimpleTree(LibFcn):
    name = prefix + "simpleTree"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"treeNode": P.WildRecord("T", {"field": P.EnumFields("F", "D"), "operator": P.String(), "value": P.Wildcard("V"), "pass": P.Union([P.WildRecord("T", {}), P.Wildcard("S")]), "fail": P.Union([P.WildRecord("T", {}), P.Wildcard("S")])})}], P.Wildcard("S"))
    errcodeBase = 32060
    def __call__(self, state, scope, pos, paramTypes, datum, treeNode):
        treeNodeTypeName = paramTypes[1]["name"]
        node = treeNode
        while True:
            if simpleComparison(paramTypes, datum, node, True, state.parser, self.errcodeBase + 1, self.errcodeBase + 0, self.name, pos):
                union = node["pass"]
            else:
                union = node["fail"]
            if union is None:
                node = None
                break
            else:
                (utype, node), = union.items()
                if utype != treeNodeTypeName:
                    break
        return node
provide(SimpleTree())
