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

def simpleComparison(paramTypes, datum, comparison, missingOperators, parser):
    field = comparison["field"]
    fieldValue = datum[field]
    operator = comparison["operator"]
    value = comparison["value"]

    fieldValueType = [x for x in paramTypes[0]["fields"] if x["name"] == field][0]["type"]
    valueType = [x for x in paramTypes[1]["fields"] if x["name"] == "value"][0]["type"]

    if operator == "in" or operator == "notIn":
        valueAvroType = parser.getAvroType(valueType)

        if isinstance(valueAvroType, AvroArray) and valueAvroType.items.accepts(parser.getAvroType(fieldValueType)):
            pass
        elif isinstance(valueAvroType, AvroUnion) and any(isinstance(x, AvroArray) and x.items.accepts(parser.getAvroType(fieldValueType)) for x in valueAvroType.types):
            value = value["array"]
        else:
            raise PFARuntimeException("bad value type")
            
        containedInList = (fieldValue in value)
        if operator == "in":
            return containedInList
        else:
            return not containedInList

    if not missingOperators:
        if isinstance(fieldValueType, (list, tuple)):
            withoutNull = [x for x in fieldValueType if x != "null"]
            if len(withoutNull) == 1:
                fieldValueType = withoutNull[0]
            else:
                fieldValueType = withoutNull

    if (valueType in ("int", "long", "float", "double") and \
        fieldValueType in ("int", "long", "float", "double")) or \
       parser.getAvroType(valueType).accepts(parser.getAvroType(fieldValueType)):
        pass
    else:
        raise PFARuntimeException("bad value type")

    if isinstance(fieldValueType, (tuple, list)) and fieldValue is not None:
        fieldValue, = fieldValue.values()
    if isinstance(valueType, (tuple, list)) and value is not None:
        value, = value.values()

    if not missingOperators and fieldValue is None:
        return None

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
    elif operator == "alwaysTrue":
        return True
    elif operator == "alwaysFalse":
        return False
    elif operator == "isMissing" and missingOperators:
        return (fieldValue is None)
    elif operator == "notMissing" and missingOperators:
        return (fieldValue is not None)
    else:
        raise PFARuntimeException("invalid comparison operator")

class SimpleTest(LibFcn):
    name = prefix + "simpleTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"comparison": P.WildRecord("T", {"field": P.EnumFields("F", "D"), "operator": P.String(), "value": P.Wildcard("V")})}], P.Boolean())
    def __call__(self, state, scope, paramTypes, datum, comparison):
        return simpleComparison(paramTypes, datum, comparison, True, state.parser)
provide(SimpleTest())

class MissingTest(LibFcn):
    name = prefix + "missingTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"comparison": P.WildRecord("T", {"field": P.EnumFields("F", "D"), "operator": P.String(), "value": P.Wildcard("V")})}], P.Union([P.Null(), P.Boolean()]))
    def __call__(self, state, scope, paramTypes, datum, comparison):
        return simpleComparison(paramTypes, datum, comparison, False, state.parser)
provide(MissingTest())

class SurrogateTest(LibFcn):
    name = prefix + "surrogateTest"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"comparisons": P.Array(P.WildRecord("T", {}))}, {"missingTest": P.Fcn([P.Wildcard("D"), P.Wildcard("T")], P.Union([P.Null(), P.Boolean()]))}], P.Boolean())
    def __call__(self, state, scope, paramTypes, datum, comparisons, missingTest):
        for comparison in comparisons:
            result = callfcn(state, scope, missingTest, [datum, comparison])
            if result is not None:
                return result
        raise PFARuntimeException("no successful surrogate")
provide(SurrogateTest())

class SimpleWalk(LibFcn):
    name = prefix + "simpleWalk"
    sig = Sig([{"datum": P.WildRecord("D", {})}, {"treeNode": P.WildRecord("T", {"pass": P.Union([P.WildRecord("T", {}), P.Wildcard("S")]), "fail": P.Union([P.WildRecord("T", {}), P.Wildcard("S")])})}, {"test": P.Fcn([P.WildRecord("D", {}), P.WildRecord("T", {})], P.Boolean())}], P.Wildcard("S"))
    def __call__(self, state, scope, paramTypes, datum, treeNode, test):
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
