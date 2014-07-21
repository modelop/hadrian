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
import unittest

from titus.datatype import *
from titus.signature import *
import titus.P as P

class TestSignature(unittest.TestCase):
    def typeEquality(self, x, y):
        if isinstance(x, AvroType) and isinstance(y, AvroType):
            return x.accepts(y) and y.accepts(x)
        elif isinstance(x, FcnType) and isinstance(y, FcnType):
            return all(xt.accepts(yt) and yt.accepts(xt) for xt, yt in zip(x.params, y.params)) and x.ret.accepts(y.ret) and y.ret.accepts(x.ret)
        else:
            return False

    def matches(self, x, y):
        if x is None and y is None:
            return True
        elif x is None or y is None:
            return False
        else:
            (xparams, xret) = x
            (yparams, yret) = y
            return all(self.typeEquality(xp, yp) for xp, yp in zip(xparams, yparams)) and self.typeEquality(xret, yret)
            
    def testPassThroughExactParameterMatches(self):
        self.assertTrue(self.matches(Sig([{"x": P.Null()}], P.Null()).accepts([AvroNull()]), ([AvroNull()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroInt()]), ([AvroInt()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroLong()]), ([AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroFloat()]), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroDouble()]), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Bytes()}], P.Null()).accepts([AvroBytes()]), ([AvroBytes()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.String()}], P.Null()).accepts([AvroString()]), ([AvroString()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Union([P.Int()])}], P.Null()).accepts([AvroUnion([AvroInt()])]), ([AvroUnion([AvroInt()])], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10)}], P.Null()).accepts([AvroFixed(10, "MyFixed")]), ([AvroFixed(10, "MyFixed")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10, "MyFixed")}], P.Null()).accepts([AvroFixed(10, "MyFixed")]), ([AvroFixed(10, "MyFixed")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"])}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")]), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"], "MyEnum")}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")]), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()})}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()}, "MyRecord")}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroNull())))

    def testNotMatchAntiPatterns(self):
        self.assertTrue(self.matches(Sig([{"x": P.Null()}], P.Null()).accepts([AvroInt()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroLong()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroFloat()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroDouble()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroBytes()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Bytes()}], P.Null()).accepts([AvroString()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.String()}], P.Null()).accepts([AvroArray(AvroInt())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroMap(AvroInt())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroUnion([AvroInt()])]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Union([P.Int()])}], P.Null()).accepts([AvroFixed(10, "MyFixed")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10, "YourFixed")}], P.Null()).accepts([AvroFixed(10, "MyFixed")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10)}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"], "YourEnum")}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"])}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()}, "YourRecord")}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()})}], P.Null()).accepts([AvroNull()]), None))

    def testPromoteNumbers(self):
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroInt()]), ([AvroInt()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroLong()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroFloat()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroDouble()]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroInt()]), ([AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroLong()]), ([AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroFloat()]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroDouble()]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroInt()]), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroLong()]), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroFloat()]), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroDouble()]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroInt()]), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroLong()]), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroDouble()]), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroDouble()]), ([AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroLong())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroFloat())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroDouble())]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroLong())]), ([AvroArray(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroFloat())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroDouble())]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroLong())]), ([AvroArray(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroFloat())]), ([AvroArray(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroDouble())]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroLong())]), ([AvroArray(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroDouble())]), ([AvroArray(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroDouble())]), ([AvroArray(AvroDouble())], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroLong())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroFloat())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroDouble())]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroLong())]), ([AvroMap(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroFloat())]), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroDouble())]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroLong())]), ([AvroMap(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroFloat())]), ([AvroMap(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroDouble())]), None))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroLong())]), ([AvroMap(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroDouble())]), ([AvroMap(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroDouble())]), ([AvroMap(AvroDouble())], AvroNull())))

    def testLooselyMatchFunctionReferences(self):
        self.assertTrue(self.matches(Sig([{"x": P.Fcn([P.Long()], P.Long())}], P.Null()).accepts([FcnType([AvroLong()], AvroLong())]), ([FcnType([AvroLong()], AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fcn([P.Long()], P.Long())}], P.Null()).accepts([FcnType([AvroLong()], AvroInt())]), ([FcnType([AvroLong()], AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fcn([P.Long()], P.Long())}], P.Null()).accepts([FcnType([AvroDouble()], AvroLong())]), ([FcnType([AvroDouble()], AvroLong())], AvroNull())))

    def testMatchWildcards(self):
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroNull()]), ([AvroNull()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroInt()]), ([AvroInt()], AvroInt())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroLong()]), ([AvroLong()], AvroLong())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroFloat()]), ([AvroFloat()], AvroFloat())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroDouble()]), ([AvroDouble()], AvroDouble())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroBytes()]), ([AvroBytes()], AvroBytes())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroString()]), ([AvroString()], AvroString())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroInt())], AvroArray(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroInt())], AvroMap(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroUnion([AvroInt()])]), ([AvroUnion([AvroInt()])], AvroUnion([AvroInt()]))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroFixed(10, "MyFixed")]), ([AvroFixed(10, "MyFixed")], AvroFixed(10, "MyFixed"))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroEnum(["one", "two", "three"], "MyEnum")]), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroEnum(["one", "two", "three"], "MyEnum"))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroRecord([AvroField("one", AvroInt())], "MyRecord"))))

    def testMatchNestedWildcards(self):
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroNull())]), ([AvroArray(AvroNull())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroInt())], AvroInt())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroLong())]), ([AvroArray(AvroLong())], AvroLong())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroFloat())]), ([AvroArray(AvroFloat())], AvroFloat())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroDouble())]), ([AvroArray(AvroDouble())], AvroDouble())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroBytes())]), ([AvroArray(AvroBytes())], AvroBytes())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroString())]), ([AvroArray(AvroString())], AvroString())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroArray(AvroInt()))]), ([AvroArray(AvroArray(AvroInt()))], AvroArray(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroMap(AvroInt()))]), ([AvroArray(AvroMap(AvroInt()))], AvroMap(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroUnion([AvroInt()]))]), ([AvroArray(AvroUnion([AvroInt()]))], AvroUnion([AvroInt()]))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroFixed(10, "MyFixed"))]), ([AvroArray(AvroFixed(10, "MyFixed"))], AvroFixed(10, "MyFixed"))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroEnum(["one", "two", "three"], "MyEnum"))]), ([AvroArray(AvroEnum(["one", "two", "three"], "MyEnum"))], AvroEnum(["one", "two", "three"], "MyEnum"))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroRecord([AvroField("one", AvroInt())], "MyRecord"))]), ([AvroArray(AvroRecord([AvroField("one", AvroInt())], "MyRecord"))], AvroRecord([AvroField("one", AvroInt())], "MyRecord"))))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroNull()]), ([AvroNull()], AvroArray(AvroNull()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroInt()]), ([AvroInt()], AvroArray(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroLong()]), ([AvroLong()], AvroArray(AvroLong()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroFloat()]), ([AvroFloat()], AvroArray(AvroFloat()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroDouble()]), ([AvroDouble()], AvroArray(AvroDouble()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroBytes()]), ([AvroBytes()], AvroArray(AvroBytes()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroString()]), ([AvroString()], AvroArray(AvroString()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroArray(AvroInt())]), ([AvroArray(AvroInt())], AvroArray(AvroArray(AvroInt())))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroMap(AvroInt())]), ([AvroMap(AvroInt())], AvroArray(AvroMap(AvroInt())))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroUnion([AvroInt()])]), ([AvroUnion([AvroInt()])], AvroArray(AvroUnion([AvroInt()])))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroFixed(10, "MyFixed")]), ([AvroFixed(10, "MyFixed")], AvroArray(AvroFixed(10, "MyFixed")))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroEnum(["one", "two", "three"], "MyEnum")]), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroArray(AvroEnum(["one", "two", "three"], "MyEnum")))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroArray(AvroRecord([AvroField("one", AvroInt())], "MyRecord")))))

    def testUseWildcardsToNormalizeNumericalTypes(self):
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroInt()]), ([AvroInt(), AvroInt()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroLong()]), ([AvroLong(), AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroFloat()]), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroDouble()]), ([AvroDouble(), AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroInt()]), ([AvroLong(), AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroLong()]), ([AvroLong(), AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroFloat()]), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroDouble()]), ([AvroDouble(), AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroInt()]), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroLong()]), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroFloat()]), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroDouble()]), ([AvroDouble(), AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroInt()]), ([AvroDouble(), AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroLong()]), ([AvroDouble(), AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroFloat()]), ([AvroDouble(), AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroDouble()]), ([AvroDouble(), AvroDouble()], AvroNull())))

    def testMatchWildRecords(self):
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Int()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")]), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroRecord([AvroField("one", AvroInt())], "MyRecord"))))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Int()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")]), ([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")], AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord"))))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.String()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Double()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")]), None))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Int()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("uno", AvroInt()), AvroField("two", AvroString())], "MyRecord")]), None))

if __name__ == "__main__":
    unittest.main()
