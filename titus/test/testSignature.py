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
    version = PFAVersion(0, 8, 1)

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
            (dummy, xparams, xret) = x
            (yparams, yret) = y
            return all(self.typeEquality(xp, yp) for xp, yp in zip(xparams, yparams)) and self.typeEquality(xret, yret)
            
    def testPassThroughExactParameterMatches(self):
        self.assertTrue(self.matches(Sig([{"x": P.Null()}], P.Null()).accepts([AvroNull()], self.version), ([AvroNull()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroInt()], self.version), ([AvroInt()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroLong()], self.version), ([AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroFloat()], self.version), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroDouble()], self.version), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Bytes()}], P.Null()).accepts([AvroBytes()], self.version), ([AvroBytes()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.String()}], P.Null()).accepts([AvroString()], self.version), ([AvroString()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Union([P.Int()])}], P.Null()).accepts([AvroUnion([AvroInt()])], self.version), ([AvroUnion([AvroInt()])], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10)}], P.Null()).accepts([AvroFixed(10, "MyFixed")], self.version), ([AvroFixed(10, "MyFixed")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10, "MyFixed")}], P.Null()).accepts([AvroFixed(10, "MyFixed")], self.version), ([AvroFixed(10, "MyFixed")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"])}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")], self.version), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"], "MyEnum")}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")], self.version), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()})}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()}, "MyRecord")}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroNull())))

    def testNotMatchAntiPatterns(self):
        self.assertTrue(self.matches(Sig([{"x": P.Null()}], P.Null()).accepts([AvroInt()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroLong()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroFloat()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroDouble()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroBytes()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Bytes()}], P.Null()).accepts([AvroString()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.String()}], P.Null()).accepts([AvroArray(AvroInt())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroMap(AvroInt())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroUnion([AvroInt()])], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Union([P.Int()])}], P.Null()).accepts([AvroFixed(10, "MyFixed")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10, "YourFixed")}], P.Null()).accepts([AvroFixed(10, "MyFixed")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Fixed(10)}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"], "YourEnum")}], P.Null()).accepts([AvroEnum(["one", "two", "three"], "MyEnum")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Enum(["one", "two", "three"])}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()}, "YourRecord")}], P.Null()).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Record({"one": P.Int()})}], P.Null()).accepts([AvroNull()], self.version), None))

    def testPromoteNumbers(self):
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroInt()], self.version), ([AvroInt()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroLong()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroFloat()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Int()}], P.Null()).accepts([AvroDouble()], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroInt()], self.version), ([AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroLong()], self.version), ([AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroFloat()], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Long()}], P.Null()).accepts([AvroDouble()], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroInt()], self.version), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroLong()], self.version), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroFloat()], self.version), ([AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Float()}], P.Null()).accepts([AvroDouble()], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroInt()], self.version), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroLong()], self.version), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroDouble()], self.version), ([AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Double()}], P.Null()).accepts([AvroDouble()], self.version), ([AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroLong())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroFloat())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Int())}], P.Null()).accepts([AvroArray(AvroDouble())], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroLong())], self.version), ([AvroArray(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroFloat())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Long())}], P.Null()).accepts([AvroArray(AvroDouble())], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroLong())], self.version), ([AvroArray(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroFloat())], self.version), ([AvroArray(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Float())}], P.Null()).accepts([AvroArray(AvroDouble())], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroLong())], self.version), ([AvroArray(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroDouble())], self.version), ([AvroArray(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Double())}], P.Null()).accepts([AvroArray(AvroDouble())], self.version), ([AvroArray(AvroDouble())], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroLong())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroFloat())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Int())}], P.Null()).accepts([AvroMap(AvroDouble())], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroLong())], self.version), ([AvroMap(AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroFloat())], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Long())}], P.Null()).accepts([AvroMap(AvroDouble())], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroLong())], self.version), ([AvroMap(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroFloat())], self.version), ([AvroMap(AvroFloat())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Float())}], P.Null()).accepts([AvroMap(AvroDouble())], self.version), None))

        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroLong())], self.version), ([AvroMap(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroDouble())], self.version), ([AvroMap(AvroDouble())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Map(P.Double())}], P.Null()).accepts([AvroMap(AvroDouble())], self.version), ([AvroMap(AvroDouble())], AvroNull())))

    def testLooselyMatchFunctionReferences(self):
        self.assertTrue(self.matches(Sig([{"x": P.Fcn([P.Long()], P.Long())}], P.Null()).accepts([FcnType([AvroLong()], AvroLong())], self.version), ([FcnType([AvroLong()], AvroLong())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fcn([P.Long()], P.Long())}], P.Null()).accepts([FcnType([AvroLong()], AvroInt())], self.version), ([FcnType([AvroLong()], AvroInt())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Fcn([P.Long()], P.Long())}], P.Null()).accepts([FcnType([AvroDouble()], AvroLong())], self.version), ([FcnType([AvroDouble()], AvroLong())], AvroNull())))

    def testMatchWildcards(self):
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroNull()], self.version), ([AvroNull()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroInt()], self.version), ([AvroInt()], AvroInt())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroLong()], self.version), ([AvroLong()], AvroLong())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroFloat()], self.version), ([AvroFloat()], AvroFloat())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroDouble()], self.version), ([AvroDouble()], AvroDouble())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroBytes()], self.version), ([AvroBytes()], AvroBytes())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroString()], self.version), ([AvroString()], AvroString())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroInt())], AvroArray(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroInt())], AvroMap(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroUnion([AvroInt()])], self.version), ([AvroUnion([AvroInt()])], AvroUnion([AvroInt()]))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroFixed(10, "MyFixed")], self.version), ([AvroFixed(10, "MyFixed")], AvroFixed(10, "MyFixed"))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroEnum(["one", "two", "three"], "MyEnum")], self.version), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroEnum(["one", "two", "three"], "MyEnum"))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroRecord([AvroField("one", AvroInt())], "MyRecord"))))

    def testMatchNestedWildcards(self):
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroNull())], self.version), ([AvroArray(AvroNull())], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroInt())], AvroInt())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroLong())], self.version), ([AvroArray(AvroLong())], AvroLong())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroFloat())], self.version), ([AvroArray(AvroFloat())], AvroFloat())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroDouble())], self.version), ([AvroArray(AvroDouble())], AvroDouble())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroBytes())], self.version), ([AvroArray(AvroBytes())], AvroBytes())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroString())], self.version), ([AvroArray(AvroString())], AvroString())))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroArray(AvroInt()))], self.version), ([AvroArray(AvroArray(AvroInt()))], AvroArray(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroMap(AvroInt()))], self.version), ([AvroArray(AvroMap(AvroInt()))], AvroMap(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroUnion([AvroInt()]))], self.version), ([AvroArray(AvroUnion([AvroInt()]))], AvroUnion([AvroInt()]))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroFixed(10, "MyFixed"))], self.version), ([AvroArray(AvroFixed(10, "MyFixed"))], AvroFixed(10, "MyFixed"))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroEnum(["one", "two", "three"], "MyEnum"))], self.version), ([AvroArray(AvroEnum(["one", "two", "three"], "MyEnum"))], AvroEnum(["one", "two", "three"], "MyEnum"))))
        self.assertTrue(self.matches(Sig([{"x": P.Array(P.Wildcard("A"))}], P.Wildcard("A")).accepts([AvroArray(AvroRecord([AvroField("one", AvroInt())], "MyRecord"))], self.version), ([AvroArray(AvroRecord([AvroField("one", AvroInt())], "MyRecord"))], AvroRecord([AvroField("one", AvroInt())], "MyRecord"))))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroNull()], self.version), ([AvroNull()], AvroArray(AvroNull()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroInt()], self.version), ([AvroInt()], AvroArray(AvroInt()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroLong()], self.version), ([AvroLong()], AvroArray(AvroLong()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroFloat()], self.version), ([AvroFloat()], AvroArray(AvroFloat()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroDouble()], self.version), ([AvroDouble()], AvroArray(AvroDouble()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroBytes()], self.version), ([AvroBytes()], AvroArray(AvroBytes()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroString()], self.version), ([AvroString()], AvroArray(AvroString()))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroArray(AvroInt())], self.version), ([AvroArray(AvroInt())], AvroArray(AvroArray(AvroInt())))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroMap(AvroInt())], self.version), ([AvroMap(AvroInt())], AvroArray(AvroMap(AvroInt())))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroUnion([AvroInt()])], self.version), ([AvroUnion([AvroInt()])], AvroArray(AvroUnion([AvroInt()])))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroFixed(10, "MyFixed")], self.version), ([AvroFixed(10, "MyFixed")], AvroArray(AvroFixed(10, "MyFixed")))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroEnum(["one", "two", "three"], "MyEnum")], self.version), ([AvroEnum(["one", "two", "three"], "MyEnum")], AvroArray(AvroEnum(["one", "two", "three"], "MyEnum")))))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A")}], P.Array(P.Wildcard("A"))).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroArray(AvroRecord([AvroField("one", AvroInt())], "MyRecord")))))

    def testUseWildcardsToNormalizeNumericalTypes(self):
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroInt()], self.version), ([AvroInt(), AvroInt()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroLong()], self.version), ([AvroLong(), AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroFloat()], self.version), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroInt(), AvroDouble()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroInt()], self.version), ([AvroLong(), AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroLong()], self.version), ([AvroLong(), AvroLong()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroFloat()], self.version), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroLong(), AvroDouble()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroInt()], self.version), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroLong()], self.version), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroFloat()], self.version), ([AvroFloat(), AvroFloat()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroFloat(), AvroDouble()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))

        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroInt()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroLong()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroFloat()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))
        self.assertTrue(self.matches(Sig([{"x": P.Wildcard("A", set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()]))}, {"y": P.Wildcard("A")}], P.Null()).accepts([AvroDouble(), AvroDouble()], self.version), ([AvroDouble(), AvroDouble()], AvroNull())))

    def testMatchWildRecords(self):
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Int()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], self.version), ([AvroRecord([AvroField("one", AvroInt())], "MyRecord")], AvroRecord([AvroField("one", AvroInt())], "MyRecord"))))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Int()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")], self.version), ([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")], AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord"))))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.String()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Double()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroString())], "MyRecord")], self.version), None))
        self.assertTrue(self.matches(Sig([{"x": P.WildRecord("A", {"one": P.Int()})}], P.Wildcard("A")).accepts([AvroRecord([AvroField("uno", AvroInt()), AvroField("two", AvroString())], "MyRecord")], self.version), None))

if __name__ == "__main__":
    unittest.main()
