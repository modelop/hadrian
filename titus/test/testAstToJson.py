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

from titus.pfaast import *
from titus.datatype import *

class TestAstToJson(unittest.TestCase):
    def testEngineConfig(self):
        self.assertEqual(
        json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [],
        {},
        None,
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}'''))

        self.assertEqual(
        json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {},
        None,
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}]
}'''))

        self.assertEqual(
        json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
        None,
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}}
}'''))

        self.assertEqual(
        json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
        None,
        None,
        {"private": Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED)},
        {},
        None,
        None,
        None,
        {},
        {}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}}
}'''))

        self.assertEqual(json.loads(EngineConfig(
        "test",
         Method.MAP,
         AvroInt(),
         AvroString(),
         [Call("+", [LiteralInt(2), LiteralInt(2)])],
         [Call("+", [LiteralInt(2), LiteralInt(2)])],
         [Call("+", [LiteralInt(2), LiteralInt(2)])],
         {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
         None,
         None,
         {"private": Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED)},
         {"private": Pool(AvroInt(), {}, False, False, CellPoolSource.EMBEDDED)},
         None,
         None,
         None,
         {},
         {}).toJson(lineNumbers=False)),
         json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}}
}'''))

        self.assertEqual(json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
        None,
        None,
        {"private": Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED)},
        {"private": Pool(AvroInt(), {}, False, False, CellPoolSource.EMBEDDED)},
        12345,
        None,
        None,
        {},
        {}).toJson(lineNumbers=False)),
    json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}},
  "randseed":12345
}'''))

        self.assertEqual(json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
        None,
        None,
        {"private": Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED)},
        {"private": Pool(AvroInt(), {}, False, False, CellPoolSource.EMBEDDED)},
        12345,
        "hello",
        None,
        {},
        {}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}},
  "randseed":12345,
  "doc":"hello"
}'''))

        self.assertEqual(json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
        None,
        None,
        {"private": Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED)},
        {"private": Pool(AvroInt(), {}, False, False, CellPoolSource.EMBEDDED)},
        12345,
        "hello",
        None,
        {"internal": "data"},
        {}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}},
  "randseed":12345,
  "doc":"hello",
  "metadata":{"internal":"data"}
}'''))

        self.assertEqual(json.loads(EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        [Call("+", [LiteralInt(2), LiteralInt(2)])],
        {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
        None,
        None,
        {"private": Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED)},
        {"private": Pool(AvroInt(), {}, False, False, CellPoolSource.EMBEDDED)},
        12345,
        "hello",
        None,
        {"internal": "data"},
        {"param": json.loads("3")}).toJson(lineNumbers=False)),
        json.loads('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}},
  "randseed":12345,
  "doc":"hello",
  "metadata":{"internal":"data"},
  "options":{"param":3}
}'''))

    def testCell(self):
        self.assertEqual(json.loads(Cell(AvroInt(), "0", False, False, CellPoolSource.EMBEDDED).toJson(lineNumbers=False)), json.loads('''{"type":"int","init":0,"shared":false,"rollback":false}'''))

    def testPool(self):
        self.assertEqual(json.loads(Pool(AvroInt(), {}, False, False, CellPoolSource.EMBEDDED).toJson(lineNumbers=False)), json.loads('''{"type":"int","init":{},"shared":false,"rollback":false}'''))
        self.assertEqual(json.loads(Pool(AvroInt(), {"one": "1"}, False, False, CellPoolSource.EMBEDDED).toJson(lineNumbers=False)), json.loads('''{"type":"int","init":{"one":1},"shared":false,"rollback":false}'''))

    def testDefineFunction(self):
        self.assertEqual(json.loads(FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()]).toJson(lineNumbers=False)), json.loads('''{"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}'''))

    def testCallFunction(self):
        self.assertEqual(json.loads(Call("+", [LiteralInt(2), LiteralInt(2)]).toJson(lineNumbers=False)), json.loads('''{"+":[2,2]}'''))

    def testCallWithFcn(self):
        self.assertEqual(json.loads(Call("sort", [Ref("array"), FcnRef("byname")]).toJson(lineNumbers=False)), json.loads('''{"sort":["array",{"fcn": "byname"}]}'''))

    def testCallWithFcnDef(self):
        self.assertEqual(json.loads(Call("sort", [Ref("array"), FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])]).toJson(lineNumbers=False)),
                         json.loads('''{"sort":["array",{"params": [{"x": "int"}, {"y": "string"}], "ret": "null", "do": [null]}]}'''))

    def testRef(self):
        self.assertEqual(json.loads(Ref("x").toJson(lineNumbers=False)), "x")

    def testNull(self):
        self.assertEqual(json.loads(LiteralNull().toJson(lineNumbers=False)), json.loads("null"))

    def testBoolean(self):
        self.assertEqual(json.loads(LiteralBoolean(True).toJson(lineNumbers=False)), json.loads("true"))
        self.assertEqual(json.loads(LiteralBoolean(False).toJson(lineNumbers=False)), json.loads("false"))

    def testInt(self):
        self.assertEqual(json.loads(LiteralInt(2).toJson(lineNumbers=False)), json.loads('''2'''))

    def testLong(self):
        self.assertEqual(json.loads(LiteralLong(2).toJson(lineNumbers=False)), json.loads('''{"long":2}'''))

    def testFloat(self):
        self.assertEqual(json.loads(LiteralFloat(2.5).toJson(lineNumbers=False)), json.loads('''{"float":2.5}'''))

    def testDouble(self):
        self.assertEqual(json.loads(LiteralDouble(2.2).toJson(lineNumbers=False)), json.loads("2.2"))

    def testString(self):
        self.assertEqual(json.loads(LiteralString("hello").toJson(lineNumbers=False)), json.loads('''{"string":"hello"}'''))

    def testBase64(self):
        self.assertEqual(json.loads(LiteralBase64("hello".encode("utf-8")).toJson(lineNumbers=False)), json.loads('''{"base64":"aGVsbG8="}'''))

    def testLiteral(self):
        self.assertEqual(json.loads(Literal(AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())], "SimpleRecord"), '''{"one": 1, "two": 2.2, "three": "THREE"}''').toJson(lineNumbers=False)),
        json.loads('''{"type":{"type":"record","name":"SimpleRecord","fields":[{"name":"one","type":"int"},{"name":"two","type":"double"},{"name":"three","type":"string"}]},"value":{"one":1,"two":2.2,"three":"THREE"}}'''))

    def testNewRecord(self):
        self.assertEqual(json.loads(NewObject({"one": LiteralInt(1), "two": LiteralDouble(2.2), "three": LiteralString("THREE")},
                                   AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())], "SimpleRecord"), AvroTypeBuilder()).toJson(lineNumbers=False)),
        json.loads('''{"new":{"one":1,"two":2.2,"three":{"string":"THREE"}},"type":{"type":"record","name":"SimpleRecord","fields":[{"name":"one","type":"int"},{"name":"two","type":"double"},{"name":"three","type":"string"}]}}'''))

    def testNewArray(self):
        self.assertEqual(json.loads(NewArray([LiteralInt(1), LiteralInt(2), LiteralInt(3)], AvroArray(AvroInt()), AvroTypeBuilder()).toJson(lineNumbers=False)), json.loads('''{"new":[1,2,3],"type":{"type":"array","items":"int"}}'''))

    def testDo(self):
        self.assertEqual(json.loads(Do([Ref("x"), Ref("y"), Ref("z")]).toJson(lineNumbers=False)), json.loads('''{"do":["x","y","z"]}'''))

    def testLet(self):
        self.assertEqual(json.loads(Let({"x": LiteralInt(3), "y": LiteralInt(4)}).toJson(lineNumbers=False)), json.loads('''{"let":{"x":3,"y":4}}'''))

    def testSet(self):
        self.assertEqual(json.loads(SetVar({"x": LiteralInt(3), "y": LiteralInt(4)}).toJson(lineNumbers=False)), json.loads('''{"set":{"x":3,"y":4}}'''))

    def testAttrGet(self):
        self.assertEqual(json.loads(AttrGet(Ref("a"), [Ref("a"), LiteralInt(1), LiteralString("b")]).toJson(lineNumbers=False)), json.loads('''{"attr":"a","path":["a",1,{"string":"b"}]}'''))

    def testAttrSet(self):
        self.assertEqual(json.loads(AttrTo(Ref("a"), [Ref("a"), LiteralInt(1), LiteralString("b")], LiteralDouble(2.2)).toJson(lineNumbers=False)), json.loads('''{"attr":"a","path":["a",1,{"string":"b"}],"to":2.2}'''))

    def testCellGet(self):
        self.assertEqual(json.loads(CellGet("c", []).toJson(lineNumbers=False)), json.loads('''{"cell":"c"}'''))
        self.assertEqual(json.loads(CellGet("c", [Ref("a"), LiteralInt(1), LiteralString("b")]).toJson(lineNumbers=False)), json.loads('''{"cell":"c","path":["a",1,{"string":"b"}]}'''))

    def testCellSet(self):
        self.assertEqual(json.loads(CellTo("c", [], LiteralDouble(2.2)).toJson(lineNumbers=False)), json.loads('''{"cell":"c","to":2.2}'''))
        self.assertEqual(json.loads(CellTo("c", [Ref("a"), LiteralInt(1), LiteralString("b")], LiteralDouble(2.2)).toJson(lineNumbers=False)), json.loads('''{"cell":"c","path":["a",1,{"string":"b"}],"to":2.2}'''))

    def testPoolGet(self):
        self.assertEqual(json.loads(PoolGet("p", [Ref("a"), LiteralInt(1), LiteralString("b")]).toJson(lineNumbers=False)), json.loads('''{"pool":"p","path":["a",1,{"string":"b"}]}'''))

    def testPoolSet(self):
        self.assertEqual(json.loads(PoolTo("p", [Ref("a"), LiteralInt(1), LiteralString("b")], LiteralDouble(2.2), LiteralDouble(2.2)).toJson(lineNumbers=False)), json.loads('''{"pool":"p","path":["a",1,{"string":"b"}],"to":2.2,"init":2.2}'''))

    def testIf(self):
        self.assertEqual(json.loads(If(LiteralBoolean(True), [Ref("x")], None).toJson(lineNumbers=False)), json.loads('''{"if":true,"then":["x"]}'''))
        self.assertEqual(json.loads(If(LiteralBoolean(True), [Ref("x")], [Ref("y")]).toJson(lineNumbers=False)), json.loads('''{"if":true,"then":["x"],"else":["y"]}'''))

    def testCond(self):
        self.assertEqual(json.loads(Cond([If(LiteralBoolean(False), [Ref("x")], None), If(LiteralBoolean(True), [Ref("y")], None)], None).toJson(lineNumbers=False)),
                         json.loads('''{"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}]}'''))
        self.assertEqual(json.loads(Cond([If(LiteralBoolean(False), [Ref("x")], None), If(LiteralBoolean(True), [Ref("y")], None)], [Ref("z")]).toJson(lineNumbers=False)),
                         json.loads('''{"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}],"else":["z"]}'''))

    def testWhile(self):
        self.assertEqual(json.loads(While(LiteralBoolean(True), [Call("+", [LiteralInt(2), LiteralInt(2)])]).toJson(lineNumbers=False)),
                         json.loads('''{"while":true,"do":[{"+":[2,2]}]}'''))

    def testDoUntil(self):
        self.assertEqual(json.loads(DoUntil([Call("+", [LiteralInt(2), LiteralInt(2)])], LiteralBoolean(True)).toJson(lineNumbers=False)),
                         json.loads('''{"do":[{"+":[2,2]}],"until":true}'''))

    def testFor(self):
        self.assertEqual(json.loads(For({"i": LiteralInt(0)}, Call("<", [Ref("i"), LiteralInt(10)]), {"i": Call("+", [Ref("i"), LiteralInt(1)])}, [Ref("i")], False).toJson(lineNumbers=False)),
                         json.loads('''{"for":{"i":0},"while":{"<":["i",10]},"step":{"i":{"+":["i",1]}},"do":["i"]}'''))

    def testForeach(self):
        self.assertEqual(json.loads(Foreach("x", Literal(AvroArray(AvroInt()), '''[1, 2, 3]'''), [Ref("x")], False).toJson(lineNumbers=False)),
                         json.loads('''{"foreach":"x","in":{"type":{"type":"array","items":"int"},"value":[1,2,3]},"do":["x"],"seq":false}'''))

    def testForkeyval(self):
        self.assertEqual(json.loads(Forkeyval("k", "v", Literal(AvroMap(AvroInt()), '''{"one": 1, "two": 2, "three": 3}'''), [Ref("k")]).toJson(lineNumbers=False)),
                         json.loads('''{"forkey":"k","forval":"v","in":{"type":{"type":"map","values":"int"},"value":{"one":1,"two":2,"three":3}},"do":["k"]}'''))

    def testCast(self):
        self.assertEqual(json.loads(CastBlock(LiteralInt(3), [CastCase(AvroString(), "x", [Ref("x")]), CastCase(AvroInt(), "x", [Ref("x")])], False).toJson(lineNumbers=False)),
                         json.loads('''{"cast":3,"cases":[{"as":"string","named":"x","do":["x"]},{"as":"int","named":"x","do":["x"]}],"partial":false}'''))

    def testDoc(self):
        self.assertEqual(json.loads(Doc("hello").toJson(lineNumbers=False)), json.loads('''{"doc":"hello"}'''))

    def testError(self):
        self.assertEqual(json.loads(Error("hello", None).toJson(lineNumbers=False)), json.loads('''{"error":"hello"}'''))
        self.assertEqual(json.loads(Error("hello", 3).toJson(lineNumbers=False)), json.loads('''{"error":"hello","code":3}'''))

    def testLog(self):
        self.assertEqual(json.loads(Log([LiteralString("hello")], None).toJson(lineNumbers=False)), json.loads('''{"log":[{"string":"hello"}]}'''))
        self.assertEqual(json.loads(Log([LiteralString("hello")], "DEBUG").toJson(lineNumbers=False)), json.loads('''{"log":[{"string":"hello"}],"namespace":"DEBUG"}'''))
        self.assertEqual(json.loads(Log([Call("+", [LiteralInt(2), LiteralInt(2)])], None).toJson(lineNumbers=False)), json.loads('''{"log":[{"+":[2,2]}]}'''))

if __name__ == "__main__":
    unittest.main()
