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
from titus.reader import jsonToAst
from titus.datatype import *

class TestJsonToAst(unittest.TestCase):
    def testEngineConfig(self):
        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": {"+": [2, 2]}
}'''))

        self.assertEqual(
            EngineConfig(
            "test",
            Method.EMIT,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "method": "emit",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}'''))

        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            {},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}]
}'''))

        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}}
}'''))

        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {},
            None,
            {"private": Cell(AvroInt(), "0", False, False)},
            {"private": Pool(AvroInt(), {}, False, False)},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}}
}'''))

        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {},
            None,
            {},
            {},
            12345,
            "hello",
            None,
            {"internal": "data"},
            {"param": json.loads("3")}),
            jsonToAst('''{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "randseed":12345,
  "doc":"hello",
  "metadata":{"internal":"data"},
  "options":{"param":3}
}'''))

        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
            None,
            {"private": Cell(AvroInt(), "0", False, False)},
            {"private": Pool(AvroInt(), {}, False, False)},
            12345,
            "hello",
            None,
            {"internal": "data"},
            {"param": json.loads("3")}),
            jsonToAst('''{
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
        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {},
            None,
            {"private": Cell(AvroArray(AvroString()), "[]", False, False)},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "cells":{"private":{"type":{"type": "array", "items": "string"},"init":[],"shared":false,"rollback":false}}
}'''))

    def testDefineFunction(self):
        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {"f": FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}}
}'''))

    def testCallFunction(self):
        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("+", [LiteralInt(2), LiteralInt(2)])],
            [],
            {},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}'''))

    def testCallWithFcn(self):
        self.assertEqual(
            EngineConfig(
            "test",
            Method.MAP,
            AvroInt(),
            AvroString(),
            [],
            [Call("sort", [Ref("array"), FcnRef("byname")])],
            [],
            {},
            None,
            {},
            {},
            None,
            None,
            None,
            {},
            {}),
            jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"sort":["array",{"fcn": "byname"}]}]
}'''))

    def testCallWithFcnDef(self):
        x = EngineConfig("test", Method.MAP, AvroInt(), AvroString(), [], [Call("sort", [Ref("array"), FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])])], [], {}, None, {}, {}, None, None, None, {}, {})
        y = jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"sort":["array",{"params": [{"x": "int"}, {"y": "string"}], "ret": "null", "do": [null]}]}]
}''')
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [Call("sort", [Ref("array"), FcnDef([{"x": AvroInt()}, {"y": AvroString()}], AvroNull(), [LiteralNull()])])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"sort":["array",{"params": [{"x": "int"}, {"y": "string"}], "ret": "null", "do": [null]}]}]
}'''))

    def testRef(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [Ref("x")],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": ["x"]
}'''))

    def testNull(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [LiteralNull()],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [null]
}'''))

    def testBoolean(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [LiteralBoolean(True), LiteralBoolean(False)],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [true, false]
}'''))

    def testInt(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [LiteralInt(2)],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [2]
}'''))

    def testLong(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroLong(),
        AvroString(),
        [],
        [LiteralLong(2)],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "long",
  "output": "string",
  "action": [{"long": 2}]
}'''))
  
    def testFloat(self):
      self.assertEqual(
      EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      [],
      [LiteralFloat(2.5)],
      [],
      {},
      None,
      {},
      {},
      None,
      None,
      None,
      {},
      {}),
      jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"float": 2.5}]
}'''))

    def testDouble(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [LiteralDouble(2.2)],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [2.2]
}'''))

    def testString(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [LiteralString("hello")],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"string": "hello"}]
}'''))
  
    def testBase64(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [LiteralBase64("hello".encode("utf-8"))],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"base64": "aGVsbG8="}]
}'''))

    def testLiteral(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [Literal(AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())], "SimpleRecord"), '''{"one": 1, "two": 2.2, "three": "THREE"}''')],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"type":{"type":"record","name":"SimpleRecord","fields":[{"name":"one","type":"int"},{"name":"two","type":"double"},{"name":"three","type":"string"}]},"value":{"one":1,"two":2.2,"three":"THREE"}}]
}'''))
  
    def testNewRecord(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [NewObject({"one": LiteralInt(1), "two": LiteralDouble(2.2), "three": LiteralString("THREE")},
                       AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())], "SimpleRecord"), AvroTypeBuilder())],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"new":{"one":{"int":1},"two":2.2,"three":{"string":"THREE"}},"type":{"type":"record","name":"SimpleRecord","fields":[{"name":"one","type":"int"},{"name":"two","type":"double"},{"name":"three","type":"string"}]}}]
}'''))
  
    def testNewArray(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [NewArray([LiteralInt(1), LiteralInt(2), LiteralInt(3)], AvroArray(AvroInt()), AvroTypeBuilder())],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"new":[1,2,3],"type":{"type":"array","items":"int"}}]
}'''))
  
    def testDo(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [Do([Ref("x"), Ref("y"), Ref("z")])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"do":["x","y","z"]}]
}'''))
  
    def testLet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [Let({"x": LiteralInt(3), "y": LiteralInt(4)})],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"let":{"x":3,"y":4}}]
}'''))
  
    def testSet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [SetVar({"x": LiteralInt(3), "y": LiteralInt(4)})],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"set":{"x":3,"y":4}}]
}'''))
  
    def testAttrGet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [AttrGet(Ref("c"), [Ref("a"), LiteralInt(1), LiteralString("b")])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"attr":"c","path":["a",1,{"string":"b"}]}]
}'''))
  
    def testAttrSet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [AttrTo(Ref("c"), [Ref("a"), LiteralInt(1), LiteralString("b")], LiteralDouble(2.2), None)],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"attr":"c","path":["a",1,{"string":"b"}],"to":2.2}]
}'''))
  
    def testCellGet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [CellGet("c", [])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c"}]
}'''))
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [CellGet("c", [Ref("a"), LiteralInt(1), LiteralString("b")])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c","path":["a",1,{"string":"b"}]}]
}'''))
  
    def testCellSet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [CellTo("c", [], LiteralDouble(2.2))],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c","to":2.2}]
}'''))
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [CellTo("c", [Ref("a"), LiteralInt(1), LiteralString("b")], LiteralDouble(2.2))],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c","path":["a",1,{"string":"b"}],"to":2.2}]
}'''))
  
    def testPoolGet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [PoolGet("p", [Ref("a"), LiteralInt(1), LiteralString("b")])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"pool":"p","path":["a",1,{"string":"b"}]}]
}'''))
  
    def testPoolSet(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [PoolTo("p", [Ref("a"), LiteralInt(1), LiteralString("b")], LiteralDouble(2.2), LiteralDouble(2.2))],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"pool":"p","path":["a",1,{"string":"b"}],"to":2.2,"init":2.2}]
}'''))

    def testIf(self):
        self.assertEqual(
        EngineConfig(
        "test",
        Method.MAP,
        AvroInt(),
        AvroString(),
        [],
        [If(LiteralBoolean(True), [Ref("x")], None), If(LiteralBoolean(True), [Ref("x")], [Ref("y")])],
        [],
        {},
        None,
        {},
        {},
        None,
        None,
        None,
        {},
        {}),
        jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"if":true,"then":["x"]}, {"if":true,"then":["x"],"else":["y"]}]
}'''))

    def testCond(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [Cond([If(LiteralBoolean(False), [Ref("x")], None), If(LiteralBoolean(True), [Ref("y")], None)], None),
               Cond([If(LiteralBoolean(False), [Ref("x")], None), If(LiteralBoolean(True), [Ref("y")], None)], [Ref("z")])],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}]},
             {"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}],"else":["z"]}]
}'''))
  
    def testWhile(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [While(LiteralBoolean(True), [Call("+", [LiteralInt(2), LiteralInt(2)])])],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"while":true,"do":[{"+":[2,2]}]}]
}'''))
  
    def testDoUntil(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [DoUntil([Call("+", [LiteralInt(2), LiteralInt(2)])], LiteralBoolean(True))],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"do":[{"+":[2,2]}],"until":true}]
}'''))
  
    def testFor(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [For({"i": LiteralInt(0)}, Call("<", [Ref("i"), LiteralInt(10)]), {"i": Call("+", [Ref("i"), LiteralInt(1)])}, [Ref("i")], False)],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"for":{"i":0},"while":{"<":["i",10]},"step":{"i":{"+":["i",1]}},"do":["i"]}]
}'''))
  
    def testForeach(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [Foreach("x", Literal(AvroArray(AvroInt()), '''[1, 2, 3]'''), [Ref("x")], False)],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"foreach":"x","in":{"type":{"type":"array","items":"int"},"value":[1,2,3]},"do":["x"],"seq":false}]
}'''))
  
    def testForkeyval(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [Forkeyval("k", "v", Literal(AvroMap(AvroInt()), '''{"one": 1, "two": 2, "three": 3}'''), [Ref("k")])],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"forkey":"k","forval":"v","in":{"type":{"type":"map","values":"int"},"value":{"one":1,"two":2,"three":3}},"do":["k"]}]
}'''))
  
    def testCast(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [CastBlock(LiteralInt(3), [CastCase(AvroString(), "x", [Ref("x")]), CastCase(AvroInt(), "x", [Ref("x")])], True)],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cast":{"int":3},"cases":[{"as":"string","named":"x","do":["x"]},{"as":"int","named":"x","do":["x"]}],"partial":true}]
}'''))
  
    def testDoc(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [Doc("hello")],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"doc":"hello"}]
}'''))
  
    def testError(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [Error("hello", None), Error("hello", 3)],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"error":"hello"}, {"error":"hello","code":3}]
}'''))
  
    def testLog(self):
        self.assertEqual(
          EngineConfig(
          "test",
          Method.MAP,
          AvroInt(),
          AvroString(),
          [],
          [Log([LiteralString("hello")], None),
           Log([LiteralString("hello")], "DEBUG"),
           Log([Call("+", [LiteralInt(2), LiteralInt(2)])], None)],
          [],
          {},
          None,
          {},
          {},
          None,
          None,
          None,
          {},
          {}),
          jsonToAst('''{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"log":[{"string":"hello"}]},
             {"log":[{"string":"hello"}],"namespace":"DEBUG"},
             {"log":[{"+":[2,2]}]}]
}'''))
  
if __name__ == "__main__":
    unittest.main()
