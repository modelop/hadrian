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
import random
import unittest

import titus.prettypfa
import titus.pfaast
from titus.errors import PrettyPfaException

class TestPrettyPfaParsing(unittest.TestCase):
    def testStructure(self):
        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
method: fold
input: double
output: double
begin: 2 + 2
action: input + tally
end: {3 + 3; 4 + 4}
zero: [1, 2, 3]
randseed: 12345
doc: "hello world"
version: 12
metadata: {this: good}
options: {"wow": "wee"}
''', lineNumbers=False, check=False), {"name": "Test", "input": "double", "output": "double", "method": "fold", "begin": [{"+": [2, 2]}], "action": [{"+": ["input", "tally"]}], "end": [{"+": [3, 3]}, {"+": [4, 4]}], "zero": [1, 2, 3], "randseed": 12345, "doc": "hello world", "version": 12, "metadata": {"this": "good"}, "options": {"wow": "wee"}})

    def testCells(self):
        def test(expr, asjson):
            self.assertEqual(json.loads(titus.prettypfa.json(r'''
input: double
output: double
action: null
cells: HERE
'''.replace("HERE", expr), lineNumbers=False, check=False))["cells"], asjson)

        test("some(double) = 3", {"some": {"type": "double", "init": 3, "shared": False, "rollback": False}})
        test("some(double) = 3; other(string) = hello", {"some": {"type": "double", "init": 3, "shared": False, "rollback": False},
                                                         "other": {"type": "string", "init": "hello", "shared": False, "rollback": False}})
        test("some(double) = 3; other(string) = hello;", {"some": {"type": "double", "init": 3, "shared": False, "rollback": False},
                                                         "other": {"type": "string", "init": "hello", "shared": False, "rollback": False}})
        test("some(type: double) = 3", {"some": {"type": "double", "init": 3, "shared": False, "rollback": False}})
        test("some(double, shared: true) = 3", {"some": {"type": "double", "init": 3, "shared": True, "rollback": False}})
        test("some(double, rollback: true) = 3", {"some": {"type": "double", "init": 3, "shared": False, "rollback": True}})
        test("some(type: double, shared: true) = 3", {"some": {"type": "double", "init": 3, "shared": True, "rollback": False}})
        test("some(type: double, rollback: true) = 3", {"some": {"type": "double", "init": 3, "shared": False, "rollback": True}})

    def testPools(self):
        def test(expr, asjson):
            self.assertEqual(json.loads(titus.prettypfa.json(r'''
input: double
output: double
action: null
pools: HERE
'''.replace("HERE", expr), lineNumbers=False, check=False))["pools"], asjson)

        test("some(double) = {one: 3}", {"some": {"type": "double", "init": {"one": 3}, "shared": False, "rollback": False}})
        test("some(double) = {one: 3}; other(string) = {two: hello}", {"some": {"type": "double", "init": {"one": 3}, "shared": False, "rollback": False},
                                                                       "other": {"type": "string", "init": {"two": "hello"}, "shared": False, "rollback": False}})
        test("some(double) = {one: 3}; other(string) = {two: hello};", {"some": {"type": "double", "init": {"one": 3}, "shared": False, "rollback": False},
                                                                        "other": {"type": "string", "init": {"two": "hello"}, "shared": False, "rollback": False}})
        test("some(type: double) = {one: 3}", {"some": {"type": "double", "init": {"one": 3}, "shared": False, "rollback": False}})
        test("some(double, shared: true) = {one: 3}", {"some": {"type": "double", "init": {"one": 3}, "shared": True, "rollback": False}})
        test("some(double, rollback: true) = {one: 3}", {"some": {"type": "double", "init": {"one": 3}, "shared": False, "rollback": True}})
        test("some(type: double, shared: true) = {one: 3}", {"some": {"type": "double", "init": {"one": 3}, "shared": True, "rollback": False}})
        test("some(type: double, rollback: true) = {one: 3}", {"some": {"type": "double", "init": {"one": 3}, "shared": False, "rollback": True}})

    def testFunctions(self):
        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: null
fcns: square = fcn(-> double) x**2
''', lineNumbers=False, check=False)["fcns"], {"square": {"params": [], "ret": "double", "do": [{"**": ["x", 2]}]}})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: null
fcns: square = fcn(x: double -> double) x**2
''', lineNumbers=False, check=False)["fcns"], {"square": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: null
fcns: square = fcn(x: double, y: double -> double) x**2
''', lineNumbers=False, check=False)["fcns"], {"square": {"params": [{"x": "double"}, {"y": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: null
fcns:
  square = fcn(x: double -> double) x**2;
  cube = fcn(x: double -> double) x**3;
''', lineNumbers=False, check=False)["fcns"], {"square": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]},
                                               "cube": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 3]}]}})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: null
fcns: square = fcn(x: double -> double) {null; x**2}
''', lineNumbers=False, check=False)["fcns"], {"square": {"params": [{"x": "double"}], "ret": "double", "do": [None, {"**": ["x", 2]}]}})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: null
fcns:
  square = fcn(x: double -> double) {null; x**2};
  cube = fcn(x: double -> double) {null; x**3};
''', lineNumbers=False, check=False)["fcns"], {"square": {"params": [{"x": "double"}], "ret": "double", "do": [None, {"**": ["x", 2]}]},
                                               "cube": {"params": [{"x": "double"}], "ret": "double", "do": [None, {"**": ["x", 3]}]}})

    def testLiteralString(self):
        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: "hey there"
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "hey there"}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: "hey\nthere"
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "hey\nthere"}]})
        
        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: "hey\tthere"
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "hey\tthere"}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: "hey\"there"
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "hey\"there"}]})

        self.assertEqual(titus.prettypfa.jsonNode(r"""
name: Test
input: double
output: double
action: 'hey\tthere'
""", lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "hey\\tthere"}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: {
//    "hey there";
    "you guys";
}
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "you guys"}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: {
    "hey // there";
    "you guys";
}
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"string": "hey // there"}, {"string": "you guys"}]})

    def testLiteralNumbers(self):
        def test(expr, cls):
            self.assertTrue(isinstance(titus.prettypfa.ast(r'''
name: Test
input: double
output: double
action: HERE
'''.replace("HERE", expr), check=False).action[0], cls))

        test("null", titus.pfaast.LiteralNull)
        test("true", titus.pfaast.LiteralBoolean)
        test("false", titus.pfaast.LiteralBoolean)
        test("2", titus.pfaast.LiteralInt)
        test("-2", titus.pfaast.LiteralInt)
        test("2.", titus.pfaast.LiteralDouble)
        test("2.0", titus.pfaast.LiteralDouble)
        test("2e5", titus.pfaast.LiteralDouble)
        test("2.e5", titus.pfaast.LiteralDouble)
        test("2.0e5", titus.pfaast.LiteralDouble)
        test("2E5", titus.pfaast.LiteralDouble)
        test("2.E5", titus.pfaast.LiteralDouble)
        test("2.0E5", titus.pfaast.LiteralDouble)
        test("-2.", titus.pfaast.LiteralDouble)
        test("-2.0", titus.pfaast.LiteralDouble)
        test("-2e5", titus.pfaast.LiteralDouble)
        test("-2.e5", titus.pfaast.LiteralDouble)
        test("-2.0e5", titus.pfaast.LiteralDouble)
        test("-2E5", titus.pfaast.LiteralDouble)
        test("-2.E5", titus.pfaast.LiteralDouble)
        test("-2.0E5", titus.pfaast.LiteralDouble)

    def testLiterals(self):
        def test(expr, cls, asnode):
            text = r'''
name: Test
input: enum([uno, dos, tres], Numero)
output: enum([uno, dos, tres], super.duper.Numero)
action: HERE
'''.replace("HERE", expr)

            self.assertTrue(isinstance(titus.prettypfa.ast(text, check=False).action[0], cls))
            self.assertEqual(json.loads(titus.prettypfa.json(text, lineNumbers=False, check=False))["action"][0], asnode)

        test("json(null, null)", titus.pfaast.LiteralNull, None)
        test("json(int, 13)", titus.pfaast.LiteralInt, 13)
        test("json(long, 13)", titus.pfaast.LiteralLong, {"long": 13})
        test("json(float, 13.2)", titus.pfaast.LiteralFloat, {"float": 13.2})
        test("json(double, 13.2)", titus.pfaast.LiteralDouble, 13.2)
        test(r"json(string, hello)", titus.pfaast.LiteralString, {"string": "hello"})
        test(r'''json(string, "he\"llo")''', titus.pfaast.LiteralString, {"string": "he\"llo"})
        test("json(string, 'hel\"lo\')", titus.pfaast.LiteralString, {'string': 'hel"lo'})
        test('json(bytes, "aGVsbG8=")', titus.pfaast.LiteralBase64, {"base64": "aGVsbG8="})
        test("json(array(double), [])", titus.pfaast.Literal, {"type": {"type": "array", "items": "double"}, "value": []})
        test("json(array(double), [1, 2, 3])", titus.pfaast.Literal, {"type": {"type": "array", "items": "double"}, "value": [1, 2, 3]})
        test("json(array(array(double)), [[1, 2, 3], [4, 5, 6], [7, 8, 9]])", titus.pfaast.Literal, {"type": {"type": "array", "items": {"type": "array", "items": "double"}}, "value": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]})
        test(r"json(map(double), {})", titus.pfaast.Literal, {"type": {"type": "map", "values": "double"}, "value": {}})
        test(r"json(map(double), {on.e: 1, two: 2, three: 3})", titus.pfaast.Literal, {"type": {"type": "map", "values": "double"}, "value": {"on.e": 1, "two": 2, "three": 3}})
        test(r"json(map(double), {'on.e': 1, 'two': 2, 'three': 3})", titus.pfaast.Literal, {"type": {"type": "map", "values": "double"}, "value": {"on.e": 1, "two": 2, "three": 3}})
        test(r'json(map(double), {"on.e": 1, "two": 2, "three": 3})', titus.pfaast.Literal, {"type": {"type": "map", "values": "double"}, "value": {"on.e": 1, "two": 2, "three": 3}})
        test(r'json(array(map(double)), [{"on.e": 1, "two": 2, "three": 3}, {}])', titus.pfaast.Literal, {"type": {"type": "array", "items": {"type": "map", "values": "double"}}, "value": [{"on.e": 1, "two": 2, "three": 3}, {}]})
        test(r'json(record(one: int, two: double, three: string, TestRecord), {one: 1, two: 2.2, three: "THREE"})', titus.pfaast.Literal, {"type": {"type": "record", "name": "TestRecord", "fields": [{"name": "one", "type": "int"}, {"name": "two", "type": "double"}, {"name": "three", "type": "string"}]}, "value": {"one": 1, "two": 2.2, "three": "THREE"}})
        test("int(13)", titus.pfaast.LiteralInt, 13)
        test("long(13)", titus.pfaast.LiteralLong, {"long": 13})
        test("float(13.2)", titus.pfaast.LiteralFloat, {"float": 13.2})
        test("double(13.2)", titus.pfaast.LiteralDouble, 13.2)
        test("string(hey)", titus.pfaast.LiteralString, {"string": "hey"})
        test('string("hey")', titus.pfaast.LiteralString, {"string": "hey"})
        test("string('hey')", titus.pfaast.LiteralString, {"string": "hey"})
        test('bytes("aGVsbG8=")', titus.pfaast.LiteralBase64, {"base64": "aGVsbG8="})
        test("Numero@uno", titus.pfaast.Literal, {"type": "Numero", "value": "uno"})
        test("super.duper.Numero@uno", titus.pfaast.Literal, {"type": "super.duper.Numero", "value": "uno"})

    def testNew(self):
        def test(expr, cls, asnode):
            text = r'''
name: Test
input: double
output: double
action: HERE
'''.replace("HERE", expr)

            self.assertTrue(isinstance(titus.prettypfa.ast(text, check=False).action[0], cls))
            self.assertEqual(json.loads(titus.prettypfa.json(text, lineNumbers=False, check=False))["action"][0], asnode)

        test("new(array(int), 1, 2, 3)", titus.pfaast.NewArray, {"type": {"type": "array", "items": "int"}, "new": [1, 2, 3]})
        test("new(map(int), one: 1, two: 2, three: 3)", titus.pfaast.NewObject, {"type": {"type": "map", "values": "int"}, "new": {"one": 1, "two": 2, "three": 3}})

    def testExpressionBlock(self):
        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: 2 + 2
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: {2 + 2; 3 + 3}
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}, {"+": [3, 3]}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action:
    2 + 2;
    3 + 3
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}, {"+": [3, 3]}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: {2 + 2; 3 + 3;}
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}, {"+": [3, 3]}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action:
    2 + 2;
    3 + 3;
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}, {"+": [3, 3]}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: {2 + 2; {3 + 3; 4 + 4}}
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}, {"do": [{"+": [3, 3]}, {"+": [4, 4]}]}]})

        self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: {
    2 + 2;
    {
        3 + 3;
        4 + 4;
    }
}
''', lineNumbers=False, check=False), {"name": "Test",
                                       "input": "double",
                                       "output": "double",
                                       "method": "map",
                                       "action": [{"+": [2, 2]}, {"do": [{"+": [3, 3]}, {"+": [4, 4]}]}]})

    def testPrecedence(self):
        def test(expr, asjson):
            self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: HERE
'''.replace("HERE", expr), lineNumbers=False, check=False), {"name": "Test",
                                                             "input": "double",
                                                             "output": "double",
                                                             "method": "map",
                                                             "action": [asjson]})

        test("1 == 2 && 3 == 4",               {"&&": [{"==": [1, 2]}, {"==": [3, 4]}]})
        test("1 == 2 || 3 == 4",               {"||": [{"==": [1, 2]}, {"==": [3, 4]}]})
        test("1 == 2 ^^ 3 == 4",               {"^^": [{"==": [1, 2]}, {"==": [3, 4]}]})
        test("!1 == 2",                        {"!": [{"==": [1, 2]}]})
        test("1 == 2  ||  3 == 4  &&  4 == 5", {"||": [{"==": [1, 2]}, {"&&": [{"==": [3, 4]}, {"==": [4, 5]}]}]})
        test("1 == 2  ||  true  &&  4 == 5", {"||": [{"==": [1, 2]}, {"&&": [True, {"==": [4, 5]}]}]})
        test("1 == 2 + 3",                     {"==": [1, {"+": [2, 3]}]})
        test("1 + 2 == 3",                     {"==": [{"+": [1, 2]}, 3]})
        test("1 + 2 != 3",                     {"!=": [{"+": [1, 2]}, 3]})
        test("1 + 2 < 3",                      {"<": [{"+": [1, 2]}, 3]})
        test("1 + 2 <= 3",                     {"<=": [{"+": [1, 2]}, 3]})
        test("1 + 2 > 3",                      {">": [{"+": [1, 2]}, 3]})
        test("1 + 2 >= 3",                     {">=": [{"+": [1, 2]}, 3]})
        test("1 + -two * 3",                   {"+": [1, {"*": [{"u-": ["two"]}, 3]}]})
        test("1 + ~two * 3",                   {"+": [1, {"*": [{"~": ["two"]}, 3]}]})
        test("1 + 2 * 3",                      {"+": [1, {"*": [2, 3]}]})
        test("1 + 2 * 3 % 4 %% 5",             {"+": [1, {"%%": [{"%": [{"*": [2, 3]}, 4]}, 5]}]})
        test("1 * 2**3",                       {"*": [1, {"**": [2, 3]}]})
        test("(1 + 2) * 3",                    {"*": [{"+": [1, 2]}, 3]})
        test("1 + 2 * 3 - 4",                  {"-": [{"+": [1, {"*": [2, 3]}]}, 4]})
        test("1 + 2 * 3 / 4 - 5",              {"-": [{"+": [1, {"/": [{"*": [2, 3]}, 4]}]}, 5]})
        test("1 + 2 * 3 idiv 4 - 5",           {"-": [{"+": [1, {"//": [{"*": [2, 3]}, 4]}]}, 5]})

    def testTypeExpressions(self):
        def test(expr, asjson):
            self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: HERE
output: double
action: null
'''.replace("HERE", expr), lineNumbers=False, check=False), {"name": "Test",
                                                             "input": asjson,
                                                             "output": "double",
                                                             "method": "map",
                                                             "action": [None]})
        test("null", "null")
        test("boolean", "boolean")
        test("int", "int")
        test("long", "long")
        test("float", "float")
        test("double", "double")
        test("bytes", "bytes")
        test("string", "string")
        test("fixed(10, fixie)", {"type": "fixed", "size": 10, "name": "fixie"})
        test("fixed(10, com.wowie.fixie)", {"type": "fixed", "size": 10, "name": "fixie", "namespace": "com.wowie"})
        test("enum([], TestEnum)", {"type": "enum", "name": "TestEnum", "symbols": []})
        test("enum([one, two], TestEnum)", {"type": "enum", "name": "TestEnum", "symbols": ["one", "two"]})
        test("enum([one, two, three], TestEnum)", {"type": "enum", "name": "TestEnum", "symbols": ["one", "two", "three"]})
        test("record(TestRecord)", {"type": "record", "name": "TestRecord", "fields": []})
        test("record(x: int, TestRecord)", {"type": "record", "name": "TestRecord", "fields": [{"name": "x", "type": "int"}]})
        test("record(TestRecord, x: int)", {"type": "record", "name": "TestRecord", "fields": [{"name": "x", "type": "int"}]})
        test("record(com.wowie.TestRecord, x: int)", {"type": "record", "name": "TestRecord", "namespace": "com.wowie", "fields": [{"name": "x", "type": "int"}]})
        test("record(TestRecord, x: int, y: double)", {"type": "record", "name": "TestRecord", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "double"}]})
        test("array(double)", {"type": "array", "items": "double"})
        test("map(double)", {"type": "map", "values": "double"})
        test("array(map(double))", {"type": "array", "items": {"type": "map", "values": "double"}})
        test("union(boolean, int)", ["boolean", "int"])
        test("union(boolean, int, string)", ["boolean", "int", "string"])
        test("union(boolean, array(double))", ["boolean", {"type": "array", "items": "double"}])

    def testFcndef(self):
        def test(expr, asjson):
            self.assertEqual(titus.prettypfa.jsonNode(r'''
name: Test
input: double
output: double
action: HERE
'''.replace("HERE", expr), lineNumbers=False, check=False), {"name": "Test",
                                                             "input": "double",
                                                             "output": "double",
                                                             "method": "map",
                                                             "action": [asjson]})

        test("f(fcn(-> string) {2 + 2})", {"f": [{"params": [], "ret": "string", "do": [{"+": [2, 2]}]}]})
        test("f(fcn(x: int -> string) {2 + 2})", {"f": [{"params": [{"x": "int"}], "ret": "string", "do": [{"+": [2, 2]}]}]})
        test("f(fcn(x: int, y: double -> string) {2 + 2})", {"f": [{"params": [{"x": "int"}, {"y": "double"}], "ret": "string", "do": [{"+": [2, 2]}]}]})
        test("f(fcn(x: int -> string) 2 + 2)", {"f": [{"params": [{"x": "int"}], "ret": "string", "do": [{"+": [2, 2]}]}]})
        test("f(fcn(x: int -> string) {2 + 2; 3 + 3})", {"f": [{"params": [{"x": "int"}], "ret": "string", "do": [{"+": [2, 2]}, {"+": [3, 3]}]}]})

    def testFcnref(self):
        self.assertEqual(titus.prettypfa.jsonNode(r'''
input: double
output: double
action: f(somefunc(this: that, other: 2 + 2))
''', lineNumbers=False, check=False)["action"][0]["f"][0], {"fcn": "somefunc", "fill": {"this": "that", "other": {"+": [2, 2]}}})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: f(m.sqrt)
''', lineNumbers=False, check=False)["action"][0]["f"][0], {"fcn": "m.sqrt"})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: apply(f(2), 1, 2, 3)
''', lineNumbers=False, check=False)["action"][0], {"call": {"f": [2]}, "args": [1, 2, 3]})

    def testWeirdFunctions(self):
        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cmp(1, 2)
''', lineNumbers=False, check=False)["action"][0], {"cmp": [1, 2]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x) { as (xx: int) xx }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx"]}], "partial": False})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x) { as (xx: int) {xx; 2} }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx", 2]}], "partial": False})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x) { as (xx: int) xx as (yy: double) yy }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx"]}, {"as": "double", "named": "yy", "do": ["yy"]}], "partial": False})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x) { as (xx: int) {xx; 2} as (yy: double) {yy; 3;} }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx", 2]}, {"as": "double", "named": "yy", "do": ["yy", 3]}], "partial": False})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x) { as (xx: int) xx as (yy: double) yy as (zz: array(int)) zz }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx"]}, {"as": "double", "named": "yy", "do": ["yy"]}, {"as": {"type": "array", "items": "int"}, "named": "zz", "do": ["zz"]}], "partial": False})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x, partial: true) { as (xx: int) xx }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx"]}], "partial": True})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: cast (x, partial: false) { as (xx: int) xx }
''', lineNumbers=False, check=False)["action"][0], {"cast": "x", "cases": [{"as": "int", "named": "xx", "do": ["xx"]}], "partial": False})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: ifnotnull (x: getx()) x
''', lineNumbers=False, check=False)["action"][0], {"ifnotnull": {"x": {"getx": []}}, "then": ["x"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: ifnotnull (x: getx(), y: gety()) x + y
''', lineNumbers=False, check=False)["action"][0], {"ifnotnull": {"x": {"getx": []}, "y": {"gety": []}}, "then": [{"+": ["x", "y"]}]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: ifnotnull (x: getx(), y: gety()) {x + y; z}
''', lineNumbers=False, check=False)["action"][0], {"ifnotnull": {"x": {"getx": []}, "y": {"gety": []}}, "then": [{"+": ["x", "y"]}, "z"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: ifnotnull (x: getx(), y: gety()) x + y else z
''', lineNumbers=False, check=False)["action"][0], {"ifnotnull": {"x": {"getx": []}, "y": {"gety": []}}, "then": [{"+": ["x", "y"]}], "else": ["z"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: ifnotnull (x: getx(), y: gety()) x + y else {z; zz}
''', lineNumbers=False, check=False)["action"][0], {"ifnotnull": {"x": {"getx": []}, "y": {"gety": []}}, "then": [{"+": ["x", "y"]}], "else": ["z", "zz"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: upcast(array(int), input)
''', lineNumbers=False, check=False)["action"][0], {"upcast": "input", "as": {"type": "array", "items": "int"}})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: doc("this is important")
''', lineNumbers=False, check=False)["action"][0], {"doc": "this is important"})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: error("this is terrible")
''', lineNumbers=False, check=False)["action"][0], {"error": "this is terrible"})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: error(terrible)
''', lineNumbers=False, check=False)["action"][0], {"error": "terrible"})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: error(terrible, code: 2)
''', lineNumbers=False, check=False)["action"][0], {"error": "terrible", "code": 2})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try 2 + 2
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try {2 + 2}
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try {2 + 2; 3 + 3}
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}, {"+": [3, 3]}]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try {2 + 2; 3 + 3;}
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}, {"+": [3, 3]}]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try(one, "two", \'three\') 2 + 2
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}], "filter": ["one", "two", "three"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try(one, "two", \'three\') {2 + 2}
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}], "filter": ["one", "two", "three"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: try(one, "two", \'three\') {2 + 2; 3 + 3;}
''', lineNumbers=False, check=False)["action"][0], {"try": [{"+": [2, 2]}, {"+": [3, 3]}], "filter": ["one", "two", "three"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: log(something)
''', lineNumbers=False, check=False)["action"][0], {"log": ["something"]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: log("something")
''', lineNumbers=False, check=False)["action"][0], {"log": [{"string": "something"}]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: log(1, 2, 3)
''', lineNumbers=False, check=False)["action"][0], {"log": [1, 2, 3]})

        self.assertEqual(titus.prettypfa.jsonNode('''
input: double
output: double
action: log(1, 2, 3, namespace: wowie)
''', lineNumbers=False, check=False)["action"][0], {"log": [1, 2, 3], "namespace": "wowie"})

    def testControl(self):
        def test(expr, asjson):
            converted = json.loads(titus.prettypfa.json(r'''
input: double
output: double
action: HERE
'''.replace("HERE", expr), lineNumbers=False, check=False))["action"][0]
            self.assertEqual(converted, asjson)

        test("if (a()) b()", {"if": {"a": []}, "then": [{"b": []}]})
        test("if (a()) { b(); c(); d() }", {"if": {"a": []}, "then": [{"b": []}, {"c": []}, {"d": []}]})
        test("if (a()) b() else c()", {"if": {"a": []}, "then": [{"b": []}], "else": [{"c": []}]})
        test("if (a()) b() else {c(); d()}", {"if": {"a": []}, "then": [{"b": []}], "else": [{"c": []}, {"d": []}]})
        test("if (a) b else if (c) d", {"cond": [{"if": "a", "then": ["b"]}, {"if": "c", "then": ["d"]}]})
        test("if (a) {b; bb} else if (c) d", {"cond": [{"if": "a", "then": ["b", "bb"]}, {"if": "c", "then": ["d"]}]})
        test("if (a) b else if (c) {d; dd}", {"cond": [{"if": "a", "then": ["b"]}, {"if": "c", "then": ["d", "dd"]}]})
        test("if (a) b else if (c) d else if (e) f", {"cond": [{"if": "a", "then": ["b"]}, {"if": "c", "then": ["d"]}, {"if": "e", "then": ["f"]}]})
        test("if (a) b else if (c) d else e", {"cond": [{"if": "a", "then": ["b"]}, {"if": "c", "then": ["d"]}], "else": ["e"]})
        test("if (a) b else if (c) d else {e; ee}", {"cond": [{"if": "a", "then": ["b"]}, {"if": "c", "then": ["d"]}], "else": ["e", "ee"]})

        test("while (a()) b()", {"while": {"a": []}, "do": [{"b": []}]})
        test("while (a) b", {"while": "a", "do": ["b"]})
        test("while (a) {b; bb}", {"while": "a", "do": ["b", "bb"]})
        test("do a until (b)", {"do": ["a"], "until": "b"})
        test("do {a; aa} until (b)", {"do": ["a", "aa"], "until": "b"})

        test("for (i = 0;  i < 10;  i = i + 1) i", {"for": {"i": 0}, "while": {"<": ["i", 10]}, "step": {"i": {"+": ["i", 1]}}, "do": ["i"]})
        test("for (i = 0;  i < 10;  i = i + 1) {null; i}", {"for": {"i": 0}, "while": {"<": ["i", 10]}, "step": {"i": {"+": ["i", 1]}}, "do": [None, "i"]})
        test("for (i = 0, j = 0;  i < 10;  i = i + 1) i", {"for": {"i": 0, "j": 0}, "while": {"<": ["i", 10]}, "step": {"i": {"+": ["i", 1]}}, "do": ["i"]})
        test("for (i = 0;  i < 10;  i = i + 1, j = j + 1) i", {"for": {"i": 0}, "while": {"<": ["i", 10]}, "step": {"i": {"+": ["i", 1]}, "j": {"+": ["j", 1]}}, "do": ["i"]})

        test("foreach (x : array) do123", {"foreach": "x", "in": "array", "do": ["do123"], "seq": False})
        test("foreach (x : array) {do1; do2; do3}", {"foreach": "x", "in": "array", "do": ["do1", "do2", "do3"], "seq": False})
        test("foreach (x : array, seq: true) do123", {"foreach": "x", "in": "array", "do": ["do123"], "seq": True})
        test("foreach (x : array, seq: false) do123", {"foreach": "x", "in": "array", "do": ["do123"], "seq": False})
        test("foreach (key, value : map) do123", {"forkey": "key", "forval": "value", "in": "map", "do": ["do123"]})
        test("foreach (key, value : map) {do1; do2; do3}", {"forkey": "key", "forval": "value", "in": "map", "do": ["do1", "do2", "do3"]})

    def testExtraction(self):
        def test(expr, asjson):
            converted = json.loads(titus.prettypfa.json(r'''
input: double
output: double
action: HERE
cells:
  some(double) = 3
pools:
  other(double) = {one: 3}
'''.replace("HERE", expr), lineNumbers=False, check=False))["action"][0]
            self.assertEqual(converted, asjson)

        test("one.two.three", {"attr": "one", "path": [{"string": "two"}, {"string": "three"}]})
        test("one[two]", {"attr": "one", "path": ["two"]})
        test("one['two']", {"attr": "one", "path": [{"string": "two"}]})
        test("one[two, 3]", {"attr": "one", "path": ["two", 3]})
        test("one[two, 3, f(4)]", {"attr": "one", "path": ["two", 3, {"f": [4]}]})
        test("one.two[three]", {"attr": "one", "path": [{"string": "two"}, "three"]})
        test("one.two[3]", {"attr": "one", "path": [{"string": "two"}, 3]})
        test("some[two]", {"cell": "some", "path": ["two"]})
        test("some.two", {"cell": "some", "path": [{"string": "two"}]})
        test("some.two[3]", {"cell": "some", "path": [{"string": "two"}, 3]})
        test("other[two]", {"pool": "other", "path": ["two"]})
        test("other.two", {"pool": "other", "path": [{"string": "two"}]})
        test("other.two[3]", {"pool": "other", "path": [{"string": "two"}, 3]})

    def testAssignments(self):
        def test(expr, asjson):
            converted = json.loads(titus.prettypfa.json(r'''
input: double
output: double
action: HERE
cells:
  some(double) = 3
pools:
  other(double) = {one: 3}
fcns:
  square = fcn(x: double -> double) x**2
'''.replace("HERE", expr), lineNumbers=False, check=False))["action"]
            self.assertEqual(converted, asjson)

        test("var two = 1 + 1", [{"let": {"two": {"+": [1, 1]}}}])
        test("two = 1 + 1", [{"set": {"two": {"+": [1, 1]}}}])
        test("var two = 1 + 1, three = 2 + 1", [{"let": {"two": {"+": [1, 1]}, "three": {"+": [2, 1]}}}])
        test("{var two = 1 + 1; three = 2}", [{"let": {"two": {"+": [1, 1]}}}, {"set": {"three": 2}}])
        test("{var two = 1 + 1, three = 2}", [{"let": {"two": {"+": [1, 1]}, "three": 2}}])
        test("one.two = 3", [{"attr": "one", "path": [{"string": "two"}], "to": 3}])
        test("some.two = 3", [{"cell": "some", "path": [{"string": "two"}], "to": 3}])
        test("other.two = 3", [{"pool": "other", "path": [{"string": "two"}], "to": 3, "init": 3}])
        test("one[two] = 3", [{"attr": "one", "path": ["two"], "to": 3}])
        test("one['two'] = 3", [{"attr": "one", "path": [{"string": "two"}], "to": 3}])
        test("one['two', 2] = 3", [{"attr": "one", "path": [{"string": "two"}, 2], "to": 3}])
        test("some[two] = 3", [{"cell": "some", "path": ["two"], "to": 3}])
        test("some['two'] = 3", [{"cell": "some", "path": [{"string": "two"}], "to": 3}])
        test("some['two', 2] = 3", [{"cell": "some", "path": [{"string": "two"}, 2], "to": 3}])
        test("other[two] = 3", [{"pool": "other", "path": ["two"], "to": 3, "init": 3}])
        test("other['two'] = 3", [{"pool": "other", "path": [{"string": "two"}], "to": 3, "init": 3}])
        test("other['two', 2] = 3", [{"pool": "other", "path": [{"string": "two"}, 2], "to": 3, "init": 3}])
        test("one.un[two] = 3", [{"attr": "one", "path": [{"string": "un"}, "two"], "to": 3}])
        test("one.un['two'] = 3", [{"attr": "one", "path": [{"string": "un"}, {"string": "two"}], "to": 3}])
        test("one.un['two', 2] = 3", [{"attr": "one", "path": [{"string": "un"}, {"string": "two"}, 2], "to": 3}])
        test("some.un[two] = 3", [{"cell": "some", "path": [{"string": "un"}, "two"], "to": 3}])
        test("some.un['two'] = 3", [{"cell": "some", "path": [{"string": "un"}, {"string": "two"}], "to": 3}])
        test("some.un['two', 2] = 3", [{"cell": "some", "path": [{"string": "un"}, {"string": "two"}, 2], "to": 3}])
        test("other.un[two] = 3", [{"pool": "other", "path": [{"string": "un"}, "two"], "to": 3, "init": 3}])
        test("other.un['two'] = 3", [{"pool": "other", "path": [{"string": "un"}, {"string": "two"}], "to": 3, "init": 3}])
        test("other.un['two', 2] = 3", [{"pool": "other", "path": [{"string": "un"}, {"string": "two"}, 2], "to": 3, "init": 3}])
        test("one.two to u.square", [{"attr": "one", "path": [{"string": "two"}], "to": {"fcn": "u.square"}}])
        test("one[two] to u.square", [{"attr": "one", "path": ["two"], "to": {"fcn": "u.square"}}])
        test("one.two to fcn(x: double -> double) x**2", [{"attr": "one", "path": [{"string": "two"}], "to": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}}])
        test("one[two] to fcn(x: double -> double) x**2", [{"attr": "one", "path": ["two"], "to": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}}])
        test("some.two to u.square", [{"cell": "some", "path": [{"string": "two"}], "to": {"fcn": "u.square"}}])
        test("some[two] to u.square", [{"cell": "some", "path": ["two"], "to": {"fcn": "u.square"}}])
        test("some.two to fcn(x: double -> double) x**2", [{"cell": "some", "path": [{"string": "two"}], "to": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}}])
        test("some[two] to fcn(x: double -> double) x**2", [{"cell": "some", "path": ["two"], "to": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}}])
        test("other.two to u.square init 2", [{"pool": "other", "path": [{"string": "two"}], "to": {"fcn": "u.square"}, "init": 2}])
        test("other[two] to u.square init 2", [{"pool": "other", "path": ["two"], "to": {"fcn": "u.square"}, "init": 2}])
        test("other.two to fcn(x: double -> double) x**2 init 2", [{"pool": "other", "path": [{"string": "two"}], "to": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}, "init": 2}])
        test("other[two] to fcn(x: double -> double) x**2 init 2", [{"pool": "other", "path": ["two"], "to": {"params": [{"x": "double"}], "ret": "double", "do": [{"**": ["x", 2]}]}, "init": 2}])
        test("other.two to fcn(x: double -> double) {null; x**2} init 2", [{"pool": "other", "path": [{"string": "two"}], "to": {"params": [{"x": "double"}], "ret": "double", "do": [None, {"**": ["x", 2]}]}, "init": 2}])
        test("other[two] to fcn(x: double -> double) {null; x**2} init 2", [{"pool": "other", "path": ["two"], "to": {"params": [{"x": "double"}], "ret": "double", "do": [None, {"**": ["x", 2]}]}, "init": 2}])

        test("(2 + 2)[x]", [{"attr": {"+": [2, 2]}, "path": ["x"]}])
        test("(2 + 2)['x']", [{"attr": {"+": [2, 2]}, "path": [{"string": "x"}]}])
        test("(2 + 2)[x, 'x', 2]", [{"attr": {"+": [2, 2]}, "path": ["x", {"string": "x"}, 2]}])
        test("(2 + 2)[x, 'x', 2, 3 + 3]", [{"attr": {"+": [2, 2]}, "path": ["x", {"string": "x"}, 2, {"+": [3, 3]}]}])
        test("(2 + 2)[x] = 44", [{"attr": {"+": [2, 2]}, "path": ["x"], "to": 44}])
        test("(2 + 2)['x'] = 44", [{"attr": {"+": [2, 2]}, "path": [{"string": "x"}], "to": 44}])
        test("(2 + 2)[x, 'x', 2] = 44", [{"attr": {"+": [2, 2]}, "path": ["x", {"string": "x"}, 2], "to": 44}])
        test("(2 + 2)[x, 'x', 2, 3 + 3] = 44", [{"attr": {"+": [2, 2]}, "path": ["x", {"string": "x"}, 2, {"+": [3, 3]}], "to": 44}])

    def testWhitespace(self):
        asjson = {"name": "Test",
                  "input": "double",
                  "output": "double",
                  "method": "map",
                  "action": [{"+": [2, 2]}]}

        def ws():
            out = [" "] * random.randint(0, 2) + ["\t"] * random.randint(0, 2)
            random.shuffle(out)
            return "".join(out)

        def cr():
            return "\n" * random.randint(1, 3)

        template = '''<CR1>name:<WS1>Test<WS2><CR2>input:<WS3>double<WS4><CR3>output:<WS5>double<WS6><CR4>action:<WS7>2<WS8>+<WS9>2<WS10>'''

        for x in xrange(100):
            test = template.replace("<WS1>", ws()).replace("<WS2>", ws()).replace("<WS3>", ws()).replace("<WS4>", ws()).replace("<WS5>", ws()).replace("<WS6>", ws()).replace("<WS7>", ws()).replace("<WS8>", ws()).replace("<WS9>", ws()).replace("<WS10>", ws()).replace("<CR1>", cr()).replace("<CR2>", cr()).replace("<CR3>", cr()).replace("<CR4>", cr())
            try:
                tested = titus.prettypfa.jsonNode(test, lineNumbers=False, check=False)
            except PrettyPfaException:
                print
                print test
            else:
                if tested != asjson:
                    print
                    print test
                self.assertEqual(tested, asjson)

if __name__ == "__main__":
    unittest.main()
