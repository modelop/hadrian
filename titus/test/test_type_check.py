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

import json
import unittest

from titus.pfaast import *
from titus.reader import *
from titus.datatype import *

class TestTypeCheck(unittest.TestCase):
    def typeEquality(self, x, y):
        return x.accepts(y) and y.accepts(x)
    
    def testLiterals(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''null''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''true''')), AvroBoolean()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''false''')), AvroBoolean()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"int": 12}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"long": 12}''')), AvroLong()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"float": 12}''')), AvroFloat()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"double": 12}''')), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''12''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''12.0''')), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''12.4''')), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"string": "hello"}''')), AvroString()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''["hello"]''')), AvroString()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"base64": "aGVsbG8="}''')), AvroBytes()))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "null", "value": null}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "boolean", "value": true}''')), AvroBoolean()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "boolean", "value": false}''')), AvroBoolean()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "int", "value": 12}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "long", "value": 12}''')), AvroLong()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "float", "value": 12}''')), AvroFloat()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "double", "value": 12}''')), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": "string", "value": "hello"}''')), AvroString()))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": ["null", "boolean"], "value": null}''')), AvroUnion([AvroNull(), AvroBoolean()])))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": ["null", "boolean"], "value": true}''')), AvroUnion([AvroNull(), AvroBoolean()])))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}''')), AvroArray(AvroInt())))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "array", "items": ["int", "null"]}, "value": [1, null, 3]}''')), AvroArray(AvroUnion([AvroInt(), AvroNull()]))))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}''')), AvroMap(AvroInt())))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "map", "values": ["int", "null"]}, "value": {"one": 1, "two": null, "three": 3}}''')), AvroMap(AvroUnion([AvroInt(), AvroNull()]))))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "enum", "name": "MyEnum", "symbols": ["one", "two", "three"]}, "value": "two"}''')), AvroEnum(["one", "two", "three"], "MyEnum")))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "fixed", "name": "MyFixed", "size": 10}, "value": "hellohello"}''')), AvroFixed(10, "MyFixed")))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "record", "name": "MyRecord", "fields": [{"name": "one", "type": "int"}, {"name": "two", "type": "double"}, {"name": "three", "type": "string"}]}, "value": {"one": 12, "two": 12.4, "three": "hello"}}''')), AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())], "MyRecord")))

        t = '''{"type": "record", "name": "MyTree", "fields": [{"name": "left", "type": ["null", "MyTree"]}, {"name": "right", "type": ["null", "MyTree"]}]}'''
        myTree = ForwardDeclarationParser().parse([t])[t]
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"type": {"type": "record", "name": "MyTree", "fields": [{"name": "left", "type": ["null", "MyTree"]}, {"name": "right", "type": ["null", "MyTree"]}]}, "value": {"left": {"left": {"left": null, "right": null}, "right": null}, "right": {"left": null, "right": {"left": null, "right": null}}}}''')), myTree))

    def testNew(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"new": [1, 2, 3], "type": {"type": "array", "items": "int"}}''')), AvroArray(AvroInt())))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"new": [1, 2, ["three"]], "type": {"type": "array", "items": "int"}}''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"new": [1, 2, ["three"]], "type": {"type": "array", "items": ["string", "int"]}}''')), AvroArray(AvroUnion([AvroInt(), AvroString()]))))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"new": {"one": 1, "two": 2, "three": 3}, "type": {"type": "map", "values": "int"}}''')), AvroMap(AvroInt())))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"new": {"one": 1, "two": 2, "three": ["three"]}, "type": {"type": "map", "values": "int"}}''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"new": {"one": 1, "two": 2, "three": ["three"]}, "type": {"type": "map", "values": ["int", "string"]}}''')), AvroMap(AvroUnion([AvroInt(), AvroString()]))))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"new": {"one": 1, "two": 2.2, "three": ["three"]}, "type": {"type": "record", "name": "MyRecord", "fields": [{"name": "one", "type": "int"}, {"name": "two", "type": "double"}, {"name": "three", "type": "string"}]}}''')), AvroRecord([AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())], "MyRecord")))

    def testDo(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [12, 12.4, ["hello"]]}''')), AvroString()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [12, ["hello"], 12.4]}''')), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [["hello"], 12.4, 12]}''')), AvroInt()))

    def testLet(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12}}, "x"]}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}, "x"]}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}, "y"]}''')), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}, "z"]}''')), AvroString()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}]}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"let": {"x": 12}}''')), AvroNull()))
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"let": {}}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"let": {"y": {"let": {"x": 12}}}}''')))
        inferType(jsonToAst.expr('''{"let": {"y": {"do": {"let": {"x": 12}}}}}'''))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"y": {"do": {"let": {"x": 12}}}}}, "y"]}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12}}, {"let": {"x": 12}}]}''')))

    def testSet(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12}}, {"set": {"x": 999}}, "x"]}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12}}, {"set": {"x": 999}}]}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"set": {"x": 999}}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"do": [{"set": {"x": 999}}]}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"do": [{"set": {"x": 999}}, {"let": {"x": 12}}]}''')))
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"set": {}}''')))
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12}}, {"set": {}}]}''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12.4}}, {"set": {"x": 999}}, "x"]}''')), AvroDouble()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"do": [{"let": {"x": 12}}, {"set": {"x": 99.9}}, "x"]}''')))

    def testRef(self):
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''"x"''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''"x"'''), {"x": AvroInt()}), AvroInt()))

    def testAttr(self):
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": []}'''), {"x": AvroArray(AvroInt())}))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [2]}'''), {"x": AvroArray(AvroInt())}), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [1, 2, 3]}'''), {"x": AvroArray(AvroArray(AvroArray(AvroInt())))}), AvroInt()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [1]}'''), {"x": AvroInt()}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [1, 2, 3, 4]}'''), {"x": AvroArray(AvroArray(AvroArray(AvroInt())))}))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": ["y"]}'''), {"x": AvroArray(AvroInt()), "y": AvroInt()}), AvroInt()))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"]]}'''), {"x": AvroMap(AvroInt())}), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"], ["two"], ["three"]]}'''), {"x": AvroMap(AvroMap(AvroMap(AvroInt())))}), AvroInt()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"], ["two"], ["three"], ["four"]]}'''), {"x": AvroMap(AvroMap(AvroMap(AvroInt())))}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"]]}'''), {"x": AvroInt()}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [2]}'''), {"x": AvroMap(AvroInt())}))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": ["y"]}'''), {"x": AvroMap(AvroInt()), "y": AvroString()}), AvroInt()))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"], 1, ["one"], 1]}'''), {"x": AvroMap(AvroArray(AvroMap(AvroArray(AvroInt()))))}), AvroInt()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"], 1, ["one"], 1]}'''), {"x": AvroMap(AvroArray(AvroArray(AvroMap(AvroInt()))))}))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"]]}'''), {"x": AvroRecord([AvroField("one", AvroInt())], "MyRecord")}), AvroInt()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": ["y"]}'''), {"x": AvroRecord([AvroField("one", AvroInt())], "MyRecord"), "y": AvroString()}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [["two"]]}'''), {"x": AvroRecord([AvroField("one", AvroInt())], "MyRecord")}))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": ["y", 1, ["one"], 1]}'''), {"x": AvroMap(AvroArray(AvroRecord([AvroField("one", AvroArray(AvroInt()))], "MyRecord"))), "y": AvroString()}), AvroInt()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [["one"], 1, "y", 1]}'''), {"x": AvroMap(AvroArray(AvroRecord([AvroField("one", AvroArray(AvroInt()))], "MyRecord"))), "y": AvroString()}))

    def testAttrTo(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": 12}'''), {"x": AvroArray(AvroInt())}), AvroArray(AvroInt())))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": 12.4}'''), {"x": AvroArray(AvroInt())}))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}}'''), {"x": AvroArray(AvroInt())}), AvroArray(AvroInt())))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}}'''), {"x": AvroArray(AvroInt())}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": {"params": [{"x": "string"}], "ret": "int", "do": 12}}'''), {"x": AvroArray(AvroInt())}))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": {"fcn": "u.f"}}'''), {"x": AvroArray(AvroInt())}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "int", "do": "x"}'''))}), AvroArray(AvroInt())))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": {"fcn": "u.f"}}'''), {"x": AvroArray(AvroInt())}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}'''))}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"attr": "x", "path": [2], "to": {"fcn": "u.f"}}'''), {"x": AvroArray(AvroInt())}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "string"}], "ret": "int", "do": 12}'''))}))

    def testCell(self):
        for shared in True, False:
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x"}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": []}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [2]}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [1, 2, 3]}'''), cells={"x": Cell(AvroArray(AvroArray(AvroArray(AvroInt()))), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [1]}'''), cells={"x": Cell(AvroInt(), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [1, 2, 3, 4]}'''), cells={"x": Cell(AvroArray(AvroArray(AvroArray(AvroInt()))), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": ["y"]}'''), {"y": AvroInt()}, cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"]]}'''), cells={"x": Cell(AvroMap(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"], ["two"], ["three"]]}'''), cells={"x": Cell(AvroMap(AvroMap(AvroMap(AvroInt()))), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"], ["two"], ["three"], ["four"]]}'''), cells={"x": Cell(AvroMap(AvroMap(AvroMap(AvroInt()))), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"]]}'''), cells={"x": Cell(AvroInt(), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [2]}'''), cells={"x": Cell(AvroMap(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": ["y"]}'''), {"y": AvroString()}, cells={"x": Cell(AvroMap(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"], 1, ["one"], 1]}'''), cells={"x": Cell(AvroMap(AvroArray(AvroMap(AvroArray(AvroInt())))), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"], 1, ["one"], 1]}'''), cells={"x": Cell(AvroMap(AvroArray(AvroArray(AvroMap(AvroInt())))), "", shared, False, CellPoolSource.EMBEDDED)}))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"]]}'''), cells={"x": Cell(AvroRecord([AvroField("one", AvroInt())], "MyRecord"), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": ["y"]}'''), {"y": AvroString()}, cells={"x": Cell(AvroRecord([AvroField("one", AvroInt())], "MyRecord"), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [["two"]]}'''), cells={"x": Cell(AvroRecord([AvroField("one", AvroInt())], "MyRecord"), "", shared, False, CellPoolSource.EMBEDDED)}))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": ["y", 1, ["one"], 1]}'''), {"y": AvroString()}, cells={"x": Cell(AvroMap(AvroArray(AvroRecord([AvroField("one", AvroArray(AvroInt()))], "MyRecord"))), "", shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [["one"], 1, "y", 1]}'''), {"y": AvroString()}, cells={"x": Cell(AvroMap(AvroArray(AvroRecord([AvroField("one", AvroArray(AvroInt()))], "MyRecord"))), "", shared, False, CellPoolSource.EMBEDDED)}))

    def testCellTo(self):
        for shared in True, False:
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": 12}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": 12.4}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": {"params": [{"x": "string"}], "ret": "int", "do": 12}}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": {"fcn": "u.f"}}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "int", "do": "x"}'''))}), AvroArray(AvroInt())))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": {"fcn": "u.f"}}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}'''))}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cell": "x", "path": [2], "to": {"fcn": "u.f"}}'''), cells={"x": Cell(AvroArray(AvroInt()), "", shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "string"}], "ret": "int", "do": 12}'''))}))

    def testPool(self):
        for shared in True, False:
            self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": []}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"]]}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2]}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 1, 2, 3]}'''), pools={"x": Pool(AvroArray(AvroArray(AvroArray(AvroInt()))), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 1]}'''), pools={"x": Pool(AvroInt(), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 1, 2, 3, 4]}'''), pools={"x": Pool(AvroArray(AvroArray(AvroArray(AvroInt()))), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], "y"]}'''), {"y": AvroInt()}, pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"]]}'''), pools={"x": Pool(AvroMap(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"], ["two"], ["three"]]}'''), pools={"x": Pool(AvroMap(AvroMap(AvroMap(AvroInt()))), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"], ["two"], ["three"], ["four"]]}'''), pools={"x": Pool(AvroMap(AvroMap(AvroMap(AvroInt()))), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"]]}'''), pools={"x": Pool(AvroInt(), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2]}'''), pools={"x": Pool(AvroMap(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], "y"]}'''), {"y": AvroString()}, pools={"x": Pool(AvroMap(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"], 1, ["one"], 1]}'''), pools={"x": Pool(AvroMap(AvroArray(AvroMap(AvroArray(AvroInt())))), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"], 1, ["one"], 1]}'''), pools={"x": Pool(AvroMap(AvroArray(AvroArray(AvroMap(AvroInt())))), {}, shared, False, CellPoolSource.EMBEDDED)}))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"]]}'''), pools={"x": Pool(AvroRecord([AvroField("one", AvroInt())], "MyRecord"), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], "y"]}'''), {"y": AvroString()}, pools={"x": Pool(AvroRecord([AvroField("one", AvroInt())], "MyRecord"), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["two"]]}'''), pools={"x": Pool(AvroRecord([AvroField("one", AvroInt())], "MyRecord"), {}, shared, False, CellPoolSource.EMBEDDED)}))

            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], "y", 1, ["one"], 1]}'''), {"y": AvroString()}, pools={"x": Pool(AvroMap(AvroArray(AvroRecord([AvroField("one", AvroArray(AvroInt()))], "MyRecord"))), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroInt()))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], ["one"], 1, "y", 1]}'''), {"y": AvroString()}, pools={"x": Pool(AvroMap(AvroArray(AvroRecord([AvroField("one", AvroArray(AvroInt()))], "MyRecord"))), {}, shared, False, CellPoolSource.EMBEDDED)}))

    def testPoolTo(self):
        for shared in True, False:
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": 12, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertRaises(PFASemanticException, lambda: self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": 12.4, "init": {"value": [999.9], "type": {"type": "array", "items": "double"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroNull())))

            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}, "init": {"value": [999.9], "type": {"type": "array", "items": "double"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}), AvroArray(AvroInt())))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "string"}], "ret": "int", "do": 12}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}))

            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999.9], "type": {"type": "array", "items": "double"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "int", "do": "x"}'''))}))
            self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "int", "do": "x"}'''))}), AvroArray(AvroInt())))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}'''))}))
            self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}'''), pools={"x": Pool(AvroArray(AvroInt()), {}, shared, False, CellPoolSource.EMBEDDED)}, fcns={"u.f": UserFcn.fromFcnDef("u.f", jsonToAst.fcn('''{"params": [{"x": "string"}], "ret": "int", "do": 12}'''))}))

    def testIf(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"if": true, "then": 12}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"if": true, "then": 12, "else": 999}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"if": true, "then": 12, "else": [["hello"]]}''')), AvroUnion([AvroInt(), AvroString()])))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"if": true, "then": 12, "else": {"type": ["string", "int"], "value": 12}}''')), AvroUnion([AvroInt(), AvroString()])))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"if": true, "then": {"type": ["int", "null"], "value": 12}, "else": 12}''')), AvroUnion([AvroInt(), AvroNull()])))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"if": true, "then": {"type": ["int", "null"], "value": 12}, "else": {"type": ["string", "int"], "value": 12}}''')), AvroUnion([AvroInt(), AvroNull(), AvroString()])))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"if": null, "then": 12}''')))

    def testCond(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cond": [{"if": true, "then": 12}]}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cond": [{"if": true, "then": 12}, {"if": true, "then": 12}, {"if": true, "then": 12}]}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cond": [{"if": true, "then": 12}], "else": 999}''')), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cond": [{"if": true, "then": 12}, {"if": true, "then": 12}, {"if": true, "then": 12}], "else": 999}''')), AvroInt()))

        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cond": [{"if": true, "then": 12}, {"if": true, "then": {"string": "hello"}}, {"if": true, "then": {"base64": "aGVsbG8="}}]}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cond": [{"if": true, "then": 12}, {"if": true, "then": {"string": "hello"}}, {"if": true, "then": {"base64": "aGVsbG8="}}], "else": 999}''')), AvroUnion([AvroInt(), AvroString(), AvroBytes()])))

        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"cond": []}''')))
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"cond": [], "else": 999}''')))

    def testWhile(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"while": true, "do": 12}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"while": null, "do": 12}''')))

    def testDoUntil(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": 12, "until": true}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"do": 12, "until": null}''')))

    def testFor(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": 12}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": "x"}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": "y"}''')))
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"for": {}, "while": true, "step": {"x": 1}, "do": 12}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": null, "step": {"x": 1}, "do": 12}''')))
        self.assertRaises(PFASyntaxException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {}, "do": 12}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"y": 1}, "do": 12}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 2.2}, "do": 12}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": 12}'''), {"x": AvroInt()}))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": {"set": {"x": 12}}}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": {"set": {"x": 12.4}}}''')))

    def testForeach(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": 12}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": "x"}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": 12}'''), {"x": AvroInt()}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": "y"}''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": {"set": {"x": 12}}}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": {"set": {"x": 12.4}}}''')))

    def testForkeyval(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": "k"}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": "v"}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": 12}''')))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}'''), {"k": AvroString()}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}'''), {"v": AvroInt()}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": "z"}''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"k": ["hello"]}}}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"k": 12}}}''')))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"v": 12}}}''')), AvroNull()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"v": ["hello"]}}}''')))

    def testCastCase(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}]}'''), {"x": AvroUnion([AvroInt(), AvroString()])}), AvroInt()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}], "partial": true}'''), {"x": AvroUnion([AvroInt(), AvroString()])}), AvroNull()))

        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}, {"as": "bytes", "named": "xb", "do": 999}]}'''), {"x": AvroUnion([AvroInt(), AvroString()])}))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}]}'''), {"x": AvroUnion([AvroInt(), AvroString(), AvroBytes()])}))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}], "partial": true}'''), {"x": AvroUnion([AvroInt(), AvroString(), AvroBytes()])}), AvroNull()))

    def testUpcast(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"upcast": "x", "as": "double"}'''), {"x": AvroInt()}), AvroDouble()))
        self.assertRaises(PFASemanticException, lambda: inferType(jsonToAst.expr('''{"upcast": "x", "as": "int"}'''), {"x": AvroDouble()}))

    def testIfNotNull(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"ifnotnull": {"x": "input"}, "then": "x", "else": 12.4}'''), {"input": AvroUnion([AvroDouble(), AvroNull()])}), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"ifnotnull": {"x": "input", "y": "input"}, "then": {"+": ["x", "y"]}, "else": 12.4}'''), {"input": AvroUnion([AvroDouble(), AvroNull()])}), AvroDouble()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"ifnotnull": {"x": "input"}, "then": "x"}'''), {"input": AvroUnion([AvroDouble(), AvroNull()])}), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"ifnotnull": {"x": "input"}, "then": "x", "else": 12.4}'''), {"input": AvroUnion([AvroDouble(), AvroString(), AvroNull()])}), AvroUnion([AvroDouble(), AvroString()])))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"ifnotnull": {"x": "input"}, "then": "x", "else": [["whatever"]]}'''), {"input": AvroUnion([AvroDouble(), AvroNull()])}), AvroUnion([AvroDouble(), AvroString()])))

    def testDocErrorLog(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"doc": "hello"}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"do": {"error": "hello"}}''')), AvroNull()))
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"log": [["hello"]]}''')), AvroNull()))

    def testCallPatterns(self):
        self.assertTrue(self.typeEquality(inferType(jsonToAst.expr('''{"+": [2, 2]}''')), AvroInt()))
        # HERE

if __name__ == "__main__":
    unittest.main()
