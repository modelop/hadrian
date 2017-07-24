// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test.scala.typecheck

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.map.ObjectMapper
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import test.scala._

@RunWith(classOf[JUnitRunner])
class TypeCheckSuite extends FlatSpec with Matchers {
  "Type checker" must "test literals" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""null""")) should be (AvroNull())


    inferType(jsonToAst.expr("""true""")) should be (AvroBoolean())
    inferType(jsonToAst.expr("""false""")) should be (AvroBoolean())
    inferType(jsonToAst.expr("""{"int": 12}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"long": 12}""")) should be (AvroLong())
    inferType(jsonToAst.expr("""{"float": 12}""")) should be (AvroFloat())
    inferType(jsonToAst.expr("""{"double": 12}""")) should be (AvroDouble())
    inferType(jsonToAst.expr("""12""")) should be (AvroInt())
    inferType(jsonToAst.expr("""12.0""")) should be (AvroDouble())
    inferType(jsonToAst.expr("""12.4""")) should be (AvroDouble())
    inferType(jsonToAst.expr("""{"string": "hello"}""")) should be (AvroString())
    inferType(jsonToAst.expr("""["hello"]""")) should be (AvroString())
    inferType(jsonToAst.expr("""{"base64": "aGVsbG8="}""")) should be (AvroBytes())

    inferType(jsonToAst.expr("""{"type": "null", "value": null}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"type": "boolean", "value": true}""")) should be (AvroBoolean())
    inferType(jsonToAst.expr("""{"type": "boolean", "value": false}""")) should be (AvroBoolean())
    inferType(jsonToAst.expr("""{"type": "int", "value": 12}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"type": "long", "value": 12}""")) should be (AvroLong())
    inferType(jsonToAst.expr("""{"type": "float", "value": 12}""")) should be (AvroFloat())
    inferType(jsonToAst.expr("""{"type": "double", "value": 12}""")) should be (AvroDouble())
    inferType(jsonToAst.expr("""{"type": "string", "value": "hello"}""")) should be (AvroString())

    inferType(jsonToAst.expr("""{"type": ["null", "boolean"], "value": null}""")) should be (AvroUnion(List(AvroNull(), AvroBoolean())))
    inferType(jsonToAst.expr("""{"type": ["null", "boolean"], "value": true}""")) should be (AvroUnion(List(AvroNull(), AvroBoolean())))

    inferType(jsonToAst.expr("""{"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}""")) should be (AvroArray(AvroInt()))
    inferType(jsonToAst.expr("""{"type": {"type": "array", "items": ["int", "null"]}, "value": [1, null, 3]}""")) should be (AvroArray(AvroUnion(List(AvroInt(), AvroNull()))))
    inferType(jsonToAst.expr("""{"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}""")) should be (AvroMap(AvroInt()))
    inferType(jsonToAst.expr("""{"type": {"type": "map", "values": ["int", "null"]}, "value": {"one": 1, "two": null, "three": 3}}""")) should be (AvroMap(AvroUnion(List(AvroInt(), AvroNull()))))

    inferType(jsonToAst.expr("""{"type": {"type": "enum", "name": "MyEnum", "symbols": ["one", "two", "three"]}, "value": "two"}""")) should be (AvroEnum(List("one", "two", "three"), "MyEnum"))
    inferType(jsonToAst.expr("""{"type": {"type": "fixed", "name": "MyFixed", "size": 10}, "value": "hellohello"}""")) should be (AvroFixed(10, "MyFixed"))

    inferType(jsonToAst.expr("""{"type": {"type": "record", "name": "MyRecord", "fields": [{"name": "one", "type": "int"}, {"name": "two", "type": "double"}, {"name": "three", "type": "string"}]}, "value": {"one": 12, "two": 12.4, "three": "hello"}}""")) should be (AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyRecord"))

    val t = """{"type": "record", "name": "MyTree", "fields": [{"name": "left", "type": ["null", "MyTree"]}, {"name": "right", "type": ["null", "MyTree"]}]}"""
    val myTree = (new ForwardDeclarationParser).parse(List(t))(t)
    inferType(jsonToAst.expr("""{"type": {"type": "record", "name": "MyTree", "fields": [{"name": "left", "type": ["null", "MyTree"]}, {"name": "right", "type": ["null", "MyTree"]}]}, "value": {"left": {"left": {"left": null, "right": null}, "right": null}, "right": {"left": null, "right": {"left": null, "right": null}}}}""")) should be (myTree)
  }

  it must "test new" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"new": [1, 2, 3], "type": {"type": "array", "items": "int"}}""")) should be (AvroArray(AvroInt()))
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"new": [1, 2, ["three"]], "type": {"type": "array", "items": "int"}}""")) }
    inferType(jsonToAst.expr("""{"new": [1, 2, ["three"]], "type": {"type": "array", "items": ["string", "int"]}}""")) should be (AvroArray(AvroUnion(List(AvroString(), AvroInt()))))

    inferType(jsonToAst.expr("""{"new": {"one": 1, "two": 2, "three": 3}, "type": {"type": "map", "values": "int"}}""")) should be (AvroMap(AvroInt()))
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"new": {"one": 1, "two": 2, "three": ["three"]}, "type": {"type": "map", "values": "int"}}""")) }
    inferType(jsonToAst.expr("""{"new": {"one": 1, "two": 2, "three": ["three"]}, "type": {"type": "map", "values": ["int", "string"]}}""")) should be (AvroMap(AvroUnion(List(AvroInt(), AvroString()))))

    inferType(jsonToAst.expr("""{"new": {"one": 1, "two": 2.2, "three": ["three"]}, "type": {"type": "record", "name": "MyRecord", "fields": [{"name": "one", "type": "int"}, {"name": "two", "type": "double"}, {"name": "three", "type": "string"}]}}""")) should be (AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyRecord"))
  }

  it must "test do" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"do": [12, 12.4, ["hello"]]}""")) should be (AvroString())
    inferType(jsonToAst.expr("""{"do": [12, ["hello"], 12.4]}""")) should be (AvroDouble())
    inferType(jsonToAst.expr("""{"do": [["hello"], 12.4, 12]}""")) should be (AvroInt())
  }

  it must "test let" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12}}, "x"]}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}, "x"]}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}, "y"]}""")) should be (AvroDouble())
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}, "z"]}""")) should be (AvroString())
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}]}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"let": {"x": 12, "y": 12.4, "z": ["hello"]}}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"let": {"x": 12}}""")) should be (AvroNull())
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"let": {}}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"let": {"y": {"let": {"x": 12}}}}""")) }
    inferType(jsonToAst.expr("""{"let": {"y": {"do": {"let": {"x": 12}}}}}"""))
    inferType(jsonToAst.expr("""{"do": [{"let": {"y": {"do": {"let": {"x": 12}}}}}, "y"]}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12}}, {"let": {"x": 12}}]}""")) }
  }

  it must "test set" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12}}, {"set": {"x": 999}}, "x"]}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12}}, {"set": {"x": 999}}]}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"set": {"x": 999}}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"do": [{"set": {"x": 999}}]}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"do": [{"set": {"x": 999}}, {"let": {"x": 12}}]}""")) }
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"set": {}}""")) }
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12}}, {"set": {}}]}""")) }
    inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12.4}}, {"set": {"x": 999}}, "x"]}""")) should be (AvroDouble())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"do": [{"let": {"x": 12}}, {"set": {"x": 99.9}}, "x"]}""")) }
  }

  it must "test ref" taggedAs(TypeCheck) in {
    intercept[PFASemanticException] { inferType(jsonToAst.expr(""""x"""")) }
    inferType(jsonToAst.expr(""""x""""), Map("x" -> AvroInt())) should be (AvroInt())
  }

  it must "test attr" taggedAs(TypeCheck) in {
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"attr": "x", "path": []}"""), Map("x" -> AvroArray(AvroInt()))) }
    inferType(jsonToAst.expr("""{"attr": "x", "path": [2]}"""), Map("x" -> AvroArray(AvroInt()))) should be (AvroInt())
    inferType(jsonToAst.expr("""{"attr": "x", "path": [1, 2, 3]}"""), Map("x" -> AvroArray(AvroArray(AvroArray(AvroInt()))))) should be (AvroInt())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [1]}"""), Map("x" -> AvroInt())) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [1, 2, 3, 4]}"""), Map("x" -> AvroArray(AvroArray(AvroArray(AvroInt()))))) }
    inferType(jsonToAst.expr("""{"attr": "x", "path": ["y"]}"""), Map("x" -> AvroArray(AvroInt()), "y" -> AvroInt())) should be (AvroInt())
    inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"]]}"""), Map("x" -> AvroMap(AvroInt()))) should be (AvroInt())
    inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"], ["two"], ["three"]]}"""), Map("x" -> AvroMap(AvroMap(AvroMap(AvroInt()))))) should be (AvroInt())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"], ["two"], ["three"], ["four"]]}"""), Map("x" -> AvroMap(AvroMap(AvroMap(AvroInt()))))) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"]]}"""), Map("x" -> AvroInt())) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [2]}"""), Map("x" -> AvroMap(AvroInt()))) }
    inferType(jsonToAst.expr("""{"attr": "x", "path": ["y"]}"""), Map("x" -> AvroMap(AvroInt()), "y" -> AvroString())) should be (AvroInt())
    inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"], 1, ["one"], 1]}"""), Map("x" -> AvroMap(AvroArray(AvroMap(AvroArray(AvroInt())))))) should be (AvroInt())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"], 1, ["one"], 1]}"""), Map("x" -> AvroMap(AvroArray(AvroArray(AvroMap(AvroInt())))))) }
    inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"]]}"""), Map("x" -> AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (AvroInt())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": ["y"]}"""), Map("x" -> AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), "y" -> AvroString())) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [["two"]]}"""), Map("x" -> AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) }
    inferType(jsonToAst.expr("""{"attr": "x", "path": ["y", 1, ["one"], 1]}"""), Map("x" -> AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord"))), "y" -> AvroString())) should be (AvroInt())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [["one"], 1, "y", 1]}"""), Map("x" -> AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord"))), "y" -> AvroString())) }
  }

  it must "test attr-to" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": 12}"""), Map("x" -> AvroArray(AvroInt()))) should be (AvroArray(AvroInt()))
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": 12.4}"""), Map("x" -> AvroArray(AvroInt()))) }

    inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}}"""), Map("x" -> AvroArray(AvroInt()))) should be (AvroArray(AvroInt()))
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}}"""), Map("x" -> AvroArray(AvroInt()))) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": {"params": [{"x": "string"}], "ret": "int", "do": 12}}"""), Map("x" -> AvroArray(AvroInt()))) }

    inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": {"fcn": "u.f"}}"""), Map("x" -> AvroArray(AvroInt())), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "int", "do": "x"}""")))) should be (AvroArray(AvroInt()))
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": {"fcn": "u.f"}}"""), Map("x" -> AvroArray(AvroInt())), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}""")))) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"attr": "x", "path": [2], "to": {"fcn": "u.f"}}"""), Map("x" -> AvroArray(AvroInt())), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "string"}], "ret": "int", "do": 12}""")))) }
  }

  it must "test cell" taggedAs(TypeCheck) in {
    for (shared <- List(true, false)) {
      inferType(jsonToAst.expr("""{"cell": "x"}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      inferType(jsonToAst.expr("""{"cell": "x", "path": []}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      inferType(jsonToAst.expr("""{"cell": "x", "path": [2]}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"cell": "x", "path": [1, 2, 3]}"""), cells = Map("x" -> Cell(AvroArray(AvroArray(AvroArray(AvroInt()))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroArray(AvroArray(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [1]}"""), cells = Map("x" -> Cell(AvroInt(), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroInt()), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [1, 2, 3, 4]}"""), cells = Map("x" -> Cell(AvroArray(AvroArray(AvroArray(AvroInt()))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroArray(AvroArray(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"cell": "x", "path": ["y"]}"""), Map("y" -> AvroInt()), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"]]}"""), cells = Map("x" -> Cell(AvroMap(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"], ["two"], ["three"]]}"""), cells = Map("x" -> Cell(AvroMap(AvroMap(AvroMap(AvroInt()))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroMap(AvroMap(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"], ["two"], ["three"], ["four"]]}"""), cells = Map("x" -> Cell(AvroMap(AvroMap(AvroMap(AvroInt()))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroMap(AvroMap(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"]]}"""), cells = Map("x" -> Cell(AvroInt(), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroInt()), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [2]}"""), cells = Map("x" -> Cell(AvroMap(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"cell": "x", "path": ["y"]}"""), Map("y" -> AvroString()), cells = Map("x" -> Cell(AvroMap(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"], 1, ["one"], 1]}"""), cells = Map("x" -> Cell(AvroMap(AvroArray(AvroMap(AvroArray(AvroInt())))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroArray(AvroMap(AvroArray(AvroInt()))))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"], 1, ["one"], 1]}"""), cells = Map("x" -> Cell(AvroMap(AvroArray(AvroArray(AvroMap(AvroInt())))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroArray(AvroArray(AvroMap(AvroInt()))))), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"]]}"""), cells = Map("x" -> Cell(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": ["y"]}"""), Map("y" -> AvroString()), cells = Map("x" -> Cell(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [["two"]]}"""), cells = Map("x" -> Cell(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"cell": "x", "path": ["y", 1, ["one"], 1]}"""), Map("y" -> AvroString()), cells = Map("x" -> Cell(AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord"))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord")))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [["one"], 1, "y", 1]}"""), Map("y" -> AvroString()), cells = Map("x" -> Cell(AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord"))), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord")))), shared, false, CellPoolSource.EMBEDDED))) }
    }
  }

  it must "test cell-to" taggedAs(TypeCheck) in {
    for (shared <- List(true, false)) {
      inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": 12}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": 12.4}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }

      inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": {"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": {"params": [{"x": "string"}], "ret": "int", "do": 12}}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }

      inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": {"fcn": "u.f"}}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "int", "do": "x"}""")))) should be (AvroArray(AvroInt()))
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": {"fcn": "u.f"}}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}""")))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cell": "x", "path": [2], "to": {"fcn": "u.f"}}"""), cells = Map("x" -> Cell(AvroArray(AvroInt()), EmbeddedJsonDomCellSource(JsonArray(Seq()), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "string"}], "ret": "int", "do": 12}""")))) }
    }
  }

  it must "test pool" taggedAs(TypeCheck) in {
    for (shared <- List(true, false)) {
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"]]}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2]}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 1, 2, 3]}"""), pools = Map("x" -> Pool(AvroArray(AvroArray(AvroArray(AvroInt()))), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroArray(AvroArray(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 1]}"""), pools = Map("x" -> Pool(AvroInt(), EmbeddedJsonDomPoolSource(Map(), AvroInt()), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 1, 2, 3, 4]}"""), pools = Map("x" -> Pool(AvroArray(AvroArray(AvroArray(AvroInt()))), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroArray(AvroArray(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], "y"]}"""), Map("y" -> AvroInt()), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"]]}"""), pools = Map("x" -> Pool(AvroMap(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"], ["two"], ["three"]]}"""), pools = Map("x" -> Pool(AvroMap(AvroMap(AvroMap(AvroInt()))), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroMap(AvroMap(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"], ["two"], ["three"], ["four"]]}"""), pools = Map("x" -> Pool(AvroMap(AvroMap(AvroMap(AvroInt()))), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroMap(AvroMap(AvroInt())))), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"]]}"""), pools = Map("x" -> Pool(AvroInt(), EmbeddedJsonDomPoolSource(Map(), AvroInt()), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2]}"""), pools = Map("x" -> Pool(AvroMap(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], "y"]}"""), Map("y" -> AvroString()), pools = Map("x" -> Pool(AvroMap(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"], 1, ["one"], 1]}"""), pools = Map("x" -> Pool(AvroMap(AvroArray(AvroMap(AvroArray(AvroInt())))), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroArray(AvroMap(AvroArray(AvroInt()))))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"], 1, ["one"], 1]}"""), pools = Map("x" -> Pool(AvroMap(AvroArray(AvroArray(AvroMap(AvroInt())))), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroArray(AvroArray(AvroMap(AvroInt()))))), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"]]}"""), pools = Map("x" -> Pool(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), EmbeddedJsonDomPoolSource(Map(), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], "y"]}"""), Map("y" -> AvroString()), pools = Map("x" -> Pool(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), EmbeddedJsonDomPoolSource(Map(), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["two"]]}"""), pools = Map("x" -> Pool(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"), EmbeddedJsonDomPoolSource(Map(), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], "y", 1, ["one"], 1]}"""), Map("y" -> AvroString()), pools = Map("x" -> Pool(AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord"))), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord")))), shared, false, CellPoolSource.EMBEDDED))) should be (AvroInt())
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], ["one"], 1, "y", 1]}"""), Map("y" -> AvroString()), pools = Map("x" -> Pool(AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord"))), EmbeddedJsonDomPoolSource(Map(), AvroMap(AvroArray(AvroRecord(List(AvroField("one", AvroArray(AvroInt()))), "MyRecord")))), shared, false, CellPoolSource.EMBEDDED))) }
    }
  }

  it must "test pool-to" taggedAs(TypeCheck) in {
    for (shared <- List(true, false)) {
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": 12, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": 12.4, "init": {"value": [999.9], "type": {"type": "array", "items": "double"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }

      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}, "init": {"value": [999.9], "type": {"type": "array", "items": "double"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "int"}], "ret": "int", "do": "x"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) should be (AvroArray(AvroInt()))
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"params": [{"x": "string"}], "ret": "int", "do": 12}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED))) }

      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999.9], "type": {"type": "array", "items": "double"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "int", "do": "x"}""")))) }
      inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "int", "do": "x"}""")))) should be (AvroArray(AvroInt()))
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "int"}], "ret": "string", "do": [["hello"]]}""")))) }
      intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"pool": "x", "path": [["p"], 2], "to": {"fcn": "u.f"}, "init": {"value": [999], "type": {"type": "array", "items": "int"}}}"""), pools = Map("x" -> Pool(AvroArray(AvroInt()), EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())), shared, false, CellPoolSource.EMBEDDED)), fcns = Map("u.f" -> UserFcn.fromFcnDef("u.f", jsonToAst.fcn("""{"params": [{"x": "string"}], "ret": "int", "do": 12}""")))) }
    }
  }

  it must "test if" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"if": true, "then": 12}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"if": true, "then": 12, "else": 999}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"if": true, "then": 12, "else": [["hello"]]}""")) should be (AvroUnion(List(AvroInt(), AvroString())))
    inferType(jsonToAst.expr("""{"if": true, "then": 12, "else": {"type": ["string", "int"], "value": 12}}""")) should be (AvroUnion(List(AvroInt(), AvroString())))
    inferType(jsonToAst.expr("""{"if": true, "then": {"type": ["int", "null"], "value": 12}, "else": 12}""")) should be (AvroUnion(List(AvroInt(), AvroNull())))
    inferType(jsonToAst.expr("""{"if": true, "then": {"type": ["int", "null"], "value": 12}, "else": {"type": ["string", "int"], "value": 12}}""")) should be (AvroUnion(List(AvroInt(), AvroNull(), AvroString())))
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"if": null, "then": 12}""")) }
  }

  it must "test cond" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"cond": [{"if": true, "then": 12}]}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"cond": [{"if": true, "then": 12}, {"if": true, "then": 12}, {"if": true, "then": 12}]}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"cond": [{"if": true, "then": 12}], "else": 999}""")) should be (AvroInt())
    inferType(jsonToAst.expr("""{"cond": [{"if": true, "then": 12}, {"if": true, "then": 12}, {"if": true, "then": 12}], "else": 999}""")) should be (AvroInt())

    inferType(jsonToAst.expr("""{"cond": [{"if": true, "then": 12}, {"if": true, "then": {"string": "hello"}}, {"if": true, "then": {"base64": "aGVsbG8="}}]}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"cond": [{"if": true, "then": 12}, {"if": true, "then": {"string": "hello"}}, {"if": true, "then": {"base64": "aGVsbG8="}}], "else": 999}""")) should be (AvroUnion(List(AvroInt(), AvroString(), AvroBytes())))

    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"cond": []}""")) }
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"cond": [], "else": 999}""")) }
  }

  it must "test while" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"while": true, "do": 12}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"while": null, "do": 12}""")) }
  }

  it must "test do-until" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"do": 12, "until": true}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"do": 12, "until": null}""")) }
  }

  it must "test for" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": 12}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": "x"}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": "y"}""")) }
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"for": {}, "while": true, "step": {"x": 1}, "do": 12}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": null, "step": {"x": 1}, "do": 12}""")) }
    intercept[PFASyntaxException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {}, "do": 12}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"y": 1}, "do": 12}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 2.2}, "do": 12}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": 12}"""), Map("x" -> AvroInt())) }
    inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": {"set": {"x": 12}}}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"for": {"x": 0}, "while": true, "step": {"x": 1}, "do": {"set": {"x": 12.4}}}""")) }
  }

  it must "test foreach" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": 12}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": "x"}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": 12}"""), Map("x" -> AvroInt())) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": "y"}""")) }
    inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": {"set": {"x": 12}}}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"foreach": "x", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": {"set": {"x": 12.4}}}""")) }
  }

  it must "test forkeyval" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": "k"}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": "v"}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "array", "items": "int"}, "value": [1, 2, 3]}, "do": 12}""")) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}"""), Map("k" -> AvroString())) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": 12}"""), Map("v" -> AvroInt())) }
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": "z"}""")) }
    inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"k": ["hello"]}}}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"k": 12}}}""")) }
    inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"v": 12}}}""")) should be (AvroNull())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"forkey": "k", "forval": "v", "in": {"type": {"type": "map", "values": "int"}, "value": {"one": 1, "two": 2, "three": 3}}, "do": {"set": {"v": ["hello"]}}}""")) }
  }

  it must "test cast-case" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}]}"""), Map("x" -> AvroUnion(List(AvroInt(), AvroString())))) should be (AvroInt())
    inferType(jsonToAst.expr("""{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}], "partial": true}"""), Map("x" -> AvroUnion(List(AvroInt(), AvroString())))) should be (AvroNull())

    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}, {"as": "bytes", "named": "xb", "do": 999}]}"""), Map("x" -> AvroUnion(List(AvroInt(), AvroString())))) }

    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}]}"""), Map("x" -> AvroUnion(List(AvroInt(), AvroString(), AvroBytes())))) }

    inferType(jsonToAst.expr("""{"cast": "x", "cases": [{"as": "int", "named": "xi", "do": 12}, {"as": "string", "named": "xs", "do": 999}], "partial": true}"""), Map("x" -> AvroUnion(List(AvroInt(), AvroString(), AvroBytes())))) should be (AvroNull())
  }

  it must "test upcast" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"upcast": "x", "as": "double"}"""), Map("x" -> AvroInt())) should be (AvroDouble())
    intercept[PFASemanticException] { inferType(jsonToAst.expr("""{"upcast": "x", "as": "int"}"""), Map("x" -> AvroDouble())) }
  }

  it must "test ifnotnull" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"ifnotnull": {"x": "input"}, "then": "x", "else": 12.4}"""), Map("input" -> AvroUnion(List(AvroDouble(), AvroNull())))) should be (AvroDouble())
    inferType(jsonToAst.expr("""{"ifnotnull": {"x": "input", "y": "input"}, "then": {"+": ["x", "y"]}, "else": 12.4}"""), Map("input" -> AvroUnion(List(AvroDouble(), AvroNull())))) should be (AvroDouble())
    inferType(jsonToAst.expr("""{"ifnotnull": {"x": "input"}, "then": "x"}{}"""), Map("input" -> AvroUnion(List(AvroDouble(), AvroNull())))) should be (AvroNull())
    inferType(jsonToAst.expr("""{"ifnotnull": {"x": "input"}, "then": "x", "else": 12.4}"""), Map("input" -> AvroUnion(List(AvroDouble(), AvroString(), AvroNull())))) should be (AvroUnion(List(AvroDouble(), AvroString())))
    inferType(jsonToAst.expr("""{"ifnotnull": {"x": "input"}, "then": "x", "else": [["whatever"]]}"""), Map("input" -> AvroUnion(List(AvroDouble(), AvroNull())))) should be (AvroUnion(List(AvroDouble(), AvroString())))
  }

  it must "test doc, error, log" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"doc": "hello"}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"do": {"error": "hello"}}""")) should be (AvroNull())
    inferType(jsonToAst.expr("""{"log": [["hello"]]}""")) should be (AvroNull())
  }

  it must "test call patterns" taggedAs(TypeCheck) in {
    inferType(jsonToAst.expr("""{"+": [2, 2]}""")) should be (AvroInt())
    // HERE

  }
}
