// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// 
// Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test.scala.asttojson

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.JsonNode

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.util._
import test.scala._

@RunWith(classOf[JUnitRunner])
class AstToJsonSuite extends FlatSpec with Matchers {
  def checkAstToJson(ast: Ast, json: String): Unit =
    convertFromJson(ast.toString) should be (convertFromJson(json))

  "AST to JSON" must "engine config" taggedAs(AstToJson) in {
    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      Nil,
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Nil,
      Map(),
      None,
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map(),
      Map()),
      """{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map(),
      None,
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map(),
      Map()),
      """{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}]
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map(),
      Map()),
      """{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}}
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map(),
      None,
      None,
      None,
      Map(),
      Map()),
      """{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}},
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}}
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      None,
      None,
      None,
      Map(),
      Map()),
      """{
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
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      Some(12345),
      None,
      None,
      Map(),
      Map()),
      """{
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
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      Some(12345),
      Some("hello"),
      None,
      Map(),
      Map()),
      """{
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
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      Some(12345),
      Some("hello"),
      None,
      Map("internal" -> "data"),
      Map()),
      """{
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
}""")

    checkAstToJson(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      Some(12345),
      Some("hello"),
      None,
      Map("internal" -> "data"),
      Map("param" -> convertFromJson("3"))),
      """{
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
}""")
  }

  it must "cell" taggedAs(AstToJson) in {
    checkAstToJson(Cell(AvroInt(), "0", false, false), """{"type":"int","init":0,"shared":false,"rollback":false}""")
  }

  it must "pool" taggedAs(AstToJson) in {
    checkAstToJson(Pool(AvroInt(), Map[String, String](), false, false), """{"type":"int","init":{},"shared":false,"rollback":false}""")
    checkAstToJson(Pool(AvroInt(), Map[String, String]("one" -> "1"), false, false), """{"type":"int","init":{"one":1},"shared":false,"rollback":false}""")
  }

  it must "define function" taggedAs(AstToJson) in {
    checkAstToJson(FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull())), """{"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}""")
  }

  it must "call function" taggedAs(AstToJson) in {
    checkAstToJson(Call("+", List(LiteralInt(2), LiteralInt(2))), """{"+":[2,2]}""")
  }

  it must "call with fcn" taggedAs(AstToJson) in {
    checkAstToJson(Call("sort", List(Ref("array"), FcnRef("byname"))), """{"sort":["array",{"fcn": "byname"}]}""")
  }

  it must "call with fcn def" taggedAs(JsonToAst) in {
    checkAstToJson(Call("sort", List(Ref("array"), FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull())))),
      """{"sort":["array",{"params": [{"x": "int"}, {"y": "string"}], "ret": "null", "do": [null]}]}""")
  }

  it must "ref" taggedAs(AstToJson) in {
    checkAstToJson(Ref("x"), """"x"""")
  }

  it must "null" taggedAs(AstToJson) in {
    checkAstToJson(LiteralNull(), "null")
  }

  it must "boolean" taggedAs(AstToJson) in {
    checkAstToJson(LiteralBoolean(true), "true")
    checkAstToJson(LiteralBoolean(false), "false")
  }

  it must "int" taggedAs(AstToJson) in {
    checkAstToJson(LiteralInt(2), """2""")
  }

  it must "long" taggedAs(AstToJson) in {
    checkAstToJson(LiteralLong(2), """{"long":2}""")
  }

  it must "float" taggedAs(AstToJson) in {
    checkAstToJson(LiteralFloat(2.5f), """{"float":2.5}""")
  }

  it must "double" taggedAs(AstToJson) in {
    checkAstToJson(LiteralDouble(2.2), "2.2")
  }

  it must "string" taggedAs(AstToJson) in {
    checkAstToJson(LiteralString("hello"), """{"string":"hello"}""")
  }

  it must "base64" taggedAs(AstToJson) in {
    checkAstToJson(LiteralBase64("hello".getBytes), """{"base64":"aGVsbG8="}""")
  }

  it must "literal" taggedAs(AstToJson) in {
    checkAstToJson(Literal(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "SimpleRecord"), """{"one": 1, "two": 2.2, "three": "THREE"}"""),
    """{"type":{"type":"record","fields":[{"name":"one","type":"int"},{"name":"two","type":"double"},{"name":"three","type":"string"}],"name":"SimpleRecord"},"value":{"one":1,"two":2.2,"three":"THREE"}}""")
  }

  it must "new record" taggedAs(AstToJson) in {
    checkAstToJson(NewObject(Map("one" -> LiteralInt(1), "two" -> LiteralDouble(2.2), "three" -> LiteralString("THREE")),
      AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "SimpleRecord"), new AvroTypeBuilder),
      """{"new":{"one":1,"two":2.2,"three":{"string":"THREE"}},"type":{"type":"record","fields":[{"name":"one","type":"int"},{"name":"two","type":"double"},{"name":"three","type":"string"}],"name":"SimpleRecord"}}""")
  }

  it must "new array" taggedAs(AstToJson) in {
    checkAstToJson(NewArray(List(LiteralInt(1), LiteralInt(2), LiteralInt(3)), AvroArray(AvroInt()), new AvroTypeBuilder), """{"new":[1,2,3],"type":{"type":"array","items":"int"}}""")
  }

  it must "do" taggedAs(AstToJson) in {
    checkAstToJson(Do(List(Ref("x"), Ref("y"), Ref("z"))), """{"do":["x","y","z"]}""")
  }

  it must "let" taggedAs(AstToJson) in {
    checkAstToJson(Let(Map("x" -> LiteralInt(3), "y" -> LiteralInt(4))), """{"let":{"x":3,"y":4}}""")
  }

  it must "set" taggedAs(AstToJson) in {
    checkAstToJson(SetVar(Map("x" -> LiteralInt(3), "y" -> LiteralInt(4))), """{"set":{"x":3,"y":4}}""")
  }

  it must "attr-get" taggedAs(AstToJson) in {
    checkAstToJson(AttrGet(Ref("a"), List(Ref("a"), LiteralInt(1), LiteralString("b"))), """{"attr":"a","path":["a",1,{"string":"b"}]}""")
  }

  it must "attr-set" taggedAs(AstToJson) in {
    checkAstToJson(AttrTo(Ref("a"), List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2)), """{"attr":"a","path":["a",1,{"string":"b"}],"to":2.2}""")
  }

  it must "cell-get" taggedAs(AstToJson) in {
    checkAstToJson(CellGet("c", Nil), """{"cell":"c"}""")
    checkAstToJson(CellGet("c", List(Ref("a"), LiteralInt(1), LiteralString("b"))), """{"cell":"c","path":["a",1,{"string":"b"}]}""")
  }

  it must "cell-set" taggedAs(AstToJson) in {
    checkAstToJson(CellTo("c", Nil, LiteralDouble(2.2)), """{"cell":"c","to":2.2}""")
    checkAstToJson(CellTo("c", List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2)), """{"cell":"c","path":["a",1,{"string":"b"}],"to":2.2}""")
  }

  it must "pool-get" taggedAs(AstToJson) in {
    checkAstToJson(PoolGet("p", List(Ref("a"), LiteralInt(1), LiteralString("b"))), """{"pool":"p","path":["a",1,{"string":"b"}]}""")
  }

  it must "pool-set" taggedAs(AstToJson) in {
    checkAstToJson(PoolTo("p", List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2), LiteralDouble(2.2)), """{"pool":"p","path":["a",1,{"string":"b"}],"to":2.2,"init":2.2}""")
  }

  it must "if" taggedAs(AstToJson) in {
    checkAstToJson(If(LiteralBoolean(true), List(Ref("x")), None), """{"if":true,"then":["x"]}""")
    checkAstToJson(If(LiteralBoolean(true), List(Ref("x")), Some(List(Ref("y")))), """{"if":true,"then":["x"],"else":["y"]}""")
  }

  it must "cond" taggedAs(AstToJson) in {
    checkAstToJson(Cond(List(If(LiteralBoolean(false), List(Ref("x")), None), If(LiteralBoolean(true), List(Ref("y")), None)), None),
      """{"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}]}""")
    checkAstToJson(Cond(List(If(LiteralBoolean(false), List(Ref("x")), None), If(LiteralBoolean(true), List(Ref("y")), None)), Some(List(Ref("z")))),
      """{"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}],"else":["z"]}""")
  }

  it must "while" taggedAs(AstToJson) in {
    checkAstToJson(While(LiteralBoolean(true), List(Call("+", List(LiteralInt(2), LiteralInt(2))))),
      """{"while":true,"do":[{"+":[2,2]}]}""")
  }

  it must "do-until" taggedAs(AstToJson) in {
    checkAstToJson(DoUntil(List(Call("+", List(LiteralInt(2), LiteralInt(2)))), LiteralBoolean(true)),
      """{"do":[{"+":[2,2]}],"until":true}""")
  }

  it must "for" taggedAs(AstToJson) in {
    checkAstToJson(For(Map("i" -> LiteralInt(0)), Call("<", List(Ref("i"), LiteralInt(10))), Map("i" -> Call("+", List(Ref("i"), LiteralInt(1)))), List(Ref("i"))),
      """{"for":{"i":0},"while":{"<":["i",10]},"step":{"i":{"+":["i",1]}},"do":["i"]}""")
  }

  it must "foreach" taggedAs(AstToJson) in {
    checkAstToJson(Foreach("x", Literal(AvroArray(AvroInt()), """[1, 2, 3]"""), List(Ref("x")), false),
      """{"foreach":"x","in":{"type":{"type":"array","items":"int"},"value":[1,2,3]},"do":["x"],"seq":false}""")
  }

  it must "forkeyval" taggedAs(AstToJson) in {
    checkAstToJson(Forkeyval("k", "v", Literal(AvroMap(AvroInt()), """{"one": 1, "two": 2, "three": 3}"""), List(Ref("k"))),
      """{"forkey":"k","forval":"v","in":{"type":{"type":"map","values":"int"},"value":{"one":1,"two":2,"three":3}},"do":["k"]}""")
  }
  
  it must "ifnotnull" taggedAs(JsonToAst) in {
    checkAstToJson(IfNotNull(Map("x" -> Ref("input")), List(Ref("x")), Some(List(LiteralInt(12)))),
      """{"ifnotnull":{"x":"input"},"then":["x"],"else":[12]}""")
    checkAstToJson(IfNotNull(Map("x" -> Ref("input")), List(Ref("x")), None),
      """{"ifnotnull":{"x":"input"},"then":["x"]}""")
  }

  it must "cast" taggedAs(AstToJson) in {
    checkAstToJson(CastBlock(LiteralInt(3), List(CastCase(AvroString(), "x", List(Ref("x"))), CastCase(AvroInt(), "x", List(Ref("x")))), false),
      """{"cast":3,"cases":[{"as":"string","named":"x","do":["x"]},{"as":"int","named":"x","do":["x"]}],"partial":false}""")
  }

  it must "doc" taggedAs(AstToJson) in {
    checkAstToJson(Doc("hello"), """{"doc":"hello"}""")
  }

  it must "error" taggedAs(AstToJson) in {
    checkAstToJson(Error("hello", None), """{"error":"hello"}""")
    checkAstToJson(Error("hello", Some(3)), """{"error":"hello","code":3}""")
  }

  it must "log" taggedAs(AstToJson) in {
    checkAstToJson(Log(List(LiteralString("hello")), None), """{"log":[{"string":"hello"}]}""")
    checkAstToJson(Log(List(LiteralString("hello")), Some("DEBUG")), """{"log":[{"string":"hello"}],"namespace":"DEBUG"}""")
    checkAstToJson(Log(List(Call("+", List(LiteralInt(2), LiteralInt(2)))), None), """{"log":[{"+":[2,2]}]}""")
  }

}
