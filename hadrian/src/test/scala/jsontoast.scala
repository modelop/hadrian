package test.scala.jsontoast

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonFactory

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.util._
import com.opendatagroup.hadrian.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class JsonToAstSuite extends FlatSpec with Matchers {
  def checkJsonToAst(ast: Ast, json: String): Unit = jsonToAst(json) should be (ast)

  "JSON to AST" must "engine config" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.EMIT,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "method": "emit",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "begin": [{"+": [2, 2]}],
  "action": [{"+": [2, 2]}],
  "end": [{"+": [2, 2]}]
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}}
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map(),
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "cells":{"private":{"type":"int","init":0,"shared":false,"rollback":false}},
  "pools":{"private":{"type":"int","init":{},"shared":false,"rollback":false}}
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      Some(12345),
      Some("hello"),
      Some(convertFromJson("""{"internal": "data"}""")),
      Map("param" -> convertFromJson("3"))),
      """{
  "name": "test",
  "method": "map",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "randseed":12345,
  "doc":"hello",
  "metadata":{"internal":"data"},
  "options":{"param":3}
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      Map("private" -> Cell(AvroInt(), "0", false, false)),
      Map("private" -> Pool(AvroInt(), Map[String, String](), false, false)),
      Some(12345),
      Some("hello"),
      Some(convertFromJson("""{"internal": "data"}""")),
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

  it must "cell" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map(),
      None,
      Map("private" -> Cell(AvroArray(AvroString()), "[]", false, false)),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "cells":{"private":{"type":{"type": "array", "items": "string"},"init":[],"shared":false,"rollback":false}}
}""")
  }

  it must "define function" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map("f" -> FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}],
  "fcns": {"f": {"params":[{"x":"int"},{"y":"string"}],"ret":"null","do":[null]}}
}""")
  }

  it must "call function" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("+", List(LiteralInt(2), LiteralInt(2)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"+": [2, 2]}]
}""")
  }

  it must "call with fcn" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("sort", List(Ref("array"), FcnRef("byname")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"sort":["array",{"fcnref": "byname"}]}]
}""")
  }

  it must "call with fcn def" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Call("sort", List(Ref("array"), FcnDef(List("x" -> AvroInt(), "y" -> AvroString()), AvroNull(), List(LiteralNull()))))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"sort":["array",{"params": [{"x": "int"}, {"y": "string"}], "ret": "null", "do": [null]}]}]
}""")
  }

  it must "ref" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Ref("x")),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": ["x"]
}""")
  }

  it must "null" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralNull()),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [null]
}""")
  }

  it must "boolean" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralBoolean(true), LiteralBoolean(false)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [true, false]
}""")
  }

  it must "int" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralInt(2)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [2]
}""")
  }

  it must "long" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroLong(),
      AvroString(),
      List(),
      List(LiteralLong(2)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "long",
  "output": "string",
  "action": [{"long": 2}]
}""")
  }

  it must "float" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralFloat(2.5F)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"float": 2.5}]
}""")
  }

  it must "double" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralDouble(2.2)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [2.2]
}""")
  }

  it must "string" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralString("hello")),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"string": "hello"}]
}""")
  }

  it must "base64" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(LiteralBase64("hello".getBytes)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"base64": "aGVsbG8="}]
}""")
  }

  it must "literal" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Literal(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "SimpleRecord"), """{"one": 1, "two": 2.2, "three": "THREE"}""")),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"type":{"type":"record","name":"SimpleRecord","doc":"","fields":[{"name":"one","type":"int","doc":""},{"name":"two","type":"double","doc":""},{"name":"three","type":"string","doc":""}]},"value":{"one":1,"two":2.2,"three":"THREE"}}]
}""")
  }

  it must "new record" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(NewObject(Map("one" -> LiteralInt(1), "two" -> LiteralDouble(2.2), "three" -> LiteralString("THREE")),
      AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "SimpleRecord"), new AvroTypeBuilder)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"new":{"one":{"int":1},"two":2.2,"three":{"string":"THREE"}},"type":{"type":"record","name":"SimpleRecord","doc":"","fields":[{"name":"one","type":"int","doc":""},{"name":"two","type":"double","doc":""},{"name":"three","type":"string","doc":""}]}}]
}""")
  }

  it must "new array" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(NewArray(List(LiteralInt(1), LiteralInt(2), LiteralInt(3)), AvroArray(AvroInt()), new AvroTypeBuilder)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"new":[1,2,3],"type":{"type":"array","items":"int"}}]
}""")
  }

  it must "do" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Do(List(Ref("x"), Ref("y"), Ref("z")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"do":["x","y","z"]}]
}""")
  }

  it must "let" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Let(Map("x" -> LiteralInt(3), "y" -> LiteralInt(4)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"let":{"x":3,"y":4}}]
}""")
  }

  it must "set" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(SetVar(Map("x" -> LiteralInt(3), "y" -> LiteralInt(4)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"set":{"x":3,"y":4}}]
}""")
  }

  it must "attr-get" taggedAs(AstToJson) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(AttrGet(Ref("c"), List(Ref("a"), LiteralInt(1), LiteralString("b")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"attr":"c","path":["a",1,{"string":"b"}]}]
}""")
  }

  it must "attr-set" taggedAs(AstToJson) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(AttrTo(Ref("c"), List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2), None)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"attr":"c","path":["a",1,{"string":"b"}],"to":2.2}]
}""")
  }

  it must "cell-get" taggedAs(AstToJson) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(CellGet("c", Nil)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c"}]
}""")
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(CellGet("c", List(Ref("a"), LiteralInt(1), LiteralString("b")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c","path":["a",1,{"string":"b"}]}]
}""")
  }

  it must "cell-set" taggedAs(AstToJson) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(CellTo("c", Nil, LiteralDouble(2.2))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c","to":2.2}]
}""")
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(CellTo("c", List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cell":"c","path":["a",1,{"string":"b"}],"to":2.2}]
}""")
  }

  it must "pool-get" taggedAs(AstToJson) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(PoolGet("p", List(Ref("a"), LiteralInt(1), LiteralString("b")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"pool":"p","path":["a",1,{"string":"b"}]}]
}""")
  }

  it must "pool-set" taggedAs(AstToJson) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(PoolTo("p", List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2), None)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"pool":"p","path":["a",1,{"string":"b"}],"to":2.2}]
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(PoolTo("p", List(Ref("a"), LiteralInt(1), LiteralString("b")), LiteralDouble(2.2), Some(LiteralDouble(2.2)))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"pool":"p","path":["a",1,{"string":"b"}],"to":2.2,"init":2.2}]
}""")
  }

  it must "if" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(If(LiteralBoolean(true), List(Ref("x")), None), If(LiteralBoolean(true), List(Ref("x")), Some(List(Ref("y"))))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"if":true,"then":["x"]}, {"if":true,"then":["x"],"else":["y"]}]
}""")
  }

  it must "cond" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Cond(List(If(LiteralBoolean(false), List(Ref("x")), None), If(LiteralBoolean(true), List(Ref("y")), None)), None),
           Cond(List(If(LiteralBoolean(false), List(Ref("x")), None), If(LiteralBoolean(true), List(Ref("y")), None)), Some(List(Ref("z"))))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}]},
             {"cond":[{"if":false,"then":["x"]},{"if":true,"then":["y"]}],"else":["z"]}]
}""")
  }

  it must "while" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(While(LiteralBoolean(true), List(Call("+", List(LiteralInt(2), LiteralInt(2)))))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"while":true,"do":[{"+":[2,2]}]}]
}""")
  }

  it must "do-until" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(DoUntil(List(Call("+", List(LiteralInt(2), LiteralInt(2)))), LiteralBoolean(true))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"do":[{"+":[2,2]}],"until":true}]
}""")
  }

  it must "for" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(For(Map("i" -> LiteralInt(0)), Call("<", List(Ref("i"), LiteralInt(10))), Map("i" -> Call("+", List(Ref("i"), LiteralInt(1)))), List(Ref("i")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"for":{"i":0},"while":{"<":["i",10]},"step":{"i":{"+":["i",1]}},"do":["i"]}]
}""")
  }

  it must "foreach" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Foreach("x", Literal(AvroArray(AvroInt()), """[1, 2, 3]"""), List(Ref("x")), false)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"foreach":"x","in":{"type":{"type":"array","items":"int"},"value":[1,2,3]},"do":["x"],"seq":false}]
}""")
  }

  it must "forkeyval" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Forkeyval("k", "v", Literal(AvroMap(AvroInt()), """{"one": 1, "two": 2, "three": 3}"""), List(Ref("k")))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"forkey":"k","forval":"v","in":{"type":{"type":"map","values":"int"},"value":{"one":1,"two":2,"three":3}},"do":["k"]}]
}""")
  }

  it must "cast" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(CastBlock(LiteralInt(3), List(CastCase(AvroString(), "x", List(Ref("x"))), CastCase(AvroInt(), "x", List(Ref("x")))), true)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"cast":{"int":3},"cases":[{"as":"string","named":"x","do":["x"]},{"as":"int","named":"x","do":["x"]}],"partial":true}]
}""")
  }

  it must "ifnotnull" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroUnion(List(AvroInt(), AvroNull())),
      AvroInt(),
      List(),
      List(IfNotNull(Map("x" -> Ref("input")), List(Ref("x")), Some(List(LiteralInt(12))))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": ["int", "null"],
  "output": "int",
  "action": [{"ifnotnull": {"x": "input"}, "then": ["x"], "else": 12}]
}""")

    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroUnion(List(AvroInt(), AvroNull())),
      AvroNull(),
      List(),
      List(IfNotNull(Map("x" -> Ref("input")), List(Ref("x")), None)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": ["int", "null"],
  "output": "null",
  "action": [{"ifnotnull": {"x": "input"}, "then": ["x"]}]
}""")
  }

  it must "doc" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Doc("hello")),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"doc":"hello"}]
}""")
  }

  it must "error" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Error("hello", None), Error("hello", Some(3))),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"error":"hello"}, {"error":"hello","code":3}]
}""")
  }

  it must "log" taggedAs(JsonToAst) in {
    checkJsonToAst(EngineConfig(
      "test",
      Method.MAP,
      AvroInt(),
      AvroString(),
      List(),
      List(Log(List(LiteralString("hello")), None),
           Log(List(LiteralString("hello")), Some("DEBUG")),
           Log(List(Call("+", List(LiteralInt(2), LiteralInt(2)))), None)),
      List(),
      Map(),
      None,
      Map(),
      Map(),
      None,
      None,
      None,
      Map()),
      """{
  "name": "test",
  "input": "int",
  "output": "string",
  "action": [{"log":[{"string":"hello"}]},
             {"log":[{"string":"hello"}],"namespace":"DEBUG"},
             {"log":[{"+":[2,2]}]}]
}""")
  }

}
