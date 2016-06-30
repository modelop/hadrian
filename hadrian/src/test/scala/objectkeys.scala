package test.scala.objectkeys

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.errors.PFASyntaxException
import com.opendatagroup.hadrian.reader.jsonToAst
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import test.scala.JsonToAst

@RunWith(classOf[JUnitRunner])
class ObjectKeySuite extends FlatSpec with Matchers {

  def checkJsonToAst(ast: Ast, json: String): Unit = jsonToAst(json) should be (ast)

  def getEngineConfig(
    inputSchema: AvroType,
    outputSchema: AvroType,
    actions: Seq[Expression]
  ): EngineConfig = EngineConfig(
    "test",
    Method.MAP,
    inputSchema,
    outputSchema,
    List(),
    actions,
    List(),
    Map(),
    None,
    None,
    Map(),
    Map(),
    None,
    None,
    None,
    Map(),
    Map()
  )

  "the AST" must "correctly parse literal maps with non-symbol-friendly keys" taggedAs(JsonToAst) in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": "string",
        |  "output": {"type": "map", "values": "int"},
        |  "action": [
        |    {"type": {"type": "map", "values": "int"}, "value": {"5": 5, "_hello": 1}}
        |  ]
        |}
      """.stripMargin
    val actions = List(Literal(AvroMap(AvroInt()), "{\"5\": 5, \"_hello\": 1}"))
    val ast = getEngineConfig(AvroString(), AvroMap(AvroInt()), actions)
    checkJsonToAst(ast, json)

  }

  it must "correctly parse new maps with non-symbol-friendly keys" taggedAs(JsonToAst) in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": "string",
        |  "output": {"type": "map", "values": "int"},
        |  "action": [
        |    {"type": {"type": "map", "values": "int"}, "new": {"5": 5, "_hello": 1}}
        |  ]
        |}
      """.stripMargin
    val actions = List(
      NewObject(Map("5"->LiteralInt(5), "_hello"->LiteralInt(1)), AvroMap(AvroInt()))
    )
    val ast = getEngineConfig(AvroString(), AvroMap(AvroInt()), actions)
    checkJsonToAst(ast, json)
  }

  it must "throw an exception when Let gets non-symbol keys" in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": "string",
        |  "output": "string",
        |  "action": [
        |    {"let": {"1": {"string": "hello"}}},
        |    {"type": "string", "value": "EXCEPTION!"}
        |  ]
        |}
      """.stripMargin

    an [PFASyntaxException] should be thrownBy jsonToAst(json)

    an [PFASyntaxException] should be thrownBy Let(Map("1"->LiteralString("hello")))
  }

  it must "allow Let with valid symbol keys" in {
    val json = """
      |{
      |  "name": "test",
      |  "input": "string",
      |  "output": "string",
      |  "action": [
      |    {"let": {"x": {"string": "hello"}}},
      |    "x"
      |  ]
      |}
      """.stripMargin
    val actions = List(Let(Map("x"->LiteralString("hello"))), Ref("x"))
    val ast = getEngineConfig(AvroString(), AvroString(), actions)
    checkJsonToAst(ast, json)
  }

  it must "throw an exception when For gets non-symbol keys in init or step" in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": "string",
        |  "output": "int",
        |  "action": [
        |    {
        |      "for": {"1": 5},
        |      "while": {"<=": ["1", 10]},
        |      "step": {"1": {"+": ["1", 1]}},
        |      "do": "1"
        |    }
        |  ]
      """.stripMargin
    an[PFASyntaxException] should be thrownBy jsonToAst(json)

    an[PFASyntaxException] should be thrownBy For(
      Map("1"->LiteralInt(5), "x"->LiteralInt(2)),
      Call("<=", Seq(Ref("1"), LiteralInt(10))),
      Map("1"->Call("+", Seq(Ref("1"), LiteralInt(1)))),
      Seq(Ref("x"))
    )
  }

  it must "accept For with valid symbol keys in init and step" in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": "string",
        |  "output": "int",
        |  "action": [
        |    {
        |      "for": {"x": 5},
        |      "while": {"<=": ["x", 10]},
        |      "step": {"x": {"+": ["x", 1]}},
        |      "do": "x"
        |    }
        |  ]
        |}
      """.stripMargin
    val actions = List(
      For(
        Map("x"->LiteralInt(5)),
        Call("<=", Seq(Ref("x"), LiteralInt(10))),
        Map("x"->Call("+", Seq(Ref("x"), LiteralInt(1)))),
        Seq(Ref("x"))
      )
    )
    val ast = getEngineConfig(AvroString(), AvroInt(), actions)
    checkJsonToAst(ast, json)
  }

  it must "throw an exception when IfNotNull gets non-symbol strings in exprs" in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": ["null", "int"],
        |  "output": "int",
        |  "action": [
        |    {
        |      "ifnotnull": {"1": "input"},
        |      "then": "1",
        |      "else": 0
        |    }
        |  ]
        |}
      """.stripMargin

    an [PFASyntaxException] should be thrownBy jsonToAst(json)

    an [PFASyntaxException] should be thrownBy {
      IfNotNull(Map("1"->Ref("input")), Seq(Ref("1")), Some(Seq(LiteralInt(0))))
    }
  }

  it must "not throw an exception when IfNotNull gets valid symbol strings in exprs" in {
    val json =
    """
      |{
      |  "name": "test",
      |  "input": ["null", "int"],
      |  "output": "int",
      |  "action": [
      |    {
      |      "ifnotnull": {"x": "input"},
      |      "then": "x",
      |      "else": 0
      |    }
      |  ]
      |}
    """.stripMargin

    val actions = Seq(
      IfNotNull(Map("x"->Ref("input")), Seq(Ref("x")), Some(Seq(LiteralInt(0))))
    )
    val ast = getEngineConfig(
      AvroUnion(Seq(AvroNull(), AvroInt())),
      AvroInt(),
      actions
    )
    checkJsonToAst(ast, json)
  }

  it must "throw an exception when Unpack gets invalid symbol strings in exprs" in {
    val json =
      """
        |{
        |  "name": "test",
        |  "input": "bytes",
        |  "output": "int",
        |  "action": [
        |    {
        |      "unpack": "input",
        |      "format": [{"1": "int32"}],
        |      "then": "1",
        |      "else": 0
        |    }
        |  ]
        |}
      """.stripMargin

    an [PFASyntaxException] should be thrownBy jsonToAst(json)

    an [PFASyntaxException] should be thrownBy {
      Unpack(Ref("input"), Seq(("1", "int32")), Seq(Ref("1")), Some(Seq(LiteralInt(0))), None)
    }
  }
}