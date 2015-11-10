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

package test.scala.randomjson

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Random

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.BinaryNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.DoubleNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.JsonNodeFactory
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.ObjectNode
import org.codehaus.jackson.node.TextNode

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
class RandomJsonSuite extends FlatSpec with Matchers {
  val factory = JsonNodeFactory.instance

  def m(pairs: (String, Any)*): ObjectNode = {
    val out = factory.objectNode
    for ((k, value) <- pairs) value match {
      case null => out.put(k, NullNode.getInstance)
      case x: Boolean => out.put(k, if (x) BooleanNode.getTrue else BooleanNode.getFalse)
      case x: Int => out.put(k, new IntNode(x))
      case x: Long => out.put(k, new LongNode(x))
      case x: Float => out.put(k, new DoubleNode(x))
      case x: Double => out.put(k, new DoubleNode(x))
      case x: String => out.put(k, new TextNode(x))
      case x: JsonNode => out.put(k, x)
    }
    out
  }

  def a(items: Any*): ArrayNode = {
    val out = factory.arrayNode
    for (item <- items) item match {
      case null => out.add(NullNode.getInstance)
      case x: Boolean => out.add(if (x) BooleanNode.getTrue else BooleanNode.getFalse)
      case x: Int => out.add(new IntNode(x))
      case x: Long => out.add(new LongNode(x))
      case x: Float => out.add(new DoubleNode(x))
      case x: Double => out.add(new DoubleNode(x))
      case x: String => out.add(new TextNode(x))
      case x: JsonNode => out.add(x)
    }
    out
  }

  val randomSeed = 54321
  val rng = new Random(randomSeed)
  val alphanumeric = rng.alphanumeric.iterator

  def data(): JsonNode = {
    rng.nextInt(9) match {
      case 0 => NullNode.getInstance
      case 1 => if (rng.nextBoolean()) BooleanNode.getTrue else BooleanNode.getFalse
      case 2 => new IntNode(rng.nextInt())
      case 3 => new LongNode(rng.nextLong())
      case 4 => new DoubleNode(rng.nextFloat())
      case 5 => new DoubleNode(rng.nextDouble())
      case 6 => new TextNode(rng.nextString(rng.nextInt(30)))
      case 7 => m((for (i <- 0 until rng.nextInt(3)) yield (rng.nextString(rng.nextInt(10)) -> data())): _*)
      case 8 => a((for (i <- 0 until rng.nextInt(3)) yield data()): _*)
    }
  }

  def stringMap(): ObjectNode =
    m((for (i <- 0 until rng.nextInt(3)) yield (rng.nextString(rng.nextInt(10)) -> rng.nextString(rng.nextInt(10)))): _*)

  val oldNames = mutable.Set[String]()
  def name(maxLength: Int): String = {
    var out: String = null
    while (out == null  ||  oldNames.contains(out)) {
      val first: Char = alphanumeric find { x => ('A' <= x  &&  x <= 'Z') } get
      val rest: String = alphanumeric.take(rng.nextInt(maxLength) - 1).mkString
      out = first + rest
    }
    oldNames.add(out)
    out
  }

  def names(number: Int, maxLength: Int): ArrayNode =
    a((for (i <- 0 until number) yield name(maxLength)): _*)

  def string(maxLength: Int): String = if (rng.nextBoolean()) rng.nextString(rng.nextInt(maxLength)) else name(maxLength)

  def strings(number: Int, maxLength: Int): ArrayNode =
    a((for (i <- 0 until number) yield string(maxLength)): _*)

  def fields(number: Int): ArrayNode = a(
    (for (i <- 0 until number) yield {
      val out = m("name" -> name(10), "type" -> avroType())
      //// FIXME: have to ensure that default satisfies type
      // if (rng.nextBoolean())
      //   out.put("default", data())
      if (rng.nextBoolean())
        out.put("order", List("ascending", "descending", "ignore")(rng.nextInt(3)))
      if (rng.nextBoolean())
        out.put("aliases", names(rng.nextInt(3), 10))
      if (rng.nextBoolean())
        out.put("doc", string(30))
      out
    }): _*)

  def avroType(parentIsUnion: Boolean = false): JsonNode = {
    rng.nextInt(if (parentIsUnion) 21 else 22) match {
      case 0 => new TextNode("null")
      case 1 => m("type" -> "null")
      case 2 => new TextNode("boolean")
      case 3 => m("type" -> "boolean")
      case 4 => new TextNode("int")
      case 5 => m("type" -> "int")
      case 6 => new TextNode("long")
      case 7 => m("type" -> "long")
      case 8 => new TextNode("float")
      case 9 => m("type" -> "float")
      case 10 => new TextNode("double")
      case 11 => m("type" -> "double")
      case 12 => new TextNode("bytes")
      case 13 => m("type" -> "bytes")
      case 14 => {
        val out = m("type" -> "fixed", "name" -> name(10), "size" -> rng.nextInt(100))
        if (rng.nextBoolean())
          out.put("namespace", name(10))
        if (rng.nextBoolean())
          out.put("aliases", names(rng.nextInt(3), 10))
        if (rng.nextBoolean())
          out.put("doc", string(30))
        out
      }
      case 15 => new TextNode("string")
      case 16 => m("type" -> "string")
      case 17 => {
        val out = m("type" -> "enum", "name" -> name(10), "symbols" -> names(rng.nextInt(10), 10))
        if (rng.nextBoolean())
          out.put("namespace", name(10))
        if (rng.nextBoolean())
          out.put("aliases", names(rng.nextInt(3), 10))
        if (rng.nextBoolean())
          out.put("doc", string(30))
        out
      }
      case 18 => m("type" -> "array", "items" -> avroType())
      case 19 => m("type" -> "map", "values" -> avroType())
      case 20 => {
        val out = m("type" -> "record", "name" -> name(10), "fields" -> fields(rng.nextInt(3)))
        if (rng.nextBoolean())
          out.put("namespace", name(10))
        if (rng.nextBoolean())
          out.put("aliases", names(rng.nextInt(3), 10))
        if (rng.nextBoolean())
          out.put("doc", string(30))
        out
      }
      case 21 => {
        val subtypes: Map[String, JsonNode] =
          (for (i <- 1 until rng.nextInt(2) + 2) yield avroType(parentIsUnion = true)).map({
            case x: TextNode => (x.getTextValue, m("type" -> x))
            case x: ObjectNode => (x.get("type").getTextValue, x)
          }).toMap
        a(subtypes.values.toSeq: _*)
      }
    }
  }

  def expressions(length: Int): ArrayNode = a((for (i <- 0 until length) yield expression()): _*)

  def arguments(length: Int): ArrayNode = a((for (i <- 0 until length) yield if (rng.nextInt(10) == 0) argument() else expression()): _*)

  def argument(): ObjectNode =
    if (rng.nextBoolean())
      m("fcn" -> name(10))
    else {
      val params = a((for (j <- 0 until rng.nextInt(3)) yield m(name(10) -> avroType())): _*)
      val ret = avroType()
      m("params" -> params, "ret" -> ret, "do" -> expressions(1 + rng.nextInt(2)))
    }

  def nameExprPairs(length: Int): ObjectNode = m((for (i <- 0 until length) yield name(10) -> expression()): _*)

  def expression(): JsonNode = rng.nextInt(34) match {
    case 0 => m(("u." + name(10)) -> arguments(rng.nextInt(3)))
    case 1 => NullNode.getInstance
    case 2 => if (rng.nextBoolean()) BooleanNode.getTrue else BooleanNode.getFalse
    case 3 => new LongNode(rng.nextLong())
    case 4 => new DoubleNode(rng.nextDouble())
    case 5 => new TextNode(name(10))
    case 6 => m("int" -> rng.nextInt())
    case 7 => m("float" -> rng.nextFloat())
    case 8 => m("string" -> string(30))
    case 9 => m("base64" -> new BinaryNode({
      val out = Array.ofDim[Byte](100)
      rng.nextBytes(out)
      out
    }))
    case 10 => m("type" -> avroType(), "value" -> data())
    case 11 => m("do" -> expressions(1 + rng.nextInt(2)))
    case 12 => m("let" -> nameExprPairs(1 + rng.nextInt(2)))
    case 13 => m("set" -> nameExprPairs(1 + rng.nextInt(2)))
    case 14 => m("attr" -> name(10), "path" -> expressions(1 + rng.nextInt(2)))
    case 15 => m("attr" -> name(10), "path" -> expressions(1 + rng.nextInt(2)), "to" -> expression())
    case 16 => m("cell" -> name(10), "path" -> expressions(rng.nextInt(3)))
    case 17 => m("cell" -> name(10), "path" -> expressions(rng.nextInt(3)), "to" -> expression())
    case 18 => m("pool" -> name(10), "path" -> expressions(1 + rng.nextInt(2)), "to" -> expression(), "init" -> expression())
    case 19 => m("if" -> expression(), "then" -> expressions(1 + rng.nextInt(2)))
    case 20 => m("if" -> expression(), "then" -> expressions(1 + rng.nextInt(2)), "else" -> expressions(1 + rng.nextInt(2)))
    case 21 => m("cond" -> a((for (i <- 0 until (1 + rng.nextInt(2))) yield m("if" -> expression(), "then" -> expressions(1 + rng.nextInt(2)))): _*))
    case 22 => m("cond" -> a((for (i <- 0 until (1 + rng.nextInt(2))) yield m("if" -> expression(), "then" -> expressions(1 + rng.nextInt(2)))): _*), "else" -> expressions(1 + rng.nextInt(2)))
    case 23 => m("while" -> expression(), "do" -> expressions(1 + rng.nextInt(2)))
    case 24 => m("do" -> expressions(1 + rng.nextInt(2)), "until" -> expression())
    case 25 => m("for" -> nameExprPairs(1 + rng.nextInt(2)), "while" -> expression(), "step" -> nameExprPairs(1 + rng.nextInt(2)), "do" -> expressions(1 + rng.nextInt(2)))
    case 26 => m("foreach" -> name(10), "in" -> expression(), "do" -> expressions(1 + rng.nextInt(2)), "seq" -> rng.nextBoolean())
    case 27 => m("forkey" -> name(10), "forval" -> name(10), "in" -> expression(), "do" -> expressions(1 + rng.nextInt(2)))
    case 28 => m("cast" -> expression(), "cases" -> a((for (i <- 0 until 2) yield m("as" -> avroType(), "named" -> name(10), "do" -> expressions(1 + rng.nextInt(2)))): _*))
    case 29 => m("doc" -> string(30))
    case 30 => m("error" -> string(10))
    case 31 => m("error" -> string(10), "code" -> (-1 - Math.abs(rng.nextInt())))
    case 32 => m("log" -> expressions(1 + rng.nextInt(2)))
    case 33 => m("log" -> expressions(1 + rng.nextInt(2)), "namespace" -> name(10))
  }

  def functions(length: Int): ObjectNode = m(
    (for (i <- 0 until length) yield {
      val params = a((for (j <- 0 until rng.nextInt(3)) yield m(name(10) -> avroType())): _*)
      val ret = avroType()
      name(10) -> m("params" -> params, "ret" -> ret, "do" -> expressions(1 + rng.nextInt(2)))
    }): _*)

  def cells(length: Int): ObjectNode = m(
    (for (i <- 0 until length) yield {
      val shared = rng.nextBoolean()
      val rollback =
        if (shared)
          false
        else
          rng.nextBoolean()
      name(10) -> m("type" -> avroType(), "init" -> data(), "shared" -> shared, "rollback" -> rollback)
    }): _*)

  def pools(length: Int): ObjectNode = m(
    (for (i <- 0 until length) yield {
      val shared = rng.nextBoolean()
      val rollback =
        if (shared)
          false
        else
          rng.nextBoolean()
      name(10) -> m("type" -> avroType(), "init" -> m("some" -> data()), "shared" -> shared, "rollback" -> rollback)
    }): _*)

  def engineConfig(): ObjectNode = {
    val out =
      m("name" -> name(10),
        "input" -> avroType(),
        "output" -> avroType(),
        "action" -> expressions(1 + rng.nextInt(2))
      )
    val method =
    if (rng.nextBoolean())
      List("map", "emit", "fold")(rng.nextInt(3))
    else
      "map"
    if (method != "map")
      out.put("method", method)
    if (rng.nextBoolean())
      out.put("begin", expressions(rng.nextInt(3)))
    if (rng.nextBoolean())
      out.put("end", expressions(rng.nextInt(3)))
    if (rng.nextBoolean())
      out.put("fcns", functions(rng.nextInt(3)))
    if (method == "fold") {
      out.put("zero", data())
      out.put("merge", expressions(1 + rng.nextInt(2)))
    }
    if (rng.nextBoolean())
      out.put("cells", cells(rng.nextInt(3)))
    if (rng.nextBoolean())
      out.put("pools", pools(rng.nextInt(3)))
    if (rng.nextBoolean())
      out.put("randseed", rng.nextLong())
    if (rng.nextBoolean())
      out.put("doc", string(30))
    if (rng.nextBoolean())
      out.put("metadata", stringMap())
    if (rng.nextBoolean())
      out.put("options", m((for (i <- 0 until rng.nextInt(3)) yield string(10) -> data()): _*))
    out
  }

  var jsons: List[String] = Nil
  var asts: List[Ast] = Nil

  "Random PFA" must "generate valid configurations" taggedAs(RandomJson) in {
    for (i <- 0 until 100) yield {
      try {
        oldNames.clear()
        val json = engineConfig().toString
        if (json.size > 100)
          println(json.substring(0, 100) + "...")
        else
          println(json)

        jsons = json :: jsons
      }
      catch {
        case _: java.lang.StackOverflowError =>
      }
    }
    jsons = jsons.reverse
  }

  it must "be able to convert them to AST" taggedAs(RandomJson) in {
    for (oldJson <- jsons) {
      val ast = jsonToAst(oldJson)
      val newJson = ast.toJson()

      if (newJson.size > 100)
        println(newJson.substring(0, 100) + "...")
      else
        println(newJson)

      asts = ast :: asts
    }
    asts = asts.reverse
  }

  it must "convert to JSON that converts back to the same AST (full cycle)" taggedAs(RandomJson) in {
    for (ast <- asts) {
      val newJson = ast.toJson()
      if (newJson.size > 100)
        println(newJson.substring(0, 100) + "...")
      else
        println(newJson)

      jsonToAst(newJson) should be (ast)
    }
  }
}
