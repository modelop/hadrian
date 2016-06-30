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

package com.opendatagroup.hadrian

import java.lang.StringBuilder
import java.io.ByteArrayOutputStream

import scala.collection.mutable

import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonLocation
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.JsonToken
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.JsonNodeFactory

import com.opendatagroup.hadrian.ast.validSymbolName
import com.opendatagroup.hadrian.ast.validFunctionName
import com.opendatagroup.hadrian.ast.Ast
import com.opendatagroup.hadrian.ast.Method
import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.ast.CellPoolSource
import com.opendatagroup.hadrian.ast.CellSource
import com.opendatagroup.hadrian.ast.PoolSource
import com.opendatagroup.hadrian.ast.EmbeddedJsonDomCellSource
import com.opendatagroup.hadrian.ast.EmbeddedJsonDomPoolSource
import com.opendatagroup.hadrian.ast.ExternalJsonCellSource
import com.opendatagroup.hadrian.ast.ExternalJsonPoolSource
import com.opendatagroup.hadrian.ast.ExternalAvroCellSource
import com.opendatagroup.hadrian.ast.ExternalAvroPoolSource
import com.opendatagroup.hadrian.ast.Cell
import com.opendatagroup.hadrian.ast.Pool
import com.opendatagroup.hadrian.ast.Argument
import com.opendatagroup.hadrian.ast.Expression
import com.opendatagroup.hadrian.ast.LiteralValue
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef
import com.opendatagroup.hadrian.ast.FcnRefFill
import com.opendatagroup.hadrian.ast.CallUserFcn
import com.opendatagroup.hadrian.ast.Call
import com.opendatagroup.hadrian.ast.Ref
import com.opendatagroup.hadrian.ast.LiteralNull
import com.opendatagroup.hadrian.ast.LiteralBoolean
import com.opendatagroup.hadrian.ast.LiteralInt
import com.opendatagroup.hadrian.ast.LiteralLong
import com.opendatagroup.hadrian.ast.LiteralFloat
import com.opendatagroup.hadrian.ast.LiteralDouble
import com.opendatagroup.hadrian.ast.LiteralString
import com.opendatagroup.hadrian.ast.LiteralBase64
import com.opendatagroup.hadrian.ast.Literal
import com.opendatagroup.hadrian.ast.NewObject
import com.opendatagroup.hadrian.ast.NewArray
import com.opendatagroup.hadrian.ast.Do
import com.opendatagroup.hadrian.ast.Let
import com.opendatagroup.hadrian.ast.SetVar
import com.opendatagroup.hadrian.ast.AttrGet
import com.opendatagroup.hadrian.ast.AttrTo
import com.opendatagroup.hadrian.ast.CellGet
import com.opendatagroup.hadrian.ast.CellTo
import com.opendatagroup.hadrian.ast.PoolGet
import com.opendatagroup.hadrian.ast.PoolTo
import com.opendatagroup.hadrian.ast.PoolDel
import com.opendatagroup.hadrian.ast.If
import com.opendatagroup.hadrian.ast.Cond
import com.opendatagroup.hadrian.ast.While
import com.opendatagroup.hadrian.ast.DoUntil
import com.opendatagroup.hadrian.ast.For
import com.opendatagroup.hadrian.ast.Foreach
import com.opendatagroup.hadrian.ast.Forkeyval
import com.opendatagroup.hadrian.ast.CastCase
import com.opendatagroup.hadrian.ast.CastBlock
import com.opendatagroup.hadrian.ast.Upcast
import com.opendatagroup.hadrian.ast.IfNotNull
import com.opendatagroup.hadrian.ast.BinaryFormatter
import com.opendatagroup.hadrian.ast.Pack
import com.opendatagroup.hadrian.ast.Unpack
import com.opendatagroup.hadrian.ast.Doc
import com.opendatagroup.hadrian.ast.Error
import com.opendatagroup.hadrian.ast.Try
import com.opendatagroup.hadrian.ast.Log

import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroNumber
import com.opendatagroup.hadrian.datatype.AvroRaw
import com.opendatagroup.hadrian.datatype.AvroIdentifier
import com.opendatagroup.hadrian.datatype.AvroContainer
import com.opendatagroup.hadrian.datatype.AvroMapping
import com.opendatagroup.hadrian.datatype.AvroNull
import com.opendatagroup.hadrian.datatype.AvroBoolean
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroBytes
import com.opendatagroup.hadrian.datatype.AvroFixed
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroEnum
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion
import com.opendatagroup.hadrian.datatype.AvroPlaceholder
import com.opendatagroup.hadrian.datatype.AvroTypeBuilder

import com.opendatagroup.hadrian.errors.PFASyntaxException
import com.opendatagroup.hadrian.util.escapeJson
import com.opendatagroup.hadrian.util.pos
import com.opendatagroup.hadrian.util.uniqueEngineName
import com.opendatagroup.hadrian.util.convertToJson
import com.opendatagroup.hadrian.yaml.yamlToJson

package reader {
  /** Reads PFA from serialized JSON into an abstract syntax tree.
    */
  object jsonToAst extends Function1[String, EngineConfig] {
    val jsonFactory = new JsonFactory
    jsonFactory.enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)

    val objectMapper = new ObjectMapper
    jsonFactory.setCodec(objectMapper)

    private val NumberPattern = "[0-9]+".r
    
    /** @param src input JSON
      * @return a PFA configuration that has passed syntax but not semantics checks
      */
    def apply(src: java.io.File): EngineConfig = parserToAst(jsonFactory.createJsonParser(src))
    /** @param src input JSON
      * @return a PFA configuration that has passed syntax but not semantics checks
      */
    def apply(src: java.io.InputStream): EngineConfig = parserToAst(jsonFactory.createJsonParser(src))
    /** @param src input JSON
      * @return a PFA configuration that has passed syntax but not semantics checks
      */
    def apply(src: String): EngineConfig = parserToAst(jsonFactory.createJsonParser(src))

    private def parserToAst(parser: JsonParser): EngineConfig = {
      val avroTypeBuilder = new AvroTypeBuilder
      val result =
        try {
          readEngineConfig(parser, parser.nextToken(), avroTypeBuilder)
        }
        finally {
          parser.close()
        }
      avroTypeBuilder.resolveTypes()
      result
    }

    /** Read a single expression from JSON, not a whole PFA document.
      * 
      * @param src input JSON
      * @return a PFA configuration that has passed syntax but not semantics checks
      */
    def expr(src: String): Expression = {
      val parser = jsonFactory.createJsonParser(src)
      val avroTypeBuilder = new AvroTypeBuilder
      val result =
        try {
          readExpression(parser, parser.nextToken(), "<expr>", "", avroTypeBuilder)
        }
        finally {
          parser.close()
        }
      avroTypeBuilder.resolveTypes()
      result
    }

    /** Read a list of expressions from JSON, not a whole PFA document.
      * 
      * @param src input JSON
      * @return a PFA configuration that has passed syntax but not semantics checks
      */
    def exprs(src: String): Seq[Expression] = {
      val parser = jsonFactory.createJsonParser(src)
      val avroTypeBuilder = new AvroTypeBuilder
      val result =
        try {
          readExpressionArray(parser, parser.nextToken(), "<exprs>", "", avroTypeBuilder)
        }
        finally {
          parser.close()
        }
      avroTypeBuilder.resolveTypes()
      result
    }

    /** Read a user-defined function from JSON, not a whole PFA document.
      * 
      * @param src input JSON
      * @return a PFA configuration that has passed syntax but not semantics checks
      */
    def fcn(src: String): FcnDef = {
      val parser = jsonFactory.createJsonParser(src)
      val avroTypeBuilder = new AvroTypeBuilder
      val result =
        try {
          readFcnDef(parser, parser.nextToken(), "<fcn>", "", avroTypeBuilder)
        }
        finally {
          parser.close()
        }
      avroTypeBuilder.resolveTypes()
      result
    }

    private val tokenMessage = Map(
      JsonToken.END_ARRAY -> """end of array ("]" character)""",
      JsonToken.END_OBJECT -> """end of object ("}" character)""",
      JsonToken.FIELD_NAME -> """field name""",
      JsonToken.NOT_AVAILABLE -> """nothing""",
      JsonToken.START_ARRAY -> """start of array ("[" character)""",
      JsonToken.START_OBJECT -> """start of object ("{" character)""",
      JsonToken.VALUE_EMBEDDED_OBJECT -> """embedded object""",
      JsonToken.VALUE_FALSE -> """false""",
      JsonToken.VALUE_NULL -> """null""",
      JsonToken.VALUE_NUMBER_FLOAT -> """floating-point number""",
      JsonToken.VALUE_NUMBER_INT -> """integer""",
      JsonToken.VALUE_STRING -> """string""",
      JsonToken.VALUE_TRUE -> """true"""
    )

    private def jsonAt(parser: JsonParser): String =
      "JSON line:col %d:%d".format(parser.getCurrentLocation.getLineNr, parser.getCurrentLocation.getColumnNr)

    private def readEngineConfig(parser: JsonParser, token: JsonToken, avroTypeBuilder: AvroTypeBuilder): EngineConfig = token match {
      case null => throw new PFASyntaxException("empty input", Some(pos("", "")))
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        val keys = mutable.Set[String]()
        var _name: String = null
        var _method: Method.Method = Method.MAP
        var _input: AvroPlaceholder = null
        var _output: AvroPlaceholder = null
        var _begin: Seq[Expression] = Nil
        var _action: Seq[Expression] = null
        var _end: Seq[Expression] = Nil
        var _fcns = Map[String, FcnDef]()
        var _zero: Option[String] = None
        var _merge: Option[Seq[Expression]] = None
        var _cells = Map[String, Cell]()
        var _pools = Map[String, Pool]()
        var _randseed: Option[Long] = None
        var _doc: Option[String] = None
        var _version: Option[Int] = None
        var _metadata = Map[String, String]()
        var _options = Map[String, JsonNode]()

        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), key, _at)
          else {
            keys.add(key)
            key match {
              case "name" =>      _name = readString(parser, parser.nextToken(), key, _at)
              case "method" => readString(parser, parser.nextToken(), key, _at) match {
                case "map" =>     _method = Method.MAP
                case "emit" =>    _method = Method.EMIT
                case "fold" =>    _method = Method.FOLD
                case x => throw new PFASyntaxException("expected one of \"map\", \"emit\", \"fold\", not \"%s\"".format(x), Some(pos(key, _at)))
              }
              case "input" =>     _input = readAvroPlaceholder(parser, parser.nextToken(), key, _at, avroTypeBuilder)
              case "output" =>    _output = readAvroPlaceholder(parser, parser.nextToken(), key, _at, avroTypeBuilder)
              case "begin" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _begin = readExpressionArray(parser, x, key, _at, avroTypeBuilder)
                case x => _begin = List(readExpression(parser, x, key, _at, avroTypeBuilder))
              }
              case "action" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _action = readExpressionArray(parser, x, key, _at, avroTypeBuilder)
                case x => _action = List(readExpression(parser, x, key, _at, avroTypeBuilder))
              }
              case "end" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _end = readExpressionArray(parser, x, key, _at, avroTypeBuilder)
                case x => _end = List(readExpression(parser, x, key, _at, avroTypeBuilder))
              }
              case "fcns" =>      _fcns = readFcnDefMap(parser, parser.nextToken(), key, _at, avroTypeBuilder)
              case "zero" =>      _zero = Some(readJsonToString(parser, parser.nextToken(), key, _at))
              case "merge" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _merge = Some(readExpressionArray(parser, x, key, _at, avroTypeBuilder))
                case x => _merge = Some(List(readExpression(parser, x, key, _at, avroTypeBuilder)))
              }
              case "cells" =>     _cells = readCells(parser, parser.nextToken(), key, _at, avroTypeBuilder)
              case "pools" =>     _pools = readPools(parser, parser.nextToken(), key, _at, avroTypeBuilder)
              case "randseed" =>  _randseed = Some(readLong(parser, parser.nextToken(), key, _at))
              case "doc" =>       _doc = Some(readString(parser, parser.nextToken(), key, _at))
              case "version" =>   _version = Some(readInt(parser, parser.nextToken(), key, _at))
              case "metadata" =>  _metadata = readStringMap(parser, parser.nextToken(), key, _at)
              case "options" =>   _options = readJsonNodeMap(parser, parser.nextToken(), key, _at)
              case x => throw new PFASyntaxException("unexpected top-level field: %s".format(x), Some(pos("", _at)))
            }
          }
          subtoken = parser.nextToken()
        }

        if (_name == null)
          _name = uniqueEngineName()

        val required = Set("action", "input", "output")
        if ((keys intersect required) != required)
          throw new PFASyntaxException("missing top-level fields: %s".format((required diff keys).mkString(", ")), Some(pos("", _at)))
        else
          EngineConfig(_name, _method, _input, _output, _begin, _action, _end, _fcns, _zero, _merge, _cells, _pools, _randseed, _doc, _version, _metadata, _options, Some(pos("", _at)))
      }
      case x => throw new PFASyntaxException("PFA engine must be a JSON object, not %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos("", jsonAt(parser))))
    }

    private[hadrian] def ingestJsonAndIgnore(parser: JsonParser, token: JsonToken, dot: String): Unit = token match {
      case JsonToken.START_ARRAY =>
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_ARRAY) {
          ingestJsonAndIgnore(parser, subtoken, dot)
          subtoken = parser.nextToken()
        }
      case JsonToken.START_OBJECT =>
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          ingestJsonAndIgnore(parser, parser.nextToken(), dot)
          subtoken = parser.nextToken()
        }
      case _ =>
    }

    private def ingestJsonAsDom(parser: JsonParser, token: JsonToken, dot: String, strings: mutable.Map[String, JsonString], ints: mutable.Map[Long, JsonLong]): JsonDom = token match {
      case JsonToken.VALUE_NULL => JsonNull
      case JsonToken.VALUE_TRUE => JsonTrue
      case JsonToken.VALUE_FALSE => JsonFalse
      case JsonToken.VALUE_NUMBER_INT =>
        val x = parser.getLongValue
        if (Math.abs(x) <= 100)
          ints.get(x) match {
            case Some(y) => y
            case None =>
              val y = JsonLong(x)
              ints(x) = y
              y
          }
        else
          JsonLong(x)
      case JsonToken.VALUE_NUMBER_FLOAT =>
        JsonDouble(parser.getDoubleValue)
      case JsonToken.VALUE_STRING =>
        val x = parser.getText
        strings.get(x) match {
          case Some(y) => y
          case None =>
            val y = JsonString(x)
            strings(x) = y
            y
        }
      case JsonToken.START_ARRAY =>
        val builder = Vector.newBuilder[JsonDom]
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_ARRAY) {
          builder += ingestJsonAsDom(parser, subtoken, dot, strings, ints)
          subtoken = parser.nextToken()
        }
        JsonArray(builder.result)
      case JsonToken.START_OBJECT =>
        val builder = Map.newBuilder[JsonString, JsonDom]
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          if (parser.getCurrentName == "@")
            ingestJsonAndIgnore(parser, parser.nextToken(), dot)
          else {
            val key = parser.getCurrentName
            val jsonKey = strings.get(key) match {
              case Some(y) => y
              case None =>
                val y = JsonString(key)
                strings(key) = y
                y
            }

            builder += (jsonKey -> ingestJsonAsDom(parser, parser.nextToken(), dot, strings, ints))
          }
          subtoken = parser.nextToken()
        }
        JsonObject(builder.result)
      case x => throw new PFASyntaxException("expected object, array, null, boolean, number, or string, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(dot))
    }

    private def readJsonToDom(parser: JsonParser, token: JsonToken, dot: String, at: String): JsonDom =
      ingestJsonAsDom(parser, token, dot, mutable.Map[String, JsonString](), mutable.Map[Long, JsonLong]())

    private def ingestJsonAsBytes(parser: JsonParser, token: JsonToken, dot: String, baos: ByteArrayOutputStream): Unit = token match {
      case JsonToken.VALUE_NULL => baos.write("null".getBytes, 0, 4)
      case JsonToken.VALUE_TRUE => baos.write("true".getBytes, 0, 4)
      case JsonToken.VALUE_FALSE => baos.write("false".getBytes, 0, 5)
      case JsonToken.VALUE_NUMBER_INT =>
        val x = parser.getText.getBytes
        baos.write(x, 0, x.size)
      case JsonToken.VALUE_NUMBER_FLOAT => 
        val x = parser.getText.getBytes
        baos.write(x, 0, x.size)
      case JsonToken.VALUE_STRING => 
        val x = ("\"" + escapeJson(parser.getText) + "\"").getBytes
        baos.write(x, 0, x.size)
      case JsonToken.START_ARRAY => {
        baos.write("[".getBytes, 0, 1)
        var first = true
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_ARRAY) {
          if (first)
            first = false
          else
            baos.write(",".getBytes, 0, 1)
          ingestJsonAsBytes(parser, subtoken, dot, baos)
          subtoken = parser.nextToken()
        }
        baos.write("]".getBytes, 0, 1)
      }
      case JsonToken.START_OBJECT => {
        baos.write("{".getBytes, 0, 1)
        var first = true
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          if (parser.getCurrentName == "@")
            ingestJsonAsBytes(parser, parser.nextToken(), dot, new ByteArrayOutputStream)   // skip it
          else {
            if (first)
              first = false
            else
              baos.write(",".getBytes, 0, 1)
            val key = ("\"" + escapeJson(parser.getCurrentName) + "\":").getBytes
            baos.write(key, 0, key.size)
            ingestJsonAsBytes(parser, parser.nextToken(), dot, baos)
          }
          subtoken = parser.nextToken()
        }
        baos.write("}".getBytes, 0, 1)
      }
      case _ =>
    }

    private def readJsonToBytes(parser: JsonParser, token: JsonToken, dot: String, at: String): Array[Byte] = {
      val baos = new ByteArrayOutputStream
      ingestJsonAsBytes(parser, token, dot, baos)
      baos.toByteArray
    }

    private def readJsonToString(parser: JsonParser, token: JsonToken, dot: String, at: String): String =
      new String(readJsonToBytes(parser, token, dot, at))

    private def readJsonNode(parser: JsonParser, token: JsonToken, dot: String, at: String): JsonNode =
      jsonFactory.createJsonParser(readJsonToString(parser, token, dot, at)).readValueAsTree

    private def readAvroPlaceholder(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): AvroPlaceholder =
      avroTypeBuilder.makePlaceholder(readJsonToString(parser, token, dot, at))

    private def readJsonNodeMap(parser: JsonParser, token: JsonToken, dot: String, at: String): Map[String, JsonNode] = token match {
      case JsonToken.START_OBJECT => {
        var items = List[(String, JsonNode)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key != "@")
            items = (key, readJsonNode(parser, parser.nextToken(), dot + " -> " + key, at)) :: items
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of JSON objects, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readJsonToBytesMap(parser: JsonParser, token: JsonToken, dot: String, at: String): Map[String, Array[Byte]] = token match {
      case JsonToken.START_OBJECT => {
        var items = List[(String, Array[Byte])]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key != "@")
            items = (key, readJsonToBytes(parser, parser.nextToken(), dot + " -> " + key, at)) :: items
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of JSON objects, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readJsonToDomMap(parser: JsonParser, token: JsonToken, dot: String, at: String): Map[String, JsonDom] = token match {
      case JsonToken.START_OBJECT => {
        var items = List[(String, JsonDom)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key != "@")
            items = (key, readJsonToDom(parser, parser.nextToken(), dot + " -> " + key, at)) :: items
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of JSON objects, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readBoolean(parser: JsonParser, token: JsonToken, dot: String, at: String): Boolean = token match {
      case JsonToken.VALUE_TRUE => true
      case JsonToken.VALUE_FALSE => false
      case x => throw new PFASyntaxException("expected boolean, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readInt(parser: JsonParser, token: JsonToken, dot: String, at: String): Int = token match {
      case JsonToken.VALUE_NUMBER_INT => parser.getIntValue
      case x => throw new PFASyntaxException("expected whole number (32-bit precision), found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readLong(parser: JsonParser, token: JsonToken, dot: String, at: String): Long = token match {
      case JsonToken.VALUE_NUMBER_INT => parser.getLongValue
      case x => throw new PFASyntaxException("expected whole number, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readFloat(parser: JsonParser, token: JsonToken, dot: String, at: String): Float = token match {
      case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT => parser.getFloatValue
      case x => throw new PFASyntaxException("expected number (32-bit precision), found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readDouble(parser: JsonParser, token: JsonToken, dot: String, at: String): Double = token match {
      case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT => parser.getDoubleValue
      case x => throw new PFASyntaxException("expected number, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readStringExpressionPairs(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Seq[(String, Expression)] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[(String, Expression)]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          val pair = readExpressionMap(parser, subtoken, dot + " -> %d".format(counter), at, avroTypeBuilder, false)
          val pairNoAt = pair - "@"
          if (pairNoAt.size != 1)
            throw new PFASyntaxException("expected array of {string: expression} pairs, found map of length %d".format(pairNoAt.size), Some(pos(dot, at)))
          items = (pairNoAt.keys.head, pairNoAt.values.head) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of {string: expression} pairs, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readStringPairs(parser: JsonParser, token: JsonToken, dot: String, at: String): Seq[(String, String)] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[(String, String)]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          val pair = try {
            readStringMap(parser, subtoken, dot + " -> %d".format(counter), at)
          }
          catch {
            case err: PFASyntaxException =>
              throw new PFASyntaxException("expected array of {string: string} pairs, found non-string map", err.pos)
          }
          val pairNoAt = pair - "@"
          if (pairNoAt.size != 1)
            throw new PFASyntaxException("expected array of {string: string} pairs, found map of length %d".format(pairNoAt.size), Some(pos(dot, at)))
          items = (pairNoAt.keys.head, pairNoAt.values.head) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of {string: string} pairs, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readStringArray(parser: JsonParser, token: JsonToken, dot: String, at: String): Seq[String] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[String]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          items = readString(parser, subtoken, dot + " -> %d".format(counter), at) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of strings, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readStringOrIntArray(parser: JsonParser, token: JsonToken, dot: String, at: String): Seq[Either[String, Int]] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[Either[String, Int]]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          items = readStringOrInt(parser, subtoken, dot + " -> %d".format(counter), at) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of strings or integers, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readStringMap(parser: JsonParser, token: JsonToken, dot: String, at: String): Map[String, String] = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, String)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else
            items = (key, readString(parser, parser.nextToken(), dot + " -> " + key, _at)) :: items
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of strings, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readString(parser: JsonParser, token: JsonToken, dot: String, at: String): String = token match {
      case JsonToken.VALUE_STRING => parser.getText
      case x => throw new PFASyntaxException("expected string, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readStringOrInt(parser: JsonParser, token: JsonToken, dot: String, at: String): Either[String, Int] = token match {
      case JsonToken.VALUE_STRING => Left(parser.getText)
      case JsonToken.VALUE_NUMBER_INT => Right(parser.getIntValue)
      case x => throw new PFASyntaxException("expected string or integer, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readBase64(parser: JsonParser, token: JsonToken, dot: String, at: String): Array[Byte] = token match {
      case JsonToken.VALUE_STRING => try {
        parser.getBinaryValue
      }
      catch {
        case err: JsonParseException => throw new PFASyntaxException("expected base64 data, found \"%s\"".format(parser.getText), Some(pos(dot, at)))
      }
      case x => throw new PFASyntaxException("expected base64 data, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readExpressionArray(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Seq[Expression] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[Expression]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          items = readExpression(parser, subtoken, dot + " -> %d".format(counter), at, avroTypeBuilder) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of expressions, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readExpressionMap(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder, keysMustBeSymbols: Boolean): Map[String, Expression] = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, Expression)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else if (!keysMustBeSymbols  ||  validSymbolName(key))
            items = (key, readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)) :: items
          else
            throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(key), Some(pos(dot, at)))
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of expressions, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readCastCaseArray(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Seq[CastCase] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[CastCase]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          items = readCastCase(parser, subtoken, dot + " -> %d".format(counter), at, avroTypeBuilder) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of cast-cases, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readCastCase(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): CastCase = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        val keys = mutable.Set[String]()
        var _as: AvroPlaceholder = null
        var _named: String = null
        var _body: Seq[Expression] = null

        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else {
            keys.add(key)
            key match {
              case "as" =>    _as = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "named" => _named = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "do" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _body = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _body = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }
              case x => throw new PFASyntaxException("unexpected field in cast-case: %s".format(x), Some(pos(dot, _at)))
            }
          }
          subtoken = parser.nextToken()
        }

        if (_named != null  &&  !validSymbolName(_named))
          throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(_named), Some(pos(dot, at)))

        val required = Set("as", "named", "do")
        if (keys != required)
          throw new PFASyntaxException("wrong set of fields for a cast-case: %s".format(keys.mkString(", ")), Some(pos(dot, _at)))
        else
          CastCase(_as, _named, _body, Some(pos(dot, _at)))
      }
      case x => throw new PFASyntaxException("expected cast-case, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readExpression(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Expression =
      readArgument(parser, token, dot, at, avroTypeBuilder) match {
        case expr: Expression => expr
        case arg: Argument => throw new PFASyntaxException("argument appears outside of argument list", Some(pos(dot, at)))
      }

    private def readArgumentArray(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Seq[Argument] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[Argument]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          items = readArgument(parser, subtoken, dot + " -> %d".format(counter), at, avroTypeBuilder) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of arguments, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readArgumentMap(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Map[String, Argument] = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, Argument)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else
            items = (key, readArgument(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)) :: items
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of arguments, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readArgument(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Argument = token match {
      case JsonToken.VALUE_NULL => LiteralNull(Some(pos(dot, at)))
      case JsonToken.VALUE_TRUE => LiteralBoolean(true, Some(pos(dot, at)))
      case JsonToken.VALUE_FALSE => LiteralBoolean(false, Some(pos(dot, at)))
      case JsonToken.VALUE_NUMBER_INT => {
        val value = parser.getBigIntegerValue
        if (value.compareTo(java.math.BigInteger.valueOf(java.lang.Integer.MIN_VALUE)) >= 0  &&  value.compareTo(java.math.BigInteger.valueOf(java.lang.Integer.MAX_VALUE)) <= 0)
          LiteralInt(value.intValue, Some(pos(dot, at)))
        else if (value.compareTo(java.math.BigInteger.valueOf(java.lang.Long.MIN_VALUE)) >= 0  &&  value.compareTo(java.math.BigInteger.valueOf(java.lang.Long.MAX_VALUE)) <= 0)
          LiteralLong(parser.getLongValue, Some(pos(dot, at)))
        else
          throw new PFASyntaxException("integer out of range: " + value.toString, Some(pos(dot, at)))
      }
      case JsonToken.VALUE_NUMBER_FLOAT => LiteralDouble(parser.getDoubleValue, Some(pos(dot, at)))
      case JsonToken.VALUE_STRING => parser.getText.split("""\.""").toList match {
        case Nil =>
          throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(parser.getText), Some(pos(dot, at)))

        case item :: Nil =>
          if (validSymbolName(item))
            Ref(item, Some(pos(dot, at)))
          else
            throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(item), Some(pos(dot, at)))

        case base :: path =>
          if (validSymbolName(base))
            AttrGet(Ref(base), path.map(x => x match {
              case NumberPattern() => LiteralInt(x.toInt, Some(pos(dot, at)))
              case _ => LiteralString(x, Some(pos(dot, at)))
            }), Some(pos(dot, at)))
          else
            throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(base), Some(pos(dot, at)))
      }

      case JsonToken.START_ARRAY => {
        val result: String =
          if (parser.nextToken() == JsonToken.VALUE_STRING) {
            val str = parser.getText
            if (parser.nextToken() == JsonToken.END_ARRAY)
              str
            else
              null
          }
          else
            null
        if (result == null)
          throw new PFASyntaxException("expecting expression, which may be [\"string\"], but no other array can be used as an expression", Some(pos(dot, at)))
        LiteralString(result, Some(pos(dot, at)))
      }

      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        val keys = mutable.Set[String]()

        var _int: Int = 0
        var _long: Long = 0L
        var _float: Float = 0.0F
        var _double: Double = 0.0
        var _string: String = null
        var _bytes: Array[Byte] = null
        var _avroType: AvroPlaceholder = null
        var _value: String = null

        var _let: Map[String, Expression] = null
        var _set: Map[String, Expression] = null
        var _forlet: Map[String, Expression] = null
        var _forstep: Map[String, Expression] = null
        var _ifnotnull: Map[String, Expression] = null

        var _body: Seq[Expression] = null
        var _thenClause: Seq[Expression] = null
        var _elseClause: Seq[Expression] = null
        var _log: Seq[Expression] = null
        var _path: Seq[Expression] = Nil   // important: must be Nil instead of null because some forms have an empty list as default
        var _callwithargs: Seq[Expression] = null
        var _trycatch: Seq[Expression] = null

        var _attr: Expression = null
        var _ifPredicate: Expression = null
        var _whilePredicate: Expression = null
        var _until: Expression = null
        var _unpack: Expression = null
        var _del: Expression = null

        var _cond: Seq[Expression] = null
        var _cases: Seq[CastCase] = null

        var _foreach: String = null
        var _forkey: String = null
        var _forval: String = null
        var _fcnref: String = null
        var _cell: String = null
        var _pool: String = null

        var _in: Expression = null
        var _cast: Expression = null
        var _upcast: Expression = null
        var _init: Expression = null
        var _callwith: Expression = null

        var _seq: Boolean = true
        var _partial: Boolean = false

        var _doc: String = null
        var _error: String = null
        var _code: Int = 0
        var _namespace: String = null

        var _newObject: Map[String, Expression] = null
        var _newArray: Seq[Expression] = null
        
        var _params: Seq[(String, AvroPlaceholder)] = null
        var _ret: AvroPlaceholder = null
        var _as: AvroPlaceholder = null

        var _callName: String = null
        var _callArgs: Seq[Argument] = null
        var _to: Argument = null
        var _fill: Map[String, Argument] = null
        var _filter = Seq[Either[String, Int]]()
        var _format: Seq[(String, String)] = null
        var _pack: Seq[(String, Expression)] = null

        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else {
            keys.add(key)
            key match {
              case "int" =>       _int = readInt(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "long" =>      _long = readLong(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "float" =>     _float = readFloat(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "double" =>    _double = readDouble(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "string" =>    _string = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "base64" =>    _bytes = readBase64(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "type" =>      _avroType = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "value" =>     _value = readJsonToString(parser, parser.nextToken(), dot + " -> " + key, _at)

              case "let" =>       _let = readExpressionMap(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder, true)
              case "set" =>       _set = readExpressionMap(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder, true)
              case "for" =>       _forlet = readExpressionMap(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder, true)
              case "step" =>      _forstep = readExpressionMap(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder, true)
              case "ifnotnull" => _ifnotnull = readExpressionMap(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder, true)

              case "do" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _body = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _body = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }
              case "then" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _thenClause = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _thenClause = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }
              case "else" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _elseClause = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _elseClause = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }
              case "log" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _log = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _log = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }
              case "path" =>      _path = readExpressionArray(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "args" =>      _callwithargs = readExpressionArray(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "try" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _trycatch = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _trycatch = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }

              case "attr" =>      _attr = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "if" =>        _ifPredicate = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "while" =>     _whilePredicate = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "until" =>     _until = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "unpack" =>    _unpack = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "del" =>       _del = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case "cond" => {
                _cond = readExpressionArray(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
                if (!_cond.forall({case If(_, _, None, _) => true; case _ => false}))
                  throw new PFASyntaxException("cond expression must only contain else-less if expressions", Some(pos(dot, _at)))
              }

              case "cases" =>     _cases = readCastCaseArray(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case "foreach" =>   _foreach = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "forkey" =>    _forkey = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "forval" =>    _forval = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "fcn" =>       _fcnref = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "cell" =>      _cell = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "pool" =>      _pool = readString(parser, parser.nextToken(), dot + " -> " + key, _at)

              case "in" =>        _in = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "cast" =>      _cast = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "upcast" =>    _upcast = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "init" =>      _init = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "call" =>      _callwith = readExpression(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case "seq" =>       _seq = readBoolean(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "partial" =>   _partial = readBoolean(parser, parser.nextToken(), dot + " -> " + key, _at)

              case "doc" =>       _doc = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "error" =>     _error = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "code" =>      _code = readInt(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "namespace" => _namespace = readString(parser, parser.nextToken(), dot + " -> " + key, _at)

              case "new" => parser.nextToken() match {
                case x @ JsonToken.START_OBJECT => _newObject = readExpressionMap(parser, x, dot + " -> " + key, _at, avroTypeBuilder, false);  _newArray = null
                case x @ JsonToken.START_ARRAY => _newArray = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder);  _newObject = null
                case x => throw new PFASyntaxException("\"new\" must be an object (map, record) or an array, not " + x.toString, Some(pos(dot, at)))
              }

              case "params" =>    _params = readParams(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "ret" =>       _ret = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "as" =>        _as = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case "to" =>        _to = readArgument(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case "fill" =>      _fill = readArgumentMap(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case "filter" =>    _filter = readStringOrIntArray(parser, parser.nextToken(), dot + " -> " + key, _at)

              case "format" =>    _format = readStringPairs(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "pack" =>      _pack = readStringExpressionPairs(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)

              case x => {
                _callName = x
                parser.nextToken() match {
                  case x @ JsonToken.START_ARRAY => _callArgs = readArgumentArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                  case x => _callArgs = List(readArgument(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
                }
              }
            }
          }
          subtoken = parser.nextToken()
        }

        if (keys.contains("foreach")  &&  !validSymbolName(_foreach))
          throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(_foreach), Some(pos(dot, at)))
        if (keys.contains("forkey")  &&  !validSymbolName(_forkey))
          throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(_forkey), Some(pos(dot, at)))
        if (keys.contains("forval")  &&  !validSymbolName(_forval))
          throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(_forval), Some(pos(dot, at)))
        if (keys.contains("fcn")  &&  !validFunctionName(_fcnref))
          throw new PFASyntaxException("\"%s\" is not a valid function name".format(_fcnref), Some(pos(dot, at)))

        if (keys == Set("int"))                                      LiteralInt(_int, Some(pos(dot, _at)))
        else if (keys == Set("long"))                                LiteralLong(_long, Some(pos(dot, _at)))
        else if (keys == Set("float"))                               LiteralFloat(_float, Some(pos(dot, _at)))
        else if (keys == Set("double"))                              LiteralDouble(_double, Some(pos(dot, _at)))
        else if (keys == Set("string"))                              LiteralString(_string, Some(pos(dot, _at)))
        else if (keys == Set("base64"))                              LiteralBase64(_bytes, Some(pos(dot, _at)))
        else if (keys == Set("type", "value"))                       Literal(_avroType, _value, Some(pos(dot, _at)))

        else if (keys == Set("new", "type")  &&  _newObject != null) NewObject(_newObject, _avroType, Some(pos(dot, _at)))
        else if (keys == Set("new", "type")  &&  _newArray != null)  NewArray(_newArray, _avroType, Some(pos(dot, _at)))

        else if (keys == Set("do"))                                  Do(_body, Some(pos(dot, _at)))
        else if (keys == Set("let"))                                 Let(_let, Some(pos(dot, _at)))
        else if (keys == Set("set"))                                 SetVar(_set, Some(pos(dot, _at)))

        else if (keys == Set("attr", "path"))                        AttrGet(_attr, _path, Some(pos(dot, _at)))
        else if (keys == Set("attr", "path", "to"))                  AttrTo(_attr, _path, _to, Some(pos(dot, _at)))
        else if (keys == Set("cell")  ||
                 keys == Set("cell", "path"))                        CellGet(_cell, _path, Some(pos(dot, _at)))
        else if (keys == Set("cell", "to")  ||
                 keys == Set("cell", "path", "to"))                  CellTo(_cell, _path, _to, Some(pos(dot, _at)))
        else if (keys == Set("pool", "path"))                        PoolGet(_pool, _path, Some(pos(dot, _at)))
        else if (keys == Set("pool", "path", "to", "init"))          PoolTo(_pool, _path, _to, _init, Some(pos(dot, _at)))
        else if (keys == Set("pool", "del"))                         PoolDel(_pool, _del, Some(pos(dot, _at)))

        else if (keys == Set("if", "then"))                          If(_ifPredicate, _thenClause, None, Some(pos(dot, _at)))
        else if (keys == Set("if", "then", "else"))                  If(_ifPredicate, _thenClause, Some(_elseClause), Some(pos(dot, _at)))
        else if (keys == Set("cond"))                                Cond(_cond.map(_.asInstanceOf[If]), None, Some(pos(dot, _at)))
        else if (keys == Set("cond", "else"))                        Cond(_cond.map(_.asInstanceOf[If]), Some(_elseClause), Some(pos(dot, _at)))

        else if (keys == Set("while", "do"))                         While(_whilePredicate, _body, Some(pos(dot, _at)))
        else if (keys == Set("do", "until"))                         DoUntil(_body, _until, Some(pos(dot, _at)))
        else if (keys == Set("for", "while", "step", "do"))          For(_forlet, _whilePredicate, _forstep, _body, Some(pos(dot, _at)))
        else if (keys == Set("foreach", "in", "do")  ||
                 keys == Set("foreach", "in", "do", "seq"))          Foreach(_foreach, _in, _body, _seq, Some(pos(dot, _at)))
        else if (keys == Set("forkey", "forval", "in", "do"))        Forkeyval(_forkey, _forval, _in, _body, Some(pos(dot, _at)))

        else if (keys == Set("cast", "cases")  ||
                 keys == Set("cast", "cases", "partial"))            CastBlock(_cast, _cases, _partial, Some(pos(dot, _at)))
        else if (keys == Set("upcast", "as"))                        Upcast(_upcast, _as, Some(pos(dot, _at)))
        else if (keys == Set("ifnotnull", "then"))                   IfNotNull(_ifnotnull, _thenClause, None, Some(pos(dot, _at)))
        else if (keys == Set("ifnotnull", "then", "else"))           IfNotNull(_ifnotnull, _thenClause, Some(_elseClause), Some(pos(dot, _at)))

        else if (keys == Set("pack"))                                Pack(_pack, Some(pos(dot, _at)))
        else if (keys == Set("unpack", "format", "then"))            Unpack(_unpack, _format, _thenClause, None, Some(pos(dot, _at)))
        else if (keys == Set("unpack", "format", "then", "else"))    Unpack(_unpack, _format, _thenClause, Some(_elseClause), Some(pos(dot, _at)))

        else if (keys == Set("doc"))                                 Doc(_doc, Some(pos(dot, _at)))

        else if (keys == Set("error"))                               Error(_error, None, Some(pos(dot, _at)))
        else if (keys == Set("error", "code"))                       Error(_error, Some(_code), Some(pos(dot, _at)))
        else if (keys == Set("try"))                                 Try(_trycatch, None, Some(pos(dot, _at)))
        else if (keys == Set("try", "filter"))                       Try(_trycatch, Some(_filter), Some(pos(dot, _at)))
        else if (keys == Set("log"))                                 Log(_log, None, Some(pos(dot, _at)))
        else if (keys == Set("log", "namespace"))                    Log(_log, Some(_namespace), Some(pos(dot, _at)))

        // FcnDef and FcnRef can only be arguments, not expressions
        else if (keys == Set("params", "ret", "do"))                 FcnDef(_params, _ret, _body, Some(pos(dot, _at)))
        else if (keys == Set("fcn"))                                 FcnRef(_fcnref, Some(pos(dot, _at)))
        else if (keys == Set("fcn", "fill"))                         FcnRefFill(_fcnref, _fill, Some(pos(dot, _at)))

        else if (keys == Set("call", "args"))                        CallUserFcn(_callwith, _callwithargs, Some(pos(dot, _at)))

        // function call is anything else
        else if (keys.size == 1  &&
                 !Set("args", "as", "attr", "base64", "call", "cases", "cast", "cell", "code", "cond", "del", "do", "doc", "double", "else",
                      "error", "fcn", "fill", "filter", "float", "for", "foreach", "forkey", "format", "forval", "if", "ifnotnull", "in", "init",
                      "int", "let", "log", "long", "namespace", "new", "pack", "params", "partial", "path", "pool", "ret", "seq",
                      "set", "step", "string", "then", "to", "try", "type", "unpack", "until", "upcast", "value", "while").contains(keys.head))
                                                                     Call(_callName, _callArgs, Some(pos(dot, _at)))
        else throw new PFASyntaxException("unrecognized special form: %s (not enough arguments? too many?)".format(keys.mkString(" ")), Some(pos(dot, _at)))
      }
      case x => throw new PFASyntaxException("expected expression, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readFcnDefMap(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Map[String, FcnDef] = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, FcnDef)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else if (validFunctionName(key))
            items = (key, readFcnDef(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)) :: items
          else
            throw new PFASyntaxException("\"%s\" is not a valid function name".format(key), Some(pos(dot, at)))
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of function definitions, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readFcnDef(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): FcnDef = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        val keys = mutable.Set[String]()
        var _params: Seq[(String, AvroPlaceholder)] = null
        var _ret: AvroPlaceholder = null
        var _body: Seq[Expression] = null

        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else {
            keys.add(key)
            key match {
              case "params" => _params = readParams(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "ret" =>    _ret = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "do" => parser.nextToken() match {
                case x @ JsonToken.START_ARRAY => _body = readExpressionArray(parser, x, dot + " -> " + key, _at, avroTypeBuilder)
                case x => _body = List(readExpression(parser, x, dot + " -> " + key, _at, avroTypeBuilder))
              }
              case x => throw new PFASyntaxException("unexpected field in function definition: %s".format(x), Some(pos(dot, _at)))
            }
          }
          subtoken = parser.nextToken()
        }

        val required = Set("params", "ret", "do")
        if (keys != required)
          throw new PFASyntaxException("wrong set of fields for a function definition: %s".format(keys.mkString(", ")), Some(pos(dot, _at)))
        else
          FcnDef(_params, _ret, _body, Some(pos(dot, _at)))
      }
      case x => throw new PFASyntaxException("expected function definition, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readParams(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Seq[(String, AvroPlaceholder)] = token match {
      case JsonToken.START_ARRAY => {
        var items = List[(String, AvroPlaceholder)]()
        var subtoken = parser.nextToken()
        var counter = 0
        while (subtoken != JsonToken.END_ARRAY) {
          items = readParam(parser, subtoken, dot + " -> %d".format(counter), at, avroTypeBuilder) :: items
          subtoken = parser.nextToken()
        }
        items.reverse
      }
      case x => throw new PFASyntaxException("expected array of function parameters, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readParam(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): (String, AvroPlaceholder) = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, AvroPlaceholder)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else if (validSymbolName(key))
            items = (key, readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)) :: items
          else
            throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(key), Some(pos(dot, at)))
          subtoken = parser.nextToken()
        }

        if (items.size == 1)
          items(0)
        else
          throw new PFASyntaxException("function parameter name-type map should have only one pair", Some(pos(dot, _at)))
      }
      case x => throw new PFASyntaxException("expected function parameter name-type singleton map, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readCells(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Map[String, Cell] = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, Cell)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else if (validSymbolName(key))
            items = (key, readCell(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)) :: items
          else
            throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(key), Some(pos(dot, at)))
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of cells, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readURL(value: String) =
      if (value.matches("""[a-zA-Z][a-zA-Z0-9\+\-\.]*://.*"""))
        new java.net.URL(value)
      else
        new java.io.File(value).toURI.toURL

    private def readCell(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Cell = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        val keys = mutable.Set[String]()
        var _avroType: AvroPlaceholder = null
        var _initDom: JsonDom = null
        var _shared: Boolean = false
        var _rollback: Boolean = false
        var _source: String = "embedded"

        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else {
            keys.add(key)
            key match {
              case "type" => _avroType = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "init" => _initDom = readJsonToDom(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "shared" => _shared = readBoolean(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "rollback" => _rollback = readBoolean(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "source" => _source = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case x => throw new PFASyntaxException("unexpected cell property: %s".format(x), Some(pos(dot, _at)))
            }
          }
          subtoken = parser.nextToken()
        }
        
        if (!keys.contains("type")  ||  !keys.contains("init")  ||  !keys.subsetOf(Set("type", "init", "shared", "rollback", "source")))
          throw new PFASyntaxException("wrong set of fields for cell: %s".format(keys.mkString(", ")), Some(pos(dot, _at)))
        else
          _source match {
            case "embedded" => Cell(_avroType, EmbeddedJsonDomCellSource(_initDom, _avroType), _shared, _rollback, CellPoolSource.EMBEDDED, Some(pos(dot, _at)))
            case "json" => _initDom match {
              case JsonString(value) =>
                try {
                  Cell(_avroType, ExternalJsonCellSource(readURL(value), _avroType), _shared, _rollback, CellPoolSource.JSON, Some(pos(dot, _at)))
                }
                catch {
                  case _: java.net.MalformedURLException => throw new PFASyntaxException("init should be a URL for source: json, not \"%s\"".format(value), Some(pos(dot, _at)))
                }
              case x => throw new PFASyntaxException("source: json requires init to be a string", Some(pos(dot, _at)))
            }
            case "avro" => _initDom match {
              case JsonString(value) =>
                try {
                  Cell(_avroType, ExternalAvroCellSource(readURL(value), _avroType), _shared, _rollback, CellPoolSource.AVRO, Some(pos(dot, _at)))
                }
                catch {
                  case _: java.net.MalformedURLException => throw new PFASyntaxException("init should be a URL for source: avro, not \"%s\"".format(value), Some(pos(dot, _at)))
                }
              case x => throw new PFASyntaxException("source: avro requires init to be a string", Some(pos(dot, _at)))
            }
            case x => throw new PFASyntaxException("unrecognized source type \"%s\"".format(x), Some(pos(dot, _at)))
          }
      }
      case x => throw new PFASyntaxException("expected cell, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readPools(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Map[String, Pool] = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        var items = List[(String, Pool)]()
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else if (validSymbolName(key))
            items = (key, readPool(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)) :: items
          else
            throw new PFASyntaxException("\"%s\" is not a valid symbol name".format(key), Some(pos(dot, at)))
          subtoken = parser.nextToken()
        }
        items.toMap
      }
      case x => throw new PFASyntaxException("expected map of pools, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }

    private def readPool(parser: JsonParser, token: JsonToken, dot: String, at: String, avroTypeBuilder: AvroTypeBuilder): Pool = token match {
      case JsonToken.START_OBJECT => {
        var _at = jsonAt(parser)

        val keys = mutable.Set[String]()
        var _avroType: AvroPlaceholder = null
        var _initDom: JsonDom = null
        var _shared: Boolean = false
        var _rollback: Boolean = false
        var _source: String = "embedded"

        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          val key = parser.getCurrentName
          if (key == "@")
            _at = readString(parser, parser.nextToken(), dot + " -> " + key, at)
          else {
            keys.add(key)
            key match {
              case "type" => _avroType = readAvroPlaceholder(parser, parser.nextToken(), dot + " -> " + key, _at, avroTypeBuilder)
              case "init" => _initDom = readJsonToDom(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "shared" => _shared = readBoolean(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "rollback" => _rollback = readBoolean(parser, parser.nextToken(), dot + " -> " + key, _at)
              case "source" => _source = readString(parser, parser.nextToken(), dot + " -> " + key, _at)
              case x => throw new PFASyntaxException("unexpected pool property: %s".format(x), Some(pos(dot, _at)))
            }
          }
          subtoken = parser.nextToken()
        }
        
        if (!keys.contains("type")  ||  !keys.subsetOf(Set("type", "init", "shared", "rollback", "source")))
          throw new PFASyntaxException("wrong set of fields for pool: %s".format(keys.mkString(", ")), Some(pos(dot, _at)))
        else
          _source match {
            case "embedded" => _initDom match {
              case null =>
                Pool(_avroType, EmbeddedJsonDomPoolSource(Map(), _avroType), _shared, _rollback, CellPoolSource.EMBEDDED, Some(pos(dot, _at)))
              case JsonObject(entries) =>
                Pool(_avroType, EmbeddedJsonDomPoolSource(entries map {case (JsonString(k), v) => (k, v)}, _avroType), _shared, _rollback, CellPoolSource.EMBEDDED, Some(pos(dot, _at)))
              case _ =>
                throw new PFASyntaxException("source: embedded for pool requires init to be a JSON object", Some(pos(dot, _at)))
            }
            case "json" => _initDom match {
              case JsonString(value) =>
                try {
                  Pool(_avroType, ExternalJsonPoolSource(readURL(value), _avroType), _shared, _rollback, CellPoolSource.JSON, Some(pos(dot, _at)))
                }
                catch {
                  case _: java.net.MalformedURLException => throw new PFASyntaxException("init should be a URL for source: json, not \"%s\"".format(value), Some(pos(dot, _at)))
                }
              case x => throw new PFASyntaxException("source: json requires init to be a string", Some(pos(dot, _at)))
            }
            case "avro" => _initDom match {
              case JsonString(value) =>
                try {
                  Pool(_avroType, ExternalAvroPoolSource(readURL(value), _avroType), _shared, _rollback, CellPoolSource.AVRO, Some(pos(dot, _at)))
                }
                catch {
                  case _: java.net.MalformedURLException => throw new PFASyntaxException("init should be a URL for source: avro, not \"%s\"".format(value), Some(pos(dot, _at)))
                }
              case x => throw new PFASyntaxException("source: avro requires init to be a string", Some(pos(dot, _at)))
            }
            case x => throw new PFASyntaxException("unrecognized source type \"%s\"".format(x), Some(pos(dot, _at)))
          }
      }
      case x => throw new PFASyntaxException("expected pool, found %s".format(tokenMessage.getOrElse(x, x.toString)), Some(pos(dot, at)))
    }
  }

  /** Storage for uninterpreted JSON, such as cell or pool data whose types have not been established yet.
    */
  trait JsonDom {
  /** Convert to a Jackson JSON node.
    */
    def jsonNode: JsonNode
  /** Convert to a JSON string.
    */
    def json: String = convertToJson(jsonNode)
  }

  /** Represents a JSON object (i.e. map, dict, associative array...).
    */
  case class JsonObject(entries: Map[JsonString, JsonDom]) extends JsonDom {
    def jsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      entries foreach {case (k, v) => out.put(k.value, v.jsonNode)}
      out
    }
  }

  /** Represents a JSON array.
    */
  case class JsonArray(entries: Seq[JsonDom]) extends JsonDom {
    def jsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.arrayNode
      entries foreach {x => out.add(x.jsonNode)}
      out
    }
  }

  /** Represents JSON `null`.
    */
  case object JsonNull extends JsonDom {
    val jsonNode = JsonNodeFactory.instance.nullNode
  }

  /** Represents JSON `true`.
    */
  case object JsonTrue extends JsonDom {
    val jsonNode = JsonNodeFactory.instance.booleanNode(true)
  }

  /** Represents JSON `false`.
    */
  case object JsonFalse extends JsonDom {
    val jsonNode = JsonNodeFactory.instance.booleanNode(false)
  }

  /** Represents a JSON string.
    */
  case class JsonString(value: String) extends JsonDom {
    def jsonNode = JsonNodeFactory.instance.textNode(value)
  }

  /** Represents a JSON whole number.
    */
  case class JsonLong(value: Long) extends JsonDom {
    def jsonNode = JsonNodeFactory.instance.numberNode(value)
    private val internal: java.lang.Integer =
      if (Math.abs(value) <= 100)
        java.lang.Integer.valueOf(value.toInt)
      else
        null
  /** Convert to a `java.lang.Integer` object.
    */
    def asJavaInteger: java.lang.Integer =
      if (internal != null)
        internal
      else
        java.lang.Integer.valueOf(value.toInt)
  }

  /** Represents a JSON floating point number.
    */
  case class JsonDouble(value: Double) extends JsonDom {
    def jsonNode = JsonNodeFactory.instance.numberNode(value)
  }
}
