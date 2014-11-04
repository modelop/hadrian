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

package com.opendatagroup.hadrian

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

import org.codehaus.jackson.JsonNode
import org.codehaus.janino.JavaSourceClassLoader
import org.codehaus.janino.util.resource.Resource
import org.codehaus.janino.util.resource.ResourceFinder

import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.lang3.StringEscapeUtils

import com.opendatagroup.hadrian.ast.SymbolTable
import com.opendatagroup.hadrian.ast.FunctionTable
import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.TaskResult
import com.opendatagroup.hadrian.ast.Task
import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.ast.UserFcn
import com.opendatagroup.hadrian.ast.Ast
import com.opendatagroup.hadrian.ast.Method
import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.ast.Cell
import com.opendatagroup.hadrian.ast.Pool
import com.opendatagroup.hadrian.ast.Argument
import com.opendatagroup.hadrian.ast.Expression
import com.opendatagroup.hadrian.ast.LiteralValue
import com.opendatagroup.hadrian.ast.PathIndex
import com.opendatagroup.hadrian.ast.ArrayIndex
import com.opendatagroup.hadrian.ast.MapIndex
import com.opendatagroup.hadrian.ast.RecordIndex
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
import com.opendatagroup.hadrian.ast.Doc
import com.opendatagroup.hadrian.ast.Error
import com.opendatagroup.hadrian.ast.Try
import com.opendatagroup.hadrian.ast.Log

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.ExceptionType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroCompiled
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
import com.opendatagroup.hadrian.datatype.ForwardDeclarationParser

import com.opendatagroup.hadrian.data.PFADatumReader
import com.opendatagroup.hadrian.data.PFADatumWriter
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFASpecificData
import com.opendatagroup.hadrian.errors.PFAInitializationException
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFASemanticException
import com.opendatagroup.hadrian.errors.PFATimeoutException
import com.opendatagroup.hadrian.errors.PFAUserException
import com.opendatagroup.hadrian.options.EngineOptions
import com.opendatagroup.hadrian.reader.jsonToAst
import com.opendatagroup.hadrian.shared.SharedMap
import com.opendatagroup.hadrian.shared.SharedMapInMemory
import com.opendatagroup.hadrian.shared.SharedState
import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.util.escapeJson
import com.opendatagroup.hadrian.yaml.yamlToJson

package object jvmcompiler {
  //////////////////////////////////////////////////////////// Janino-based compiler tools

  def lineNumbers(src: String): String =
    src.split("\n").zipWithIndex map { case (line, i) => "%3d:  %s".format(i + 1, line) } mkString("\n")

  object JVMNameMangle {
    def s(symbolName: String): String = "s_" + symbolName
    def par(parameterName: String): String = "par_" + parameterName
    def c(cellName: String): String = "c_" + cellName
    def p(poolName: String): String = "p_" + poolName
    def f(functionName: String): String = "f_" + functionName.replace(".", "$")

    // Type names must be equal to the fully-qualified name from the Schema or else
    // Avro does bad things (SpecificData.createSchema, comment mentions "hack").
    def t(namespace: Option[String], name: String, qualified: Boolean): String =
      t(namespace match {case None => null.asInstanceOf[String]; case Some(x) => x}, name, qualified)

    def t(namespace: String, name: String, qualified: Boolean): String =
      if (qualified)
        (namespace match {case null | "" => ""; case x => x + "."}) + name
      else
        name
  }

  def javaType(avroType: AvroType, boxed: Boolean, qualified: Boolean, generic: Boolean): String = avroType match {
    case _: AvroNull => "Void"
    case _: AvroBoolean => if (boxed) "Boolean" else "boolean"
    case _: AvroInt => if (boxed) "Integer" else "int"
    case _: AvroLong => if (boxed) "Long" else "long"
    case _: AvroFloat => if (boxed) "Float" else "float"
    case _: AvroDouble => if (boxed) "Double" else "double"
    case _: AvroBytes => "byte[]"
    case _: AvroString => "String"
    case x: AvroArray => "com.opendatagroup.hadrian.data.PFAArray<%s>".format(javaType(x.items, true, qualified, generic))
    case x: AvroMap => "com.opendatagroup.hadrian.data.PFAMap<%s>".format(javaType(x.values, true, qualified, generic))
    case _: AvroFixed if (generic) => "com.opendatagroup.hadrian.data.PFAFixed"
    case _: AvroEnum if (generic) => "com.opendatagroup.hadrian.data.PFAEnumSymbol"
    case _: AvroRecord if (generic) => "com.opendatagroup.hadrian.data.PFARecord"
    case AvroFixed(_, name, namespace, _, _) => JVMNameMangle.t(namespace, name, qualified)
    case AvroEnum(_, name, namespace, _, _) => JVMNameMangle.t(namespace, name, qualified)
    case AvroRecord(_, name, namespace, _, _) => JVMNameMangle.t(namespace, name, qualified)
    case _: AvroUnion => "Object"
  }

  def javaSchema(avroType: AvroType, construct: Boolean): String = avroType match {
    case _: AvroNull => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)"
    case _: AvroBoolean => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN)"
    case _: AvroInt => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)"
    case _: AvroLong => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)"
    case _: AvroFloat => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT)"
    case _: AvroDouble => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE)"
    case _: AvroBytes => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES)"
    case _: AvroString => "org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)"
    case x: AvroArray => "org.apache.avro.Schema.createArray(%s)".format(javaSchema(x.items, construct))
    case x: AvroMap => "org.apache.avro.Schema.createMap(%s)".format(javaSchema(x.values, construct))
    case _: AvroFixed | _: AvroEnum | _: AvroRecord if (construct) =>
      "new org.apache.avro.Schema.Parser().parse(\"%s\")".format(StringEscapeUtils.escapeJava(avroType.schema.toString))
    case AvroFixed(_, name, namespace, _, _) => JVMNameMangle.t(namespace, name, true) + ".getClassSchema()"
    case AvroEnum(_, name, namespace, _, _) => JVMNameMangle.t(namespace, name, true) + ".getClassSchema()"
    case AvroRecord(_, name, namespace, _, _) => JVMNameMangle.t(namespace, name, true) + ".getClassSchema()"
    case AvroUnion(types) => "org.apache.avro.Schema.createUnion(java.util.Arrays.asList(%s))".format(types.map(javaSchema(_, construct)).mkString(","))
  }
}

package jvmcompiler {
  //////////////////////////////////////////////////////////// interfaces to be used on the Java side
  object W {
    def s[X](x: X): Unit = {}

    def n[X](x: X): java.lang.Void = null
    def nn[X, Y](x: X, y: Y): java.lang.Void = null

    def do2[X, Y](x: X, y: Y): Y = y

    def either[X](value: Either[Exception, X]): X = value match {
      case Left(err) => throw new PFARuntimeException(err.getMessage, err)
      case Right(x) => x
    }

    def getOrElse(map: java.util.Map[String, AnyRef], get: String, orElse: AnyRef): AnyRef = {
      if (map.containsKey(get))
        map.get(get)
      else
        orElse
    }
    def getOrFailPool(map: java.util.Map[String, AnyRef], get: String, name: String): AnyRef = {
      if (map.containsKey(get))
        map.get(get)
      else
        throw new PFARuntimeException("\"%s\" not found in pool \"%s\"".format(get, name))
    }

    def asBool(x: Boolean): Boolean = x
    def asBool(x: java.lang.Boolean): Boolean = x.booleanValue
    def asBool(x: AnyRef): Boolean = x.asInstanceOf[java.lang.Boolean].booleanValue

    def asJBool(x: Boolean): java.lang.Boolean = java.lang.Boolean.valueOf(x)
    def asJBool(x: java.lang.Boolean): java.lang.Boolean = x
    def asJBool(x: AnyRef): java.lang.Boolean = x.asInstanceOf[java.lang.Boolean]

    def asInt(x: Int): Int = x
    def asInt(x: Long): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range: " + x.toString)
      else
        x.toInt
    def asInt(x: java.lang.Integer): Int = x.intValue
    def asInt(x: java.lang.Long): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range: " + x.toString)
      else
        x.intValue
    def asInt(x: AnyRef): Int = x match {
      case x1: java.lang.Integer => x1.intValue
      case x2: java.lang.Long => asInt(x2)
    }

    def asJInt(x: Int): java.lang.Integer = java.lang.Integer.valueOf(x)
    def asJInt(x: Long): java.lang.Integer =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range: " + x.toString)
      else
        java.lang.Integer.valueOf(x.toInt)
    def asJInt(x: java.lang.Integer): java.lang.Integer = x
    def asJInt(x: java.lang.Long): java.lang.Integer =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range: " + x.toString)
      else
        java.lang.Integer.valueOf(x.intValue)
    def asJInt(x: AnyRef): java.lang.Integer = x match {
      case x1: java.lang.Integer => x1
      case x2: java.lang.Long => asJInt(x2)
    }

    def asLong(x: Int): Long = x.toLong
    def asLong(x: Long): Long = x
    def asLong(x: java.lang.Integer): Long = x.longValue
    def asLong(x: java.lang.Long): Long = x.longValue
    def asLong(x: AnyRef): Long = x match {
      case x1: java.lang.Integer => x1.longValue
      case x2: java.lang.Long => x2.longValue
    }

    def asJLong(x: Int): java.lang.Long = java.lang.Long.valueOf(x)
    def asJLong(x: Long): java.lang.Long = java.lang.Long.valueOf(x)
    def asJLong(x: java.lang.Integer): java.lang.Long = java.lang.Long.valueOf(x.intValue)
    def asJLong(x: java.lang.Long): java.lang.Long = x
    def asJLong(x: AnyRef): java.lang.Long = x match {
      case x1: java.lang.Integer => java.lang.Long.valueOf(x1.longValue)
      case x2: java.lang.Long => x2
    }

    def asFloat(x: Int): Float = x.toFloat
    def asFloat(x: Long): Float = x.toFloat
    def asFloat(x: Float): Float = x
    def asFloat(x: java.lang.Integer): Float = x.floatValue
    def asFloat(x: java.lang.Long): Float = x.floatValue
    def asFloat(x: java.lang.Float): Float = x.floatValue
    def asFloat(x: AnyRef): Float = x match {
      case x1: java.lang.Integer => x1.floatValue
      case x2: java.lang.Long => x2.floatValue
      case x3: java.lang.Float => x3.floatValue
    }

    def asJFloat(x: Int): java.lang.Float = java.lang.Float.valueOf(x)
    def asJFloat(x: Long): java.lang.Float = java.lang.Float.valueOf(x)
    def asJFloat(x: Float): java.lang.Float = java.lang.Float.valueOf(x)
    def asJFloat(x: java.lang.Integer): java.lang.Float = java.lang.Float.valueOf(x.intValue)
    def asJFloat(x: java.lang.Long): java.lang.Float = java.lang.Float.valueOf(x.longValue)
    def asJFloat(x: java.lang.Float): java.lang.Float = x
    def asJFloat(x: AnyRef): java.lang.Float = x match {
      case x1: java.lang.Integer => java.lang.Float.valueOf(x1.floatValue)
      case x2: java.lang.Long => java.lang.Float.valueOf(x2.floatValue)
      case x3: java.lang.Float => x3
    }

    def asDouble(x: Int): Double = x.toDouble
    def asDouble(x: Long): Double = x.toDouble
    def asDouble(x: Float): Double = x.toDouble
    def asDouble(x: Double): Double = x
    def asDouble(x: java.lang.Integer): Double = x.doubleValue
    def asDouble(x: java.lang.Long): Double = x.doubleValue
    def asDouble(x: java.lang.Float): Double = x.doubleValue
    def asDouble(x: java.lang.Double): Double = x.doubleValue
    def asDouble(x: AnyRef): Double = x match {
      case x1: java.lang.Integer => x1.doubleValue
      case x2: java.lang.Long => x2.doubleValue
      case x3: java.lang.Float => x3.doubleValue
      case x4: java.lang.Double => x4.doubleValue
    }

    def asJDouble(x: Int): java.lang.Double = java.lang.Double.valueOf(x)
    def asJDouble(x: Long): java.lang.Double = java.lang.Double.valueOf(x)
    def asJDouble(x: Float): java.lang.Double = java.lang.Double.valueOf(x)
    def asJDouble(x: Double): java.lang.Double = java.lang.Double.valueOf(x)
    def asJDouble(x: java.lang.Integer): java.lang.Double = java.lang.Double.valueOf(x.intValue)
    def asJDouble(x: java.lang.Long): java.lang.Double = java.lang.Double.valueOf(x.longValue)
    def asJDouble(x: java.lang.Float): java.lang.Double = java.lang.Double.valueOf(x.floatValue)
    def asJDouble(x: java.lang.Double): java.lang.Double = x
    def asJDouble(x: AnyRef): java.lang.Double = x match {
      case x1: java.lang.Integer => java.lang.Double.valueOf(x1.doubleValue)
      case x2: java.lang.Long => java.lang.Double.valueOf(x2.doubleValue)
      case x3: java.lang.Float => java.lang.Double.valueOf(x3.doubleValue)
      case x4: java.lang.Double => x4
    }

    def wrapExpr(x: String, avroType: AvroType, boxed: Boolean): String = (avroType, boxed) match {
      case (_: AvroBoolean, false) => "com.opendatagroup.hadrian.jvmcompiler.W.asBool(" + x + ")"
      case (_: AvroInt, false) => "com.opendatagroup.hadrian.jvmcompiler.W.asInt(" + x + ")"
      case (_: AvroLong, false) => "com.opendatagroup.hadrian.jvmcompiler.W.asLong(" + x + ")"
      case (_: AvroFloat, false) => "com.opendatagroup.hadrian.jvmcompiler.W.asFloat(" + x + ")"
      case (_: AvroDouble, false) => "com.opendatagroup.hadrian.jvmcompiler.W.asDouble(" + x + ")"
      case (_: AvroBoolean, true) => "com.opendatagroup.hadrian.jvmcompiler.W.asJBool(" + x + ")"
      case (_: AvroInt, true) => "com.opendatagroup.hadrian.jvmcompiler.W.asJInt(" + x + ")"
      case (_: AvroLong, true) => "com.opendatagroup.hadrian.jvmcompiler.W.asJLong(" + x + ")"
      case (_: AvroFloat, true) => "com.opendatagroup.hadrian.jvmcompiler.W.asJFloat(" + x + ")"
      case (_: AvroDouble, true) => "com.opendatagroup.hadrian.jvmcompiler.W.asJDouble(" + x + ")"
      case _ => x
    }

    def trycatch[X <: AnyRef](f: () => X, hasFilter: Boolean, filters: Array[String]): X = {
      try {
        f()
      }
      catch {
        case _: PFARuntimeException | _: PFAUserException if (!hasFilter) => null.asInstanceOf[X]
        case err: PFARuntimeException if (hasFilter  &&  filters.contains(err.message)) => null.asInstanceOf[X]
        case err: PFAUserException if (hasFilter  &&  filters.contains(err.message)) => null.asInstanceOf[X]
      }
    }
  }

  abstract class PFAEngineBase {
    private val specificData = new PFASpecificData(getClass.getClassLoader)

    def checkClock(): Unit

    var log: Function2[String, Option[String], Unit] =
      (str, namespace) => namespace match {
        case Some(x) => println(x + ": " + str)
        case None => println(str)
      }

    private var _config: EngineConfig = null
    def config: EngineConfig = _config
    private var _options: EngineOptions = null
    def options: EngineOptions = _options
    private var _callGraph: Map[String, Set[String]] = null
    def callGraph: Map[String, Set[String]] = _callGraph
    private var _typeParser: ForwardDeclarationParser = null
    def typeParser: ForwardDeclarationParser = _typeParser

    var _metadata: PFAMap[String] = null
    var _actionsStarted: Long = 0L
    var _actionsFinished: Long = 0L
    def actionsStarted: Long = _actionsStarted
    def actionsFinished: Long = _actionsFinished

    def calledBy(fcnName: String, exclude: Set[String] = Set[String]()): Set[String] =
      if (exclude.contains(fcnName))
        Set[String]()
      else
        callGraph.get(fcnName) match {
          case None => Set[String]()
          case Some(functions) => {
            val newExclude = exclude + fcnName
            val nextLevel = functions flatMap {f => calledBy(f, newExclude)}
            functions ++ nextLevel
          }
        }
    def callDepth(fcnName: String, exclude: Set[String] = Set[String](), startingDepth: Double = 0): Double =
      if (exclude.contains(fcnName))
        java.lang.Double.POSITIVE_INFINITY
      else
        callGraph.get(fcnName) match {
          case None => startingDepth
          case Some(functions) => {
            val newExclude = exclude + fcnName
            var deepest = startingDepth
            for (f <- functions) {
              val fdepth = callDepth(f, newExclude, startingDepth + 1)
              if (fdepth > deepest)
                deepest = fdepth
            }
            deepest
          }
        }
    def isRecursive(fcnName: String): Boolean = calledBy(fcnName).contains(fcnName)
    def hasRecursive(fcnName: String): Boolean = callDepth(fcnName) == java.lang.Double.POSITIVE_INFINITY
    def hasSideEffects(fcnName: String): Boolean = {
      val reach = calledBy(fcnName)
      reach.contains(CellTo.desc)  ||  reach.contains(PoolTo.desc)
    }

    private var _namedTypes: Map[String, AvroType] = null
    def namedTypes: Map[String, AvroType] = _namedTypes
    private var _inputType: AvroType = null
    def inputType: AvroType = _inputType
    private var _outputType: AvroType = null
    def outputType: AvroType = _outputType

    private var _randomGenerator: Random = null
    def randomGenerator = _randomGenerator

    val cellsToRollback = mutable.Map[String, java.lang.reflect.Field]()
    val poolsToRollback = mutable.Map[String, java.lang.reflect.Field]()
    val savedCells = mutable.Map[String, Any]()
    val savedPools = mutable.Map[String, java.util.HashMap[String, AnyRef]]()
    private val privateCells = mutable.Map[String, java.lang.reflect.Field]()
    private var publicCells: SharedMap = null
    private val privatePools = mutable.Map[String, java.util.HashMap[String, AnyRef]]()
    private val publicPools = mutable.Map[String, SharedMap]()

    def initialize(config: EngineConfig, options: EngineOptions, sharedState: Option[SharedState], thisClass: java.lang.Class[_], context: EngineConfig.Context): Unit = {
      _config = config
      _options = options
      _callGraph =
        (context.fcns map {case (fname, fctx) => (fname, fctx.calls)}) ++
          Map("(begin)" -> context.begin._3) ++
          Map("(action)" -> context.action._3) ++
          Map("(end)" -> context.end._3)

      _typeParser = context.parser

      _metadata = PFAMap.fromMap(config.metadata)

      // make sure that functions used in CellTo and PoolTo do not themselves call CellTo or PoolTo (which could lead to deadlock)
      config collect {
        case CellTo(_, _, FcnRef(x, _), _) =>
          if (hasSideEffects(x))
            throw new PFAInitializationException("cell-to references function \"%s\", which has side-effects".format(x))

        case CellTo(_, _, FcnRefFill(x, _, _), _) =>
          if (hasSideEffects(x))
            throw new PFAInitializationException("cell-to references function \"%s\", which has side-effects".format(x))

        case CellTo(_, _, FcnDef(_, _, body, _), _) =>
          body collect {
            case _: CellTo =>
              throw new PFAInitializationException("inline function in cell-to invokes another cell-to")
            case _: PoolTo =>
              throw new PFAInitializationException("inline function in cell-to invokes pool-to")
            case Call(x, _, _) =>
              if (hasSideEffects(x))
                throw new PFAInitializationException("inline function in cell-to calls function \"%s\", which has side-effects".format(x))
          }

        case PoolTo(_, _, FcnRef(x, _), _, _) =>
          if (hasSideEffects(x))
            throw new PFAInitializationException("cell-to references function \"%s\", which has side-effects".format(x))

        case PoolTo(_, _, FcnRefFill(x, _, _), _, _) =>
          if (hasSideEffects(x))
            throw new PFAInitializationException("cell-to references function \"%s\", which has side-effects".format(x))

        case PoolTo(_, _, FcnDef(_, _, body, _), _, _) =>
          body collect {
            case _: CellTo =>
              throw new PFAInitializationException("inline function in pool-to invokes another cell-to")
            case _: PoolTo =>
              throw new PFAInitializationException("inline function in pool-to invokes pool-to")
            case Call(x, _, _) =>
              if (hasSideEffects(x))
                throw new PFAInitializationException("inline function in pool-to calls function \"%s\", which has side-effects".format(x))
          }
      }

      // fill the unshared cells
      for ((cname, cell) <- config.cells if (!cell.shared)) {
        val field = thisClass.getDeclaredField(JVMNameMangle.c(cname))
        field.setAccessible(true)

        val value = fromJson(cell.init, cell.avroType.schema)
        cell.avroType match {
          case _: AvroBoolean => field.setBoolean(this, value.asInstanceOf[java.lang.Boolean].booleanValue)
          case _: AvroInt => field.setInt(this, value.asInstanceOf[java.lang.Integer].intValue)
          case _: AvroLong => field.setLong(this, value.asInstanceOf[java.lang.Long].longValue)
          case _: AvroFloat => field.setFloat(this, value.asInstanceOf[java.lang.Float].floatValue)
          case _: AvroDouble => field.setDouble(this, value.asInstanceOf[java.lang.Double].doubleValue)
          case _ => field.set(this, value)
        }

        if (cell.rollback)
          cellsToRollback(cname) = field
        privateCells(cname) = field
      }

      // fill the shared cells
      val sharedCells = sharedState match {
        case Some(x) => x.cells
        case None => new SharedMapInMemory
      }
      thisClass.getDeclaredField("sharedCells").set(this, sharedCells)
      sharedCells.initialize(config.cells filter {_._2.shared} map {_._1} toSet,
        (name: String) => {
          val cell = config.cells(name)
          fromJson(cell.init, cell.avroType.schema)
        })
      publicCells = sharedCells

      // fill the unshared pools
      for ((pname, pool) <- config.pools if (!pool.shared)) {
        val field = thisClass.getDeclaredField(JVMNameMangle.p(pname))
        field.setAccessible(true)

        val schema = pool.avroType.schema
        val hashMap = new java.util.HashMap[String, AnyRef]()
        field.set(this, hashMap)
        for ((key, valueString) <- pool.init)
          hashMap.put(key, fromJson(valueString, schema))

        if (pool.rollback)
          poolsToRollback(pname) = field
        privatePools(pname) = hashMap
      }

      // fill the shared pools
      for ((pname, pool) <- config.pools if (pool.shared)) {
        val schema = pool.avroType.schema

        val state = sharedState match {
          case Some(x) => x.pools.get(pname) match {
            case Some(y) => y
            case None => throw new PFAInitializationException("shared state object provided by host has no pool named \"%s\"".format(pname))
          }
          case None => new SharedMapInMemory
        }

        thisClass.getDeclaredField(JVMNameMangle.p(pname)).set(this, state)
        state.initialize(pool.init.keys.toSet,
          (name: String) => fromJson(pool.init(name), schema))
        publicPools(pname) = state
      }

      _inputType = context.input
      _outputType = context.output
      _namedTypes = context.compiledTypes map {x => (x.fullName, x)} toMap

      // ...

      config.randseed match {
        case Some(x) => _randomGenerator = new Random(x)
        case None => _randomGenerator = new Random()
      }
    }

    def rollbackSave() {
      savedCells.clear()
      savedPools.clear()

      // save the cells
      for ((cname, field) <- cellsToRollback) {
        config.cells(cname).avroType match {
          case _: AvroBoolean => savedCells(cname) = field.getBoolean(this)
          case _: AvroInt => savedCells(cname) = field.getInt(this)
          case _: AvroLong => savedCells(cname) = field.getLong(this)
          case _: AvroFloat => savedCells(cname) = field.getFloat(this)
          case _: AvroDouble => savedCells(cname) = field.getDouble(this)
          case _ => savedCells(cname) = field.get(this)
        }
      }

      // save the pools
      for ((pname, field) <- poolsToRollback)
        savedPools(pname) = field.get(this).asInstanceOf[java.util.HashMap[String, AnyRef]].clone().asInstanceOf[java.util.HashMap[String, AnyRef]]
    }

    def rollback() {
      // roll back the cells
      for ((cname, field) <- cellsToRollback) {
        config.cells(cname).avroType match {
          case _: AvroBoolean => field.setBoolean(this, savedCells(cname).asInstanceOf[Boolean])
          case _: AvroInt => field.setInt(this, savedCells(cname).asInstanceOf[Int])
          case _: AvroLong => field.setLong(this, savedCells(cname).asInstanceOf[Long])
          case _: AvroFloat => field.setFloat(this, savedCells(cname).asInstanceOf[Float])
          case _: AvroDouble => field.setDouble(this, savedCells(cname).asInstanceOf[Double])
          case _ => field.set(this, savedCells(cname))
        }
      }

      // roll back the pools
      for ((pname, field) <- poolsToRollback)
        field.set(this, savedPools(pname))
    }

    def getRunlock: AnyRef

    private def cellState(cell: String): Any = cellState(cell, config.cells(cell).shared)
    private def cellState(cell: String, shared: Boolean): Any =
      if (shared)
        publicCells.get(cell, Array[com.opendatagroup.hadrian.shared.PathIndex]()) match {
          case Left(err) => throw err
          case Right(x) => x
        }
      else
        privateCells(cell).get(this)

    private def poolState(pool: String): Map[String, Any] = poolState(pool, config.pools(pool).shared)
    private def poolState(pool: String, shared: Boolean): Map[String, Any] =
      if (shared)
        publicPools(pool).toMap
      else {
        privatePools(pool).entrySet.iterator map {entry => (entry.getKey, entry.getValue)} toMap
      }

    private def toBoxed(x: Any): AnyRef = x match {
      case y: Boolean => java.lang.Boolean.valueOf(y)
      case y: Int => java.lang.Integer.valueOf(y)
      case y: Long => java.lang.Long.valueOf(y)
      case y: Float => java.lang.Float.valueOf(y)
      case y: Double => java.lang.Double.valueOf(y)
      case y: AnyRef => y
    }

    def snapshot(): EngineConfig = getRunlock.synchronized {
      val newCells = config.cells map {
        case (cname, Cell(avroPlaceholder, _, shared, rollback, _)) =>
          (cname, Cell(avroPlaceholder, toJson(toBoxed(cellState(cname, shared)), avroPlaceholder.avroType.schema), shared, rollback))
      }

      val newPools = config.pools map {
        case (pname, Pool(avroPlaceholder, _, shared, rollback, _)) =>
          (pname, Pool(avroPlaceholder, poolState(pname, shared) map {case (k, v) => (k, toJson(toBoxed(v), avroPlaceholder.avroType.schema))}, shared, rollback))
      }

      EngineConfig(
        config.name,
        config.method,
        config.inputPlaceholder,
        config.outputPlaceholder,
        config.begin,
        config.action,
        config.end,
        config.fcns,
        config.zero,
        newCells,
        newPools,
        config.randseed,
        config.doc,
        config.version,
        config.metadata,
        config.options,
        config.pos)
    }

    def fromJsonHashMap(json: String, schema: Schema): AnyRef = {
      val reader = new PFADatumReader[AnyRef](specificData, true)
      reader.setSchema(schema)
      val decoder = DecoderFactory.get.jsonDecoder(schema, json)
      reader.read(null, decoder)
    }

    def fromJson(json: String, schema: Schema): AnyRef = {
      val reader = new PFADatumReader[AnyRef](specificData)
      reader.setSchema(schema)
      val decoder = DecoderFactory.get.jsonDecoder(schema, json)
      reader.read(null, decoder)
    }

    def fromAvro(avro: Array[Byte], schema: Schema): AnyRef = {
      val reader = new PFADatumReader[AnyRef](specificData)
      reader.setSchema(schema)
      val decoder = DecoderFactory.get.validatingDecoder(schema, DecoderFactory.get.binaryDecoder(avro, null))
      reader.read(null, decoder)
    }

    def toJson(obj: AnyRef, schema: Schema): String = {
      val out = new java.io.ByteArrayOutputStream
      val encoder = EncoderFactory.get.jsonEncoder(schema, out)
      val writer = new PFADatumWriter[AnyRef](schema, specificData)
      writer.write(obj, encoder)
      encoder.flush()
      out.toString
    }

    def toAvro(obj: AnyRef, schema: Schema): Array[Byte] = {
      val out = new java.io.ByteArrayOutputStream
      val encoder = EncoderFactory.get.validatingEncoder(schema, EncoderFactory.get.binaryEncoder(out, null))
      val writer = new PFADatumWriter[AnyRef](schema, specificData)
      writer.write(obj, encoder)
      encoder.flush()
      out.toByteArray
    }

    def fromJson(json: String, avroType: AvroType): AnyRef      = fromJson(json, avroType.schema)
    def fromAvro(avro: Array[Byte], avroType: AvroType): AnyRef = fromAvro(avro, avroType.schema)
    def toJson(obj: AnyRef, avroType: AvroType): String         = toJson(obj, avroType.schema)
    def toAvro(obj: AnyRef, avroType: AvroType): Array[Byte]    = toAvro(obj, avroType.schema)

    def avroInputIterator[X](inputStream: InputStream): DataFileStream[X] = {    // DataFileStream is a java.util.Iterator
      val reader = new PFADatumReader[X](specificData)
      reader.setSchema(inputType.schema)
      new DataFileStream[X](inputStream, reader)
    }

    def jsonInputIterator[X](inputStream: InputStream): java.util.Iterator[X] = {
      val reader = new PFADatumReader[X](specificData)
      reader.setSchema(inputType.schema)
      val scanner = new java.util.Scanner(inputStream)

      new java.util.Iterator[X] {
        def hasNext(): Boolean = scanner.hasNextLine
        def next(): X = {
          val json = scanner.nextLine()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
        def remove(): Unit = throw new java.lang.UnsupportedOperationException
      }
    }

    def jsonInputIterator[X](inputIterator: java.util.Iterator[String]): java.util.Iterator[X] = {
      val reader = new PFADatumReader[X](specificData)
      reader.setSchema(inputType.schema)

      new java.util.Iterator[X] {
        def hasNext(): Boolean = inputIterator.hasNext
        def next(): X = {
          val json = inputIterator.next()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
        def remove(): Unit = throw new java.lang.UnsupportedOperationException
      }
    }

    def jsonInputIterator[X](inputIterator: scala.collection.Iterator[String]): scala.collection.Iterator[X] = {
      val reader = new PFADatumReader[X](specificData)
      reader.setSchema(inputType.schema)

      new scala.collection.Iterator[X] {
        override def hasNext: Boolean = inputIterator.hasNext
        override def next(): X = {
          val json = inputIterator.next()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
      }
    }

    def avroOutputDataFileWriter(fileName: String): DataFileWriter[AnyRef] = {
      val writer = new PFADatumWriter[AnyRef](outputType.schema, specificData)
      val out = new DataFileWriter[AnyRef](writer)
      out.create(outputType.schema, new java.io.File(fileName))
      out
    }

    def jsonOutput(obj: AnyRef): String = toJson(obj, outputType.schema)

  }

  trait PFAEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] {
    var log: Function2[String, Option[String], Unit]
    val config: EngineConfig
    val options: EngineOptions
    val callGraph: Map[String, Set[String]]
    def calledBy(fcnName: String, exclude: Set[String] = Set[String]()): Set[String]
    def callDepth(fcnName: String, exclude: Set[String] = Set[String](), startingDepth: Double = 0): Double
    def isRecursive(fcnName: String): Boolean
    def hasRecursive(fcnName: String): Boolean
    def hasSideEffects(fcnName: String): Boolean

    def typeParser: ForwardDeclarationParser

    val classLoader: java.lang.ClassLoader
    def begin(): Unit
    def action(input: INPUT): OUTPUT
    def end(): Unit

    def snapshot: EngineConfig

    val namedTypes: Map[String, AvroCompiled]
    val inputType: AvroType
    val outputType: AvroType
    def fromJson(json: String, avroType: AvroType): AnyRef
    def fromAvro(avro: Array[Byte], avroType: AvroType): AnyRef
    def toJson(obj: AnyRef, avroType: AvroType): String
    def toAvro(obj: AnyRef, avroType: AvroType): Array[Byte]
    def avroInputIterator[X](inputStream: InputStream): DataFileStream[X]    // DataFileStream is a java.util.Iterator
    def jsonInputIterator[X](inputStream: InputStream): java.util.Iterator[X]
    def jsonInputIterator[X](inputIterator: java.util.Iterator[String]): java.util.Iterator[X]
    def jsonInputIterator[X](inputIterator: scala.collection.Iterator[String]): scala.collection.Iterator[X]
    def avroOutputDataFileWriter(fileName: String): DataFileWriter[AnyRef]
    def jsonOutput(obj: AnyRef): String

    def randomGenerator: Random

    def fcn0(name: String): Function0[AnyRef]
    def fcn1(name: String): Function1[AnyRef, AnyRef]
    def fcn2(name: String): Function2[AnyRef, AnyRef, AnyRef]
    def fcn3(name: String): Function3[AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn4(name: String): Function4[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn5(name: String): Function5[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn6(name: String): Function6[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn7(name: String): Function7[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn8(name: String): Function8[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn9(name: String): Function9[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn10(name: String): Function10[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn11(name: String): Function11[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn12(name: String): Function12[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn13(name: String): Function13[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn14(name: String): Function14[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn15(name: String): Function15[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn16(name: String): Function16[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn17(name: String): Function17[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn18(name: String): Function18[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn19(name: String): Function19[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn20(name: String): Function20[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn21(name: String): Function21[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
    def fcn22(name: String): Function22[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
  }
  object PFAEngine {
    def fromAst(engineConfig: EngineConfig, options: Map[String, JsonNode] = Map[String, JsonNode](), sharedState: Option[SharedState] = None, multiplicity: Int = 1, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): Seq[PFAEngine[AnyRef, AnyRef]] = {
      val engineOptions = new EngineOptions(engineConfig.options, options)

      val (context: EngineConfig.Context, code) =
        engineConfig.walk(new JVMCompiler, SymbolTable.blank, FunctionTable.blank, engineOptions)

      val javaCode = code.asInstanceOf[JavaCode]
      val engineName = javaCode.name.get

      val fakeFiles =
        (javaCode.dependencies map {case (n, v) => (n.replace(".", "/") + ".java", v.toString)}) ++
          Map(engineName + ".java" -> code.toString)

      if (debug)
        for ((name, text) <- fakeFiles) {
          println("============================================================")
          println(name)
          println(lineNumbers(text))
        }

      val resourceFinder = new ResourceFinder {
        def findResource(resourceName: String) = fakeFiles.get(resourceName) match {
          case Some(x) => new Resource {
            def getFileName = resourceName
            def lastModified = 0L
            def open = new ByteArrayInputStream(x.getBytes)
          }
          case None => null
        }
      }

      val javaSourceClassLoader = new JavaSourceClassLoader(parentClassLoader match {
        case Some(cl) => cl
        case None => getClass.getClassLoader
      }, resourceFinder, null)

      val clazz = javaSourceClassLoader.loadClass(engineName)
      val constructor = clazz.getConstructor(classOf[EngineConfig], classOf[EngineOptions], classOf[Option[SharedState]], classOf[EngineConfig.Context])

      val sharedStateToUse = sharedState match {
        case None => Some(new SharedState(new SharedMapInMemory, engineConfig.pools.keys map {(_, new SharedMapInMemory)} toMap))
        case x => x
      }

      try {
        Vector.fill(multiplicity)(constructor.newInstance(engineConfig, engineOptions, sharedStateToUse, context).asInstanceOf[PFAEngine[AnyRef, AnyRef]])
      }
      catch {
        case err: java.lang.reflect.InvocationTargetException => throw err.getCause
      }
    }

    def fromJson(src: AnyRef, options: Map[String, JsonNode] = Map[String, JsonNode](), sharedState: Option[SharedState] = None, multiplicity: Int = 1, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): Seq[PFAEngine[AnyRef, AnyRef]] = src match {
      case x: String => fromAst(jsonToAst(x), options, sharedState, multiplicity, parentClassLoader, debug)
      case x: java.io.File => fromAst(jsonToAst(x), options, sharedState, multiplicity, parentClassLoader, debug)
      case x: java.io.InputStream => fromAst(jsonToAst(x), options, sharedState, multiplicity, parentClassLoader, debug)
      case x => throw new IllegalArgumentException("cannot read model from objects of type " + src.getClass.getName)
    }

    def fromYaml(src: String, options: Map[String, JsonNode] = Map[String, JsonNode](), sharedState: Option[SharedState] = None, multiplicity: Int = 1, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): Seq[PFAEngine[AnyRef, AnyRef]] =
      fromJson(yamlToJson(src), options, sharedState, multiplicity, parentClassLoader, debug)
  }

  trait PFAMapEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] extends PFAEngine[INPUT, OUTPUT]

  trait PFAEmitEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] extends PFAEngine[INPUT, OUTPUT] {
    var emit: Function1[OUTPUT, Unit]
  }

  trait PFAFoldEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] extends PFAEngine[INPUT, OUTPUT] {
    var tally: OUTPUT
  }

  //////////////////////////////////////////////////////////// transformation of individual forms

  case class JavaCode(format: String, contents: String*) extends TaskResult {
    var name: Option[String] = None
    var dependencies = Map[String, JavaCode]()
    var placeholder: Boolean = false
    def setName(n: String): JavaCode = {name = Some(n); this}
    def setDependencies(d: Map[String, JavaCode]): JavaCode = {dependencies = d; this}
    def setAsPlaceholder(): JavaCode = {placeholder = true; this}

    override def toString() =
      if (placeholder)
        throw new Exception("attempt to use JavaCode placeholder")
      else
        format.format(contents: _*)
  }

  class JVMCompiler extends Task {
    import JVMNameMangle._

    object ReturnMethod extends Enumeration {
      type ReturnMethod = Value
      val NONE, RETURN = Value
    }

    def block(exprs: Seq[TaskResult], returnMethod: ReturnMethod.ReturnMethod, retType: AvroType): String = returnMethod match {
      case ReturnMethod.NONE =>
        (exprs map {"com.opendatagroup.hadrian.jvmcompiler.W.s(" + _.toString + ");"}).mkString("\n")
      case ReturnMethod.RETURN =>
        ((exprs.init map {"com.opendatagroup.hadrian.jvmcompiler.W.s(" + _.toString + ");"}) ++ List("return %s;".format(retType match {
          case _: AvroBoolean | _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
            W.wrapExpr(exprs.last.toString, retType, true)
          case _ =>
            "(" + javaType(retType, true, true, true) + ")" + exprs.last.toString
        }))).mkString("\n")
    }

    def symbolFields(symbols: Map[String, AvroType]): String =
      symbols.map({case (n, t) => javaType(t, false, true, true) + " " + s(n) + ";"}).mkString("\n")

    def makePathIndex(path: Seq[PathIndex]): String = path.map(_ match {
      case ArrayIndex(expr, t) => """new com.opendatagroup.hadrian.shared.I(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))""".format(expr.toString)
      case MapIndex(expr, t) => """new com.opendatagroup.hadrian.shared.M(%s)""".format(expr.toString)
      case RecordIndex(name, t) => """new com.opendatagroup.hadrian.shared.R("%s")""".format(StringEscapeUtils.escapeJava(name))
    }).mkString(", ")

    def apply(astContext: AstContext, engineOptions: EngineOptions, resolvedType: Option[Type] = None): TaskResult = astContext match {
      case EngineConfig.Context(
        engineName: String,
        method: Method.Method,
        input: AvroType,
        output: AvroType,
        compiledTypes: Set[AvroCompiled],
        (begin: Seq[TaskResult],
          beginSymbols: Map[String, AvroType],
          beginCalls: Set[String]),
        (action: Seq[TaskResult],
          actionSymbols: Map[String, AvroType],
          actionCalls: Set[String]),
        (end: Seq[TaskResult],
          endSymbols: Map[String, AvroType],
          endCalls: Set[String]),
        fcn: Map[String, FcnDef.Context],
        zero: Option[String],
        cells: Map[String, Cell],
        pools: Map[String, Pool],
        randseed: Option[Long],
        doc: Option[String],
        version: Option[Int],
        metadata: Map[String, String],
        options: Map[String, JsonNode],
        parser: ForwardDeclarationParser) => {

        val thisClassName = "PFA_" + engineName

        val dependencyBuilder = Map.newBuilder[String, JavaCode]
        dependencyBuilder.sizeHint(compiledTypes.size)
        for (avroCompiled <- compiledTypes) avroCompiled match {
          case AvroRecord(fields, name, namespace, _, _) => {
            val interfaces = engineOptions.data_pfarecord_interface match {
              case Some(x) => " implements " + x
              case None => ""
            }

            dependencyBuilder += (t(namespace, name, true) -> JavaCode("""%s
public class %s extends com.opendatagroup.hadrian.data.PFARecord%s {
    public static final org.apache.avro.Schema SCHEMA$ = %s;
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
    public org.apache.avro.Schema getSchema() { return SCHEMA$; }

    private static final String[] _fieldNames = new String[]{%s};
    public int numFields() { return _fieldNames.length; }
    public String[] fieldNames() { return _fieldNames; }
    public int fieldIndex(String name) {
        for (int i = 0;  i < numFields();  i++) {
            if (name.equals(_fieldNames[i]))
                return i;
        }
        return -1;
    }

    private static final org.apache.avro.Schema[] _fieldTypes = new org.apache.avro.Schema[]{%s};
    public org.apache.avro.Schema[] fieldTypes() { return _fieldTypes; }

    public %s() { }

%s

    public Object get(int field$) {
switch (field$) {
%s
default: throw new org.apache.avro.AvroRuntimeException("Bad index");
}
    }

    public void put(int field$, Object value$) {
switch (field$) {
%s
default: throw new org.apache.avro.AvroRuntimeException("Bad index");
}
    }

    public Object get(String field$) {
%s
else throw new org.apache.avro.AvroRuntimeException("Bad index");
}

    public void put(String field$, Object value$) {
%s
else throw new org.apache.avro.AvroRuntimeException("Bad index");
}

    public com.opendatagroup.hadrian.data.PFARecord internalUpdate(int i, Object elem) {
%s out = new %s();
%s
out.put(i, elem);
return out;
    }

    public com.opendatagroup.hadrian.data.PFARecord internalUpdate(String i, Object elem) {
%s out = new %s();
%s
out.put(i, elem);
return out;
    }

    public com.opendatagroup.hadrian.data.PFARecord multiUpdate(String[] fields, Object[] values) {
%s out = new %s();
%s
for (int i = 0;  i < fields.length;  i++) {
    out.put(fields[i], values[i]);
}
return out;
    }
}
""",          namespace map {"package " + _ + ";"} mkString(""),
              t(namespace, name, false),
              interfaces,
              javaSchema(avroCompiled, true),
              fields map {"\"" + _.name + "\""} mkString(", "),
              fields map {x: AvroField => javaSchema(x.avroType, true)} mkString(", "),
              t(namespace, name, false),
              fields map {case AvroField(fname, ftype, _, _, _, _) =>
                """public %s %s;""".format(javaType(ftype, false, true, true), s(fname))} mkString("\n"),
              (fields zipWithIndex) map {case (AvroField(fname, ftype, _, _, _, _), i) =>
                """case %d: return %s;""".format(i, s(fname))} mkString("\n"),
              (fields zipWithIndex) map {case (AvroField(fname, ftype, _, _, _, _), i) =>
                """case %d: %s = (%s)value$; break;""".format(i, s(fname), javaType(ftype, true, true, true))} mkString("\n"),
              fields map {case AvroField(fname, ftype, _, _, _, _) =>
                """if (field$.equals("%s")) return %s;""".format(StringEscapeUtils.escapeJava(fname), s(fname))} mkString("\nelse "),
              fields map {case AvroField(fname, ftype, _, _, _, _) =>
                """if (field$.equals("%s")) %s = (%s)value$;""".format(StringEscapeUtils.escapeJava(fname), s(fname), javaType(ftype, true, true, true))} mkString("\nelse "),
              t(namespace, name, false),
              t(namespace, name, false),
              fields map {case AvroField(fname, _, _, _, _, _) => """out.%s = this.%s;""".format(s(fname), s(fname))} mkString("\n"),
              t(namespace, name, false),
              t(namespace, name, false),
              fields map {case AvroField(fname, _, _, _, _, _) => """out.%s = this.%s;""".format(s(fname), s(fname))} mkString("\n"),
              t(namespace, name, false),
              t(namespace, name, false),
              fields map {case AvroField(fname, _, _, _, _, _) => """out.%s = this.%s;""".format(s(fname), s(fname))} mkString("\n")
            ))
          }

          case AvroFixed(size, name, namespace, _, _) => {
            dependencyBuilder += (t(namespace, name, true) -> JavaCode("""%s
public class %s extends com.opendatagroup.hadrian.data.PFAFixed {
    public static final org.apache.avro.Schema SCHEMA$ = %s;
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

    public int size() { return %s; }

    public %s() {
        super();
    }

    public %s(byte[] b) {
        super();
        bytes(b);
    }
}
""",          namespace map {"package " + _ + ";"} mkString(""),
              t(namespace, name, false),
              javaSchema(avroCompiled, true),
              size.toString,
              t(namespace, name, false),
              t(namespace, name, false)))
          }

          case AvroEnum(symbols, name, namespace, _, _) => {
            dependencyBuilder += (t(namespace, name, true) -> JavaCode("""%s
public class %s extends com.opendatagroup.hadrian.data.PFAEnumSymbol implements org.apache.avro.generic.GenericEnumSymbol, Comparable<org.apache.avro.generic.GenericEnumSymbol> {
    public static final org.apache.avro.Schema SCHEMA$ = %s;
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
    public org.apache.avro.Schema getSchema() { return SCHEMA$; }

    private int _value;
    public int value() { return _value; }

    public %s(org.apache.avro.Schema schema, String symbol) {
        _value = strToInt(symbol);
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        return (o instanceof %s)  &&  (this.value() == ((%s)o).value());
    }

    public int hashCode() { return intToStr(value()).hashCode(); }
    public String toString() { return intToStr(value()); }
    public int compareTo(Object that) {
        return this.value() - ((%s)that).value();
    }

    public int numSymbols() { return intToStrArray.length; }

    private static String[] intToStrArray = new String[]{%s};
    private static java.util.Map<String, Integer> strToIntMap = new java.util.HashMap<String, Integer>() {{%s}};
    public String intToStr(int i) { return intToStrArray[i]; }
    public int strToInt(String x) { return ((Integer)strToIntMap.get(x)).intValue(); }
}
""",          namespace map {"package " + _ + ";"} mkString(""),
              t(namespace, name, false),
              javaSchema(avroCompiled, true),
              t(namespace, name, false),
              t(namespace, name, false),
              t(namespace, name, false),
              t(namespace, name, false),
              symbols map {x => "\"" + StringEscapeUtils.escapeJava(x) + "\""} mkString(", "),
              (symbols zipWithIndex) map {case (x, i) => "put(\"%s\", %d);".format(StringEscapeUtils.escapeJava(x), i)} mkString(" ")))
          }

        }
        
        val beginMethod =
          """    public void begin() { synchronized(runlock) { (new Object() {
%s
public void apply() {
timeout = options().timeout_begin();
startTime = System.currentTimeMillis();
%s = "%s";
%s
%s = _metadata();
%s
} }).apply(); } }""".format(
            symbolFields(beginSymbols),
            s("name"),
            StringEscapeUtils.escapeJava(engineName),
            version match {
              case Some(x) => s("version") + " = " + x.toString + ";"
              case None => ""
            },
            s("metadata"),
            block(begin, ReturnMethod.NONE, AvroNull()))

        val (interface, actionMethod) = method match {
          case Method.MAP =>
            ("com.opendatagroup.hadrian.jvmcompiler.PFAMapEngine<%s, %s>".format(javaType(input, true, true, true), javaType(output, true, true, true)),
              """%s
    public %s action(%s input) {
synchronized(runlock) {
rollbackSave();
_actionsStarted_$eq(_actionsStarted() + 1L);
%s out;
try {
out = (new Object() {
%s
public %s apply(%s input) {
timeout = options().timeout_action();
startTime = System.currentTimeMillis();
%s = input;
%s = "%s";
%s
%s = _metadata();
%s = _actionsStarted();
%s = _actionsFinished();
%s
} }).apply(input); }
catch (Throwable err) {
rollback();
throw err;
}
_actionsFinished_$eq(_actionsFinished() + 1L);
return out;
} }""".format(
                if (!input.isInstanceOf[AvroUnion]) """    public Object action(Object input) { return (Object)action((%s)input); }""".format(javaType(input, true, true, true)) else "",
                javaType(output, true, true, true),
                javaType(input, true, true, true),
                javaType(output, true, true, true),
                symbolFields(actionSymbols),
                javaType(output, true, true, true),
                javaType(input, true, true, true),
                s("input"),
                s("name"),
                StringEscapeUtils.escapeJava(engineName),
                version match {
                  case Some(x) => s("version") + " = " + x.toString + ";"
                  case None => ""
                },
                s("metadata"),
                s("actionsStarted"),
                s("actionsFinished"),
                block(action, ReturnMethod.RETURN, output)))

          case Method.EMIT =>
            ("com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine<%s, %s>".format(javaType(input, true, true, true), javaType(output, true, true, true)),
              """    private scala.Function1<%s, scala.runtime.BoxedUnit> %s = new scala.runtime.AbstractFunction1<%s, scala.runtime.BoxedUnit>() {
%s
        public scala.runtime.BoxedUnit apply(%s output) { return null; }
};
    public scala.Function1<%s, scala.runtime.BoxedUnit> emit() { return %s; }
    public void emit_$eq(scala.Function1<%s, scala.runtime.BoxedUnit> newEmit) { %s = newEmit; }
%s
    public %s action(%s input) {
synchronized(runlock) {
rollbackSave();
_actionsStarted_$eq(_actionsStarted() + 1L);
try {
(new Object() {
%s
public %s apply(%s input) {
timeout = options().timeout_action();
startTime = System.currentTimeMillis();
%s = input;
%s = "%s";
%s
%s = _metadata();
%s = _actionsStarted();
%s = _actionsFinished();
%s
return null;
} }).apply(input); }
catch (Throwable err) {
rollback();
throw err;
}
_actionsFinished_$eq(_actionsFinished() + 1L);
return null;
} }""".format(
                javaType(output, true, true, true),
                f("emit"),
                javaType(output, true, true, true),
                if (!output.isInstanceOf[AvroUnion]) """        public Object apply(Object output) { return (Object)apply((%s)output); }""".format(javaType(output, true, true, true)) else "",
                javaType(output, true, true, true),
                javaType(output, true, true, true),
                f("emit"),
                javaType(output, true, true, true),
                f("emit"),
                if (!input.isInstanceOf[AvroUnion]) """    public Object action(Object input) { return (Object)action((%s)input); }""".format(javaType(input, true, true, true)) else "",
                javaType(output, true, true, true),
                javaType(input, true, true, true),
                symbolFields(actionSymbols),
                javaType(output, true, true, true),
                javaType(input, true, true, true),
                s("input"),
                s("name"),
                StringEscapeUtils.escapeJava(engineName),
                version match {
                  case Some(x) => s("version") + " = " + x.toString + ";"
                  case None => ""
                },
                s("metadata"),
                s("actionsStarted"),
                s("actionsFinished"),
                block(action, ReturnMethod.NONE, AvroNull())))

          case Method.FOLD =>
            ("com.opendatagroup.hadrian.jvmcompiler.PFAFoldEngine<%s, %s>".format(javaType(input, true, true, true), javaType(output, true, true, true)),
              """    private %s %s;
    public %s tally() { return %s; }
    public void tally_$eq(%s newTally) { %s = newTally; }
%s
    public void initialize(com.opendatagroup.hadrian.ast.EngineConfig config, com.opendatagroup.hadrian.options.EngineOptions options, scala.Option<com.opendatagroup.hadrian.shared.SharedState> state, Class<%s> thisClass, com.opendatagroup.hadrian.ast.EngineConfig.Context context) {
        super.initialize(config, options, state, thisClass, context);
        try {
            %s = (%s)fromJson("%s", %s);
        }
        catch (Exception err) {
            throw new com.opendatagroup.hadrian.errors.PFAInitializationException("zero does not conform to output type " + %s.toString());
        }
    }
%s
    public %s action(%s input) {
synchronized(runlock) {
rollbackSave();
_actionsStarted_$eq(_actionsStarted() + 1L);
try {
%s = (new Object() {
%s
public %s apply(%s input) {
timeout = options().timeout_action();
startTime = System.currentTimeMillis();
%s = input;
%s = "%s";
%s
%s = _metadata();
%s = _actionsStarted();
%s = _actionsFinished();
%s
} }).apply(input);
}
catch (Throwable err) {
rollback();
throw err;
}
_actionsFinished_$eq(_actionsFinished() + 1L);
return %s;
} }""".format(
                javaType(output, true, true, true),
                s("tally"),
                javaType(output, true, true, true),
                s("tally"),
                javaType(output, true, true, true),
                s("tally"),
                if (!output.isInstanceOf[AvroUnion]) """    public void tally_$eq(Object newTally) { %s = (%s)newTally; }""".format(s("tally"), javaType(output, true, true, true)) else "",
                thisClassName,
                s("tally"),
                javaType(output, true, true, true),
                StringEscapeUtils.escapeJava(zero.get),
                javaSchema(output, false),
                javaSchema(output, false),
                if (!input.isInstanceOf[AvroUnion]) """    public Object action(Object input) { return (Object)action((%s)input); }""".format(javaType(input, true, true, true)) else "",
                javaType(output, true, true, true),
                javaType(input, true, true, true),
                s("tally"),
                symbolFields(actionSymbols),
                javaType(output, true, true, true),
                javaType(input, true, true, true),
                s("input"),
                s("name"),
                StringEscapeUtils.escapeJava(engineName),
                version match {
                  case Some(x) => s("version") + " = " + x.toString + ";"
                  case None => ""
                },
                s("metadata"),
                s("actionsStarted"),
                s("actionsFinished"),
                block(action, ReturnMethod.RETURN, output),
                s("tally")))
        }

        val endMethod =
          """    public void end() { synchronized(runlock) { (new Object() {
%s
public void apply() {
timeout = options().timeout_end();
startTime = System.currentTimeMillis();
%s = "%s";
%s
%s = _metadata();
%s = _actionsStarted();
%s = _actionsFinished();
%s
%s
} }).apply(); } }""".format(
            symbolFields(endSymbols),
            s("name"),
            StringEscapeUtils.escapeJava(engineName),
            version match {
              case Some(x) => s("version") + " = " + x.toString + ";"
              case None => ""
            },
            s("metadata"),
            s("actionsStarted"),
            s("actionsFinished"),
            if (method == Method.FOLD) s("tally") + " = tally();" else "",
            block(end, ReturnMethod.NONE, AvroNull()))

        val unsharedCells =
          for ((cname, cell) <- cells if (!cell.shared)) yield
            """public %s %s;""".format(javaType(cell.avroType, false, true, false), c(cname))

        val unsharedPools = 
          for ((pname, pool) <- pools if (!pool.shared)) yield
            """public java.util.Map<String, %s> %s;""".format(javaType(pool.avroType, true, true, false), p(pname))

        val sharedPools = 
          for ((pname, pool) <- pools if (pool.shared)) yield
            """public com.opendatagroup.hadrian.shared.SharedMap %s;""".format(p(pname))

        val functions =
          for ((fname, fcnContext) <- fcn) yield {
            """public class %s extends scala.runtime.AbstractFunction%s<%s> {
%s
%s
public %s apply(%s) {
synchronized(runlock) {
checkClock();
%s
%s
} } }""".format(f(fname),
            fcnContext.params.size.toString,
            ((fcnContext.params map {case (n, t) => javaType(t, true, true, true)}) :+ javaType(fcnContext.ret, true, true, true)).mkString(", "),
            symbolFields(fcnContext.symbols),
            if (fcnContext.params.size > 0  &&  (fcnContext.params exists {case (_, _: AvroUnion) => false; case _ => true}))
              "public %s apply(%s) { return apply(%s); }".format(
                javaType(fcnContext.ret, true, true, true),
                fcnContext.params map {case (n, t) => "Object " + s(n)} mkString(", "),
                fcnContext.params map {case (n, t) => "(%s)%s".format(javaType(t, true, true, true), s(n))} mkString(", ")
              )
            else
              "",
            javaType(fcnContext.ret, true, true, true),
            fcnContext.params map {case (n, t) => javaType(t, true, true, true) + " " + par(n)} mkString(", "),
            fcnContext.params map {case (n, _) => "%s = %s;".format(s(n), par(n))} mkString("\n"),
            block(fcnContext.exprs, ReturnMethod.RETURN, fcnContext.ret))
          }

        val fcnMethods =
          for (numParams <- 0 to 22) yield
            """public scala.Function%d<%s> fcn%d(String name) { String uname = "u." + name;  %s }""".format(
              numParams,
              List.fill(numParams + 1)("Object").mkString(", "),
              numParams,
              if ((fcn count {case (fname, fcnContext) => fcnContext.params.size == numParams}) == 0)
                """throw new java.util.NoSuchElementException("this engine has no %d-parameter functions");""".format(numParams)
              else
                "if " + (for ((fname, fcnContext) <- fcn if (fcnContext.params.size == numParams)) yield
                  """(uname.equals("%s")) { return new %s(); }""".format(fname, f(fname))
                ).mkString(" else if ") +
                """ else { throw new java.util.NoSuchElementException("%d-parameter function not found: \"" + name + "\""); }""".format(numParams)
            )

        JavaCode("""public class %s extends com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase implements %s {
final com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase thisEngineBase = this;
final %s thisInterface = this;
private Object runlock = new Object();
public Object getRunlock() { return runlock; }
%s
public com.opendatagroup.hadrian.shared.SharedMap sharedCells;
%s
%s
%s
public %s(com.opendatagroup.hadrian.ast.EngineConfig config, com.opendatagroup.hadrian.options.EngineOptions options, scala.Option<com.opendatagroup.hadrian.shared.SharedState> state, com.opendatagroup.hadrian.ast.EngineConfig.Context context) { initialize(config, options, state, %s.class, context); }
public ClassLoader classLoader() { return getClass().getClassLoader(); }
private long timeout;
private long startTime;
public void checkClock() {
    if (timeout > 0  &&  System.currentTimeMillis() - startTime > timeout) {
        throw new com.opendatagroup.hadrian.errors.PFATimeoutException(String.format("exceeded timeout of %%d milliseconds", timeout));
    }
}
%s
%s
%s
%s
}""",     thisClassName,
          interface,
          interface,
          unsharedCells.mkString("\n"),
          unsharedPools.mkString("\n"),
          sharedPools.mkString("\n"),
          functions.mkString("\n"),
          thisClassName,
          thisClassName,
          fcnMethods.mkString("\n"),
          beginMethod,
          actionMethod,
          endMethod)
        .setName("PFA_" + engineName)
        .setDependencies(dependencyBuilder.result)
      }

      case Cell.Context() => JavaCode("")

      case FcnDef.Context(_, _, _, params, ret, symbols, exprs) => {
        val abstractCall =
          if (params.isEmpty  ||  params.forall({case (n, t) => t.isInstanceOf[AvroUnion]}))
            ""
          else
            """public %s apply(%s) { return apply(%s); }""".format(
              javaType(ret, true, true, true),
              params map {case (n, t) => "Object " + s(n)} mkString(", "),
              params map {case (n, t) => "((%s)(%s))".format(javaType(t, true, true, true), s(n))} mkString(", "))

        JavaCode("""(new scala.runtime.AbstractFunction%s<%s>() {
%s
public %s apply(%s) {
checkClock();
%s
%s
}
%s
})""",
          params.size.toString,
          ((params map {case (n, t) => javaType(t, true, true, true)}) :+ javaType(ret, true, true, true)).mkString(", "),
          symbolFields(symbols),
          javaType(ret, true, true, true),
          params map {case (n, t) => javaType(t, true, true, true) + " " + par(n)} mkString(", "),
          params map {case (n, _) => "%s = %s;".format(s(n), par(n))} mkString("\n"),
          block(exprs, ReturnMethod.RETURN, ret),
          abstractCall
        )
      }

      case FcnRef.Context(_, _, fcn) => resolvedType match {
        case Some(x: FcnType) => fcn.javaRef(x)
        case _ => JavaCode("").setAsPlaceholder()
      }

      case FcnRefFill.Context(fcnType, _, fcn, originalParamNames, argTypeResult) => {
        val sig = fcn.sig.asInstanceOf[Sig]
        val originalFcnType = FcnType(sig.params map {case (k, p) => P.toType(p)}, P.mustBeAvro(P.toType(sig.ret)))

        val abstractCall =
          if (fcnType.params.isEmpty  ||  fcnType.params.forall(_.isInstanceOf[AvroUnion]))
            ""
          else
            """public %s apply(%s) { return apply(%s); }""".format(
              javaType(fcnType.ret, true, true, true),
              (0 until fcnType.params.size) map {i => "Object $" + i.toString} mkString(", "),
              fcnType.params.zipWithIndex map {case (t, i) => "((%s)($%d))".format(javaType(P.mustBeAvro(t), true, true, true), i)} mkString(", "))

        var j = 0
        JavaCode("""(new scala.runtime.AbstractFunction%s<%s>() {
public %s apply(%s) { return (%s).apply(%s); }
%s
})""",
          fcnType.params.size.toString,
          ((fcnType.params map {t => javaType(P.mustBeAvro(t), true, true, true)}) :+ javaType(fcnType.ret, true, true, true)).mkString(", "),
          javaType(fcnType.ret, true, true, true),
          fcnType.params.zipWithIndex map {case (t, i) => javaType(P.mustBeAvro(t), true, true, true) + " $" + i.toString} mkString(", "),
          fcn.javaRef(originalFcnType).toString,
          originalParamNames map {argTypeResult.get(_) match {
            case Some((t: Type, r: TaskResult)) => r.toString
            case None => {
              val out = "$" + j.toString
              j += 1
              out
            }
          }} mkString(", "),
          abstractCall
        )
      }

      case CallUserFcn.Context(retType, _, name, nameToNum, nameToFcn, args, argContext, nameToParamTypes, nameToRetTypes) => {
        val cases =
          for ((n, fcn) <- nameToFcn) yield
            """        case %s: return %s;""".format(nameToNum(n), fcn.javaCode(args.collect({case x: JavaCode => x}), argContext, nameToParamTypes(n), nameToRetTypes(n)))

        JavaCode("""(new Object() {
public %s apply() {
    switch (%s.value()) {
%s
        default: throw new RuntimeException("");
    }
} }).apply()""",
              javaType(retType, false, true, true),
              name.toString,
              cases.mkString("\n"))
      }

      case Call.Context(retType, _, fcn, args, argContext, paramTypes) => fcn.javaCode(args.collect({case x: JavaCode => x}), argContext, paramTypes, retType)

      case Ref.Context(retType, _, name) => JavaCode(s(name))

      case LiteralNull.Context(_, _) => JavaCode("null")
      case LiteralBoolean.Context(_, _, value) => if (value) JavaCode("true") else JavaCode("false")
      case LiteralInt.Context(_, _, value) => JavaCode(value.toString)
      case LiteralLong.Context(_, _, value) => JavaCode(value.toString + "L")
      case LiteralFloat.Context(_, _, value) => JavaCode(value.toString + "F")
      case LiteralDouble.Context(_, _, value) => JavaCode(value.toString)
      case LiteralString.Context(_, _, value) => JavaCode("\"" + StringEscapeUtils.escapeJava(value) + "\"")
      case LiteralBase64.Context(_, _, value) => JavaCode("new byte[]{" + value.mkString(",") + "}")
      case Literal.Context(retType, _, value) => JavaCode("""((%s)fromJson("%s", %s))""", javaType(retType, true, true, true), StringEscapeUtils.escapeJava(value), javaSchema(retType, false))

      case NewObject.Context(retType, _, fields) =>
        retType match {
          case avroRecord: AvroRecord =>
            JavaCode("""(new Object() {
public %s apply() {
%s out = new %s();
%s
return (%s)out;
} }).apply()""",
              javaType(retType, false, true, true),
              javaType(retType, false, true, false),
              javaType(retType, false, true, false),
              fields map {case (name: String, expr: JavaCode) => "out.%s = %s;".format(s(name), W.wrapExpr(expr.toString, avroRecord.field(name).avroType, false))} mkString("\n"),
              javaType(retType, false, true, true))

          case avroMap: AvroMap =>
            JavaCode("""(new Object() {
public %s apply() {
%s out = com.opendatagroup.hadrian.data.PFAMap.empty(%s, %s);
%s
return out;
} }).apply()""",
              javaType(retType, false, true, false),
              javaType(retType, false, true, false),
              fields.size.toString,
              javaSchema(retType, false),
              fields map {case (name: String, expr: JavaCode) => "out.put(\"%s\", %s);".format(StringEscapeUtils.escapeJava(name), W.wrapExpr(expr.toString, avroMap.values, true))} mkString("\n"))
        }

      case NewArray.Context(retType, calls, items) =>
        JavaCode("""(new Object() {
public %s apply() {
%s out = com.opendatagroup.hadrian.data.PFAArray.empty(%s, %s);
%s
return out;
} }).apply()""",
          javaType(retType, false, true, false),
          javaType(retType, false, true, false),
          items.size.toString,
          javaSchema(retType, false),
          items map {case expr: JavaCode => "out.add(%s);".format(W.wrapExpr(expr.toString, retType.asInstanceOf[AvroArray].items, false))} mkString("\n"))

      case Do.Context(retType, _, symbols, exprs) => {
        JavaCode("""(new Object() {
%s
public %s apply() {
%s
} }).apply()""",
          symbolFields(symbols), javaType(retType, false, true, true), block(exprs, ReturnMethod.RETURN, retType))
      }

      case Let.Context(_, _, nameTypeExpr) => {
        def wrap(nameTypeExpr: List[(String, AvroType, JavaCode)]): JavaCode = nameTypeExpr match {
          case (name, t, expr) :: Nil => JavaCode("com.opendatagroup.hadrian.jvmcompiler.W.n(%s = (%s)(%s))", s(name), javaType(t, false, true, false), W.wrapExpr(expr.toString, t, false))
          case (name, t, expr) :: rest => JavaCode("com.opendatagroup.hadrian.jvmcompiler.W.nn(%s = (%s)(%s), %s)", s(name), javaType(t, false, true, false), W.wrapExpr(expr.toString, t, false), wrap(rest).toString)
        }
        wrap(nameTypeExpr.toList.collect({case (n, t, j: JavaCode) => (n, t, j)}))
      }

      case SetVar.Context(_, _, nameTypeExpr) => {
        def wrap(nameTypeExpr: List[(String, AvroType, JavaCode)]): JavaCode = nameTypeExpr match {
          case (name, t, expr) :: Nil => JavaCode("com.opendatagroup.hadrian.jvmcompiler.W.n(%s = (%s)(%s))", s(name), javaType(t, false, true, false), W.wrapExpr(expr.toString, t, false))
          case (name, t, expr) :: rest => JavaCode("com.opendatagroup.hadrian.jvmcompiler.W.nn(%s = (%s)(%s), %s)", s(name), javaType(t, false, true, false), W.wrapExpr(expr.toString, t, false), wrap(rest).toString)
        }
        wrap(nameTypeExpr.toList.collect({case (n, t, j: JavaCode) => (n, t, j)}))
      }

      case AttrGet.Context(retType, _, expr, exprType, path) => {
        var out = "((%s)(%s))".format(javaType(exprType, false, true, false), W.wrapExpr(expr.toString, exprType, false))
        for (item <- path) item match {
          case ArrayIndex(expr, t) => out = """((%s)(%s.get(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))))""".format(javaType(t, true, true, false), out, expr.toString)
          case MapIndex(expr, t) => out = """((%s)(%s.get(%s)))""".format(javaType(t, true, true, false), out, expr.toString)
          case RecordIndex(name, t) => out = """((%s)(%s.%s))""".format(javaType(t, false, true, false), out, s(name))
        }
        JavaCode(out)
      }

      case AttrTo.Context(retType, _, expr, exprType, setType, path, to, toType) => {
        val toFcn =
          if (toType.isInstanceOf[AvroType])
            """(new scala.runtime.AbstractFunction1<%s, %s>() { public %s apply(%s dummy) { return %s; } public Object apply(Object dummy) { return apply((%s)(%s)); } })""".format(
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              to.toString,
              javaType(setType, true, true, true),
              to.toString)
          else
            to.toString

        JavaCode("(%s)%s.updated(new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s)",
          javaType(exprType, false, true, false),
          W.wrapExpr(expr.toString, exprType, false),
          makePathIndex(path),
          toFcn,
          javaSchema(setType, false))
      }

      case CellGet.Context(retType, _, cell, cellType, path, shared) => {
        if (!shared) {
          var out = "((%s)(%s))".format(javaType(cellType, !path.isEmpty, true, false), c(cell))
          for (item <- path) item match {
            case ArrayIndex(expr, t) => out = """((%s)(%s.get(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))))""".format(javaType(t, true, true, false), out, expr.toString)
            case MapIndex(expr, t) => out = """((%s)(%s.get(%s)))""".format(javaType(t, true, true, false), out, expr.toString)
            case RecordIndex(name, t) => out = """((%s)(%s.%s))""".format(javaType(t, false, true, false), out, s(name))
          }
          JavaCode(out)
        }
        else {
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(sharedCells.get("%s", new com.opendatagroup.hadrian.shared.PathIndex[]{%s})))""",
            javaType(retType, true, true, false),
            cell,
            makePathIndex(path))
        }
      }

      case CellTo.Context(retType, _, cell, cellType, setType, path, to, toType, shared) => {
        val toFcn =
          if (toType.isInstanceOf[AvroType])
            """(new scala.runtime.AbstractFunction1<%s, %s>() { public %s apply(%s dummy) { return %s; } public Object apply(Object dummy) { return apply((%s)(%s)); } })""".format(
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              to.toString,
              javaType(setType, true, true, true),
              to.toString)
          else
            to.toString

        if (!shared) {
          if (path.isEmpty)
            JavaCode("(%s = (%s)%s.apply(%s))".format(c(cell), javaType(cellType, false, true, false), toFcn, c(cell)))
          else
            JavaCode("(%s = (%s)%s.updated(new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s))",
              c(cell),
              javaType(cellType, false, true, false),
              c(cell),
              makePathIndex(path),
              toFcn,
              javaSchema(setType, false))
        }
        else {
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(sharedCells.update("%s", new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, null, %s, %s)))""",
            javaType(retType, true, true, false),
            cell,
            makePathIndex(path),
            toFcn,
            javaSchema(setType, false)
          )
        }
      }

      case PoolGet.Context(retType, _, pool, path, shared) => {
        if (!shared) {
          var out = """((%s)(com.opendatagroup.hadrian.jvmcompiler.W.getOrFailPool(%s, %s, "%s")))""".format(
            javaType(path.head.asInstanceOf[MapIndex].t, true, true, false),
            p(pool),
            path.head.asInstanceOf[MapIndex].k.toString,
            StringEscapeUtils.escapeJava(pool))
          for (item <- path.tail) item match {
            case ArrayIndex(expr, t) => out = """((%s)(%s.get(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))))""".format(javaType(t, true, true, false), out, expr.toString)
            case MapIndex(expr, t) => out = """((%s)(%s.get(%s)))""".format(javaType(t, true, true, false), out, expr.toString)
            case RecordIndex(name, t) => out = """((%s)(%s.%s))""".format(javaType(t, false, true, false), out, s(name))
          }
          JavaCode(out)
        }
        else {
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(%s.get(%s, new com.opendatagroup.hadrian.shared.PathIndex[]{%s})))""",
            javaType(retType, true, true, false),
            p(pool),
            path.head.asInstanceOf[MapIndex].k.toString,
            makePathIndex(path.tail))
        }
      }

      case PoolTo.Context(retType, _, pool, poolType, setType, path, to, toType, init, shared) => {
        val toFcn =
          if (toType.isInstanceOf[AvroType])
            """(new scala.runtime.AbstractFunction1<%s, %s>() { public %s apply(%s dummy) { return %s; } public Object apply(Object dummy) { return apply((%s)(%s)); } })""".format(
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              to.toString,
              javaType(setType, true, true, true),
              to.toString)
          else
            to.toString

        if (!shared) {
          if (path.size == 1)
            JavaCode("""com.opendatagroup.hadrian.jvmcompiler.W.do2(%s.put(%s, %s.apply(com.opendatagroup.hadrian.jvmcompiler.W.getOrElse(%s, %s, %s))), %s.get(%s))""",
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString,
              toFcn,
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString,
              init.toString,
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString)
          else
            JavaCode("""com.opendatagroup.hadrian.jvmcompiler.W.do2(%s.put(%s, ((%s)(com.opendatagroup.hadrian.jvmcompiler.W.getOrElse(%s, %s, %s))).updated(new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s)), %s.get(%s))""",
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString,
              javaType(path.head.asInstanceOf[MapIndex].t, true, true, false),
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString,
              init.toString,
              makePathIndex(path.tail),
              toFcn,
              javaSchema(setType, false),
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString)
        }
        else
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(%s.update(%s, new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s, %s)))""",
            javaType(retType, true, true, false),
            p(pool),
            path.head.asInstanceOf[MapIndex].k.toString,
            makePathIndex(path.tail),
            init.toString,
            toFcn,
            javaSchema(setType, false))
      }

      case If.Context(retType, _, thenSymbols, predicate, thenClause, elseSymbols, elseClause) => (elseSymbols, elseClause) match {
        case (Some(symbols), Some(clause)) =>
          JavaCode("""(new Object() {
public %s apply() {
if (com.opendatagroup.hadrian.jvmcompiler.W.asBool(%s)) {
return (new Object() {
%s
public %s apply() {
%s
} }).apply();
}
else {
return (new Object() {
%s
public %s apply() {
%s
} }).apply();
}
} }).apply()""",
            javaType(retType, false, true, true),
            predicate.toString,
            symbolFields(thenSymbols),
            javaType(retType, false, true, true),
            block(thenClause, ReturnMethod.RETURN, retType),
            symbolFields(symbols),
            javaType(retType, false, true, true),
            block(clause, ReturnMethod.RETURN, retType))

        case (None, None) =>
          JavaCode("""(new Object() {
%s
public Void apply() {
if (com.opendatagroup.hadrian.jvmcompiler.W.asBool(%s)) {
%s
}
return null;
} }).apply()""",
            symbolFields(thenSymbols),
            predicate.toString,
            block(thenClause, ReturnMethod.NONE, retType))

        case _ => throw new RuntimeException("inconsistent call to task(If.Context)")
      }

      case Cond.Context(retType, _, complete, walkBlocks) => {
        var first = true
        val stringBlocks =
          for (walkBlock <- walkBlocks) yield
            """%s {
return (new Object() {
%s
public %s apply() {
%s
} }).apply();
}
""".format(walkBlock.pred match {
             case Some(p) if (first) => first = false; "if (com.opendatagroup.hadrian.jvmcompiler.W.asBool(%s))".format(p)
             case Some(p) => "else if (com.opendatagroup.hadrian.jvmcompiler.W.asBool(%s))".format(p)
             case None => "else"
           },
           symbolFields(walkBlock.symbols),
           javaType(retType, false, true, true),
           (if (complete)
             block(walkBlock.exprs, ReturnMethod.RETURN, retType)
           else
             block(walkBlock.exprs, ReturnMethod.NONE, retType) + "\nreturn null;"))

        JavaCode("""(new Object() {
public %s apply() {
%s%s
} }).apply()""", javaType(retType, false, true, true), stringBlocks.mkString(""), (if (complete) "" else "\nreturn null;"))
      }

      case While.Context(retType, _, symbols, predicate, loopBody) =>
        JavaCode("""(new Object() {
%s
public Void apply() {
while (%s) {
checkClock();
%s
}
return null;
} }).apply()""",
          symbolFields(symbols),
          predicate.toString,
          block(loopBody, ReturnMethod.NONE, retType))

      case DoUntil.Context(retType, _, symbols, loopBody, predicate) =>
        JavaCode("""(new Object() {
%s
public Void apply() {
do {
checkClock();
%s
} while (!%s);
return null;
} }).apply()""",
          symbolFields(symbols),
          block(loopBody, ReturnMethod.NONE, retType),
          predicate.toString)

      case For.Context(retType, _, symbols, initNameTypeExpr, predicate, loopBody, stepNameTypeExpr) =>
        JavaCode("""(new Object() {
%s
public Void apply() {
%s
while (%s) {
checkClock();
%s
%s
}
return null;
} }).apply()""",
          symbolFields(symbols),
          (for ((name, t, expr) <- initNameTypeExpr) yield
            s(name) + " = " + W.wrapExpr(expr.toString, t, false) + ";").mkString("\n"),
          predicate.toString,
          block(loopBody, ReturnMethod.NONE, retType),
          (for ((name, t, expr) <- stepNameTypeExpr) yield
            s(name) + " = " + W.wrapExpr(expr.toString, t, false) + ";").mkString("\n"))

      case Foreach.Context(retType, _, symbols, objType, objExpr, itemType, name, loopBody) =>
        JavaCode("""(new Object() {
%s
public Void apply() {
java.util.Iterator<%s> iter = %s.iterator();
while (iter.hasNext()) {
checkClock();
%s = ((%s)%s);
%s
}
return null;
} }).apply()""",
          symbolFields(symbols),
          javaType(itemType, true, true, true),
          objExpr.toString,
          s(name),
          javaType(itemType, true, true, true),
          W.wrapExpr("iter.next()", itemType, false),
          block(loopBody, ReturnMethod.NONE, retType))

      case Forkeyval.Context(retType, _, symbols, objType, objExpr, valueType, forkey, forval, loopBody) =>
        JavaCode("""(new Object() {
%s
public Void apply() {
java.util.Iterator<java.util.Map.Entry<String, %s>> iter = %s.entrySet().iterator();
while (iter.hasNext()) {
checkClock();
java.util.Map.Entry<String, %s> pair = ((java.util.Map.Entry<String, %s>)iter.next());
%s = ((String)pair.getKey());
%s = ((%s)%s);
%s
}
return null;
} }).apply()""",
          symbolFields(symbols),
          javaType(valueType, true, true, true),
          objExpr.toString,
          javaType(valueType, true, true, true),
          javaType(valueType, true, true, true),
          s(forkey),
          s(forval),
          javaType(valueType, true, true, true),
          W.wrapExpr("pair.getValue()", valueType, false),
          block(loopBody, ReturnMethod.NONE, retType))

      case CastCase.Context(retType, name, toType, _, symbols, clause) =>
        if (retType.isInstanceOf[ExceptionType])
          JavaCode("""(new Object() {
%s
public Object apply() {
%s = ((%s)%s);
%s
return defaultDummy;
} }).apply();
""",
            symbolFields(symbols),
            s(name),
            javaType(toType, true, true, true),
            W.wrapExpr("obj", toType, false),
            block(clause, ReturnMethod.NONE, retType))

        else
          JavaCode("""(new Object() {
%s
public %s apply() {
%s = ((%s)%s);
%s
} }).apply();
""",
            symbolFields(symbols),
            javaType(retType, false, true, true),
            s(name),
            javaType(toType, true, true, true),
            W.wrapExpr("obj", toType, false),
            block(clause, ReturnMethod.RETURN, retType))

      case CastBlock.Context(retType, _, exprType, expr, cases, partial) =>
        if (partial)
          JavaCode("""(new Object() {
%s obj;
%s defaultDummy = null;
public Void apply() {
obj = %s;
%s
return null;
} }).apply()""",
            javaType(exprType, true, true, true),
            javaType(retType, true, true, true),
            expr.toString,
            (for (((ctx: CastCase.Context, code: JavaCode), i) <- cases.zipWithIndex) yield {
              if (i == 0)
                "if (obj instanceof %s) { %s }".format(javaType(ctx.toType, true, true, false), code.toString)
              else
                "else if (obj instanceof %s) { %s }".format(javaType(ctx.toType, true, true, false), code.toString)
            }).mkString("\n"))
        else
          JavaCode("""(new Object() {
%s obj;
%s defaultDummy = null;
public %s apply() {
obj = %s;
%s
} }).apply()""",
            javaType(exprType, true, true, true),
            javaType(retType, true, true, true),
            javaType(retType, true, true, true),
            expr.toString,
            (for (((ctx: CastCase.Context, code: JavaCode), i) <- cases.zipWithIndex) yield {
              if (i == 0)
                "if (obj instanceof %s) { return (%s)%s }".format(
                  javaType(ctx.toType, true, true, false),
                  javaType(retType, true, true, true),
                  code.toString)
              else if (i == cases.size - 1)
                "else { return (%s)%s }".format(
                  javaType(retType, true, true, true),
                  code.toString)
              else
                "else if (obj instanceof %s) { return (%s)%s }".format(
                  javaType(ctx.toType, true, true, false),
                  javaType(retType, true, true, true),
                  code.toString)
            }).mkString("\n"))

      case Upcast.Context(retType, _, expr) =>
        JavaCode("((%s)(%s))", javaType(retType, false, true, false), W.wrapExpr(expr.toString, retType, false))

      case IfNotNull.Context(retType, calls, symbolTypeResult, thenSymbols, thenClause, elseSymbols, elseClause) => {
        val toCheckSymbols =
          (for ((symbol, avroType, _) <- symbolTypeResult) yield ("%s %s;".format(javaType(avroType, true, true, true), s(symbol)))).mkString("\n")

        val toCheckExprs =
          (for ((symbol, avroType, result) <- symbolTypeResult) yield "%s = (%s)%s;".format(s(symbol), javaType(avroType, true, true, true), result.toString)).mkString("\n")

        val predicate =
          (for ((symbol, _, _) <- symbolTypeResult) yield "(%s != null)".format(s(symbol))).mkString("&&")

        (elseSymbols, elseClause) match {
          case (Some(symbols), Some(clause)) =>
            JavaCode("""(new Object() {
%s
public %s apply() {
%s
if (%s) {
return (new Object() {
%s
public %s apply() {
%s
} }).apply();
}
else {
return (new Object() {
%s
public %s apply() {
%s
} }).apply();
}
} }).apply()""".format(toCheckSymbols,
                       javaType(retType, false, true, true),
                       toCheckExprs,
                       predicate,
                       symbolFields(thenSymbols),
                       javaType(retType, false, true, true),
                       block(thenClause, ReturnMethod.RETURN, retType),
                       symbolFields(symbols),
                       javaType(retType, false, true, true),
                       block(clause, ReturnMethod.RETURN, retType)))

          case (None, None) =>
            JavaCode("""(new Object() {
%s
%s
public Void apply() {
%s
if (%s) {
%s
}
return null;
} }).apply()""".format(toCheckSymbols,
                       symbolFields(thenSymbols),
                       toCheckExprs,
                       predicate,
                       block(thenClause, ReturnMethod.NONE, retType)))

          case _ => throw new RuntimeException("inconsistent call to task(IfNotNull.Context)")
        }
      }

      case Doc.Context(_, _) => {
        JavaCode("null")
      }

      case Error.Context(_, _, message, code) =>
        JavaCode("""(new Object() {
boolean dummy = true;
public Object apply() {
    if (dummy)
        throw new com.opendatagroup.hadrian.errors.PFAUserException("%s", %s);
    return null;
}
}).apply()""",
          StringEscapeUtils.escapeJava(message),
          (if (code == None)
            "scala.None$.MODULE$"
          else
            "scala.Option.apply(%s)".format(code.get)))

      case Try.Context(retType, _, symbols, exprs, exprType, filter) =>
        JavaCode("""com.opendatagroup.hadrian.jvmcompiler.W.trycatch(new scala.runtime.AbstractFunction0<%s>() {
%s
public %s apply() {
%s
} }, %s, new String[]{%s})""",
          javaType(exprType, true, true, false),
          symbolFields(symbols),
          javaType(exprType, true, true, false),
          block(exprs, ReturnMethod.RETURN, exprType),
          if (filter == None) "false" else "true",
          filter match {
            case Some(x) => x map {y => "\"" + StringEscapeUtils.escapeJava(y) + "\""} mkString(", ")
            case None => ""
          })

      case Log.Context(_, _, symbols, exprTypes, namespace) => {
        var space = ""
        JavaCode("""(new Object() {
%s
public Void apply() {
    String out = "";
%s
    log().apply(out, %s);
    return null;
}
}).apply()""",
          symbolFields(symbols),
          (for ((code: JavaCode, exprType) <- exprTypes) yield
            if (exprType.isInstanceOf[ExceptionType])
              code.toString + ";"
            else {
              val out = "    out += \"%s\" + toJson(%s, %s);".format(space, code.toString, javaSchema(exprType, false))
              space = " "
              out
            }).mkString("\n"),
          (if (namespace == None)
            "scala.None$.MODULE$"
          else
            "scala.Option.apply(\"%s\")".format(StringEscapeUtils.escapeJava(namespace.get))))
      }
    }
  }

}
