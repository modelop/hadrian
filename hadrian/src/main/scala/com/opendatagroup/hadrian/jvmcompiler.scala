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

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Random

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.QuoteMode

import org.codehaus.jackson.JsonNode
import org.codehaus.janino.JavaSourceClassLoader
import org.codehaus.janino.util.resource.Resource
import org.codehaus.janino.util.resource.ResourceFinder

import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumReader
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
import com.opendatagroup.hadrian.ast.CellSource
import com.opendatagroup.hadrian.ast.PoolSource
import com.opendatagroup.hadrian.ast.EmbeddedJsonDomCellSource
import com.opendatagroup.hadrian.ast.EmbeddedJsonDomPoolSource
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

import com.opendatagroup.hadrian.datatype.AvroConversions
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

import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.data.AvroDataTranslator
import com.opendatagroup.hadrian.data.AvroOutputDataStream
import com.opendatagroup.hadrian.data.JsonOutputDataStream
import com.opendatagroup.hadrian.data.CsvOutputDataStream
import com.opendatagroup.hadrian.data.OutputDataStream
import com.opendatagroup.hadrian.data.PFADataTranslator
import com.opendatagroup.hadrian.data.PFADatumReader
import com.opendatagroup.hadrian.data.PFADatumWriter
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.data.PFASpecificData
import com.opendatagroup.hadrian.data.toJsonDom
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
import com.opendatagroup.hadrian.signature.PFAVersion
import com.opendatagroup.hadrian.util.escapeJson
import com.opendatagroup.hadrian.yaml.yamlToJson

package object jvmcompiler {
  /** Version of the PFA language that this Hadrian JAR attempts to use by default.
    */
  var defaultPFAVersion = "0.8.1"

  //////////////////////////////////////////////////////////// Janino-based compiler tools

  /** Wraps debugging Java code with line numbers for readability.
    * 
    * @param src source code string for a Java in-memory file that needs line numbers for readability
    */
  def lineNumbers(src: String): String =
    src.split("\n").zipWithIndex map { case (line, i) => "%3d:  %s".format(i + 1, line) } mkString("\n")

  /** Collection of functions for prefixing names in Java, to avoid name conflicts.
    */
  object JVMNameMangle {
    /** Prefixes symbol names (variables) with `"s_"` to avoid name conflicts.
      */
    def s(symbolName: String): String = "s_" + symbolName
    /** Prefixes function parameter names with `"par_"` to avoid name conflicts.
      */
    def par(parameterName: String): String = "par_" + parameterName
    /** Prefixes cell names with `"c_"` to avoid name conflicts.
      */
    def c(cellName: String): String = "c_" + cellName
    /** Prefixes pool names with `"p_"` to avoid name conflicts.
      */
    def p(poolName: String): String = "p_" + poolName
    /** Prefixes user-defined function names with `"f_"` to avoid name conflicts.
      */
    def f(functionName: String): String = "f_" + functionName.replace(".", "$")

    // Type names must be equal to the fully-qualified name from the Schema or else
    // Avro does bad things (SpecificData.createSchema, comment mentions "hack").
    /** Fully qualifies Avro type names (but does not prefix them, because those names can be used outside of PFA).
      * 
      * @param namespace optional namespace
      * @param name name of the type
      * @param qualified if `true`, apply qualification; if `false`, just pass through
      */
    def t(namespace: Option[String], name: String, qualified: Boolean): String =
      t(namespace match {case None => null.asInstanceOf[String]; case Some(x) => x}, name, qualified)

    /** Fully qualifies Avro type names (but does not prefix them, because those names can be used outside of PFA).
      * 
      * @param namespace optional namespace (`null` or empty string are taken as no namespace)
      * @param name name of the type
      * @param qualified if `true`, apply qualification; if `false`, just pass through
      */
    def t(namespace: String, name: String, qualified: Boolean): String =
      if (qualified)
        (namespace match {case null | "" => ""; case x => x + "."}) + name
      else
        name
  }

  /** Express a [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] as its corresponding Java type.
    * 
    * @param avroType the type to express as a Java code string
    * @param boxed if `true`, use boxed primitives; if `false`, use raw primitives
    * @param qualified if `true`, fully qualify the name; if `false`, do not
    * @param generic if `true`, use a generic (abstract superclass) name; if `false`, use the most specific name
    */
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

  /** Express a [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] as Java code that constructs the corresponding `org.apache.avro.Schema` inline.
    * 
    * @param avroType the type to express as Java code that builds an Avro library Schema
    * @param construct if `true`, actually build named types; if `false`, merely qutoe their names
    */
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

  /** Create Java code to create a locator mark, pointing to the line number in the original source code.
    * 
    * @param pos optional position
    * @return Java code to call `"scala.Option.apply(string)"` to make `Some(string)` or `"scala.Option.apply(null)"` to make a `None`
    */
  def posToJava(pos: Option[String]): String = pos match {
    case None => "scala.Option.apply((String)null)"
    case Some(x) => "scala.Option.apply(\"" + StringEscapeUtils.escapeJava(x) + "\")"
  }
}

package jvmcompiler {
  //////////////////////////////////////////////////////////// interfaces to be used on the Java side
  /** Collection of utility functions to make Java code or be called by Java for routine tasks.
    * 
    * Don't ask why it's called `W`.
    */
  object W {
    /** Take an expression and return nothing (`Unit`, like a Java void function).
      */
    def s[X](x: X): Unit = {}

    /** Take an expression and return `null`.
      */
    def n[X](x: X): java.lang.Void = null
    /** Take two expressions and return `null`.
      */
    def nn[X, Y](x: X, y: Y): java.lang.Void = null

    /** Take two expressions and return the value of the second one.
      */
    def do2[X, Y](x: X, y: Y): Y = y

    /** Take a Scala `Either[Exception, X]` and throw the exception, if it contains an exception, or return the value if it contains a value.
      */
    def either[X](value: Either[Exception, X]): X = value match {
      case Left(err) => throw err
      case Right(x) => x
    }

    /** Take a Scala `Either[Exception, X]` and throw the exception, if it contains an exception, or return the value if it contains a value.
      * 
      * If the exception is `java.lang.IndexOutOfBoundsException` or `java.util.NoSuchElementException`, use the given error strings and error codes to make an informative [[com.opendatagroup.hadrian.errors.PFARuntimeException PFARuntimeException]].
      * 
      * @param value the tagged union of an exception or value
      * @param arrayErrStr string to use in the case of a `java.lang.IndexOutOfBoundsException`
      * @param arrayErrCode code to use in the case of a `java.lang.IndexOutOfBoundsException`
      * @param mapErrStr string to use in the case of a `java.util.NoSuchElementException`
      * @param mapErrCode code to use in the case of a `java.util.NoSuchElementException`
      * @param fcnName function name to report in the PFARuntimeException
      * @param pos source file location from the locator mark
      */
    def either[X](value: Either[Exception, X], arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): X = value match {
      case Left(err: java.lang.IndexOutOfBoundsException) => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
      case Left(err: java.util.NoSuchElementException) => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      case Left(err) => throw err
      case Right(x) => x
    }

    /** Attempt to extract a value by key from a map, returning `orElse` if it's not there.
      * 
      * @param map the map to extract from
      * @param get the key to extract
      * @param orElse the alternate value
      */
    def getOrElse(map: java.util.Map[String, AnyRef], get: String, orElse: AnyRef): AnyRef = {
      if (map.containsKey(get))
        map.get(get)
      else
        orElse
    }
    /** Attempt to extract a value by key from a map (assuming it's a pool), raising a [[com.opendatagroup.hadrian.errors.PFARuntimeException PFARuntimeException]] (with appropriate error string and code) if it's not there.
      * 
      * @param map the map to extract from
      * @param get the key to extract
      * @param name the name of the pool (not used)
      * @param poolErrStr string to use for the [[com.opendatagroup.hadrian.errors.PFARuntimeException PFARuntimeException]]
      * @param poolErrCode code to use for the [[com.opendatagroup.hadrian.errors.PFARuntimeException PFARuntimeException]]
      * @param fcnName function name to use for the [[com.opendatagroup.hadrian.errors.PFARuntimeException PFARuntimeException]]
      * @param pos source file location from the locator mark
      */
    def getOrFailPool(map: java.util.Map[String, AnyRef], get: String, name: String, poolErrStr: String, poolErrCode: Int, fcnName: String, pos: Option[String]): AnyRef = {
      if (map.containsKey(get))
        map.get(get)
      else
        throw new PFARuntimeException(poolErrStr, poolErrCode, fcnName, pos)
    }

    /** Convert any boolean `x` as a raw primitive boolean.
      */
    def asBool(x: Boolean): Boolean = x
    /** Convert any boolean `x` as a raw primitive boolean.
      */
    def asBool(x: java.lang.Boolean): Boolean = x.booleanValue
    /** Convert any boolean `x` as a raw primitive boolean.
      */
    def asBool(x: AnyRef): Boolean = x.asInstanceOf[java.lang.Boolean].booleanValue

    /** Convert any boolean `x` as a boxed primitive boolean.
      */
    def asJBool(x: Boolean): java.lang.Boolean = java.lang.Boolean.valueOf(x)
    /** Convert any boolean `x` as a boxed primitive boolean.
      */
    def asJBool(x: java.lang.Boolean): java.lang.Boolean = x
    /** Convert any boolean `x` as a boxed primitive boolean.
      */
    def asJBool(x: AnyRef): java.lang.Boolean = x.asInstanceOf[java.lang.Boolean]

    /** Convert any integer `x` as a raw primitive int.
      */
    def asInt(x: Int): Int = x
    /** Convert any integer `x` as a raw primitive int.
      */
    def asInt(x: Long): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range", 1000, "", None)
      else
        x.toInt
    /** Convert any integer `x` as a raw primitive int.
      */
    def asInt(x: java.lang.Integer): Int = x.intValue
    /** Convert any integer `x` as a raw primitive int.
      */
    def asInt(x: java.lang.Long): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range", 1000, "", None)
      else
        x.intValue
    /** Convert any integer `x` as a raw primitive int.
      */
    def asInt(x: AnyRef): Int = x match {
      case x1: java.lang.Integer => x1.intValue
      case x2: java.lang.Long => asInt(x2)
    }

    /** Convert any integer `x` as a boxed primitive int.
      */
    def asJInt(x: Int): java.lang.Integer = java.lang.Integer.valueOf(x)
    /** Convert any integer `x` as a boxed primitive int.
      */
    def asJInt(x: Long): java.lang.Integer =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range", 1000, "", None)
      else
        java.lang.Integer.valueOf(x.toInt)
    /** Convert any integer `x` as a boxed primitive int.
      */
    def asJInt(x: java.lang.Integer): java.lang.Integer = x
    /** Convert any integer `x` as a boxed primitive int.
      */
    def asJInt(x: java.lang.Long): java.lang.Integer =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("integer out of range", 1000, "", None)
      else
        java.lang.Integer.valueOf(x.intValue)
    /** Convert any integer `x` as a boxed primitive int.
      */
    def asJInt(x: AnyRef): java.lang.Integer = x match {
      case x1: java.lang.Integer => x1
      case x2: java.lang.Long => asJInt(x2)
    }

    /** Convert an object `x` into a boxed primitive int if it is a boxed number.
      */
    def maybeJInt(x: AnyRef): AnyRef = x match {
      case null => x
      case x1: java.lang.Integer => x1
      case _ => x
    }

    /** Convert any integer `x` as a raw primitive long.
      */
    def asLong(x: Int): Long = x.toLong
    /** Convert any integer `x` as a raw primitive long.
      */
    def asLong(x: Long): Long = x
    /** Convert any integer `x` as a raw primitive long.
      */
    def asLong(x: java.lang.Integer): Long = x.longValue
    /** Convert any integer `x` as a raw primitive long.
      */
    def asLong(x: java.lang.Long): Long = x.longValue
    /** Convert any integer `x` as a raw primitive long.
      */
    def asLong(x: AnyRef): Long = x match {
      case x1: java.lang.Integer => x1.longValue
      case x2: java.lang.Long => x2.longValue
    }

    /** Convert any integer `x` as a boxed primitive long.
      */
    def asJLong(x: Int): java.lang.Long = java.lang.Long.valueOf(x)
    /** Convert any integer `x` as a boxed primitive long.
      */
    def asJLong(x: Long): java.lang.Long = java.lang.Long.valueOf(x)
    /** Convert any integer `x` as a boxed primitive long.
      */
    def asJLong(x: java.lang.Integer): java.lang.Long = java.lang.Long.valueOf(x.intValue)
    /** Convert any integer `x` as a boxed primitive long.
      */
    def asJLong(x: java.lang.Long): java.lang.Long = x
    /** Convert any integer `x` as a boxed primitive long.
      */
    def asJLong(x: AnyRef): java.lang.Long = x match {
      case x1: java.lang.Integer => java.lang.Long.valueOf(x1.longValue)
      case x2: java.lang.Long => x2
    }

    /** Convert object `x` into a boxed primitive long if it is a boxed number.
      */
    def maybeJLong(x: AnyRef): AnyRef = x match {
      case null => x
      case x1: java.lang.Integer => java.lang.Long.valueOf(x1.longValue)
      case x2: java.lang.Long => x2
      case _ => x
    }

    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: Int): Float = x.toFloat
    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: Long): Float = x.toFloat
    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: Float): Float = x
    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: java.lang.Integer): Float = x.floatValue
    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: java.lang.Long): Float = x.floatValue
    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: java.lang.Float): Float = x.floatValue
    /** Convert any number `x` as a raw primitive float.
      */
    def asFloat(x: AnyRef): Float = x match {
      case x1: java.lang.Integer => x1.floatValue
      case x2: java.lang.Long => x2.floatValue
      case x3: java.lang.Float => x3.floatValue
    }

    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: Int): java.lang.Float = java.lang.Float.valueOf(x)
    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: Long): java.lang.Float = java.lang.Float.valueOf(x)
    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: Float): java.lang.Float = java.lang.Float.valueOf(x)
    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: java.lang.Integer): java.lang.Float = java.lang.Float.valueOf(x.intValue)
    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: java.lang.Long): java.lang.Float = java.lang.Float.valueOf(x.longValue)
    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: java.lang.Float): java.lang.Float = x
    /** Convert any number `x` as a boxed primitive float.
      */
    def asJFloat(x: AnyRef): java.lang.Float = x match {
      case x1: java.lang.Integer => java.lang.Float.valueOf(x1.floatValue)
      case x2: java.lang.Long => java.lang.Float.valueOf(x2.floatValue)
      case x3: java.lang.Float => x3
    }

    /** Convert object `x` into a boxed primtive float if it is a boxed number.
      */
    def maybeJFloat(x: AnyRef): AnyRef = x match {
      case null => x
      case x1: java.lang.Integer => java.lang.Float.valueOf(x1.floatValue)
      case x2: java.lang.Long => java.lang.Float.valueOf(x2.floatValue)
      case x3: java.lang.Float => x3
      case _ => x
    }

    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: Int): Double = x.toDouble
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: Long): Double = x.toDouble
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: Float): Double = x.toDouble
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: Double): Double = x
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: java.lang.Integer): Double = x.doubleValue
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: java.lang.Long): Double = x.doubleValue
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: java.lang.Float): Double = x.doubleValue
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: java.lang.Double): Double = x.doubleValue
    /** Convert any number `x` as a raw primitive double.
      */
    def asDouble(x: AnyRef): Double = x match {
      case x1: java.lang.Integer => x1.doubleValue
      case x2: java.lang.Long => x2.doubleValue
      case x3: java.lang.Float => x3.doubleValue
      case x4: java.lang.Double => x4.doubleValue
    }

    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: Int): java.lang.Double = java.lang.Double.valueOf(x)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: Long): java.lang.Double = java.lang.Double.valueOf(x)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: Float): java.lang.Double = java.lang.Double.valueOf(x)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: Double): java.lang.Double = java.lang.Double.valueOf(x)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: java.lang.Integer): java.lang.Double = java.lang.Double.valueOf(x.intValue)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: java.lang.Long): java.lang.Double = java.lang.Double.valueOf(x.longValue)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: java.lang.Float): java.lang.Double = java.lang.Double.valueOf(x.floatValue)
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: java.lang.Double): java.lang.Double = x
    /** Convert any number `x` as a boxed primtive double.
      */
    def asJDouble(x: AnyRef): java.lang.Double = x match {
      case x1: java.lang.Integer => java.lang.Double.valueOf(x1.doubleValue)
      case x2: java.lang.Long => java.lang.Double.valueOf(x2.doubleValue)
      case x3: java.lang.Float => java.lang.Double.valueOf(x3.doubleValue)
      case x4: java.lang.Double => x4
    }

    /** Convert object `x` into a boxed primtive double if it is a boxed number.
      */
    def maybeJDouble(x: AnyRef): AnyRef = x match {
      case null => x
      case x1: java.lang.Integer => java.lang.Double.valueOf(x1.doubleValue)
      case x2: java.lang.Long => java.lang.Double.valueOf(x2.doubleValue)
      case x3: java.lang.Float => java.lang.Double.valueOf(x3.doubleValue)
      case x4: java.lang.Double => x4
      case _ => x
    }

    /** Create Java code that casts `Strings` or converts numbers to a given type.
      * 
      * @param x Java code for an expression that needs to be cast or converted
      * @param avroType type to cast or convert it into
      * @param boxed if `true`, produce boxed primitives; if `false`, produce raw primitives
      * @return Java code as a `String`
      */
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
      case (u: AvroUnion, _) =>
        if (u.types.exists(_.isInstanceOf[AvroDouble]))
          "com.opendatagroup.hadrian.jvmcompiler.W.maybeJDouble(" + x + ")"
        else if (u.types.exists(_.isInstanceOf[AvroFloat]))
          "com.opendatagroup.hadrian.jvmcompiler.W.maybeJFloat(" + x + ")"
        else if (u.types.exists(_.isInstanceOf[AvroLong]))
          "com.opendatagroup.hadrian.jvmcompiler.W.maybeJLong(" + x + ")"
        else if (u.types.exists(_.isInstanceOf[AvroInt]))
          "com.opendatagroup.hadrian.jvmcompiler.W.maybeJInt(" + x + ")"
        else
          x
      case (AvroString(), _) => "(String)(" + x + ")"
      case _ => x
    }

    /** Determine if runtime exception `err` is included in a list of `filters`.
      * 
      * @param err the runtime exception to check
      * @param filters exception texts or code numbers (as strings)
      * @return `true` if `err` is included in the `filters`; `false` otherwise
      */
    def containsException(err: PFARuntimeException, filters: Array[String]) = filters exists {filter =>
      try {
        val code = java.lang.Integer.parseInt(filter)
        err.code == code
      }
      catch {
        case _: java.lang.NumberFormatException => err.message == filter
      }
    }

    /** Determine if user-defined exception `err` is included in a list of `filters`.
      * 
      * User-defined exceptions do not necessarily have code numbers; in which case, only text strings are checked.
      * 
      * @param err the user-defined exception to check
      * @param filters exception texts or code numbers (as strings)
      * @return `true` if `err` is included in the `filters`; `false` otherwise
      */
    def containsException(err: PFAUserException, filters: Array[String]) = filters exists {filter =>
      err.code match {
        case None => err.message == filter
        case Some(errcode) =>
          try {
            val code = java.lang.Integer.parseInt(filter)
            errcode == code
          }
          catch {
            case _: java.lang.NumberFormatException => err.message == filter
          }
      }
    }

    /** Try to evaluate function `f` and catch any PFA exceptions.
      * 
      * @param f the function to evaluate (code wrapped in a Scala function object)
      * @param hasFilter if `true`, only catch exceptions referred to in `filters`; if `false`, catch any [[com.opendatagroup.hadrian.errors.PFARuntimeException PFARuntimeException]] or [[com.opendatagroup.hadrian.errors.PFAUserException PFAUserException]]
      * @param filters array of exceptions to catch, referred to by exception text or code number, represented as a string
      */
    def trycatch[X <: AnyRef](f: () => X, hasFilter: Boolean, filters: Array[String]): X = {
      try {
        f()
      }
      catch {
        case _: PFARuntimeException | _: PFAUserException if (!hasFilter) => null.asInstanceOf[X]
        case err: PFARuntimeException if (hasFilter  &&  containsException(err, filters)) => null.asInstanceOf[X]
        case err: PFAUserException if (hasFilter  &&  containsException(err, filters)) => null.asInstanceOf[X]
      }
    }
  }

  /** Abstract class for interface [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]]; defines some functions in the Hadrian JAR so they don't have to be defined in auto-generated Java.
    */
  abstract class PFAEngineBase {
    /** An `org.apache.avro.specific.SpecificData` object specialized for this PFAEngine in its personal `java.lang.ClassLoader`.
      */
    val specificData = new PFASpecificData(getClass.getClassLoader)
    /** A `org.apache.avro.specific.SpecificDatumReader` object specialized for this PFAEngine in its personal `java.lang.ClassLoader`.
      */
    def datumReader[INPUT <: AnyRef]: DatumReader[INPUT] = new PFADatumReader[INPUT](specificData)

    /** Check the clock and raise a [[com.opendatagroup.hadrian.errors.PFATimeoutException PFATimeoutException]] if a method has taken too long.
      * 
      * Called in loops and user-defined functions.
      */
    def checkClock(): Unit

    /** Externally supplied function for handling log output from PFA.
      * 
      * By default, prints to standard out.
      * 
      * '''Arguments:'''
      * 
      *  - String to write to log
      *  - `Some(namespace)` for filtering log messages or `None`
      */
    var log: Function2[String, Option[String], Unit] =
      (str, namespace) => namespace match {
        case Some(x) => println(x + ": " + str)
        case None => println(str)
      }

    /** Abstract syntax tree that was used to create this engine.
      */
    def config: EngineConfig = _config
    private var _config: EngineConfig = null
    /** Implementation-specific configuration options.
      */
    def options: EngineOptions = _options
    private var _options: EngineOptions = null
    /** Graph of which functions can call which other functions in the engine.
      * 
      * Map from function name (special forms in parentheses) to the set of all functions it calls. This map can be traversed as a graph by repeated application.
      */
    def callGraph: Map[String, Set[String]] = _callGraph
    private var _callGraph: Map[String, Set[String]] = null
    /** The parser used to interpret Avro types in the PFA document, which may be used to find compiled types used by this engine.
      */
    def typeParser: ForwardDeclarationParser = _typeParser
    private var _typeParser: ForwardDeclarationParser = null

    /** Instance number, non-zero if this engine is part of a collection of scoring engines make from the same PFA file.
      */
    def instance: Int = _instance
    var _instance: Int = -1
    /** The "metadata" field of the original PFA file as a [[com.opendatagroup.hadrian.data.PFAMap PFAMap]] that can be accessed within PFA.
      */
    def metadata: PFAMap[String] = _metadata
    var _metadata: PFAMap[String] = null
    /** The "actionsStarted" symbol, counting the number of times the "action" method has started, which can be accessed within PFA.
      */
    def actionsStarted: Long = _actionsStarted
    var _actionsStarted: Long = 0L
    /** The "actionsFinished" symbol, counting the number of times the "action" method has finished without exceptions, which can be accessed within PFA.
      */
    def actionsFinished: Long = _actionsFinished
    var _actionsFinished: Long = 0L

    /** Determine which functions are called by `fcnName` by traversing the `callGraph` backward.
      * 
      * @param fcnName name of function to look up
      * @param exclude set of functions to exclude
      * @return set of functions that can call `fcnName`
      */
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

    /** Determine call depth of a function by traversing the `callGraph`.
      * 
      * @param fcnName name of function to look up
      * @param exclude set of functions to exclude
      * @param startingDepth used by recursion to count
      * @return integral number representing call depth as a `Double`, with positive infinity as a possible result
      */
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
    /** Determine if a function is directly recursive.
      * 
      * @param fcnName name of function to check
      * @return `true` if the function directly calls itself, `false` otherwise
      */
    def isRecursive(fcnName: String): Boolean = calledBy(fcnName).contains(fcnName)
    /** Determine if the call depth of a function is infinite.
      * 
      * @param fcnName name of function to check
      * @return `true` if the function can eventually call itself through a function that it calls, `false` otherwise
      */
    def hasRecursive(fcnName: String): Boolean = callDepth(fcnName) == java.lang.Double.POSITIVE_INFINITY
    /** Determine if a function modifies the scoring engine's persistent state.
      * 
      * @param fcnName name of function to check
      * @return `true` if the function can eventually call `(cell-to)` or `(pool-to)` on any cell or pool.
      */
    def hasSideEffects(fcnName: String): Boolean = {
      val reach = calledBy(fcnName)
      reach.contains(CellTo.desc)  ||  reach.contains(PoolTo.desc)  ||  reach.contains(PoolDel.desc)
    }

    /** Get the [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] of each compiled type. */
    def namedTypes: Map[String, AvroType] = _namedTypes
    private var _namedTypes: Map[String, AvroType] = null
    /** Get the [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] of the input. */
    def inputType: AvroType = _inputType
    private var _inputType: AvroType = null
    /** Get the [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] of the output. */
    def outputType: AvroType = _outputType
    private var _outputType: AvroType = null
    /** Report whether this is a [[com.opendatagroup.hadrian.jvmcompiler.PFAMapEngine PFAMapEngine]], [[com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine PFAEmitEngine]], or a [[com.opendatagroup.hadrian.jvmcompiler.PFAFoldEngine PFAFoldEngine]] */
    def method: Method.Method = _method
    private var _method: Method.Method = null

    /** The random number generator used by this particular scoring engine instance.
      * 
      * Note that if a `randseed` is given in the PFA file but a collection of scoring engines are generated from it, each scoring engine instance will have a ''different'' random generator seeded by a ''different'' seed.
      */
    def randomGenerator = _randomGenerator
    private var _randomGenerator: Random = null

    /** Map from cell name to reflected Java field for the cells that should be rolled back in case of an exception.
      */
    val cellsToRollback = mutable.Map[String, java.lang.reflect.Field]()
    /** Map from pool name to reflected Java field for the pools that should be rolled back in case of an exception.
      */
    val poolsToRollback = mutable.Map[String, java.lang.reflect.Field]()
    /** Temporary copies of initial cell state for cells to be rolled back.
      */
    val savedCells = mutable.Map[String, Any]()
    /** Temporary copies of initial pool state for cells to be rolled back.
      */
    val savedPools = mutable.Map[String, java.util.HashMap[String, AnyRef]]()
    private val privateCells = mutable.Map[String, java.lang.reflect.Field]()
    private var publicCells: SharedMap = null
    private val privatePools = mutable.Map[String, java.util.HashMap[String, AnyRef]]()
    private val publicPools = mutable.Map[String, SharedMap]()

    private var _thisClass: java.lang.Class[_] = null
    private var _context: EngineConfig.Context = null

    /** Revert a scoring engine to the state it had when it was first initialized.
      */
    def revert(): Unit = revert(None)
    /** Revert a scoring engine to the state it had when it was first initialized.
      * 
      * @param sharedState shared state object to use in re-initialization
      */
    def revert(sharedState: Option[SharedState]): Unit = {
      // get arguments for call to initialize
      val config = _config
      val options = _options
      val thisClass = _thisClass
      val context = _context
      val index = _instance

      // reset (almost) all fields and reinitialize

      // skip specificData; it doesn't have a mutable state
      // skip log; the user sets this one

      _config = null
      _options = null
      _callGraph = null
      _typeParser = null

      _instance = -1
      _metadata = null
      _actionsStarted = 0L   // not set by initialize, but 0L is the starting value
      _actionsFinished = 0L

      _namedTypes = null
      _inputType = null
      _outputType = null
      _method = null

      _randomGenerator = null

      cellsToRollback.clear()
      poolsToRollback.clear()
      savedCells.clear()     // not set by initialize, and in fact rollbackSave() clears it so this is redundant
      savedPools.clear()
      privateCells.clear()
      publicCells = null
      privatePools.clear()
      publicPools.clear()

      _thisClass = null
      _context = null

      // not these; the Java-side constructor fills these in
      // _inputClass = null
      // _outputClass = null

      // skip classLoader; although it's declared as a val, it is implemented as a function on the Java side
      pfaInputTranslator = null
      avroInputTranslator = null

      initialize(config, options, sharedState, thisClass, context, index)
    }

    /** Internally called by a new PFAEngine when it is first created.
      * 
      * @param config abstract syntax tree representing the contents of the PFA file
      * @param options initialization options that will be used to ''override'' any found in the PFA file
      * @param sharedState optional shared state object used to link scoring engines (can include objects that maintain state through external databases)
      * @param thisClass reference to this instance's class, via Java reflection
      * @param context context object from the type-checked abstract syntax tree
      * @param index instance number to assign
      */
    def initialize(config: EngineConfig, options: EngineOptions, sharedState: Option[SharedState], thisClass: java.lang.Class[_], context: EngineConfig.Context, index: Int): Unit = {
      _config = config
      _options = options
      _thisClass = thisClass
      _context = context
      _instance = index

      val mergeCalls = context.merge match {
        case Some((_, _, x)) => Map("(merge)" -> x)
        case None => Map[String, Set[String]]()
      }
      _callGraph =
        (context.fcns map {case (fname, fctx) => (fname, fctx.calls)}) ++
          Map("(begin)" -> context.begin._3) ++
          Map("(action)" -> context.action._3) ++
          Map("(end)" -> context.end._3) ++
          mergeCalls

      _typeParser = context.parser

      _metadata = PFAMap.fromMap(config.metadata)

      // make sure that functions used in CellTo and PoolTo do not themselves call CellTo, PoolTo, or PoolDel (which could lead to deadlock)
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

        val value = cell.init.value(specificData)
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
          cell.init.value(specificData)
        })
      publicCells = sharedCells

      // fill the unshared pools
      for ((pname, pool) <- config.pools if (!pool.shared)) {
        val field = thisClass.getDeclaredField(JVMNameMangle.p(pname))
        field.setAccessible(true)

        val hashMap = new java.util.HashMap[String, AnyRef]()
        field.set(this, hashMap)
        pool.init.initialize(specificData)
        for (key <- pool.init.keys)
          hashMap.put(key, pool.init.value(key))

        if (pool.rollback)
          poolsToRollback(pname) = field
        privatePools(pname) = hashMap
      }

      // fill the shared pools
      for ((pname, pool) <- config.pools if (pool.shared)) {
        val state = sharedState match {
          case Some(x) => x.pools.get(pname) match {
            case Some(y) => y
            case None => throw new PFAInitializationException("shared state object provided by host has no pool named \"%s\"".format(pname))
          }
          case None => new SharedMapInMemory
        }

        thisClass.getDeclaredField(JVMNameMangle.p(pname)).set(this, state)
        pool.init.initialize(specificData)
        state.initialize(pool.init.keys, (name: String) => pool.init.value(name))
        publicPools(pname) = state
      }

      _inputType = context.input
      _outputType = context.output
      _namedTypes = context.compiledTypes map {x => (x.fullName, x)} toMap

      _method = config.method

      pfaInputTranslator = new PFADataTranslator(inputType, classLoader)
      avroInputTranslator = new AvroDataTranslator(inputType, classLoader)

      // ...

      config.randseed match {
        case Some(x) =>
          _randomGenerator = new Random(x)
          0 until index foreach {skip => _randomGenerator = new Random(_randomGenerator.nextInt)}
        case None => _randomGenerator = new Random()
      }
    }

    /** Internally called to save the state of some cells and pools so that they can be rolled back in case of an exception.
      */
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

    /** Internally called to actually roll back the state of the scoring engine.
      */
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

    /** Access to the scoring engine's global lock.
      * 
      * Used by `snapshot` and other methods that need to block changes in the scoring engine.
      */
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

    /** Perform an analysis of a cell using a user-defined function.
      * 
      * @param name the name of the cell
      * @param analysis a function to perform some analysis of the cell to which it is applied; note that this function '''must not''' change the cell's state
      * @return whatever `analysis` returns
      * 
      * Note that the `analysis` function is called exactly once.
      */
    def analyzeCell[X](name: String, analysis: Any => X): X = getRunlock.synchronized {
      analysis(cellState(name, config.cells(name).shared))
    }

    /** Perform an analysis of a pool using a user-defined functions.
      * 
      * @param name the name of the pool
      * @param analysis a function to perform some analysis of each item in the pool; note that this function '''must not''' change the pool's state
      * @return a map from pool item name to whatever `analysis` returns for that pool item
      * 
      * Note that the `analysis` function is called as many times as there are items in the pool.
      */
    def analyzePool[X](name: String, analysis: Any => X): Map[String, X] = getRunlock.synchronized {
      poolState(name, config.pools(name).shared) map {case (k, v) => (k, analysis(v))}
    }

    /** Take a snapshot of one cell and represent it using objects specialized to this class (see above).
      * 
      * @param name the name of the cell
      * @return an object that may contain internal PFA data, such as instances of classes that are only found in this engine's custom `classLoader`.
      */
    def snapshotCell(name: String): AnyRef = toBoxed(cellState(name))

    /** Take a snapshot of one pool and represent it using objects specialized to this class (see above).
      * 
      * @param name the name of the pool
      * @return a Map from pool item name to objects that may contain internal PFA data, such as instances of classes that are only found in this engine's custom `classLoader`.
      */
    def snapshotPool(name: String): Map[String, AnyRef] = getRunlock.synchronized { poolState(name) } map {case (k, v) => (k, toBoxed(v))}

    /** Take a snapshot of the entire scoring engine (all cells and pools) and represent it as an abstract syntax tree that can be used to make new scoring engines.
      * 
      * Note that you can call `toJson` on the `EngineConfig` to get a string that can be written to a PFA file.
      */
    def snapshot(): EngineConfig = getRunlock.synchronized {
      val newCells = config.cells map {
        case (cname, Cell(avroPlaceholder, _, shared, rollback, source, _)) =>
          (cname, Cell(avroPlaceholder, EmbeddedJsonDomCellSource(toJsonDom(toBoxed(cellState(cname, shared)), avroPlaceholder.avroType), avroPlaceholder), shared, rollback, source))
      }

      val newPools = config.pools map {
        case (pname, Pool(avroPlaceholder, _, shared, rollback, source, _)) =>
          (pname, Pool(avroPlaceholder, EmbeddedJsonDomPoolSource(poolState(pname, shared) map {case (k, v) => (k, toJsonDom(toBoxed(v), avroPlaceholder.avroType))}, avroPlaceholder), shared, rollback, source))
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
        config.merge,
        newCells,
        newPools,
        config.randseed,
        config.doc,
        config.version,
        config.metadata,
        config.options,
        config.pos)
    }

    /** Convert data from JSON to a live object using this engine's custom classes.
      * 
      * @param json JSON data
      * @param schema Avro schema
      * @return object that can be used in this engine
      */
    def fromJson(json: Array[Byte], schema: Schema): AnyRef = {
      val reader = datumReader[AnyRef]
      reader.setSchema(schema)
      val decoder = DecoderFactory.get.jsonDecoder(schema, new ByteArrayInputStream(json))
      reader.read(null, decoder)
    }

    /** Convert data to JSON.
      * 
      * @param obj object reference
      * @param schema Avro schema
      * @return JSON string
      */
    def toJson(obj: AnyRef, schema: Schema): String = {
      val out = new java.io.ByteArrayOutputStream
      val encoder = EncoderFactory.get.jsonEncoder(schema, out)
      val writer = new PFADatumWriter[AnyRef](schema, specificData)
      writer.write(obj, encoder)
      encoder.flush()
      out.toString
    }

    /** Convert data from Avro to a live object using this engine's custom classes.
      * 
      * @param avro Avro data
      * @param schema Avro schema
      * @return object that can be used in this engine
      */
    def fromAvro(avro: Array[Byte], schema: Schema): AnyRef = {
      val reader = datumReader[AnyRef]
      reader.setSchema(schema)
      val decoder = DecoderFactory.get.validatingDecoder(schema, DecoderFactory.get.binaryDecoder(avro, null))
      reader.read(null, decoder)
    }

    /** Convert data to Avro.
      * 
      * @param obj object reference
      * @param schema Avro schema
      * @return Avro bytes
      */
    def toAvro(obj: AnyRef, schema: Schema): Array[Byte] = {
      val out = new java.io.ByteArrayOutputStream
      val encoder = EncoderFactory.get.validatingEncoder(schema, EncoderFactory.get.binaryEncoder(out, null))
      val writer = new PFADatumWriter[AnyRef](schema, specificData)
      writer.write(obj, encoder)
      encoder.flush()
      out.toByteArray
    }

    /** Convert data from JSON to a live object using this engine's custom classes.
      * 
      * @param json JSON data
      * @param avroType data type
      * @return object that can be used in this engine
      */
    def fromJson(json: Array[Byte], avroType: AvroType): AnyRef = fromJson(json, avroType.schema)
    /** Convert data from Avro to a live object using this engine's custom classes.
      * 
      * @param avro Avro data
      * @param avroType data type
      * @return object that can be used in this engine
      */
    def fromAvro(avro: Array[Byte], avroType: AvroType): AnyRef = fromAvro(avro, avroType.schema)
    /** Convert data to JSON.
      * 
      * @param obj object reference
      * @param avroType data type
      * @return JSON string
      */
    def toJson(obj: AnyRef, avroType: AvroType): String         = toJson(obj, avroType.schema)
    /** Convert data to Avro.
      * 
      * @param obj object reference
      * @param avroType data type
      * @return Avro bytes
      */
    def toAvro(obj: AnyRef, avroType: AvroType): Array[Byte]    = toAvro(obj, avroType.schema)

    /** Create an Avro iterator (subclass of `java.util.Iterator`) over Avro-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputStream serialized data
      * @return unserialized data
      */
    def avroInputIterator[X <: AnyRef](inputStream: InputStream): DataFileStream[X] = {    // DataFileStream is a java.util.Iterator
      val reader = datumReader[X]
      reader.setSchema(inputType.schema)
      val out = new DataFileStream[X](inputStream, reader)
      
      val fileSchema = AvroConversions.schemaToAvroType(out.getSchema)
      if (!inputType.accepts(fileSchema))
        throw new PFAInitializationException("InputStream has schema %s\nbut expecting schema %s".format(fileSchema, inputType))
      out
    }

    /** Create an iterator over JSON-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputStream serialized data
      * @return unserialized data
      */
    def jsonInputIterator[X <: AnyRef](inputStream: InputStream): java.util.Iterator[X] = {
      val reader = datumReader[X]
      reader.setSchema(inputType.schema)
      val scanner = new java.util.Scanner(inputStream)

      new java.util.Iterator[X] {
        def hasNext(): Boolean = scanner.hasNextLine
        def next(): X = {
          val json = scanner.nextLine()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
        override def remove(): Unit = throw new java.lang.UnsupportedOperationException
      }
    }

    /** Create an iterator over JSON-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputStream serialized data
      * @return unserialized data
      */
    def jsonInputIterator[X <: AnyRef](inputIterator: java.util.Iterator[String]): java.util.Iterator[X] = {
      val reader = datumReader[X]
      reader.setSchema(inputType.schema)

      new java.util.Iterator[X] {
        def hasNext(): Boolean = inputIterator.hasNext
        def next(): X = {
          val json = inputIterator.next()
          val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
          reader.read(null.asInstanceOf[X], decoder)
        }
        override def remove(): Unit = throw new java.lang.UnsupportedOperationException
      }
    }

    /** Create an iterator over JSON-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputIterator serialized data
      * @return unserialized data
      */
    def jsonInputIterator[X <: AnyRef](inputIterator: scala.collection.Iterator[String]): scala.collection.Iterator[X] = {
      val reader = datumReader[X]
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

    /** Create an iterator over CSV-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * Note that only records of primitives can be read from CSV because of the nature of the CSV format.
      * 
      * @param inputStream serialized data
      * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
      * @return unserialized data
      */
    def csvInputIterator[X <: AnyRef](inputStream: InputStream, csvFormat: CSVFormat = CSVFormat.DEFAULT.withHeader()): java.util.Iterator[X] = {
      val constructor = inputClass.getConstructor(classOf[Array[AnyRef]])
      constructor.setAccessible(true)
      def makeRecord(fieldValues: Array[AnyRef]) =
        constructor.newInstance(fieldValues).asInstanceOf[X]

      data.csvInputIterator(inputStream, inputType, csvFormat, None, Some(makeRecord))
    }

    /** Deserialize one JSON datum as suitable input to the `action` method.
      */
    def jsonInput[INPUT <: AnyRef](json: Array[Byte]): INPUT = fromJson(json, inputType.schema).asInstanceOf[INPUT]
    /** Deserialize one JSON datum as suitable input to the `action` method.
      */
    def jsonInput[INPUT <: AnyRef](json: String): INPUT = fromJson(json.getBytes, inputType.schema).asInstanceOf[INPUT]

    /** Create an output stream for Avro-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param outputStream the raw output stream onto which Avro bytes will be written.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def avroOutputDataStream(outputStream: java.io.OutputStream): AvroOutputDataStream = {
      val writer = new PFADatumWriter[AnyRef](outputType.schema, specificData)
      val out = new AvroOutputDataStream(writer)
      out.create(outputType.schema, outputStream)
      out
    }

    /** Create an output stream for Avro-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param file a file that will be overwritten by output.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def avroOutputDataStream(file: java.io.File): AvroOutputDataStream = {
      val writer = new PFADatumWriter[AnyRef](outputType.schema, specificData)
      val out = new AvroOutputDataStream(writer)
      out.create(outputType.schema, file)
      out
    }

    /** Create an output stream for Avro-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param fileName the name of a file that will be overwritten by output.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def avroOutputDataStream(fileName: String): AvroOutputDataStream = avroOutputDataStream(new java.io.File(fileName))

    /** Create an output stream for JSON-serializing scoring engine output.
      * 
      * Writes one JSON object per line.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param outputStream the raw output stream onto which Avro bytes will be written.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def jsonOutputDataStream(outputStream: java.io.OutputStream, writeSchema: Boolean): JsonOutputDataStream = {
      val encoder = EncoderFactory.get.jsonEncoder(outputType.schema, outputStream)
      val writer = new PFADatumWriter[AnyRef](outputType.schema, specificData)
      val out = new JsonOutputDataStream(writer, encoder, outputStream)
      if (writeSchema) {
        outputStream.write(outputType.toJson.getBytes("utf-8"))
        outputStream.write("\n".getBytes("utf-8"))
      }
      out
    }

    /** Create an output stream for JSON-serializing scoring engine output.
      * 
      * Writes one JSON object per line.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param file a file that will be overwritten by output.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def jsonOutputDataStream(file: java.io.File, writeSchema: Boolean): JsonOutputDataStream = {
      val outputStream = new java.io.FileOutputStream(file)
      val encoder = EncoderFactory.get.jsonEncoder(outputType.schema, outputStream)
      val writer = new PFADatumWriter[AnyRef](outputType.schema, specificData)
      val out = new JsonOutputDataStream(writer, encoder, outputStream)
      if (writeSchema) {
        outputStream.write(outputType.toJson.getBytes("utf-8"))
        outputStream.write("\n".getBytes("utf-8"))
      }
      out
    }

    /** Create an output stream for JSON-serializing scoring engine output.
      * 
      * Writes one JSON object per line.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param fileName the name of a file that will be overwritten by output.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def jsonOutputDataStream(fileName: String, writeSchema: Boolean): JsonOutputDataStream = jsonOutputDataStream(new java.io.File(fileName), writeSchema)

    /** Create an output stream for CSV-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * Note that only records of primitives can be written to CSV because of the nature of the CSV format.
      * 
      * @param outputStream the raw output stream onto which CSV bytes will be written.
      * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
      * @param writeHeader if `true`, write field names as the first line of the file.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      */
    def csvOutputDataStream(outputStream: java.io.OutputStream, csvFormat: CSVFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL), writeHeader: Boolean = true): CsvOutputDataStream =
      data.csvOutputDataStream(outputStream, outputType, csvFormat, writeHeader)

    /** Serialize one datum from the `action` method as JSON. */
    def jsonOutput[OUTPUT <: AnyRef](obj: OUTPUT): String = toJson(obj.asInstanceOf[AnyRef], outputType.schema)

    /** Class object for the input type. */
    def inputClass = _inputClass
    var _inputClass: java.lang.Class[AnyRef] = null
    /** Class object for the output type. */
    def outputClass = _outputClass
    var _outputClass: java.lang.Class[AnyRef] = null

    /** ClassLoader in which this scoring engine and its compiled types are compiled. */
    val classLoader: java.lang.ClassLoader
    private var pfaInputTranslator: PFADataTranslator = null
    private var avroInputTranslator: AvroDataTranslator = null

    /** Translate data that might have come from any PFAEngine class (not necessarily this one) into objects suitable for this PFAEngine's `action`.
      * 
      * @param datum objects that may have been output from another type of PFAEngine's `action`, `snapshotCell`, or `snapshotPool`.
      * @return objects that can be input for this PFAEngine's `action`.
      */
    def fromPFAData[INPUT <: AnyRef](datum: AnyRef): INPUT = pfaInputTranslator.translate(datum).asInstanceOf[INPUT]
    /** Translate data that might have been deserialized by Avro into objects suitable for this PFAEngine's `action`.
      * 
      * @param datum objects that may be Avro generic or Avro specific objects (note that Avro specific objects are subclasses of Avro generic objects)
      * @return objects that can be input for this PFAEngine's `action`.
      */
    def fromGenericAvroData[INPUT <: AnyRef](datum: AnyRef): INPUT = avroInputTranslator.translate(datum).asInstanceOf[INPUT]
  }

  /** Interface for a Hadrian scoring engine.
    * 
    * Create instances using one of [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine$ `PFAEngine`'s "static" methods]], then call `begin` once, `action` once for each datum in the data stream, and `end` once (if the stream ever ends). The rest of the functions are for
    *  - examining the scoring engine (`config`, call graph),
    *  - producing acceptable input from serialized streams, other `PFAEngines`, or the Avro library,
    *  - sending output to a serialized stream,
    *  - handling log output or emit output (see [[com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine PFAEmitEngine]]) with callbacks,
    *  - taking snapshots of the scoring engine's current state,
    *  - reverting its state, and
    *  - calling PFA user-defined functions from external sources.
    * 
    * '''Examples:'''
    * 
    * Load a PFA file as a scoring engine. Note the `.head` to extract the single scoring engine from the `Seq` this function returns.
    * 
    * {{{
    * import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
    * val engine = PFAEngine.fromJson(new java.io.File("myModel.pfa")).head
    * }}}
    * 
    * Assuming (and verifying) that `method` is map, run it over an Avro data stream.
    * 
    * {{{
    * import com.opendatagroup.hadrian.ast.Method
    * assert(engine.method == Method.MAP)
    * 
    * val inputDataStream = engine.avroInputIterator(new java.io.FileInputStream("inputData.avro"))
    * val outputDataStream = engine.avroOutputDataStream(new java.io.File("outputData.avro"))
    * 
    * engine.begin()
    * while (inputDataStream.hasNext)
    *   outputDataStream.append(engine.action(inputDataStream.next()))
    * engine.end()
    * outputDataStream.close()
    * }}}
    * 
    * Handle the case of `method` = emit engines (map and fold are the same).
    * 
    * {{{
    * import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
    * engine match {
    *   case emitEngine: PFAEmitEngine =>
    *     def emit(x) = outputDataStream.append(x)
    *     emitEngine.emit = emit
    *     emitEngine.begin()
    *     while (inputDataStream.hasNext)
    *       emitEngine.action(inputDataStream.next())
    *     emitEngine.end()
    * 
    *   case otherEngine =>
    *     otherEngine.begin()
    *     while (inputDataStream.hasNext)
    *       outputDataStream.append(otherEngine.action(inputDataStream.next()))
    *     otherEngine.end()
    * }
    * outputDataStream.close()
    * }}}
    * 
    * Take a snapshot of a changing model and write it as a new PFA file.
    * 
    * {{{
    * val snapshotFile = new java.io.FileOutputStream("snapshot.pfa")
    * snapshotFile.write(engine.snapshot.toJson(lineNumbers = false))
    * snapshotFile.close()
    * }}}
    * 
    * Take a snapshot of just one cell and write it as a JSON fragment.
    * 
    * {{{
    * import com.opendatagroup.hadrian.data.toJson
    * val myCellFile = new java.io.FileOutputStream("myCell.json")
    * myCellFile.write(toJson(engine.snapshotCell("myCell"), engine.config.cells("myCell").avroType))
    * myCellFile.close()
    * }}}
    * 
    * Calling a PFA user-defined function from an external agent.
    * 
    * {{{
    * // get the callable function object
    * val myFunction = engine.fcn2("myFunction")
    * 
    * // verify that the arguments are what we think they are
    * import com.opendatagroup.hadrian.datatype._
    * assert(engine.config.fcns("myFunction").params, Seq("x" -> AvroDouble(), "y" -> AvroString()))
    * 
    * // call it with some arguments
    * myFunction(java.lang.Double.valueOf(3.14), "hello")
    * }}}
    * 
    * '''Specialized data:'''
    * 
    * Data passed to `action`, `fcn*` or accepted from `action`, `fcn*`, `analyzeCell`, `analyzePool`, `snapshotCell`, `snapshotPool` has to satisfy a particular form. That form is:
    * 
    *  - '''null:''' null-valued `AnyRef`
    *  - '''boolean:''' `java.lang.Boolean`
    *  - '''int:''' `java.lang.Integer`
    *  - '''long:''' `java.lang.Long`
    *  - '''float:''' `java.lang.Float`
    *  - '''double:''' `java.lang.Double`
    *  - '''string:''' `String` 
    *  - '''bytes:''' `Array[Byte]`
    *  - '''array(X):''' [[com.opendatagroup.hadrian.data.PFAArray PFAArray[X]]] where `X` is an unboxed primitive if a primitive
    *  - '''map(X):''' [[com.opendatagroup.hadrian.data.PFAMap PFAMap[X]]] where `X` is a boxed primitive if a primitive
    *  - '''enum:''' subclass of [[com.opendatagroup.hadrian.data.PFAEnumSymbol PFAEnumSymbol]] that can only be found in this specific `PFAEngine`'s custom ClassLoader (cannot be created by external agent)
    *  - '''fixed:''' subclass of [[com.opendatagroup.hadrian.data.PFAFixed PFAFixed]] that can only be found in the custom ClassLoader
    *  - '''record:''' subclass of [[com.opendatagroup.hadrian.data.PFARecord PFARecord]] that can only be found in the custom ClassLoader
    *  - '''union:''' any of the above as an `AnyRef`
    * 
    * Compiled types, namely '''enum''', '''fixed''', and '''record''', have to be converted using `fromPFAData` or `fromGenericAvroData`.
    * 
    * Although all of these types are immutable in PFA, '''bytes''', '''fixed''', and '''record''' are ''mutable'' in Java, but if you modify them, the behavior of the PFA engine is undefined and likely to be wrong. Do not change these objects in place!
    */
  trait PFAEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] {
    /** Externally supplied function for handling log output from PFA.
      * 
      * By default, prints to standard out.
      * 
      * '''Arguments:'''
      * 
      *  - String to write to log
      *  - `Some(namespace)` for filtering log messages or `None`
      */
    var log: Function2[String, Option[String], Unit]

    /** Abstract syntax tree that was used to generate this engine. */
    def config: EngineConfig

    /** Implementation-specific configuration options. */
    def options: EngineOptions

    /** Graph of which functions can call which other functions in the engine.
      * 
      * Map from function name (special forms in parentheses) to the set of all functions it calls. This map can be traversed as a graph by repeated application. */
    def callGraph: Map[String, Set[String]]

    /** Determine which functions are called by `fcnName` by traversing the `callGraph` backward.
      * 
      * @param fcnName name of function to look up
      * @param exclude set of functions to exclude
      * @return set of functions that can call `fcnName`
      */
    def calledBy(fcnName: String, exclude: Set[String] = Set[String]()): Set[String]

    /** Determine call depth of a function by traversing the `callGraph`.
      * 
      * @param fcnName name of function to look up
      * @param exclude set of functions to exclude
      * @param startingDepth used by recursion to count
      * @return integral number representing call depth as a `Double`, with positive infinity as a possible result
      */
    def callDepth(fcnName: String, exclude: Set[String] = Set[String](), startingDepth: Double = 0): Double

    /** Determine if a function is directly recursive.
      * 
      * @param fcnName name of function to check
      * @return `true` if the function directly calls itself, `false` otherwise
      */
    def isRecursive(fcnName: String): Boolean

    /** Determine if the call depth of a function is infinite.
      * 
      * @param fcnName name of function to check
      * @return `true` if the function can eventually call itself through a function that it calls, `false` otherwise
      */
    def hasRecursive(fcnName: String): Boolean

    /** Determine if a function modifies the scoring engine's persistent state.
      * 
      * @param fcnName name of function to check
      * @return `true` if the function can eventually call `(cell-to)` or `(pool-to)` on any cell or pool.
      */
    def hasSideEffects(fcnName: String): Boolean

    /** The parser used to interpret Avro types in the PFA document, which may be used to find compiled types used by this engine. */
    def typeParser: ForwardDeclarationParser

    /** Instance number, non-zero if this engine is part of a collection of scoring engines made from the same PFA file. */
    def instance: Int

    /** Specialized Avro data model for this `PFAEngine`.
      * 
      * Deserializes Avro data into objects this engine can use (see the `*Input`, `*InputIterator` methods to prepare input objects).
      * 
      */
    def specificData: PFASpecificData

    /** Specialized Avro data reader for this `PFAEngine`.
      * 
      * Reads Avro streams into objects this engine can use (see the `*Input`, `*InputIterator` methods to prepare input objects).
      * 
      */
    def datumReader: DatumReader[INPUT]

    /** Class object for the input type. */
    def inputClass: java.lang.Class[AnyRef]

    /** Class object for the output type. */
    def outputClass: java.lang.Class[AnyRef]

    /** ClassLoader in which this scoring engine and its compiled types are compiled. */
    def classLoader: java.lang.ClassLoader

    /** Entry point for starting up a scoring engine: call this first. */
    def begin(): Unit

    /** Entry point for computing one datum: call this after `begin`.
      * 
      * @param input datum to compute; objects must be specialized to this class (see above).
      * @return if `method` is map, returns the transformed input; if `method` is emit, returns `null` (provide a user-defined `emit` callback to capture results!); if `method` is fold, returns the current cumulative `tally`.
      * 
      */
    def action(input: INPUT): OUTPUT

    /** Entry point for ending a scoring engine: call this after all `action` calls are complete.
      * 
      * If the input data stream is infinite, such a time may never happen.
      * 
      */
    def end(): Unit

    /** Take a snapshot of one cell and represent it using objects specialized to this class (see above).
      * 
      * @param name the name of the cell
      * @return an object that may contain internal PFA data, such as instances of classes that are only found in this engine's custom `classLoader`.
      * 
      */
    def snapshotCell(name: String): AnyRef

    /** Take a snapshot of one pool and represent it using objects specialized to this class (see above).
      * 
      * @param name the name of the pool
      * @return a Map from pool item name to objects that may contain internal PFA data, such as instances of classes that are only found in this engine's custom `classLoader`.
      * 
      */
    def snapshotPool(name: String): Map[String, AnyRef]

    /** Take a snapshot of the entire scoring engine (all cells and pools) and represent it as an abstract syntax tree that can be used to make new scoring engines.
      * 
      * Note that you can call `toJson` on the `EngineConfig` to get a string that can be written to a PFA file.
      * 
      */
    def snapshot: EngineConfig

    /** Perform an analysis of a cell using a user-defined function.
      * 
      * @param name the name of the cell
      * @param analysis a function to perform some analysis of the cell to which it is applied; note that this function '''must not''' change the cell's state
      * @return whatever `analysis` returns
      * 
      * Note that the `analysis` function is called exactly once.
      * 
      */
    def analyzeCell[X](name: String, analysis: Any => X): X

    /** Perform an analysis of a pool using a user-defined function.
      * 
      * @param name the name of the pool
      * @param analysis a function to perform some analysis of each item in the pool; note that this function '''must not''' change the pool's state
      * @return a map from pool item name to whatever `analysis` returns for that pool item
      * 
      * Note that the `analysis` function is called as many times as there are items in the pool.
      * 
      */
    def analyzePool[X](name: String, analysis: Any => X): Map[String, X]

    /** Get the [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] of each compiled type. */
    def namedTypes: Map[String, AvroCompiled]

    /** Get the [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] of the input. */
    def inputType: AvroType

    /** Get the [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] of the output. */
    def outputType: AvroType

    /** Report whether this is a [[com.opendatagroup.hadrian.jvmcompiler.PFAMapEngine PFAMapEngine]], [[com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine PFAEmitEngine]], or a [[com.opendatagroup.hadrian.jvmcompiler.PFAFoldEngine PFAFoldEngine]].
      * 
      * Note that this information is also available in `config.method`.
      * 
      */
    def method: Method.Method

    /** Create an Avro iterator (subclass of `java.util.Iterator`) over Avro-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputStream serialized data
      * @return unserialized data
      * 
      */
    def avroInputIterator[X](inputStream: InputStream): DataFileStream[X]    // DataFileStream is a java.util.Iterator

    /** Create an iterator over JSON-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputStream serialized data
      * @return unserialized data
      * 
      */
    def jsonInputIterator[X](inputStream: InputStream): java.util.Iterator[X]

    /** Create an iterator over JSON-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputIterator serialized data
      * @return unserialized data
      * 
      */
    def jsonInputIterator[X](inputIterator: java.util.Iterator[String]): java.util.Iterator[X]

    /** Create an iterator over JSON-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * @param inputIterator serialized data
      * @return unserialized data
      * 
      */
    def jsonInputIterator[X](inputIterator: scala.collection.Iterator[String]): scala.collection.Iterator[X]

    /** Create an iterator over CSV-serialized input data.
      * 
      * The objects produced by this iterator are suitable inputs to the `action` method.
      * 
      * Note that only records of primitives can be read from CSV because of the nature of the CSV format.
      * 
      * @param inputStream serialized data
      * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
      * @return unserialized data
      * 
      */
    def csvInputIterator[X](inputStream: InputStream, csvFormat: CSVFormat = CSVFormat.DEFAULT.withHeader()): java.util.Iterator[X]

    /** Deserialize one JSON datum as suitable input to the `action` method. */
    def jsonInput(json: Array[Byte]): INPUT

    /** Deserialize one JSON datum as suitable input to the `action` method. */
    def jsonInput(json: String): INPUT

    /** Create an output stream for Avro-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param outputStream the raw output stream onto which Avro bytes will be written.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      * 
      */
    def avroOutputDataStream(outputStream: java.io.OutputStream): AvroOutputDataStream

    /** Create an output stream for Avro-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param file a file that will be overwritten by output.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      * 
      */
    def avroOutputDataStream(file: java.io.File): AvroOutputDataStream

    /** Create an output stream for JSON-serializing scoring engine output.
      * 
      * Writes one JSON object per line.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param outputStream the raw output stream onto which JSON bytes will be written.
      * @param writeSchema if `true`, write an Avro schema as the first line of the file for interpreting the JSON objects.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      * 
      */
    def jsonOutputDataStream(outputStream: java.io.OutputStream, writeSchema: Boolean): JsonOutputDataStream

    /** Create an output stream for JSON-serializing scoring engine output.
      * 
      * Writes one JSON object per line.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param file a file that will be overwritten by output.
      * @param writeSchema if `true`, write an Avro schema as the first line of the file for interpreting the JSON objects.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      * 
      */
    def jsonOutputDataStream(file: java.io.File, writeSchema: Boolean): JsonOutputDataStream

    /** Create an output stream for JSON-serializing scoring engine output.
      * 
      * Writes one JSON object per line.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * @param fileName the name of a file that will be overwritten by output.
      * @param writeSchema if `true`, write an Avro schema as the first line of the file for interpreting the JSON objects.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      * 
      */
    def jsonOutputDataStream(fileName: String, writeSchema: Boolean): JsonOutputDataStream

    /** Create an output stream for CSV-serializing scoring engine output.
      * 
      * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
      * 
      * Note that only records of primitives can be written to CSV because of the nature of the CSV format.
      * 
      * @param outputStream the raw output stream onto which CSV bytes will be written.
      * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
      * @param writeHeader if `true`, write field names as the first line of the file.
      * @return an output stream with an `append` method for appending output data objects and a `close` method for flushing the buffer and closing the stream.
      * 
      */
    def csvOutputDataStream(outputStream: java.io.OutputStream, csvFormat: CSVFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL), writeHeader: Boolean = true): CsvOutputDataStream

    /** Serialize one datum from the `action` method as JSON. */
    def jsonOutput(obj: OUTPUT): String

    /** Translate data that might have come from any PFAEngine class (not necessarily this one) into objects suitable for this PFAEngine's `action`.
      * 
      * @param datum objects that may have been output from another type of PFAEngine's `action`, `snapshotCell`, or `snapshotPool`.
      * @return objects that can be input for this PFAEngine's `action`.
      */
    def fromPFAData(datum: AnyRef): INPUT

    /** Translate data that might have been deserialized by Avro into objects suitable for this PFAEngine's `action`.
      * 
      * @param datum objects that may be Avro generic or Avro specific objects (note that Avro specific objects are subclasses of Avro generic objects)
      * @return objects that can be input for this PFAEngine's `action`.
      */
    def fromGenericAvroData(datum: AnyRef): INPUT

    /** The random number generator used by this particular scoring engine instance.
      * 
      * Note that if a `randseed` is given in the PFA file but a collection of scoring engines are generated from it, each scoring engine instance will have a ''different'' random generator seeded by a ''different'' seed.
      */
    def randomGenerator: Random

    /** Restore this scoring engine's original state as defined by the PFA file it was derived from. */
    def revert(): Unit

    /** Restore this scoring engine's original state as defined by the PFA file and `sharedState` object it was derived from.
      * 
      * @param sharedState same as the `sharedState` passed to `fromJson`, `fromAst`, `factoryFromJson`, etc.
      */
    def revert(sharedState: Option[SharedState]): Unit

    /** Get a 0-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn0(name: String): Function0[AnyRef]

    /** Get a 1-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn1(name: String): Function1[AnyRef, AnyRef]

    /** Get a 2-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn2(name: String): Function2[AnyRef, AnyRef, AnyRef]

    /** Get a 3-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn3(name: String): Function3[AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 4-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn4(name: String): Function4[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 5-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn5(name: String): Function5[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 6-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn6(name: String): Function6[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 7-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn7(name: String): Function7[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 8-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn8(name: String): Function8[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 9-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn9(name: String): Function9[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 10-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn10(name: String): Function10[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 11-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn11(name: String): Function11[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 12-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn12(name: String): Function12[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 13-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn13(name: String): Function13[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 14-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn14(name: String): Function14[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 15-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn15(name: String): Function15[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 16-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn16(name: String): Function16[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 17-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn17(name: String): Function17[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 18-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn18(name: String): Function18[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 19-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn19(name: String): Function19[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 20-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn20(name: String): Function20[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 21-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn21(name: String): Function21[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]

    /** Get a 22-parameter user-defined function from the scoring engine as something that can be executed by an external agent. If the PFA did not declare a user-defined function with this name or it has a different number of parameters, this will throw a `java.util.NoSuchElementException`. */
    def fcn22(name: String): Function22[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef]
  }

  /** Companion object for Hadrian scoring engines: defines "static" methods. */
  object PFAEngine {
    /** Create a factory (0-parameter function) to make instances of this scoring engine from a PFA abstract syntax tree ([[com.opendatagroup.hadrian.ast.EngineConfig EngineConfig]]).
      * 
      * This is a Java convenience function, like the other `factoryFromAst` but accepting Java collections and `null` instead of `None` for missing parameters.
      * 
      * @param engineConfig a parsed, interpreted PFA document, i.e. produced by [[com.opendatagroup.hadrian.reader.jsonToAst jsonToAst]]
      * @param options options that override those found in the PFA document as a Java Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `null` to limit sharing to instances of a single PFA file
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `null` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a `scala.Function0` with a 0-parameter `apply` method that generates linked instances of scoring engines from the PFA document
      * 
      */
    def factoryFromAst(engineConfig: EngineConfig, options: java.util.Map[String, JsonNode], version: String, sharedState: SharedState, parentClassLoader: ClassLoader, debug: Boolean): (() => PFAEngine[AnyRef, AnyRef]) =
      factoryFromAst(engineConfig, mapAsScalaMap(options).toMap, if (version == null) defaultPFAVersion else version, if (sharedState == null) None else Some(sharedState), if (parentClassLoader == null) None else Some(parentClassLoader), debug)

    /** Create a factory (0-parameter function) to make instances of this scoring engine from a PFA abstract syntax tree ([[com.opendatagroup.hadrian.ast.EngineConfig EngineConfig]]).
      * 
      * This function is intended for use in Scala; see the other `factoryFromAst` if calling from Java.
      * 
      * @param engineConfig a parsed, interpreted PFA document, i.e. produced by [[com.opendatagroup.hadrian.reader.jsonToAst jsonToAst]]
      * @param options options that override those found in the PFA document as a Scala Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `None` to limit sharing to instances of a single PFA file
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `None` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a `scala.Function0` with a 0-parameter `apply` method that generates linked instances of scoring engines from the PFA document
      * 
      */
    def factoryFromAst(engineConfig: EngineConfig, options: Map[String, JsonNode] = Map[String, JsonNode](), version: String = defaultPFAVersion, sharedState: Option[SharedState] = None, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): (() => PFAEngine[AnyRef, AnyRef]) = {
      val engineOptions = new EngineOptions(engineConfig.options, options)
      val pfaVersion = PFAVersion.fromString(version)

      val (context: EngineConfig.Context, code) =
        engineConfig.walk(new JVMCompiler, SymbolTable.blank, FunctionTable.blank, engineOptions, pfaVersion)

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
      val constructor = clazz.getConstructor(classOf[EngineConfig], classOf[EngineOptions], classOf[Option[SharedState]], classOf[EngineConfig.Context], classOf[java.lang.Integer])

      val sharedStateToUse = sharedState match {
        case None => Some(new SharedState(new SharedMapInMemory, engineConfig.pools.keys map {(_, new SharedMapInMemory)} toMap))
        case x => x
      }

      var index = 0;
      {() =>
        try {
          synchronized {
            val out = constructor.newInstance(engineConfig, engineOptions, sharedStateToUse, context, java.lang.Integer.valueOf(index)).asInstanceOf[PFAEngine[AnyRef, AnyRef]]
            index += 1
            out
          }
        }
        catch {
          case err: java.lang.reflect.InvocationTargetException => throw err.getCause
        }
      }
    }

    /** Create a factory (0-parameter function) to make instances of this scoring engine from a JSON-formatted PFA file.
      * 
      * This is a Java convenience function, like the other `factoryFromJson` but accepting Java collections and `null` instead of `None` for missing parameters.
      * 
      * @param src a PFA document in JSON-serialized form (`String`, `java.lang.File`, or `java.lang.InputStream`)
      * @param options options that override those found in the PFA document as a Java Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `null` to limit sharing to instances of a single PFA file
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `null` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a `scala.Function0` with a 0-parameter `apply` method that generates linked instances of scoring engines from the PFA document
      * 
      */
    def factoryFromJson(src: AnyRef, options: java.util.Map[String, JsonNode], version: String, sharedState: SharedState, parentClassLoader: ClassLoader, debug: Boolean): (() => PFAEngine[AnyRef, AnyRef]) =
      factoryFromJson(src, mapAsScalaMap(options).toMap, if (version == null) defaultPFAVersion else version, if (sharedState == null) None else Some(sharedState), if (parentClassLoader == null) None else Some(parentClassLoader), debug)

    /** Create a factory (0-parameter function) to make instances of this scoring engine from a JSON-formatted PFA file.
      * 
      * This function is intended for use in Scala; see the other `factoryFromJson` if calling from Java.
      * 
      * @param src a PFA document in JSON-serialized form (`String`, `java.lang.File`, or `java.lang.InputStream`)
      * @param options options that override those found in the PFA document as a Scala Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `None` to limit sharing to instances of a single PFA file
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `None` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a `scala.Function0` with a 0-parameter `apply` method that generates linked instances of scoring engines from the PFA document
      * 
      */
    def factoryFromJson(src: AnyRef, options: Map[String, JsonNode] = Map[String, JsonNode](), version: String = defaultPFAVersion, sharedState: Option[SharedState] = None, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): (() => PFAEngine[AnyRef, AnyRef]) = src match {
      case x: String => factoryFromAst(jsonToAst(x), options, version, sharedState, parentClassLoader, debug)
      case x: java.io.File => factoryFromAst(jsonToAst(x), options, version, sharedState, parentClassLoader, debug)
      case x: java.io.InputStream => factoryFromAst(jsonToAst(x), options, version, sharedState, parentClassLoader, debug)
      case x => throw new IllegalArgumentException("cannot read model from objects of type " + src.getClass.getName)
    }

    /** Create a factory (0-parameter function) to make instances of this scoring engine from a YAML-formatted PFA file.
      * 
      * This is a Java convenience function, like the other `factoryFromYaml` but accepting Java collections and `null` instead of `None` for missing parameters.
      * 
      * @param src a PFA document in YAML-serialized form
      * @param options options that override those found in the PFA document as a Java Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `null` to limit sharing to instances of a single PFA file
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `null` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a `scala.Function0` with a 0-parameter `apply` method that generates linked instances of scoring engines from the PFA document
      * 
      */
    def factoryFromYaml(src: String, options: java.util.Map[String, JsonNode], version: String, sharedState: SharedState, parentClassLoader: ClassLoader, debug: Boolean): (() => PFAEngine[AnyRef, AnyRef]) =
      factoryFromYaml(src, mapAsScalaMap(options).toMap, if (version == null) defaultPFAVersion else version, if (sharedState == null) None else Some(sharedState), if (parentClassLoader == null) None else Some(parentClassLoader), debug)

    /** Create a factory (0-parameter function) to make instances of this scoring engine from a YAML-formatted PFA file.
      * 
      * This function is intended for use in Scala; see the other `factoryFromYaml` if calling from Java.
      * 
      * @param src a PFA document in YAML-serialized form
      * @param options options that override those found in the PFA document as a Scala Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `None` to limit sharing to instances of a single PFA file
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `None` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a `scala.Function0` with a 0-parameter `apply` method that generates linked instances of scoring engines from the PFA document
      * 
      */
    def factoryFromYaml(src: String, options: Map[String, JsonNode] = Map[String, JsonNode](), version: String = defaultPFAVersion, sharedState: Option[SharedState] = None, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): (() => PFAEngine[AnyRef, AnyRef]) =
      factoryFromJson(yamlToJson(src), options, version, sharedState, parentClassLoader, debug)

    /** Create a collection of instances of this scoring engine from a PFA abstract syntax tree ([[com.opendatagroup.hadrian.ast.EngineConfig EngineConfig]]).
      * 
      * This is a Java convenience function, like the other `fromAst` but accepting Java collections and `null` instead of `None` for missing parameters.
      * 
      * @param engineConfig a parsed, interpreted PFA document, i.e. produced by [[com.opendatagroup.hadrian.reader.jsonToAst jsonToAst]]
      * @param options options that override those found in the PFA document as a Java Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `null` to limit sharing to instances of a single PFA file
      * @param multiplicity number of instances to return
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `null` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a Java List of scoring engine instances
      * 
      */
    def fromAst(engineConfig: EngineConfig, options: java.util.Map[String, JsonNode], version: String, sharedState: SharedState, multiplicity: Int, parentClassLoader: ClassLoader, debug: Boolean): java.util.List[PFAEngine[AnyRef, AnyRef]] =
      seqAsJavaList(fromAst(engineConfig, mapAsScalaMap(options).toMap, if (version == null) defaultPFAVersion else version, if (sharedState == null) None else Some(sharedState), multiplicity, if (parentClassLoader == null) None else Some(parentClassLoader), debug))

    /** Create a collection of instances of this scoring engine from a PFA abstract syntax tree ([[com.opendatagroup.hadrian.ast.EngineConfig EngineConfig]]).
      * 
      * This function is intended for use in Scala; see the other `fromAst` if calling from Java.
      * 
      * @param engineConfig a parsed, interpreted PFA document, i.e. produced by [[com.opendatagroup.hadrian.reader.jsonToAst jsonToAst]]
      * @param options options that override those found in the PFA document as a Scala Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `None` to limit sharing to instances of a single PFA file
      * @param multiplicity number of instances to return (default is 1; a single-item collection)
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `None` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a Scala Seq of scoring engine instances
      * 
      */
    def fromAst(engineConfig: EngineConfig, options: Map[String, JsonNode] = Map[String, JsonNode](), version: String = defaultPFAVersion, sharedState: Option[SharedState] = None, multiplicity: Int = 1, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): Seq[PFAEngine[AnyRef, AnyRef]] = {
      val factory = factoryFromAst(engineConfig, options, version, sharedState, parentClassLoader, debug)
      0 until multiplicity map {x => factory()}
    }

    /** Create a collection of instances of this scoring engine from a JSON-formatted PFA file.
      * 
      * This is a Java convenience function, like the other `fromAst` but accepting Java collections and `null` instead of `None` for missing parameters.
      * 
      * @param src a PFA document in JSON-serialized form (`String`, `java.lang.File`, or `java.lang.InputStream`)
      * @param options options that override those found in the PFA document as a Java Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `null` to limit sharing to instances of a single PFA file
      * @param multiplicity number of instances to return
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `null` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a Java List of scoring engine instances
      * 
      */
    def fromJson(src: AnyRef, options: java.util.Map[String, JsonNode], version: String, sharedState: SharedState, multiplicity: Int, parentClassLoader: ClassLoader, debug: Boolean): java.util.List[PFAEngine[AnyRef, AnyRef]] =
      seqAsJavaList(fromJson(src, mapAsScalaMap(options).toMap, if (version == null) defaultPFAVersion else version, if (sharedState == null) None else Some(sharedState), multiplicity, if (parentClassLoader == null) None else Some(parentClassLoader), debug))

    /** Create a collection of instances of this scoring engine from a JSON-formatted PFA file.
      * 
      * This function is intended for use in Scala; see the other `fromJson` if calling from Java.
      * 
      * @param src a PFA document in JSON-serialized form (`String`, `java.lang.File`, or `java.lang.InputStream`)
      * @param options options that override those found in the PFA document as a Scala Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `None` to limit sharing to instances of a single PFA file
      * @param multiplicity number of instances to return (default is 1; a single-item sequence)
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `None` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a Scala Seq of scoring engine instances
      * 
      */
    def fromJson(src: AnyRef, options: Map[String, JsonNode] = Map[String, JsonNode](), version: String = defaultPFAVersion, sharedState: Option[SharedState] = None, multiplicity: Int = 1, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): Seq[PFAEngine[AnyRef, AnyRef]] = src match {
      case x: String => fromAst(jsonToAst(x), options, version, sharedState, multiplicity, parentClassLoader, debug)
      case x: java.io.File => fromAst(jsonToAst(x), options, version, sharedState, multiplicity, parentClassLoader, debug)
      case x: java.io.InputStream => fromAst(jsonToAst(x), options, version, sharedState, multiplicity, parentClassLoader, debug)
      case x => throw new IllegalArgumentException("cannot read model from objects of type " + src.getClass.getName)
    }

    /** Create a collection of instances of this scoring engine from a YAML-formatted PFA file.
      * 
      * This is a Java convenience function, like the other `fromYaml` but accepting Java collections and `null` instead of `None` for missing parameters.
      * 
      * @param src a PFA document in YAML-serialized form
      * @param options options that override those found in the PFA document as a Java Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `null` to limit sharing to instances of a single PFA file
      * @param multiplicity number of instances to return
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `null` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a Java List of scoring engine instances
      * 
      */
    def fromYaml(src: String, options: java.util.Map[String, JsonNode], version: String, sharedState: SharedState, multiplicity: Int, parentClassLoader: ClassLoader, debug: Boolean): java.util.List[PFAEngine[AnyRef, AnyRef]] =
      seqAsJavaList(fromYaml(src, mapAsScalaMap(options).toMap, if (version == null) defaultPFAVersion else version, if (sharedState == null) None else Some(sharedState), multiplicity, if (parentClassLoader == null) None else Some(parentClassLoader), debug))

    /** Create a collection of instances of this scoring engine from a YAML-formatted PFA file.
      * 
      * This function is intended for use in Scala; see the other `fromYaml` if calling from Java.
      * 
      * @param src a PFA document in YAML-serialized form
      * @param options options that override those found in the PFA document as a Scala Map of [[http://fasterxml.github.io/jackson-databind/javadoc/2.2.0/com/fasterxml/jackson/databind/JsonNode.html JsonNodes]]
      * @param version PFA version number as a "major.minor.release" string
      * @param sharedState external state for shared cells and pools to initialize from and modify; pass `None` to limit sharing to instances of a single PFA file
      * @param multiplicity number of instances to return (default is 1; a single-item collection)
      * @param parentClassLoader ClassLoader to link the new scoring engine's private ClassLoaders under; pass `None` for a reasonable default
      * @param debug if `true`, print the Java code generated by this PFA document before byte-compiling
      * @return a Scala Seq of scoring engine instances
      * 
      */
    def fromYaml(src: String, options: Map[String, JsonNode] = Map[String, JsonNode](), version: String = defaultPFAVersion, sharedState: Option[SharedState] = None, multiplicity: Int = 1, parentClassLoader: Option[ClassLoader] = None, debug: Boolean = false): Seq[PFAEngine[AnyRef, AnyRef]] =
      fromJson(yamlToJson(src), options, version, sharedState, multiplicity, parentClassLoader, debug)
  }

  /** Interface for a `method` = map Hadrian scoring engine (one that returns one output for each input).
    * 
    * See [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]] for details.
    */
  trait PFAMapEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] extends PFAEngine[INPUT, OUTPUT]

  /** Interface for a `method` = emit Hadrian scoring engine (one that returns zero or more outputs for each input).
    * 
    * This kind of engine differs from a [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]] in that it has an externally supplied `emit` callback to collect results, and `action` always returns `null`.
    * 
    * See [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]] for details.
    */
  trait PFAEmitEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] extends PFAEngine[INPUT, OUTPUT] {
    /** Externally supplied function for handling output from PFA.
      * 
      * By default, ignores all results.
      * 
      * '''Arguments:'''
      * 
      *  - output from the scoring engine, may be called zero or more times by the scoring engine's `begin`, `action`, or `end`.
      */
    var emit: Function1[OUTPUT, Unit]
  }

  /** Interface for a `method` = fold Hadrian scoring engine (one that accumulates a tally).
    * 
    * This kind of engine differs from a [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]] in that it has an externally modifiable `tally` field with the running sum or current accumulation of the scoring engine and a `merge` method to combine tallies from scoring engines running in parallel.
    * 
    * See [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]] for details.
    */
  trait PFAFoldEngine[INPUT <: AnyRef, OUTPUT <: AnyRef] extends PFAEngine[INPUT, OUTPUT] {
    /** Externally modifiable running sum or current accumulation. */
    var tally: OUTPUT
    /** Combines tallies from two scoring engines that collected data in parallel. */
    def merge(tallyOne: OUTPUT, tallyTwo: OUTPUT): OUTPUT
  }

  //////////////////////////////////////////////////////////// transformation of individual forms

  /** TaskResult for [[com.opendatagroup.hadrian.jvmcompiler.JVMCompiler JVMCompiler]], which represents Java code.
    * 
    * Not consistently used; some partial Java code results are in plain `Strings`. This class is a thin wrapper around `Strings`.
    * 
    * In some cases, Java code can't be created bottom up and temporary "placeholders" have to be used. These should not appear in the final Java string.
    * 
    */
  case class JavaCode(format: String, contents: String*) extends TaskResult {
    /** Name of the in-memory Java file. Only used at the top level. */
    var name: Option[String] = None
    /** Set the `name` member and return self. */
    def setName(n: String): JavaCode = {name = Some(n); this}
    /** Dependencies for the in-memory Java file. Only used at the top level. */
    var dependencies = Map[String, JavaCode]()
    var placeholder: Boolean = false
    def setDependencies(d: Map[String, JavaCode]): JavaCode = {dependencies = d; this}
    def setAsPlaceholder(): JavaCode = {placeholder = true; this}

    /** Throws an exception if any "placeholders" are still in use. */
    override def toString() =
      if (placeholder)
        throw new Exception("attempt to use JavaCode placeholder")
      else
        format.format(contents: _*)
  }

  /** Most common [[com.opendatagroup.hadrian.ast.Task Task]] for [[com.opendatagroup.hadrian.ast.Ast Ast]] `walk`; generates Java code from a PFA AST.
    */
  class JVMCompiler extends Task {
    import JVMNameMangle._

    object ReturnMethod extends Enumeration {
      type ReturnMethod = Value
      val NONE, RETURN = Value
    }

    /** Helper function to format a sequence of expressions as a Java string.
      * 
      * @param exprs Java code for each expression.
      * @param returnMethod if `NONE`, return `null`; if `RETURN`, return the value of the last expression
      * @param retType return type
      */
    def block(exprs: Seq[TaskResult], returnMethod: ReturnMethod.ReturnMethod, retType: AvroType): String = returnMethod match {
      case ReturnMethod.NONE =>
        (exprs map {"com.opendatagroup.hadrian.jvmcompiler.W.s(" + _.toString + ");"}).mkString("\n")
      case ReturnMethod.RETURN =>
        ((exprs.init map {"com.opendatagroup.hadrian.jvmcompiler.W.s(" + _.toString + ");"}) ++ List("return %s;".format(retType match {
          case null => exprs.last.toString
          case _: AvroBoolean | _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
            W.wrapExpr(exprs.last.toString, retType, true)
          case _ =>
            "(" + javaType(retType, true, true, true) + ")" + exprs.last.toString
        }))).mkString("\n")
    }

    /** Helper function to write symbols as fields of a Java class.
      * 
      * @param symbols symbol names and types
      * @return Java code as a `String`
      */
    def symbolFields(symbols: Map[String, AvroType]): String =
      symbols.map({case (n, t) => javaType(t, false, true, true) + " " + s(n) + ";"}).mkString("\n")

    /** Helper function to chain path indexes together.
      * 
      * @param path path indexes
      * @return Java code as a `String`
      */
    def makePathIndex(path: Seq[PathIndex]): String = path.map(_ match {
      case ArrayIndex(expr, t) => """new com.opendatagroup.hadrian.shared.I(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))""".format(expr.toString)
      case MapIndex(expr, t) => """new com.opendatagroup.hadrian.shared.M(%s)""".format(expr.toString)
      case RecordIndex(name, t) => """new com.opendatagroup.hadrian.shared.R("%s")""".format(StringEscapeUtils.escapeJava(name))
    }).mkString(", ")

    /** Table of literal values in a PFA file; used to create a table of cached literals in Java.
      */
    val literals = mutable.Map[String, JavaCode]()

    /** Create Java code from a PFA AST context.
      * 
      * @param astContext the context (post-semantics check data) about an abstract syntax tree node
      * @param engineOptions implementation options
      * @param resolvedType sometimes used when Java code can't be built from bottom-up
      * @return Java code
      */
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
        merge: Option[(Seq[TaskResult], Map[String, AvroType], Set[String])],
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

    public %s(Object[] fields) {
%s    }

%s

    public Object[] getAll() {
        return new Object[]{%s};
    }

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
              fields map {x => "\"" + StringEscapeUtils.escapeJava(x.name) + "\""} mkString(", "),
              fields map {x: AvroField => javaSchema(x.avroType, true)} mkString(", "),
              t(namespace, name, false),
              t(namespace, name, false),
              fields.zipWithIndex map {case (AvroField(fname, ftype, _, _, _, _), index: Int) => "this.%s = (%s)fields[%d];\n".format(s(fname), javaType(ftype, true, true, true), index)} mkString,
              fields map {case AvroField(fname, ftype, _, _, _, _) =>
                """public %s %s;""".format(javaType(ftype, false, true, true), s(fname))} mkString("\n"),
              fields map {case AvroField(fname, _, _, _, _, _) => s(fname)} mkString(", "),
              (fields zipWithIndex) map {case (AvroField(fname, ftype, _, _, _, _), i) =>
                """case %d: return %s;""".format(i, s(fname))} mkString("\n"),
              (fields zipWithIndex) map {case (AvroField(fname, ftype, _, _, _, _), i) =>
                """case %d: %s = (%s)value$; break;""".format(i, s(fname), javaType(ftype, true, true, true))} mkString("\n"),
              if (fields.isEmpty)
                """if (false) { throw new IllegalArgumentException(""); }"""
              else
                fields map {case AvroField(fname, ftype, _, _, _, _) =>
                  """if (field$.equals("%s")) return %s;""".format(StringEscapeUtils.escapeJava(fname), s(fname))} mkString("\nelse "),
              if (fields.isEmpty)
                "if (false) { }"
              else
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

    public %s(com.opendatagroup.hadrian.data.AnyPFAFixed that) {
        super();
        bytes(that.bytes());
    }

    public com.opendatagroup.hadrian.data.PFAFixed overlay(byte[] replacement) {
        byte[] newbytes = (byte[])this.bytes().clone();
        int length = replacement.length;
        if (length > newbytes.length)
            length = newbytes.length;
        System.arraycopy(replacement, 0, newbytes, 0, length);
        return new %s(newbytes);
    }
}
""",          namespace map {"package " + _ + ";"} mkString(""),
              t(namespace, name, false),
              javaSchema(avroCompiled, true),
              size.toString,
              t(namespace, name, false),
              t(namespace, name, false),
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

    public %s(com.opendatagroup.hadrian.data.AnyPFAEnumSymbol that) {
        _value = this.strToInt(that.toString());
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
%s = _instance();
%s
%s = _metadata();
%s
} }).apply(); } }""".format(
            symbolFields(beginSymbols),
            s("name"),
            StringEscapeUtils.escapeJava(engineName),
            s("instance"),
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
%s = _instance();
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
                s("instance"),
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
%s = _instance();
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
                s("instance"),
                version match {
                  case Some(x) => s("version") + " = " + x.toString + ";"
                  case None => ""
                },
                s("metadata"),
                s("actionsStarted"),
                s("actionsFinished"),
                block(action, ReturnMethod.NONE, AvroNull())))

          case Method.FOLD =>
            val actionMethod = """
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
%s = _instance();
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
} }""".format(if (!input.isInstanceOf[AvroUnion]) """    public Object action(Object input) { return (Object)action((%s)input); }""".format(javaType(input, true, true, true)) else "",
              javaType(output, true, true, true),
              javaType(input, true, true, true),
              s("tally"),
              symbolFields(actionSymbols),
              javaType(output, true, true, true),
              javaType(input, true, true, true),
              s("input"),
              s("name"),
              StringEscapeUtils.escapeJava(engineName),
              s("instance"),
              version match {
                case Some(x) => s("version") + " = " + x.toString + ";"
                case None => ""
              },
              s("metadata"),
              s("actionsStarted"),
              s("actionsFinished"),
              block(action, ReturnMethod.RETURN, output),
              s("tally"))

            val mergeMethod = merge match {
              case Some((mergeTasks: Seq[TaskResult], mergeSymbols: Map[String, AvroType], mergeCalls: Set[String])) =>
                """
%s
    public %s merge(%s tallyOne, %s tallyTwo) {
synchronized(runlock) {
rollbackSave();
try {
%s = (new Object() {
%s
public %s apply(%s tallyOne, %s tallyTwo) {
timeout = options().timeout_action();
startTime = System.currentTimeMillis();
%s = tallyOne;
%s = tallyTwo;
%s = "%s";
%s = _instance();
%s
%s = _metadata();
%s
} }).apply(tallyOne, tallyTwo);
}
catch (Throwable err) {
rollback();
throw err;
}
return %s;
} }""".format(if (!output.isInstanceOf[AvroUnion]) """    public Object merge(Object tallyOne, Object tallyTwo) { return (Object)merge((%s)tallyOne, (%s)tallyTwo); }""".format(javaType(output, true, true, true), javaType(output, true, true, true)) else "",
              javaType(output, true, true, true),
              javaType(output, true, true, true),
              javaType(output, true, true, true),
              s("tally"),
              symbolFields(mergeSymbols),
              javaType(output, true, true, true),
              javaType(output, true, true, true),
              javaType(output, true, true, true),
              s("tallyOne"),
              s("tallyTwo"),
              s("name"),
              StringEscapeUtils.escapeJava(engineName),
              s("instance"),
              version match {
                case Some(x) => s("version") + " = " + x.toString + ";"
                case None => ""
              },
              s("metadata"),
              block(mergeTasks, ReturnMethod.RETURN, output),
              s("tally"))

              case None => throw new Exception
            }

            ("com.opendatagroup.hadrian.jvmcompiler.PFAFoldEngine<%s, %s>".format(javaType(input, true, true, true), javaType(output, true, true, true)),
              """    private %s %s;
    public %s tally() { return %s; }
    public void tally_$eq(%s newTally) { %s = newTally; }
%s
    public void initialize(com.opendatagroup.hadrian.ast.EngineConfig config, com.opendatagroup.hadrian.options.EngineOptions options, scala.Option<com.opendatagroup.hadrian.shared.SharedState> state, Class<%s> thisClass, com.opendatagroup.hadrian.ast.EngineConfig.Context context, int index) {
        super.initialize(config, options, state, thisClass, context, index);
        try {
            %s = (%s)fromJson("%s".getBytes(), %s);
        }
        catch (Exception err) {
            throw new com.opendatagroup.hadrian.errors.PFAInitializationException("zero does not conform to output type " + %s.toString());
        }
    }
%s
%s""".format(javaType(output, true, true, true),
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
             actionMethod,
             mergeMethod))
        }

        val endMethod =
          """    public void end() { synchronized(runlock) { (new Object() {
%s
public void apply() {
timeout = options().timeout_end();
startTime = System.currentTimeMillis();
%s = "%s";
%s = _instance();
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
            s("instance"),
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
java.util.HashMap<String, Object> literals = new java.util.HashMap<String, Object>();
public %s(com.opendatagroup.hadrian.ast.EngineConfig config, com.opendatagroup.hadrian.options.EngineOptions options, scala.Option<com.opendatagroup.hadrian.shared.SharedState> state, com.opendatagroup.hadrian.ast.EngineConfig.Context context, Integer index) {
    _inputClass_$eq(%s);
    _outputClass_$eq(%s);
%s
    initialize(config, options, state, %s.class, context, index.intValue());
}
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
          javaType(input, true, true, false).replaceAll("<.*>", "") + ".class",
          javaType(output, true, true, false).replaceAll("<.*>", "") + ".class",
          literals.values.mkString("\n"),
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

      case CallUserFcn.Context(retType, _, name, nameToNum, nameToFcn, args, argContext, nameToParamTypes, nameToRetTypes, engineOptions) => {
        val cases =
          for ((n, fcn) <- nameToFcn) yield
            """        case %s: return %s;""".format(nameToNum(n), fcn.javaCode(args.collect({case x: JavaCode => x}), argContext, nameToParamTypes(n), nameToRetTypes(n), engineOptions))

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

      case Call.Context(retType, _, fcn, args, argContext, paramTypes, engineOptions) => fcn.javaCode(args.collect({case x: JavaCode => x}), argContext, paramTypes, retType, engineOptions)

      case Ref.Context(retType, _, name) => JavaCode(s(name))

      case LiteralNull.Context(_, _) => JavaCode("null")
      case LiteralBoolean.Context(_, _, value) => if (value) JavaCode("true") else JavaCode("false")
      case LiteralInt.Context(_, _, value) => JavaCode(value.toString)
      case LiteralLong.Context(_, _, value) => JavaCode(value.toString + "L")
      case LiteralFloat.Context(_, _, value) => JavaCode(value.toString + "F")
      case LiteralDouble.Context(_, _, value) => JavaCode(value.toString)
      case LiteralString.Context(_, _, value) => JavaCode("\"" + StringEscapeUtils.escapeJava(value) + "\"")
      case LiteralBase64.Context(_, _, value) => JavaCode("new byte[]{" + value.mkString(",") + "}")
      case Literal.Context(retType, _, value) =>
        val escaped = StringEscapeUtils.escapeJava(value)
        literals(escaped) = JavaCode("""    literals.put("%s", fromJson("%s".getBytes(), %s));""", escaped, escaped, javaSchema(retType, false))
        JavaCode("""((%s)literals.get("%s"))""", javaType(retType, true, true, true), escaped)

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

      case AttrGet.Context(retType, _, expr, exprType, path, pos) => {
        var out = "((%s)(%s))".format(javaType(exprType, false, true, false), W.wrapExpr(expr.toString, exprType, false))
        for (item <- path) item match {
          case ArrayIndex(expr, t) => out = """((%s)(%s.get(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))))""".format(javaType(t, true, true, false), out, expr.toString)
          case MapIndex(expr, t) => out = """((%s)(%s.get(%s)))""".format(javaType(t, true, true, false), out, expr.toString)
          case RecordIndex(name, t) => out = """((%s)(%s.%s))""".format(javaType(t, false, true, false), out, s(name))
        }
        JavaCode("""(new Object() {
public %s apply() {
try {
    return %s;
}
catch (java.lang.IndexOutOfBoundsException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("array index not found", 2000, "attr", %s, err);
}
catch (java.util.NoSuchElementException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("map key not found", 2001, "attr", %s, err);
}
} }).apply()""".format(javaType(retType, false, true, false), out, posToJava(pos), posToJava(pos)))
      }

      case AttrTo.Context(retType, _, expr, exprType, setType, path, to, toType, pos) => {
        val toFcn =
          if (toType.isInstanceOf[AvroType])
            """(new scala.runtime.AbstractFunction1<%s, %s>() { public %s apply(%s dummy) { return %s; } public Object apply(Object dummy) { return apply(%s); } })""".format(
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              W.wrapExpr(to.toString, setType, true),
              W.wrapExpr(to.toString, setType, true))
          else
            to.toString

        JavaCode("""(new Object() {
public %s apply() {
try {
    return (%s)%s.updated(new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s);
}
catch (java.lang.IndexOutOfBoundsException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("array index not found", 2002, "attr-to", %s, err);
}
catch (java.util.NoSuchElementException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("map key not found", 2003, "attr-to", %s, err);
}
} }).apply()""",
          javaType(exprType, false, true, false),                 
          javaType(exprType, false, true, false),
          W.wrapExpr(expr.toString, exprType, false),
          makePathIndex(path),
          toFcn,
          javaSchema(setType, false),
          posToJava(pos),
          posToJava(pos))
      }

      case CellGet.Context(retType, _, cell, cellType, path, shared, pos) => {
        if (!shared) {
          var out = "((%s)(%s))".format(javaType(cellType, !path.isEmpty, true, false), c(cell))
          for (item <- path) item match {
            case ArrayIndex(expr, t) => out = """((%s)(%s.get(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))))""".format(javaType(t, true, true, false), out, expr.toString)
            case MapIndex(expr, t) => out = """((%s)(%s.get(%s)))""".format(javaType(t, true, true, false), out, expr.toString)
            case RecordIndex(name, t) => out = """((%s)(%s.%s))""".format(javaType(t, false, true, false), out, s(name))
          }
          JavaCode("""(new Object() {
public %s apply() {
try {
    return %s;
}
catch (java.lang.IndexOutOfBoundsException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("array index not found", 2004, "cell", %s, err);
}
catch (java.util.NoSuchElementException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("map key not found", 2005, "cell", %s, err);
}
} }).apply()""".format(javaType(retType, false, true, false), out, posToJava(pos), posToJava(pos)))
        }
        else {
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(sharedCells.get("%s", new com.opendatagroup.hadrian.shared.PathIndex[]{%s}), "array index not found", 2004, "map key not found", 2005, "cell", %s))""",
            javaType(retType, true, true, false),
            cell,
            makePathIndex(path),
            posToJava(pos))
        }
      }

      case CellTo.Context(retType, _, cell, cellType, setType, path, to, toType, shared, pos) => {
        val toFcn =
          if (toType.isInstanceOf[AvroType])
            """(new scala.runtime.AbstractFunction1<%s, %s>() { public %s apply(%s dummy) { return %s; } public Object apply(Object dummy) { return apply(%s); } })""".format(
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              W.wrapExpr(to.toString, setType, true),
              W.wrapExpr(to.toString, setType, true))
          else
            to.toString

        if (!shared) {
          if (path.isEmpty)
            JavaCode("(%s = (%s)%s.apply(%s))".format(c(cell), javaType(cellType, false, true, false), toFcn, c(cell)))
          else
            JavaCode("""(%s = (%s)%s.updated(new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s, "array index not found", 2006, "map key not found", 2007, "cell-to", %s))""",
              c(cell),
              javaType(cellType, false, true, false),
              c(cell),
              makePathIndex(path),
              toFcn,
              javaSchema(setType, false),
              posToJava(pos))
        }
        else {
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(sharedCells.update("%s", new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, null, %s, %s), "array index not found", 2006, "map key not found", 2007, "cell-to", %s))""",
            javaType(retType, true, true, false),
            cell,
            makePathIndex(path),
            toFcn,
            javaSchema(setType, false),
            posToJava(pos))
        }
      }

      case PoolGet.Context(retType, _, pool, path, shared, pos) => {
        if (!shared) {
          var out = """((%s)(com.opendatagroup.hadrian.jvmcompiler.W.getOrFailPool(%s, %s, "%s", "map key not found", 2009, "pool", %s)))""".format(
            javaType(path.head.asInstanceOf[MapIndex].t, true, true, false),
            p(pool),
            path.head.asInstanceOf[MapIndex].k.toString,
            StringEscapeUtils.escapeJava(pool),
            posToJava(pos))
          for (item <- path.tail) item match {
            case ArrayIndex(expr, t) => out = """((%s)(%s.get(com.opendatagroup.hadrian.jvmcompiler.W.asInt(%s))))""".format(javaType(t, true, true, false), out, expr.toString)
            case MapIndex(expr, t) => out = """((%s)(%s.get(%s)))""".format(javaType(t, true, true, false), out, expr.toString)
            case RecordIndex(name, t) => out = """((%s)(%s.%s))""".format(javaType(t, false, true, false), out, s(name))
          }
          JavaCode("""(new Object() {
public %s apply() {
try {
    return %s;
}
catch (java.lang.IndexOutOfBoundsException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("array index not found", 2008, "pool", %s, err);
}
catch (java.util.NoSuchElementException err) {
    throw new com.opendatagroup.hadrian.errors.PFARuntimeException("map key not found", 2009, "pool", %s, err);
}
} }).apply()""".format(javaType(retType, false, true, false), out, posToJava(pos), posToJava(pos)))
        }
        else {
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(%s.get(%s, new com.opendatagroup.hadrian.shared.PathIndex[]{%s}), "array index not found", 2008, "map key not found", 2009, "pool", %s))""",
            javaType(retType, true, true, false),
            p(pool),
            path.head.asInstanceOf[MapIndex].k.toString,
            makePathIndex(path.tail),
            posToJava(pos))
        }
      }

      case PoolTo.Context(retType, _, pool, poolType, setType, path, to, toType, init, shared, pos) => {
        val toFcn =
          if (toType.isInstanceOf[AvroType])
            """(new scala.runtime.AbstractFunction1<%s, %s>() { public %s apply(%s dummy) { return %s; } public Object apply(Object dummy) { return apply(%s); } })""".format(
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              javaType(setType, true, true, true),
              W.wrapExpr(to.toString, setType, true),
              W.wrapExpr(to.toString, setType, true))
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
            JavaCode("""com.opendatagroup.hadrian.jvmcompiler.W.do2(%s.put(%s, ((%s)(com.opendatagroup.hadrian.jvmcompiler.W.getOrElse(%s, %s, %s))).updated(new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s, "array index not found", 2010, "map key not found", 2011, "pool-to", %s)), %s.get(%s))""",
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString,
              javaType(path.head.asInstanceOf[MapIndex].t, true, true, false),
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString,
              init.toString,
              makePathIndex(path.tail),
              toFcn,
              javaSchema(setType, false),
              posToJava(pos),
              p(pool),
              path.head.asInstanceOf[MapIndex].k.toString)
        }
        else
          JavaCode("""((%s)com.opendatagroup.hadrian.jvmcompiler.W.either(%s.update(%s, new com.opendatagroup.hadrian.shared.PathIndex[]{%s}, %s, %s, %s), "array index not found", 2010, "map key not found", 2011, "pool-to", %s))""",
            javaType(retType, true, true, false),
            p(pool),
            path.head.asInstanceOf[MapIndex].k.toString,
            makePathIndex(path.tail),
            init.toString,
            toFcn,
            javaSchema(setType, false),
            posToJava(pos))
      }

      case PoolDel.Context(retType, _, pool, del, shared) =>
        JavaCode("""com.opendatagroup.hadrian.jvmcompiler.W.n(%s.remove(%s))""", p(pool), del.toString)

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

      case Pack.Context(retType, calls, exprsDeclareRes, pos) => {
        var counter = 0
        val evaluateAndCountBytes = exprsDeclareRes flatMap {_ match {
          case BinaryFormatter.DeclarePad(value: JavaCode) =>
            counter += 1
            List("Object tmp" + counter.toString + " = " + value.toString + ";",
                 "numBytes += 1;")
          case BinaryFormatter.DeclareBoolean(value: JavaCode) =>
            counter += 1
            List("boolean tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroBoolean(), false) + ";",
                 "numBytes += 1;")
          case BinaryFormatter.DeclareByte(value: JavaCode, unsigned) =>
            counter += 1
            List("int tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroInt(), false) + ";",
                 if (unsigned) "if (tmp" + counter.toString + " >= 256) tmp" + counter.toString + " -= 256;" else "",
                 "numBytes += 1;")
          case BinaryFormatter.DeclareShort(value: JavaCode, littleEndian, unsigned) =>
            counter += 1
            List("int tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroInt(), false) + ";",
                 if (unsigned) "if (tmp" + counter.toString + " >= 65536) tmp" + counter.toString + " -= 65536;" else "",
                 "numBytes += 2;")
          case BinaryFormatter.DeclareInt(value: JavaCode, littleEndian, unsigned) =>
            counter += 1
            if (unsigned)
              List("long tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroLong(), false) + ";",
                   "if (tmp" + counter.toString + " >= 4294967296L) tmp" + counter.toString + " -= 4294967296L;",
                   "numBytes += 4;")
            else
              List("int tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroInt(), false) + ";",
                   "numBytes += 4;")
          case BinaryFormatter.DeclareLong(value: JavaCode, littleEndian, unsigned) =>
            counter += 1
            if (unsigned)
              List("double tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroDouble(), false) + ";",
                   "if (tmp" + counter.toString + " >= 1.8446744073709552e+19) tmp" + counter.toString + " -= 1.8446744073709552e+19;",
                   "numBytes += 8;")
            else
              List("long tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroLong(), false) + ";",
                   "numBytes += 8;")
          case BinaryFormatter.DeclareFloat(value: JavaCode, littleEndian) =>
            counter += 1
            List("float tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroFloat(), false) + ";",
                 "numBytes += 4;")
          case BinaryFormatter.DeclareDouble(value: JavaCode, littleEndian) =>
            counter += 1
            List("double tmp" + counter.toString + " = " + W.wrapExpr(value.toString, AvroDouble(), false) + ";",
                 "numBytes += 8;")
          case BinaryFormatter.DeclareRaw(value: JavaCode) =>
            counter += 1
            List("byte[] tmp" + counter.toString + " = " + value.toString + ";",
                 "numBytes += tmp" + counter.toString + ".length;")
          case BinaryFormatter.DeclareRawSize(value: JavaCode, size) =>
            counter += 1
            List("byte[] tmp" + counter.toString + " = " + value.toString + ";",
                 "numBytes += tmp" + counter.toString + ".length;",
                 "if (tmp" + counter.toString + ".length != " + size.toString + ") throw new com.opendatagroup.hadrian.errors.PFARuntimeException(\"raw bytes does not have specified size\", 3000, \"pack\", " + posToJava(pos) + ", null);")
          case BinaryFormatter.DeclareToNull(value: JavaCode) =>
            counter += 1
            List("byte[] tmp" + counter.toString + " = " + value.toString + ";",
                 "numBytes += tmp" + counter.toString + ".length + 1;")
          case BinaryFormatter.DeclarePrefixed(value: JavaCode) =>
            counter += 1
            List("byte[] tmp" + counter.toString + " = " + value.toString + ";",
                 "numBytes += tmp" + counter.toString + ".length + 1;",
                 "if (tmp" + counter.toString + ".length > 255) throw new com.opendatagroup.hadrian.errors.PFARuntimeException(\"length prefixed bytes is larger than 255 bytes\", 3001, \"pack\", " + posToJava(pos) + ", null);")
        }} mkString("\n")

        counter = 0
        val fillBuffer = exprsDeclareRes map {_ match {
          case BinaryFormatter.DeclarePad(value: JavaCode) =>
            counter += 1
            "bytes.put((byte)0);"
          case BinaryFormatter.DeclareBoolean(value: JavaCode) =>
            counter += 1
            "if (tmp" + counter.toString + ") { bytes.put((byte)1); } else { bytes.put((byte)0); }"
          case BinaryFormatter.DeclareByte(value: JavaCode, unsigned) =>
            counter += 1
            "bytes.put((byte)tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareShort(value: JavaCode, littleEndian, unsigned) =>
            counter += 1
            (if (littleEndian) "bytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "bytes.order(java.nio.ByteOrder.BIG_ENDIAN);") + "\nbytes.putShort((short)tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareInt(value: JavaCode, littleEndian, unsigned) =>
            counter += 1
            (if (littleEndian) "bytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "bytes.order(java.nio.ByteOrder.BIG_ENDIAN);") + "\nbytes.putInt((int)tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareLong(value: JavaCode, littleEndian, unsigned) =>
            counter += 1
            (if (littleEndian) "bytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "bytes.order(java.nio.ByteOrder.BIG_ENDIAN);") + "\nbytes.putLong((long)tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareFloat(value: JavaCode, littleEndian) =>
            counter += 1
            (if (littleEndian) "bytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "bytes.order(java.nio.ByteOrder.BIG_ENDIAN);") + "\nbytes.putFloat((float)tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareDouble(value: JavaCode, littleEndian) =>
            counter += 1
            (if (littleEndian) "bytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "bytes.order(java.nio.ByteOrder.BIG_ENDIAN);") + "\nbytes.putDouble((double)tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareRaw(value: JavaCode) =>
            counter += 1
            "bytes.put(tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareRawSize(value: JavaCode, size) =>
            counter += 1
            "bytes.put(tmp" + counter.toString + ");"
          case BinaryFormatter.DeclareToNull(value: JavaCode) =>
            counter += 1
            "bytes.put(tmp" + counter.toString + ");\nbytes.put((byte)0);"
          case BinaryFormatter.DeclarePrefixed(value: JavaCode) =>
            counter += 1
            "bytes.put((byte)tmp" + counter.toString + ".length);\nbytes.put(tmp" + counter.toString + ");"
        }} mkString("\n")

        JavaCode("""(new Object() {
public byte[] apply() {
int numBytes = 0;
%s
java.nio.ByteBuffer bytes = java.nio.ByteBuffer.allocate(numBytes);
%s
return bytes.array();
} }).apply()""".format(evaluateAndCountBytes, fillBuffer))
      }

      case Unpack.Context(retType, calls, bytes, formatter, thenSymbols, thenClause, elseSymbols, elseClause, pos) => {
        val formatSymbols = formatter map {f => "%s %s;".format(javaType(f.avroType, false, true, true), s(f.value))} mkString("\n")
        val tryToUnpack = formatter flatMap {_ match {
          case BinaryFormatter.DeclarePad(value: String) =>
            List("wrappedBytes.get();",
                 s(value) + " = null;")
          case BinaryFormatter.DeclareBoolean(value: String) =>
            List(s(value) + " = (wrappedBytes.get() != 0);")
          case BinaryFormatter.DeclareByte(value: String, unsigned) =>
            List(s(value) + " = wrappedBytes.get();",
                 if (unsigned) "if (" + s(value) + " < 0) " + s(value) + " += 256;" else "")
          case BinaryFormatter.DeclareShort(value: String, littleEndian, unsigned) =>
            List(if (littleEndian) "wrappedBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "wrappedBytes.order(java.nio.ByteOrder.BIG_ENDIAN);",
                 s(value) + " = wrappedBytes.getShort();",
                 if (unsigned) "if (" + s(value) + " < 0) " + s(value) + " += 65536;" else "")
          case BinaryFormatter.DeclareInt(value: String, littleEndian, unsigned) =>
            List(if (littleEndian) "wrappedBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "wrappedBytes.order(java.nio.ByteOrder.BIG_ENDIAN);",
                 s(value) + " = wrappedBytes.getInt();",
                 if (unsigned) "if (" + s(value) + " < 0) " + s(value) + " += 4294967296L;" else "")
          case BinaryFormatter.DeclareLong(value: String, littleEndian, unsigned) =>
            List(if (littleEndian) "wrappedBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "wrappedBytes.order(java.nio.ByteOrder.BIG_ENDIAN);",
                 s(value) + " = wrappedBytes.getLong();",
                 if (unsigned) "if (" + s(value) + " < 0) " + s(value) + " += 1.8446744073709552e+19;" else "")
          case BinaryFormatter.DeclareFloat(value: String, littleEndian) =>
            List(if (littleEndian) "wrappedBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "wrappedBytes.order(java.nio.ByteOrder.BIG_ENDIAN);",
                 s(value) + " = wrappedBytes.getFloat();")
          case BinaryFormatter.DeclareDouble(value: String, littleEndian) =>
            List(if (littleEndian) "wrappedBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN);" else "wrappedBytes.order(java.nio.ByteOrder.BIG_ENDIAN);",
                 s(value) + " = wrappedBytes.getDouble();")
          case BinaryFormatter.DeclareRawSize(value: String, size) =>
            List("if (wrappedBytes.position() + " + size.toString + " > bytes.length) throw new IllegalArgumentException();",
                 s(value) + " = java.util.Arrays.copyOfRange(bytes, wrappedBytes.position(), wrappedBytes.position() + " + size.toString + ");",
                 "wrappedBytes.position(wrappedBytes.position() + " + size.toString + ");")
          case BinaryFormatter.DeclareToNull(value: String) =>
            List("tmpInteger = wrappedBytes.position();",
                 "while (wrappedBytes.get() != 0);",
                 s(value) + " = java.util.Arrays.copyOfRange(bytes, tmpInteger, wrappedBytes.position() - 1);")
          case BinaryFormatter.DeclarePrefixed(value: String) =>
            List("tmpInteger = wrappedBytes.get();",
                 s(value) + " = java.util.Arrays.copyOfRange(bytes, wrappedBytes.position(), wrappedBytes.position() + tmpInteger);",
                 "wrappedBytes.position(wrappedBytes.position() + tmpInteger);")
        }} mkString("\n")

        (elseSymbols, elseClause) match {
          case (Some(symbols), Some(clause)) =>
            JavaCode("""(new Object() {
%s
public %s apply(byte[] bytes) {
boolean successful = true;
int tmpInteger = 0;
java.nio.ByteBuffer wrappedBytes = java.nio.ByteBuffer.wrap(bytes);
try {
%s
  successful = !wrappedBytes.hasRemaining();
}
catch (java.nio.BufferUnderflowException err) {
  successful = false;
}
catch (IllegalArgumentException err) {
  successful = false;
}
if (successful) {
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
} }).apply(%s)""".format(formatSymbols,
                         javaType(retType, false, true, true),
                         tryToUnpack,
                         symbolFields(thenSymbols),
                         javaType(retType, false, true, true),
                         block(thenClause, ReturnMethod.RETURN, retType),
                         symbolFields(symbols),
                         javaType(retType, false, true, true),
                         block(clause, ReturnMethod.RETURN, retType),
                         bytes))

          case (None, None) =>
            JavaCode("""(new Object() {
%s
%s
public Void apply(byte[] bytes) {
boolean successful = true;
int tmpInteger = 0;
java.nio.ByteBuffer wrappedBytes = java.nio.ByteBuffer.wrap(bytes);
try {
%s
  successful = !wrappedBytes.hasRemaining();
}
catch (java.nio.BufferUnderflowException err) {
  successful = false;
}
catch (IllegalArgumentException err) {
  successful = false;
}
if (successful) {
%s
}
return null;
} }).apply(%s)""".format(formatSymbols,
                         symbolFields(thenSymbols),
                         tryToUnpack,
                         block(thenClause, ReturnMethod.NONE, retType),
                         bytes))

          case _ => throw new RuntimeException("inconsistent call to task(Unpack.Context)")
        }
      }

      case Doc.Context(_, _) => {
        JavaCode("null")
      }

      case Error.Context(_, _, message, code, pos) =>
        JavaCode("""(new Object() {
boolean dummy = true;
public Object apply() {
    if (dummy)
        throw new com.opendatagroup.hadrian.errors.PFAUserException("%s", %s, %s);
    return null;
}
}).apply()""",
          StringEscapeUtils.escapeJava(message),
          (if (code == None)
            "scala.None$.MODULE$"
          else
            "scala.Option.apply(%s)".format(code.get)),
          posToJava(pos))

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
            case Some(x) => x map {case Left(y: String) => "\"" + StringEscapeUtils.escapeJava(y) + "\""; case Right(y: Int) => "\"" + y.toString + "\""} mkString(", ")
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
