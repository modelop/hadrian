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

import java.lang.reflect.Method
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.QuoteMode

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.DoubleNode
import org.codehaus.jackson.node.TextNode
import org.codehaus.jackson.node.JsonNodeFactory

import org.codehaus.janino.JavaSourceClassLoader
import org.codehaus.janino.util.resource.Resource
import org.codehaus.janino.util.resource.ResourceFinder

import org.apache.avro.AvroRuntimeException
import org.apache.avro.file.DataFileStream
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder
import org.apache.avro.reflect.ReflectData
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificFixed
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.specific.SpecificRecordBase

import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JVMNameMangle
import com.opendatagroup.hadrian.shared.PathIndex
import com.opendatagroup.hadrian.shared.I
import com.opendatagroup.hadrian.shared.M
import com.opendatagroup.hadrian.shared.R
import com.opendatagroup.hadrian.datatype.AvroCompiled
import com.opendatagroup.hadrian.datatype.AvroConversions
import com.opendatagroup.hadrian.datatype.AvroType

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
import com.opendatagroup.hadrian.datatype.AvroUnion

import com.opendatagroup.hadrian.reader.JsonDom
import com.opendatagroup.hadrian.reader.JsonObject
import com.opendatagroup.hadrian.reader.JsonArray
import com.opendatagroup.hadrian.reader.JsonNull
import com.opendatagroup.hadrian.reader.JsonTrue
import com.opendatagroup.hadrian.reader.JsonFalse
import com.opendatagroup.hadrian.reader.JsonString
import com.opendatagroup.hadrian.reader.JsonLong
import com.opendatagroup.hadrian.reader.JsonDouble

package data {
  /////////////////////////// defer to official library for general ordering, but implement numeric types with simple operators

  /** Comparison function for two Avro objects, specialized to a given `schema`.
    * 
    * @param schema object type for comparison
    */
  class Comparison(schema: Schema) extends Function2[AnyRef, AnyRef, Int] {
    def ensureUnit(x: Int): Int =
      if (x < 0) -1
      else if (x > 0) 1
      else 0
    /** Returns -1 if `x` < `y`; 0 if `x` and `y` are equal, and +1 if `x` > `y`. */
    def apply(x: AnyRef, y: AnyRef): Int =
      ensureUnit(ReflectData.get().compare(x, y, schema))
  }

  /** Comparison operator for two Avro objects, specialized to a given `schema`.
    * 
    * @param schema object type for comparison
    * @param operator integer lookup for choice of comparison operator, see below
    * 
    *  - -3: less than or equal to (`<=`)
    *  - -2: less than (`<`)
    *  - -1: not equal to (`!=`)
    *  - +1: equal to (`==`)
    *  - +2: greater than or equal to (`>=`)
    *  - +3: greater than (`>`)
    */
  class ComparisonOperator(schema: Schema, operator: Int) extends Function2[AnyRef, AnyRef, Boolean] {
    /** Returns the result of the chosen `operator` on `x` and `y`.
      */
    def apply(x: AnyRef, y: AnyRef): Boolean = {
      val cmp = ReflectData.get().compare(x, y, schema)
      operator match {
        case -3 => cmp <= 0
        case -2 => cmp < 0
        case -1 => cmp != 0
        case 1 => cmp == 0
        case 2 => cmp >= 0
        case 3 => cmp > 0
      }
    }
  }

  /** Less than function for two Avro objects, specialized to a given `schema`.
    * 
    * @param schema object type for comparison
    */
  class ComparisonOperatorLT(schema: Schema) extends Function2[AnyRef, AnyRef, Boolean] {
    /** Returns `true` if `x` is less than `y`, according to `schema`.
      */
    def apply(x: AnyRef, y: AnyRef): Boolean =
      ReflectData.get().compare(x, y, schema) < 0
  }

  /** Max function for two Avro objects, specialized to a given `cmp`.
    * 
    * @param cmp comparison function with `compareTo` rules
    */
  class GenericComparisonMax(cmp: (AnyRef, AnyRef) => java.lang.Integer) {
    /** Returns the maximum of `x` and `y`, according to `compareTo` rules.
      */
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (cmp(x, y) >= 0) x else y
  }

  /** Min function for two Avro objects, specialized to a given `cmp`.
    * 
    * @param cmp comparison function with `compareTo` rules
    */
  class GenericComparisonMin(cmp: (AnyRef, AnyRef) => java.lang.Integer) {
    /** Returns the minimum of `x` and `y`, according to `compareTo` rules.
      */
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (cmp(x, y) < 0) x else y
  }

  /** Max function for two Avro objects, specialized to a given `schema`.
    * 
    * @param schema object type for comparison
    */
  class ComparisonMax(schema: Schema) {
    /** Returns the maximum of `x` and `y`, according to `schema`.
      */
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (ReflectData.get().compare(x, y, schema) >= 0) x else y
  }

  /** Min function for two Avro objects, specialized to a given `schema`.
    * 
    * @param schema object type for comparison
    */
  class ComparisonMin(schema: Schema) {
    /** Returns the minimum of `x` and `y`, according to `schema`.
      */
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (ReflectData.get().compare(x, y, schema) < 0) x else y
  }

  /** Numerical comparison function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalComparison {
    /** Returns -1 if `x` < `y`; 0 if `x` and `y` are equal, and +1 if `x` > `y`. */
    def apply(x: Int, y: Int): Int = if (x > y) 1 else if (x < y) -1 else 0
    /** Returns -1 if `x` < `y`; 0 if `x` and `y` are equal, and +1 if `x` > `y`. */
    def apply(x: Long, y: Long): Int = if (x > y) 1 else if (x < y) -1 else 0
    /** Returns -1 if `x` < `y`; 0 if `x` and `y` are equal, and +1 if `x` > `y`. */
    def apply(x: Float, y: Float): Int =
      if (java.lang.Float.isNaN(x)) {
        if (java.lang.Float.isNaN(y))
          0
        else
          1
      }
      else {
        if (java.lang.Float.isNaN(y))
          -1
        else
          if (x > y) 1 else if (x < y) -1 else 0
      }
    /** Returns -1 if `x` < `y`; 0 if `x` and `y` are equal, and +1 if `x` > `y`. */
    def apply(x: Double, y: Double): Int =
      if (java.lang.Double.isNaN(x)) {
        if (java.lang.Double.isNaN(y))
          0
        else
          1
      }
      else {
        if (java.lang.Double.isNaN(y))
          -1
        else
          if (x > y) 1 else if (x < y) -1 else 0
      }
  }

  /** Numerical equality function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalEQ {
    /** Returns `true` if `x` equals `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Boolean = (x == y)
    /** Returns `true` if `x` equals `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Boolean = (x == y)
    /** Returns `true` if `x` equals `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Boolean =
      if (java.lang.Float.isNaN(x)  &&  java.lang.Float.isNaN(y))
        true
      else
        x == y
    /** Returns `true` if `x` equals `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Boolean =
      if (java.lang.Double.isNaN(x)  &&  java.lang.Double.isNaN(y))
        true
      else
        x == y
  }

  /** Numerical greater than or equal function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalGE {
    /** Returns `true` if `x` is greater than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Boolean = (x >= y)
    /** Returns `true` if `x` is greater than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Boolean = (x >= y)
    /** Returns `true` if `x` is greater than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Boolean =
      if (java.lang.Float.isNaN(x))
        true
      else
        x >= y
    /** Returns `true` if `x` is greater than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Boolean =
      if (java.lang.Double.isNaN(x))
        true
      else
        x >= y
  }

  /** Numerical greater than function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalGT {
    /** Returns `true` if `x` is greater than `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Boolean = (x > y)
    /** Returns `true` if `x` is greater than `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Boolean = (x > y)
    /** Returns `true` if `x` is greater than `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Boolean =
      if (java.lang.Float.isNaN(x)  &&  !java.lang.Float.isNaN(y))
        true
      else
        x > y
    /** Returns `true` if `x` is greater than `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Boolean =
      if (java.lang.Double.isNaN(x)  &&  !java.lang.Double.isNaN(y))
        true
      else
        x > y
  }

  /** Numerical not equal function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalNE {
    /** Returns `true` if `x` is not equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Boolean = (x != y)
    /** Returns `true` if `x` is not equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Boolean = (x != y)
    /** Returns `true` if `x` is not equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Boolean =
      if (java.lang.Float.isNaN(x)  &&  java.lang.Float.isNaN(y))
        false
      else if (java.lang.Float.isNaN(x)  ||  java.lang.Float.isNaN(y))
        true
      else
        x != y
    /** Returns `true` if `x` is not equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Boolean =
      if (java.lang.Double.isNaN(x)  &&  java.lang.Double.isNaN(y))
        false
      else if (java.lang.Double.isNaN(x)  ||  java.lang.Double.isNaN(y))
        true
      else
        x != y
  }

  /** Numerical less than function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalLT {
    /** Returns `true` if `x` is less than `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Boolean = (x < y)
    /** Returns `true` if `x` is less than `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Boolean = (x < y)
    /** Returns `true` if `x` is less than `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Boolean =
      if (java.lang.Float.isNaN(y)  &&  !java.lang.Float.isNaN(x))
        true
      else
        x < y
    /** Returns `true` if `x` is less than `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Boolean =
      if (java.lang.Double.isNaN(y)  &&  !java.lang.Double.isNaN(x))
        true
      else
        x < y
  }

  /** Numerical less than or equal function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalLE {
    /** Returns `true` if `x` is less than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Boolean = (x <= y)
    /** Returns `true` if `x` is less than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Boolean = (x <= y)
    /** Returns `true` if `x` is less than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Boolean =
      if (java.lang.Float.isNaN(y))
        true
      else
        x <= y
    /** Returns `true` if `x` is less than or equal to `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Boolean =
      if (java.lang.Double.isNaN(y))
        true
      else
        x <= y
  }

  /** Numerical maximum function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalMax {
    /** Returns the maximum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Int = if (x >= y) x else y
    /** Returns the maximum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Long = if (x >= y) x else y
    /** Returns the maximum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Float =
      if (java.lang.Float.isNaN(x))
        x
      else if (java.lang.Float.isNaN(y))
        y
      else if (x >= y)
        x
      else
        y
    /** Returns the maximum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Double =
      if (java.lang.Double.isNaN(x))
        x
      else if (java.lang.Double.isNaN(y))
        y
      else if (x >= y)
        x
      else
        y
  }

  /** Numerical minimum function for two Avro numbers; applies the correct Avro rules for `NaN`.
    * 
    * `NaN` is considered ''higher'' than all other numbers (including positive infinity).
    */
  object NumericalMin {
    /** Returns the minimum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Int, y: Int): Int = if (x < y) x else y
    /** Returns the minimum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Long, y: Long): Long = if (x < y) x else y
    /** Returns the minimum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Float, y: Float): Float =
      if (java.lang.Float.isNaN(x))
        y
      else if (java.lang.Float.isNaN(y))
        x
      else if (x < y)
        x
      else
        y
    /** Returns the minimum of `x` and `y`, using Avro `NaN` rules.
      */
    def apply(x: Double, y: Double): Double =
      if (java.lang.Double.isNaN(x))
        y
      else if (java.lang.Double.isNaN(y))
        x
      else if (x < y)
        x
      else
        y
  }

  /////////////////////////// replace Avro's in-memory data representation with our own classes

  /** Overrides Avro's data model with one designed for PFA.
    * 
    *  - "bytes" are loaded into raw byte arrays, rather than `java.nio.ByteBuffers`
    *  - "string" are loaded into Java `Strings`, rather than `org.apache.avro.util.Utf8`
    *  - "array" are loaded into [[com.opendatagroup.hadrian.data.PFAArray PFAArray]]
    *  - "map" are loaded into [[com.opendatagroup.hadrian.data.PFAMap PFAMap]]
    *  - "fixed" are loaded into [[com.opendatagroup.hadrian.data.GenericPFAFixed GenericPFAFixed]]
    *  - "enum" are loaded into [[com.opendatagroup.hadrian.data.GenericPFAEnumSymbol GenericPFAEnumSymbol]]
    *  - "record" are loaded into [[com.opendatagroup.hadrian.data.GenericPFARecord GenericPFARecord]]
    * 
    * @param classLoader unused formality
    */
  class PFAGenericData(classLoader: java.lang.ClassLoader) extends GenericData() {
    override def isBytes(datum: AnyRef): Boolean = datum.isInstanceOf[Array[Byte]]

    override def deepCopy[X](schema: Schema, value: X): X =
      if (value == null)
        null.asInstanceOf[X]
      else
        value

    override def createFixed(old: AnyRef, bytes: Array[Byte], schema: Schema): AnyRef =
      new GenericPFAFixed(schema, bytes)

    override def createFixed(old: AnyRef, schema: Schema): AnyRef =
      new GenericPFAFixed(schema, Array.fill[Byte](schema.getFixedSize)(0))

    override def createEnum(symbol: String, schema: Schema): AnyRef =
      new GenericPFAEnumSymbol(schema, symbol)

    override def newRecord(old: AnyRef, schema: Schema): AnyRef =
      new GenericPFARecord(schema)
  }

  /** Overrides Avro's data model with one designed for PFA with classes customized for each PFA engine.
    * 
    *  - "bytes" are loaded into raw byte arrays, rather than `java.nio.ByteBuffers`
    *  - "string" are loaded into Java `Strings`, rather than `org.apache.avro.util.Utf8`
    *  - "array" are loaded into [[com.opendatagroup.hadrian.data.PFAArray PFAArray]]
    *  - "map" are loaded into [[com.opendatagroup.hadrian.data.PFAMap PFAMap]]
    *  - "fixed" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFAFixed PFAFixed]]
    *  - "enum" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFAEnumSymbol PFAEnumSymbol]]
    *  - "record" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFARecord PFARecord]]
    * 
    * @param classLoader where the custom classes live
    */
  class PFASpecificData(classLoader: java.lang.ClassLoader) extends SpecificData(classLoader) {
    override def isBytes(datum: AnyRef): Boolean = datum.isInstanceOf[Array[Byte]]

    override def createEnum(symbol: String, schema: Schema): AnyRef =
      getClass(schema).getConstructor(classOf[Schema], classOf[String]).newInstance(schema, symbol).asInstanceOf[AnyRef]

    override def deepCopy[X](schema: Schema, value: X): X =
      if (value == null)
        null.asInstanceOf[X]
      else
        value
  }

  /** Overrides Avro's data model with one designed for PFA.
    * 
    *  - "bytes" are loaded into raw byte arrays, rather than `java.nio.ByteBuffers`
    *  - "string" are loaded into Java `Strings`, rather than `org.apache.avro.util.Utf8`
    *  - "array" are loaded into [[com.opendatagroup.hadrian.data.PFAArray PFAArray]]
    *  - "map" are loaded into [[com.opendatagroup.hadrian.data.PFAMap PFAMap]]
    *  - "fixed" are loaded into [[com.opendatagroup.hadrian.data.GenericPFAFixed GenericPFAFixed]]
    *  - "enum" are loaded into [[com.opendatagroup.hadrian.data.GenericPFAEnumSymbol GenericPFAEnumSymbol]]
    *  - "record" are loaded into [[com.opendatagroup.hadrian.data.GenericPFARecord GenericPFARecord]]
    * 
    * @param schema schema of the datum to read
    * @param pfaGenericData data model
    */
  class GenericPFADatumReader[X](schema: Schema, pfaGenericData: PFAGenericData) extends GenericDatumReader[X](schema, schema, pfaGenericData) {
    // load strings with the native Java String class (UTF-16), rather than avro.util.Utf8
    override def readString(old: AnyRef, in: Decoder): AnyRef = in.readString(null).toString
    override def createString(value: String): AnyRef = value

    // load bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def readBytes(old: AnyRef, in: Decoder): AnyRef = in.readBytes(null).array
    override def createBytes(value: Array[Byte]): AnyRef = value

    // load arrays as PFAArray[_], rather than java.util.List[_]
    // I'm not sure why Avro gives me the Schema for this one and not for the others,
    // but I'll take advantage of it and specialize the arrays for primitive types.
    override def newArray(old: AnyRef, size: Int, schema: Schema): AnyRef = PFAArray.empty(size, schema)

    // load arrays as PFAMap[_], rather than java.util.Map[Utf8, _]
    override def newMap(old: AnyRef, size: Int): AnyRef = PFAMap.empty[AnyRef](size)
  }

  /** Overrides Avro's data model with one designed for PFA.
    * 
    *  - "bytes" are loaded into raw byte arrays, rather than `java.nio.ByteBuffers`
    *  - "string" are loaded into Java `Strings`, rather than `org.apache.avro.util.Utf8`
    *  - "array" are loaded into [[com.opendatagroup.hadrian.data.PFAArray PFAArray]]
    *  - "map" are loaded into [[com.opendatagroup.hadrian.data.PFAMap PFAMap]]
    *  - "fixed" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFAFixed PFAFixed]]
    *  - "enum" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFAEnumSymbol PFAEnumSymbol]]
    *  - "record" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFARecord PFARecord]]
    * 
    * @param pfaGenericData data model that points to the custom `ClassLoader`
    */
  class PFADatumReader[X](pfaSpecificData: PFASpecificData) extends SpecificDatumReader[X](pfaSpecificData) {
    // load strings with the native Java String class (UTF-16), rather than avro.util.Utf8
    override def readString(old: AnyRef, in: Decoder): AnyRef = in.readString(null).toString
    override def createString(value: String): AnyRef = value

    // load bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def readBytes(old: AnyRef, in: Decoder): AnyRef = in.readBytes(null).array
    override def createBytes(value: Array[Byte]): AnyRef = value

    // load arrays as PFAArray[_], rather than java.util.List[_]
    // I'm not sure why Avro gives me the Schema for this one and not for the others,
    // but I'll take advantage of it and specialize the arrays for primitive types.
    override def newArray(old: AnyRef, size: Int, schema: Schema): AnyRef = PFAArray.empty(size, schema)

    // load arrays as PFAMap[_], rather than java.util.Map[Utf8, _]
    override def newMap(old: AnyRef, size: Int): AnyRef = PFAMap.empty[AnyRef](size)
  }

  /** Overrides Avro's data model with one designed for PFA.
    * 
    *  - "bytes" are loaded into raw byte arrays, rather than `java.nio.ByteBuffers`
    *  - "string" are loaded into Java `Strings`, rather than `org.apache.avro.util.Utf8`
    *  - "array" are loaded into [[com.opendatagroup.hadrian.data.PFAArray PFAArray]]
    *  - "map" are loaded into [[com.opendatagroup.hadrian.data.PFAMap PFAMap]]
    *  - "fixed" are loaded into [[com.opendatagroup.hadrian.data.GenericPFAFixed GenericPFAFixed]]
    *  - "enum" are loaded into [[com.opendatagroup.hadrian.data.GenericPFAEnumSymbol GenericPFAEnumSymbol]]
    *  - "record" are loaded into [[com.opendatagroup.hadrian.data.GenericPFARecord GenericPFARecord]]
    * 
    * @param schema of datum to write
    * @param pfaGenericData data model
    */
  class GenericPFADatumWriter[X](schema: Schema, pfaGenericData: PFAGenericData) extends GenericDatumWriter[X](schema, pfaGenericData) {
    // write bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def writeBytes(datum: AnyRef, out: Encoder): Unit =
      out.writeBytes(java.nio.ByteBuffer.wrap(datum.asInstanceOf[Array[Byte]]))
  }

  /** Overrides Avro's data model with one designed for PFA.
    * 
    *  - "bytes" are loaded into raw byte arrays, rather than `java.nio.ByteBuffers`
    *  - "string" are loaded into Java `Strings`, rather than `org.apache.avro.util.Utf8`
    *  - "array" are loaded into [[com.opendatagroup.hadrian.data.PFAArray PFAArray]]
    *  - "map" are loaded into [[com.opendatagroup.hadrian.data.PFAMap PFAMap]]
    *  - "fixed" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFAFixed PFAFixed]]
    *  - "enum" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFAEnumSymbol PFAEnumSymbol]]
    *  - "record" are loaded into a dynamically generated subclass of [[com.opendatagroup.hadrian.data.PFARecord PFARecord]]
    * 
    * @param Schema of datum to write
    * @param pfaSpecificData data model
    */
  class PFADatumWriter[X](schema: Schema, pfaSpecificData: PFASpecificData) extends SpecificDatumWriter[X](schema, pfaSpecificData) {
    // write bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def writeBytes(datum: AnyRef, out: Encoder): Unit =
      out.writeBytes(java.nio.ByteBuffer.wrap(datum.asInstanceOf[Array[Byte]]))
  }

  /** Interface for [[com.opendatagroup.hadrian.data.GenericPFARecord GenericPFARecord]] all dynamically generated subclasses of [[com.opendatagroup.hadrian.data.PFARecord PFARecord]].
    */
  trait AnyPFARecord {
    /** Access to record schema.
      */
    def getSchema: Schema
    /** Get an array of all fields, in order.
      */
    def getAll(): Array[AnyRef]
    /** Get a particular field by name.
      */
    def get(fieldName: String): AnyRef
  }

  /** PFA record with no dynamically generated code (used as an intermediate for data not assigned to any scoring engine).
    */
  class GenericPFARecord(schema: Schema) extends org.apache.avro.generic.GenericData.Record(schema) with AnyPFARecord {
    private val fieldNumbers = 0 until schema.getFields.size toArray
    def getAll() = fieldNumbers.map(get)
  }

  /** Abstract superclass for all dynamically generated record classes.
    */
  abstract class PFARecord extends SpecificRecordBase with AnyPFARecord {
    /** Another access to record schema, needed by Avro.
      */
    def getClassSchema: Schema
    /** Number of fields.
      */
    def numFields: Int
    /** Names of fields.
      */
    def fieldNames: Array[String]
    /** Get numerical field index by field name.
      */
    def fieldIndex(name: String): Int
    /** Field types, in field order.
      */
    def fieldTypes: Array[Schema]
    /** Get field value by index number (when direct access via custom Java code is not possible).
      */
    def get(field: Int): AnyRef
    /** Get field value by name (when direct access via custom Java code is not possible).
      */
    def get(field: String): AnyRef
    /** Set field value by index number (when direct access via custom Java code is not possible).
      */
    def put(field: Int, value: Any): Unit
    /** Set field value by name (when direct access via custom Java code is not possible).
      */
    def put(field: String, value: Any): Unit
    /** Set field value by index number (used internally by jvmcompiler only).
      */
    def internalUpdate(field: Int, value: Any): PFARecord
    /** Set field value by name (used internally by jvmcompiler only).
      */
    def internalUpdate(field: String, value: Any): PFARecord

    /** Set multiple fields at once (used by many library functions).
      */
    def multiUpdate(fields: Array[String], values: Array[Any]): PFARecord

    /** Helper function to ensure that numerical types are exactly right (int for int, long for long, etc.).
      * 
      * @param elem datum to convert
      * @param schema schema to convert it to
      * @return converted datum (unboxed primitives)
      */
    def converted[X](elem: X, schema: Schema): Any =
      if (schema == null)
        elem
      else
        schema.getType match {
          case Schema.Type.INT => elem match {
            case x: Int => x
            case x: Long => x.toInt
            case x: Float => x.toInt
            case x: Double => x.toInt
            case x: AnyRef => x.asInstanceOf[java.lang.Number].intValue
          }
          case Schema.Type.LONG => elem match {
            case x: Long => x
            case x: Float => x.toLong
            case x: Double => x.toLong
            case x: AnyRef => x.asInstanceOf[java.lang.Number].longValue
          }
          case Schema.Type.FLOAT => elem match {
            case x: Float => x
            case x: Double => x.toFloat
            case x: AnyRef => x.asInstanceOf[java.lang.Number].floatValue
          }
          case Schema.Type.DOUBLE => elem match {
            case x: Double => x
            case x: AnyRef => x.asInstanceOf[java.lang.Number].doubleValue
          }
          case _ => elem
        }

    /** Calls get(i).
      */
    def apply(i: String): AnyRef = get(i)
    /** Returns an updated copy of this record with one field replaced.
      * 
      * @param i numerical index of field to have replaced
      * @param elem new value for field
      * @param schema schema of field
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated[X](i: Int, elem: X, schema: Schema): PFARecord = internalUpdate(i, converted(elem, schema))
    /** Returns an updated copy of this record with one field replaced.
      * 
      * @param i name of field to have replaced
      * @param elem new value for field
      * @param schema schema of field
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated[X](i: String, elem: X, schema: Schema): PFARecord = internalUpdate(i, converted(elem, schema))

    /** Returns an updated copy of this record (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated(path: List[PathIndex], elem: AnyRef, schema: Schema): PFARecord = updated(path, (dummy: AnyRef) => elem, schema)
    /** Returns an updated copy of this record (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated(path: Array[PathIndex], elem: AnyRef, schema: Schema): PFARecord = updated(path.toList, (dummy: AnyRef) => elem, schema)
    /** Returns an updated copy of this record (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated(path: Array[PathIndex], elem: AnyRef, schema: Schema, arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): PFARecord =
      try {
        updated(path, (dummy: AnyRef) => elem, schema)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
        case err: java.util.NoSuchElementException => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      }
    /** Returns an updated copy of this record (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema): PFARecord = updated(path.toList, updator, schema)
    /** Returns an updated copy of this record (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @param arrayErrStr error message if an array path index is wrong
      * @param arrayErrCode error code if an array path index is wrong
      * @param mapErrStr error message if a map path index is wrong
      * @param mapErrCode error code if a map path index is wrong
      * @param fcnName name of the calling PFA function
      * @param pos locator mark for the calling PFA function
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema, arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): PFARecord =
      try {
        updated(path, updator, schema)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
        case err: java.util.NoSuchElementException => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      }

    /** Returns an updated copy of this record (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @return new record of the same specific type, leaving the old one untouched
      */
    def updated[Y](path: List[PathIndex], updator: Y => Y, schema: Schema): PFARecord = path match {
      case R(f) :: Nil => updated(f, updator(apply(f).asInstanceOf[Y]), schema)
      case R(f) :: rest => {
        apply(f) match {
          case x: PFAArray[_] => updated(f, x.updated(rest, updator, schema), null)
          case x: PFAMap[_] => updated(f, x.updated(rest, updator, schema), null)
          case x: PFARecord => updated(f, x.updated(rest, updator, schema), null)
          case _ => throw new IllegalArgumentException("path used on a non-container")
        }
      }
      case _ :: rest => throw new IllegalArgumentException("wrong PathIndex element used on a record")
      case Nil => throw new IllegalArgumentException("empty PathIndex used on a record")
    }

    /** Return `true` if a field named `x` exists; `false` otherwise.
      */
    def fieldNameExists(x: String): Boolean = fieldNames.contains(x)
  }

  /** Interface for [[com.opendatagroup.hadrian.data.GenericPFAFixed GenericPFAFixed]] all dynamically generated subclasses of [[com.opendatagroup.hadrian.data.PFAFixed PFAFixed]].
    */
  trait AnyPFAFixed {
    /** Access to schema.
      */
    def getSchema: Schema
    /** Get the data content of this fixed object.
      */
    def bytes(): Array[Byte]
  }

  /** PFA fixed with no dynamically generated code (used as an intermediate for data not assigned to any scoring engine).
    */
  class GenericPFAFixed(schema: Schema, b: Array[Byte]) extends org.apache.avro.generic.GenericData.Fixed(schema, b) with AnyPFAFixed

  /** Abstract superclass for all dynamically generated fixed classes.
    */
  abstract class PFAFixed extends SpecificFixed with AnyPFAFixed {
    /** Another access to fixed schema, needed by Avro.
      */
    def getClassSchema: Schema
    /** Width of fixed bytes.
      */
    def size: Int
    /** Create a new fixed object with the same specific class and a new value.
      * 
      * @param replacement new bytes: if `replacement` is shorter than `size`, bytes beyond the replacement's length are taken from the original; if `replacement` is longer than `size`, the excess bytes are truncated.
      */
    def overlay(replacement: Array[Byte]): PFAFixed
  }

  /** Interface for [[com.opendatagroup.hadrian.data.GenericPFAEnumSymbol GenericPFAEnumSymbol]] all dynamically generated subclasses of [[com.opendatagroup.hadrian.data.PFAEnumSymbol PFAEnumSymbol]].
    */
  trait AnyPFAEnumSymbol {
    /** Access to schema.
      */
    def getSchema: Schema
    def toString(): String
  }

  /** PFA enum symbol with no dynamically generated code (used as an intermediate for data not assigned to any scoring engine).
    * 
    * @param schema reference to schema
    * @param symbol symbol
    */
  class GenericPFAEnumSymbol(schema: Schema, symbol: String) extends org.apache.avro.generic.GenericData.EnumSymbol(schema, symbol) with AnyPFAEnumSymbol

  /** Abstract superclass for all dynamically generated enum classes.
    */
  abstract class PFAEnumSymbol extends AnyPFAEnumSymbol {
    /** Another access to enum schema, needed by Avro.
      */
    def getClassSchema: Schema
    /** Symbol identifier as an integer index. */
    def value: Int
    /** Number of symbols in this schema. */
    def numSymbols: Int
    /** Convert an integer identifier to its string representation. */
    def intToStr(i: Int): String
    /** Convert a string representation to its integer identifier. */
    def strToInt(x: String): Int
  }

  object PFAArray {
    /** Create an empty PFA array.
      * 
      * @param sizeHint for initializing the `Vector.Builder`
      * @param schema array type, including items type
      */
    def empty(sizeHint: Int, schema: Schema) = schema.getElementType.getType match {
      case Schema.Type.NULL => empty[java.lang.Void](sizeHint)
      case Schema.Type.BOOLEAN => empty[Boolean](sizeHint)
      case Schema.Type.INT => empty[Int](sizeHint)
      case Schema.Type.LONG => empty[Long](sizeHint)
      case Schema.Type.FLOAT => empty[Float](sizeHint)
      case Schema.Type.DOUBLE => empty[Double](sizeHint)
      case Schema.Type.BYTES => empty[Array[Byte]](sizeHint)
      case Schema.Type.FIXED => empty[PFAFixed](sizeHint)
      case Schema.Type.STRING => empty[String](sizeHint)
      case Schema.Type.ENUM => empty[PFAEnumSymbol](sizeHint)
      case Schema.Type.ARRAY => empty[PFAArray[_]](sizeHint)
      case Schema.Type.MAP => empty[java.util.Map[String, _]](sizeHint)
      case Schema.Type.RECORD => empty[PFARecord](sizeHint)
      case Schema.Type.UNION => empty[AnyRef](sizeHint)
    }
    /** Create an empty PFA array.
      * 
      * @param sizeHint for initializing the `Vector.Builder`
      */
    def empty[X](sizeHint: Int) = {
      val builder = Vector.newBuilder[X]
      builder.sizeHint(sizeHint)
      new PFAArray[X](builder, null)
    }
    /** Create an empty PFA array.
      */
    def empty[X]() = new PFAArray[X](Vector.newBuilder[X], null)
    /** Create a PFA array from a Scala `Vector` (skips `Vector.Builder` stage).
      * 
      * Primitives should be typed in Scala as raw primitives (e.g. `PFAArray[Int]`, not `PFAArray[java.lang.Integer]`).
      */
    def fromVector[X](x: Vector[X]) = new PFAArray[X](null, x)
  }

  /** Represents all arrays in PFA (generic or specific).
    * 
    * Data are stored in an immutable Scala `Vector`, which is either created directly by the `PFAArray.fromVector` companion object method or filled by Avro (see below).
    * 
    * Primitives should be typed in Scala as raw primitives (e.g. `PFAArray[Int]`, not `PFAArray[java.lang.Integer]`).
    * 
    * To interact with the Avro library, a PFAArray satisfies the `java.util.List` contract, but only minimally (most methods throw `NotImplementedException`).
    * 
    * It has two stages:
    * 
    *  - filling stage: Avro library's code calls `add`, the PFAArray grows using a `Vector.Builder`
    *  - PFA stage: after first access to `toVector`, the builder is dropped and the PFAArray becomes immutable
    * 
    * The `toVector` method should be considered lightweight (the backing `Vector` is only created once).
    * 
    * PFAArrays can also have metadata to optimize some library functions (e.g. nearest neighbor benefits from having data arranged in a kd-tree, rather than just a flat list). Library functions use this at will.
    */
  class PFAArray[X](private val builder: mutable.Builder[X, Vector[X]], private var vector: Vector[X]) extends java.util.List[X] {
    // PFAArray has two states:
    //    1. vector == null during the filling stage (Avro's code calls java.util.List.add())
    //    2. vector != null during the PFA stage (all calls are immutable)
    // Use of the java.util.List interface after it has been sealed by a PFA call causes a RuntimeException.
    // Most of the java.util.List interface causes NotImplementedErrors, since this is just to satisfy Avro.

    /** Access the Scala `Vector` behind this PFAArray.
      * 
      * After the first call to this function, it references a cached `Vector`.
      */
    def toVector: Vector[X] = {
      if (vector == null)
        vector = builder.result
      vector
    }

    /** Access to optional metadata, which is created on first access.
      */
    def metadata = {
      if (_metadata == null)
        _metadata = Map[String, Any]()
      _metadata
    }
    def metadata_=(x: Map[String, Any]) {
      _metadata = x
    }
    private var _metadata: Map[String, Any] = null

    override def toString(): String = "[" + toVector.map(x => if (x == null) "null" else x).mkString(", ") + "]"
    override def equals(that: Any): Boolean = that match {
      case thatArray: PFAArray[_] => this.toVector == thatArray.toVector
      case _ => false
    }
    override def hashCode(): Int = toVector.hashCode

    // mutable java.util.List methods
    /** The only `java.util.List` method that has been implemented (needed for filling in the Avro library).
      */
    override def add(obj: X): Boolean = {
      if (vector != null)
        throw new RuntimeException("PFAArray.add should not be used outside of Avro")
      builder += obj
      true
    }
    /** Raises `NotImplementedError`. */
    override def add(index: Int, obj: X): Unit = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def addAll(that: java.util.Collection[_ <: X]): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def addAll(index: Int, that: java.util.Collection[_ <: X]): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def clear(): Unit = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def remove(index: Int): X = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def remove(obj: AnyRef): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def removeAll(that: java.util.Collection[_]): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def retainAll(that: java.util.Collection[_]): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def set(index: Int, element: X): X = throw new NotImplementedError

    // immutable java.util.List methods
    /** Raises `NotImplementedError`. */
    override def contains(obj: AnyRef): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def containsAll(that: java.util.Collection[_]): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def get(index: Int): X = apply(index)
    /** Raises `NotImplementedError`. */
    override def indexOf(obj: AnyRef): Int = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def isEmpty(): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def iterator(): java.util.Iterator[X] = toVector.iterator
    /** Raises `NotImplementedError`. */
    override def lastIndexOf(obj: AnyRef): Int = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def listIterator(): java.util.ListIterator[X] = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def listIterator(index: Int): java.util.ListIterator[X] = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def size(): Int = toVector.size
    /** Raises `NotImplementedError`. */
    override def subList(fromIndex: Int, toIndex: Int): java.util.List[X] = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def toArray(): Array[AnyRef] = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def toArray[Y](y: Array[Y with AnyRef]): Array[Y with AnyRef] = throw new NotImplementedError

    /** Helper function to ensure that numerical types are exactly right (int for int, long for long, etc.).
      * 
      * @param elem datum to convert
      * @param schema schema to convert it to
      * @return converted datum (unboxed primitives)
      */
    def converted[X](elem: X, schema: Schema): X =
      if (schema == null)
        elem
      else
        schema.getType match {
          case Schema.Type.INT => elem match {
            case x: Int => x.asInstanceOf[X]
            case x: Long => x.toInt.asInstanceOf[X]
            case x: Float => x.toInt.asInstanceOf[X]
            case x: Double => x.toInt.asInstanceOf[X]
            case x: AnyRef => x.asInstanceOf[java.lang.Number].intValue.asInstanceOf[X]
          }
          case Schema.Type.LONG => elem match {
            case x: Long => x.asInstanceOf[X]
            case x: Float => x.toLong.asInstanceOf[X]
            case x: Double => x.toLong.asInstanceOf[X]
            case x: AnyRef => x.asInstanceOf[java.lang.Number].longValue.asInstanceOf[X]
          }
          case Schema.Type.FLOAT => elem match {
            case x: Float => x.asInstanceOf[X]
            case x: Double => x.toFloat.asInstanceOf[X]
            case x: AnyRef => x.asInstanceOf[java.lang.Number].floatValue.asInstanceOf[X]
          }
          case Schema.Type.DOUBLE => elem match {
            case x: Double => x.asInstanceOf[X]
            case x: AnyRef => x.asInstanceOf[java.lang.Number].doubleValue.asInstanceOf[X]
          }
          case _ => elem
        }

    /** Calls `toVector.apply(i)`.
      */
    def apply(i: Int): X = toVector.apply(i)
    /** Returns an updated copy of this PFAArray with one element replaced.
      * 
      * @param i numerical index of element to have replaced
      * @param elem new value for element
      * @param schema schema of element
      * @return new PFAArray, leaving the old one untouched
      */
    def updated(i: Int, elem: X, schema: Schema): PFAArray[X] = PFAArray.fromVector(toVector.updated(i, converted(elem, schema)))

    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new PFAArray, leaving the old one untouched
      */
    def updated(path: List[PathIndex], elem: X, schema: Schema): PFAArray[X] = updated(path, (dummy: X) => elem, schema)
    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new PFAArray, leaving the old one untouched
      */
    def updated(path: Array[PathIndex], elem: X, schema: Schema): PFAArray[X] = updated(path.toList, (dummy: X) => elem, schema)
    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @param arrayErrStr error message if an array path index is wrong
      * @param arrayErrCode error code if an array path index is wrong
      * @param mapErrStr error message if a map path index is wrong
      * @param mapErrCode error code if a map path index is wrong
      * @param fcnName name of the calling PFA function
      * @param pos locator mark for the calling PFA function
      * @return new PFAArray, leaving the old one untouched
      */
    def updated(path: Array[PathIndex], elem: X, schema: Schema, arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): PFAArray[X] =
      try {
        updated(path.toList, (dummy: X) => elem, schema)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
        case err: java.util.NoSuchElementException => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      }
    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @return new PFAArray, leaving the old one untouched
      */
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema): PFAArray[X] = updated(path.toList, updator, schema)
    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @param arrayErrStr error message if an array path index is wrong
      * @param arrayErrCode error code if an array path index is wrong
      * @param mapErrStr error message if a map path index is wrong
      * @param mapErrCode error code if a map path index is wrong
      * @param fcnName name of the calling PFA function
      * @param pos locator mark for the calling PFA function
      * @return new PFAArray, leaving the old one untouched
      */
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema, arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): PFAArray[X] =
      try {
        updated(path.toList, updator, schema)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
        case err: java.util.NoSuchElementException => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      }

    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @return new PFAArray, leaving the old one untouched
      */
    def updated[Y](path: List[PathIndex], updator: Y => Y, schema: Schema): PFAArray[X] = path match {
      case I(i) :: Nil => updated(i, updator(apply(i).asInstanceOf[Y]).asInstanceOf[X], schema)
      case I(i) :: rest => {
        apply(i) match {
          case x: PFAArray[_] => updated(i, x.updated(rest, updator, schema).asInstanceOf[X], null)
          case x: PFAMap[_] => updated(i, x.updated(rest, updator, schema).asInstanceOf[X], null)
          case x: PFARecord => updated(i, x.updated(rest, updator, schema).asInstanceOf[X], null)
          case _ => throw new IllegalArgumentException("path used on a non-container")
        }
      }
      case _ :: rest => throw new IllegalArgumentException("wrong PathIndex element used on an array")
      case Nil => throw new IllegalArgumentException("empty PathIndex used on an array")
    }
  }

  object PFAMap {
    /** Create an empty PFA map.
      * 
      * @param sizeHint for initializing the `Map.Builder`
      * @param schema map type, including values type
      */
    def empty(sizeHint: Int, schema: Schema) = schema.getValueType.getType match {
      case Schema.Type.NULL => empty[java.lang.Void](sizeHint)
      case Schema.Type.BOOLEAN => empty[java.lang.Boolean](sizeHint)
      case Schema.Type.INT => empty[java.lang.Integer](sizeHint)
      case Schema.Type.LONG => empty[java.lang.Long](sizeHint)
      case Schema.Type.FLOAT => empty[java.lang.Float](sizeHint)
      case Schema.Type.DOUBLE => empty[java.lang.Double](sizeHint)
      case Schema.Type.BYTES => empty[Array[Byte]](sizeHint)
      case Schema.Type.FIXED => empty[PFAFixed](sizeHint)
      case Schema.Type.STRING => empty[String](sizeHint)
      case Schema.Type.ENUM => empty[PFAEnumSymbol](sizeHint)
      case Schema.Type.ARRAY => empty[PFAArray[_]](sizeHint)
      case Schema.Type.MAP => empty[java.util.Map[String, _]](sizeHint)
      case Schema.Type.RECORD => empty[PFARecord](sizeHint)
      case Schema.Type.UNION => empty[AnyRef](sizeHint)
    }
    /** Create an empty PFA map.
      * 
      * @param sizeHint for initializing the `Map.Builder`
      */
    def empty[X <: AnyRef](sizeHint: Int) = {
      val builder = Map.newBuilder[String, X]
      builder.sizeHint(sizeHint)
      new PFAMap[X](builder, null)
    }
    /** Create an empty PFA map.
      */
    def empty[X <: AnyRef]() = new PFAMap[X](Map.newBuilder[String, X], null)
    /** Create a PFA map from a Scala `Map` (skips `Map.Builder` stage).
      * 
      * Primitives should be typed in Scala as boxed primitives (e.g. `PFAMap[java.lang.Integer]`, not `PFAMap[Int]`).
      */
    def fromMap[X <: AnyRef](x: Map[String, X]) = new PFAMap[X](null, x)
  }

  /** Represents all maps in PFA (generic or specific).
    * 
    * Data are stored in an immutable Scala `Map`, which is either created directly by the `PFAMap.fromMap` companion object method or filled by Avro (see below).
    * 
    * Primitives should be typed in Scala as boxed primitives (e.g. `PFAArray[java.lang.Integer]`, not `PFAArray[Int]`).
    * 
    * To interact with the Avro library, a PFAMap satisfies the `java.util.Map` contract, but only minimally (most methods throw `NotImplementedException`).
    * 
    * It has two stages:
    * 
    *  - filling stage: Avro library's code calls `put`, the PFAMap grows using a `Map.Builder`
    *  - PFA stage: after first access to `toMap`, the builder is dropped and the PFAMap becomes immutable
    * 
    * The `toMap` method should be considered lightweight (the backing `Map` is only created once).
    */
  class PFAMap[X <: AnyRef](private val builder: mutable.Builder[(String, X), Map[String, X]], private var map: Map[String, X]) extends java.util.Map[String, X] {
    // PFAMap has two states:
    //    1. map == null during the filling stage (Avro's code calls java.util.Map.add())
    //    2. map != null during the PFA stage (all calls are immutable)
    // Use of the java.util.Map interface after it has been sealed by a PFA call causes a RuntimeException.
    // Most of the java.util.Map interface causes NotImplementedErrors, since this is just to satisfy Avro.

    /** Access the Scala `Map` behind this PFAMap.
      * 
      * After the first call to this function, it references a cached `Map`.
      */
    def toMap: Map[String, X] = {
      if (map == null)
        map = builder.result
      map
    }

    override def toString(): String = "{" + toMap.map({case (k, v) => k.toString + ": " + (if (v == null) "null" else v.toString)}).mkString(", ") + "}"
    override def equals(that: Any): Boolean = that match {
      case thatMap: PFAMap[_] => this.toMap == thatMap.toMap
      case _ => false
    }
    override def hashCode(): Int = toMap.hashCode

    // mutable java.util.Map methods
    /** Raises `NotImplementedError`. */
    override def clear(): Unit = throw new NotImplementedError
    /** One of only three `java.util.Map` methods that have been implemented (needed for filling in the Avro library).
      */
    override def put(key: String, value: X): X = {
      if (map != null)
        throw new RuntimeException("PFAMap.add should not be used outside of Avro")
      builder += (key -> value)
      null.asInstanceOf[X]
    }
    /** Raises `NotImplementedError`. */
    override def putAll(that: java.util.Map[_ <: String, _ <: X]): Unit = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def remove(key: AnyRef): X = throw new NotImplementedError

    //////////// immutable java.util.Map contract
    /** Raises `NotImplementedError`. */
    override def containsKey(key: AnyRef): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def containsValue(value: AnyRef): Boolean = throw new NotImplementedError
    /** One of only three `java.util.Map` methods that have been implemented (needed for filling in the Avro library).
      */
    override def entrySet(): java.util.Set[java.util.Map.Entry[String, X]] = {
      val theMap = toMap
      val out = new java.util.HashSet[java.util.Map.Entry[String, X]](theMap.size)
      for ((k, v) <- theMap)
        out.add(new java.util.AbstractMap.SimpleEntry[String, X](k, v))
      out
    }
    /** One of only three `java.util.Map` methods that have been implemented (needed for filling in the Avro library).
      */
    override def get(key: AnyRef): X = key match {
      case i: String => apply(i)
      case _ => throw new java.util.NoSuchElementException("key not found: " + key.toString)
    }
    /** Raises `NotImplementedError`. */
    override def isEmpty(): Boolean = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def keySet(): java.util.Set[String] = throw new NotImplementedError
    /** Raises `NotImplementedError`. */
    override def size(): Int = toMap.size
    /** Raises `NotImplementedError`. */
    override def values(): java.util.Collection[X] = throw new NotImplementedError

    /** Helper function to ensure that numerical types are exactly right (int for int, long for long, etc.).
      * 
      * @param elem datum to convert
      * @param schema schema to convert it to
      * @return converted datum (boxed primitives)
      */
    def converted[X](elem: X, schema: Schema): X =
      if (schema == null)
        elem
      else
        schema.getType match {
          case Schema.Type.INT => java.lang.Integer.valueOf(elem.asInstanceOf[java.lang.Number].intValue).asInstanceOf[X]
          case Schema.Type.LONG => java.lang.Long.valueOf(elem.asInstanceOf[java.lang.Number].longValue).asInstanceOf[X]
          case Schema.Type.FLOAT => java.lang.Float.valueOf(elem.asInstanceOf[java.lang.Number].floatValue).asInstanceOf[X]
          case Schema.Type.DOUBLE => java.lang.Double.valueOf(elem.asInstanceOf[java.lang.Number].doubleValue).asInstanceOf[X]
          case _ => elem
        }

    /** Calls `toMap.apply(i)`.
      */
    def apply(i: String): X = toMap.apply(i)
    /** Returns an updated copy of this PFAMap with one element replaced.
      * 
      * @param i key of element to have replaced
      * @param elem new value for element
      * @param schema schema of element
      * @return new PFAMap, leaving the old one untouched
      */
    def updated(i: String, elem: X, schema: Schema): PFAMap[X] = PFAMap.fromMap(toMap.updated(i, converted(elem, schema)))

    /** Returns an updated copy of this PFAMap (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new PFAMap, leaving the old one untouched
      */
    def updated(path: List[PathIndex], elem: X, schema: Schema): PFAMap[X] = updated(path, (dummy: X) => elem, schema)
    /** Returns an updated copy of this PFAMap (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new PFAMap, leaving the old one untouched
      */
    def updated(path: Array[PathIndex], elem: X, schema: Schema): PFAMap[X] = updated(path.toList, (dummy: X) => elem, schema)
    /** Returns an updated copy of this PFAMap (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @param arrayErrStr error message if an array path index is wrong
      * @param arrayErrCode error code if an array path index is wrong
      * @param mapErrStr error message if a map path index is wrong
      * @param mapErrCode error code if a map path index is wrong
      * @param fcnName name of the calling PFA function
      * @param pos locator mark for the calling PFA function
      * @return new PFAMap, leaving the old one untouched
      */
    def updated(path: Array[PathIndex], elem: X, schema: Schema, arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): PFAMap[X] =
      try {
        updated(path.toList, (dummy: X) => elem, schema)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
        case err: java.util.NoSuchElementException => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      }
    /** Returns an updated copy of this PFAMap (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param elem new value for the subelement
      * @param schema schema of the subelement
      * @return new PFAMap, leaving the old one untouched
      */
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema): PFAMap[X] = updated(path.toList, updator, schema)
    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @param arrayErrStr error message if an array path index is wrong
      * @param arrayErrCode error code if an array path index is wrong
      * @param mapErrStr error message if a map path index is wrong
      * @param mapErrCode error code if a map path index is wrong
      * @param fcnName name of the calling PFA function
      * @param pos locator mark for the calling PFA function
      * @return new PFAArray, leaving the old one untouched
      */
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema, arrayErrStr: String, arrayErrCode: Int, mapErrStr: String, mapErrCode: Int, fcnName: String, pos: Option[String]): PFAMap[X] =
      try {
        updated(path.toList, updator, schema)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException(arrayErrStr, arrayErrCode, fcnName, pos, err)
        case err: java.util.NoSuchElementException => throw new PFARuntimeException(mapErrStr, mapErrCode, fcnName, pos, err)
      }

    /** Returns an updated copy of this PFAArray (and its substructures) with a deep element replaced.
      * 
      * @param path coordinates of the deep element to have replaced
      * @param updator function that replaces the old value with a new one
      * @param schema schema of the subelement
      * @return new PFAArray, leaving the old one untouched
      */
    def updated[Y](path: List[PathIndex], updator: Y => Y, schema: Schema): PFAMap[X] = path match {
      case M(k) :: Nil => updated(k, updator(apply(k).asInstanceOf[Y]).asInstanceOf[X], schema)
      case M(k) :: rest => {
        apply(k) match {
          case x: PFAArray[_] => updated(k, x.updated(rest, updator, schema).asInstanceOf[X], null)
          case x: PFAMap[_] => updated(k, x.updated(rest, updator, schema).asInstanceOf[X], null)
          case x: PFARecord => updated(k, x.updated(rest, updator, schema).asInstanceOf[X], null)
          case _ => throw new IllegalArgumentException("path used on a non-container")
        }
      }
      case _ :: rest => throw new IllegalArgumentException("wrong PathIndex element used on a map")
      case Nil => throw new IllegalArgumentException("empty PathIndex used on a map")
    }
  }

  /////////////////////////// PFADataTranslator translates data produced by one PFAEngine class into data readable by another PFAEngine class

  object PFADataTranslator {
    // assumes input type is exactly the same: x.accepts(y) and y.accepts(x)
    trait Translator {
      def translate(datum: AnyRef): AnyRef
    }

    class PassThrough extends Translator {
      def translate(datum: AnyRef): AnyRef = datum
    }

    class TranslateFixed(avroFixed: AvroFixed, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]) extends Translator {
      val fullName = avroFixed.fullName
      memo(fullName) = this
      val constructor = classLoader.loadClass(avroFixed.fullName).getConstructor(classOf[AnyPFAFixed])
      constructor.setAccessible(true)
      def translate(datum: AnyRef): AnyRef = constructor.newInstance(datum).asInstanceOf[AnyRef]
    }

    class TranslateEnum(avroEnum: AvroEnum, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]) extends Translator {
      val fullName = avroEnum.fullName
      memo(fullName) = this
      val constructor = classLoader.loadClass(avroEnum.fullName).getConstructor(classOf[AnyPFAEnumSymbol])
      constructor.setAccessible(true)
      def translate(datum: AnyRef): AnyRef = constructor.newInstance(datum).asInstanceOf[AnyRef]
    }

    class TranslateArray(items: Translator) extends Translator {
      def translate(datum: AnyRef): AnyRef =
        PFAArray.fromVector(datum.asInstanceOf[PFAArray[AnyRef]].toVector map {x => items.translate(x)})
    }

    class TranslateMap(values: Translator) extends Translator {
      def translate(datum: AnyRef): AnyRef = {
        PFAMap.fromMap(datum.asInstanceOf[PFAMap[AnyRef]].toMap map {case (key, x) => (key, values.translate(x))})
      }
    }

    class TranslateRecord(avroRecord: AvroRecord, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]) extends Translator {
      val fullName = avroRecord.fullName
      memo(fullName) = this
      val fieldTranslators = avroRecord.fields map {field => getTranslator(field.avroType, classLoader, memo)} toArray

      val constructor = classLoader.loadClass(avroRecord.fullName).getConstructor(classOf[Array[AnyRef]])
      constructor.setAccessible(true)

      def translate(datum: AnyRef): AnyRef = {
        val fieldData = datum.asInstanceOf[AnyPFARecord].getAll
        val convertedFields = fieldTranslators zip fieldData map {case (t, d) => t.translate(d)}
        constructor.newInstance(convertedFields).asInstanceOf[AnyRef]
      }
    }

    class TranslateUnion(translator: PartialFunction[AnyRef, AnyRef]) extends Translator {
      def translate(datum: AnyRef): AnyRef = translator(datum)
    }

    def getTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]): Translator =
      memo.getOrElse(avroType.fullName, avroType match {
        case _: AvroNull => new PassThrough
        case _: AvroBoolean => new PassThrough
        case _: AvroInt => new PassThrough
        case _: AvroLong => new PassThrough
        case _: AvroFloat => new PassThrough
        case _: AvroDouble => new PassThrough
        case _: AvroBytes => new PassThrough
        case _: AvroString => new PassThrough

        case x: AvroFixed => new TranslateFixed(x, classLoader, memo)
        case x: AvroEnum => new TranslateEnum(x, classLoader, memo)

        case x: AvroArray =>
          val items = getTranslator(x.items, classLoader, memo)
          items match {
            case _: PassThrough => new PassThrough
            case _ => new TranslateArray(items)
          }

        case x: AvroMap =>
          val values = getTranslator(x.values, classLoader, memo)
          values match {
            case _: PassThrough => new PassThrough
            case _ => new TranslateMap(values)
          }

        case x: AvroRecord => new TranslateRecord(x, classLoader, memo)

        case x: AvroUnion =>
          val types = x.types map {y => getTranslator(y, classLoader, memo)}
          val translators =
            types collect {
              case y: TranslateArray =>  {case null => null; case datum: PFAArray[_] => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateMap =>    {case null => null; case datum: PFAMap[_]   => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateFixed =>  {case null => null; case datum: AnyPFAFixed if (datum.getSchema.getFullName == y.fullName)      => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateEnum =>   {case null => null; case datum: AnyPFAEnumSymbol if (datum.getSchema.getFullName == y.fullName) => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateRecord => {case null => null; case datum: AnyPFARecord if (datum.getSchema.getFullName == y.fullName)     => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
            }
          new TranslateUnion((translators :+ ({case y => y}: PartialFunction[AnyRef, AnyRef])) reduce {_ orElse _})
      })
  }
  /** Translates data created by one [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]] into a form usable by another [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]].
    * 
    * @param avroType type of the datum
    * @param classLoader `ClassLoader` of the destination engine
    */
  class PFADataTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader) {
    /** Translator object built once with care take to avoid unnecessary conversions at runtime.
      */
    val translator = PFADataTranslator.getTranslator(avroType, classLoader, mutable.Map[String, PFADataTranslator.Translator]())
    /** Runtime conversion of `datum`.
      */
    def translate(datum: AnyRef): AnyRef = translator.translate(datum)
  }

  object AvroDataTranslator {
    // assumes input type is exactly the same: x.accepts(y) and y.accepts(x)
    trait Translator {
      def translate(datum: AnyRef): AnyRef
    }

    def unbox(value: AnyRef): Any = value match {
      case x: java.lang.Boolean => x.booleanValue
      case x: java.lang.Integer => x.intValue
      case x: java.lang.Long => x.longValue
      case x: java.lang.Float => x.floatValue
      case x: java.lang.Double => x.doubleValue
      case x => x
    }

    class PassThrough extends Translator {
      def translate(datum: AnyRef): AnyRef = datum
    }

    class TranslateBytes extends Translator {
      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[java.nio.ByteBuffer]
        avroDatum.array
      }
    }

    class TranslateString extends Translator {
      def translate(datum: AnyRef): AnyRef = datum match {
        case x: String => x
        case x: java.lang.CharSequence => x.toString
      }
    }

    class TranslateFixed(avroFixed: AvroFixed, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]) extends Translator {
      val fullName = avroFixed.fullName
      memo(fullName) = this
      val constructor = classLoader.loadClass(avroFixed.fullName).getConstructor(classOf[Array[Byte]])
      constructor.setAccessible(true)
      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[org.apache.avro.generic.GenericFixed]
        constructor.newInstance(avroDatum.bytes).asInstanceOf[AnyRef]
      }
    }

    class TranslateEnum(avroEnum: AvroEnum, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]) extends Translator {
      val fullName = avroEnum.fullName
      memo(fullName) = this
      val constructor = classLoader.loadClass(avroEnum.fullName).getConstructor(classOf[Schema], classOf[String])
      constructor.setAccessible(true)
      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[org.apache.avro.generic.GenericEnumSymbol]
        constructor.newInstance(avroEnum.schema, avroDatum.toString).asInstanceOf[AnyRef]
      }
    }

    class TranslateArray(items: Translator) extends Translator {
      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[java.util.List[AnyRef]]
        PFAArray.fromVector(avroDatum.toVector map {x => unbox(items.translate(x))})
      }
    }

    class TranslateMap(values: Translator) extends Translator {
      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[java.util.Map[String, AnyRef]]
        PFAMap.fromMap(avroDatum.toMap map {case (key, x) => (key, values.translate(x))})
      }
    }

    class TranslateRecord(avroRecord: AvroRecord, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]) extends Translator {
      val fullName = avroRecord.fullName
      memo(fullName) = this
      val fieldNames = avroRecord.fields map {field => field.name} toArray
      val fieldTranslators = avroRecord.fields map {field => getTranslator(field.avroType, classLoader, memo)} toArray

      val constructor = classLoader.loadClass(avroRecord.fullName).getConstructor(classOf[Array[AnyRef]])
      constructor.setAccessible(true)

      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[org.apache.avro.generic.GenericRecord]
        val convertedFields = Array.fill[AnyRef](fieldNames.size)(null)
        var index = 0
        while (index < fieldNames.size) {
          val avroData = avroDatum.get(fieldNames(index))
          val convertedData = fieldTranslators(index).translate(avroData)
          convertedFields(index) = convertedData
          index += 1
        }
        constructor.newInstance(convertedFields).asInstanceOf[AnyRef]
      }
    }

    class TranslateUnion(translator: PartialFunction[AnyRef, AnyRef]) extends Translator {
      def translate(datum: AnyRef): AnyRef = translator(datum)
    }

    def getTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator]): Translator =
      memo.getOrElse(avroType.fullName, avroType match {
        case _: AvroNull => new PassThrough
        case _: AvroBoolean => new PassThrough
        case _: AvroInt => new PassThrough
        case _: AvroLong => new PassThrough
        case _: AvroFloat => new PassThrough
        case _: AvroDouble => new PassThrough

        case _: AvroBytes => new TranslateBytes
        case _: AvroString => new TranslateString

        case x: AvroFixed => new TranslateFixed(x, classLoader, memo)
        case x: AvroEnum => new TranslateEnum(x, classLoader, memo)

        case x: AvroArray =>
          val items = getTranslator(x.items, classLoader, memo)
          new TranslateArray(items)

        case x: AvroMap =>
          val values = getTranslator(x.values, classLoader, memo)
          new TranslateMap(values)

        case x: AvroRecord => new TranslateRecord(x, classLoader, memo)

        case x: AvroUnion =>
          val types = x.types map {y => getTranslator(y, classLoader, memo)}
          val translators =
            types collect {
              case y: TranslateBytes =>  {case null => null; case datum: java.nio.ByteBuffer    => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateString => {case null => null; case datum: java.lang.CharSequence => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateArray =>  {case null => null; case datum: java.util.List[_]      => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateMap =>    {case null => null; case datum: java.util.Map[_, _]    => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateFixed =>  {case null => null; case datum: org.apache.avro.generic.GenericFixed if (datum.getSchema.getFullName == y.fullName)      => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateEnum =>   {case null => null; case datum: org.apache.avro.generic.GenericEnumSymbol if (datum.getSchema.getFullName == y.fullName) => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
              case y: TranslateRecord => {case null => null; case datum: org.apache.avro.generic.GenericRecord if (datum.getSchema.getFullName == y.fullName)     => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
            }

          new TranslateUnion((translators :+ ({case y => y}: PartialFunction[AnyRef, AnyRef])) reduce {_ orElse _})
      })
  }
  /** Translates data created by the Avro library (generic or specific) into a form usable by a [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]].
    * 
    * Note: assumes that the Schemas are ''exactly'' the same.
    * 
    * @param avroType type of the datum
    * @param classLoader `ClassLoader` of the destination engine
    */
  class AvroDataTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader) {
    /** Translator object built once with care take to avoid unnecessary conversions at runtime.
      */
    val translator = AvroDataTranslator.getTranslator(avroType, classLoader, mutable.Map[String, AvroDataTranslator.Translator]())
    /** Runtime conversion of `datum`.
      */
    def translate(datum: AnyRef): AnyRef = translator.translate(datum)
  }

  object ScalaDataTranslator {
    // assumes input type is exactly the same: x.accepts(y) and y.accepts(x)
    trait Translator[X] {
      def toScala(datum: AnyRef): X
      def fromScala(datum: Any): AnyRef
      def scalaClass: java.lang.Class[_]
    }

    def box(value: Any): AnyRef = value match {
      case x: Boolean => java.lang.Boolean.valueOf(x)
      case x: Int => java.lang.Integer.valueOf(x)
      case x: Long => java.lang.Long.valueOf(x)
      case x: Float => java.lang.Float.valueOf(x)
      case x: Double => java.lang.Double.valueOf(x)
      case x: AnyRef => x
    }

    def unbox(value: AnyRef): Any = value match {
      case x: java.lang.Boolean => x.booleanValue
      case x: java.lang.Integer => x.intValue
      case x: java.lang.Long => x.longValue
      case x: java.lang.Float => x.floatValue
      case x: java.lang.Double => x.doubleValue
      case x => x
    }

    class TranslateNull extends Translator[AnyRef] {
      def toScala(datum: AnyRef): AnyRef = null
      def fromScala(datum: Any): AnyRef = null.asInstanceOf[AnyRef]
      val scalaClass = classOf[AnyRef]
    }

    class TranslateBoolean extends Translator[Boolean] {
      def toScala(datum: AnyRef): Boolean = unbox(datum).asInstanceOf[Boolean]
      def fromScala(datum: Any): AnyRef = box(datum)
      val scalaClass = classOf[Boolean]
    }

    class TranslateInt extends Translator[Int] {
      def toScala(datum: AnyRef): Int = unbox(datum).asInstanceOf[Int]
      def fromScala(datum: Any): AnyRef = box(datum)
      val scalaClass = classOf[Int]
    }

    class TranslateLong extends Translator[Long] {
      def toScala(datum: AnyRef): Long = unbox(datum).asInstanceOf[Long]
      def fromScala(datum: Any): AnyRef = box(datum)
      val scalaClass = classOf[Long]
    }

    class TranslateFloat extends Translator[Float] {
      def toScala(datum: AnyRef): Float = unbox(datum).asInstanceOf[Float]
      def fromScala(datum: Any): AnyRef = box(datum)
      val scalaClass = classOf[Float]
    }

    class TranslateDouble extends Translator[Double] {
      def toScala(datum: AnyRef): Double = unbox(datum).asInstanceOf[Double]
      def fromScala(datum: Any): AnyRef = box(datum)
      val scalaClass = classOf[Double]
    }

    class TranslateBytes extends Translator[Array[Byte]] {
      def toScala(datum: AnyRef): Array[Byte] = datum.asInstanceOf[Array[Byte]]
      def fromScala(datum: Any): AnyRef = datum.asInstanceOf[AnyRef]
      val scalaClass = classOf[Array[Byte]]
    }

    class TranslateString extends Translator[String] {
      def toScala(datum: AnyRef): String = datum.asInstanceOf[String]
      def fromScala(datum: Any): AnyRef = datum.asInstanceOf[AnyRef]
      val scalaClass = classOf[String]
    }

    class TranslateFixed(avroFixed: AvroFixed, memo: mutable.Map[String, Translator[_]]) extends Translator[Array[Byte]] {
      val fullName = avroFixed.fullName
      memo(fullName) = this
      val size = avroFixed.size
      def toScala(datum: AnyRef): Array[Byte] = datum.asInstanceOf[AnyPFAFixed].bytes
      def fromScala(datum: Any): AnyRef = new GenericPFAFixed(avroFixed.schema, datum.asInstanceOf[Array[Byte]])
      val scalaClass = classOf[Array[Byte]]
    }

    class TranslateEnum(avroEnum: AvroEnum, memo: mutable.Map[String, Translator[_]]) extends Translator[String] {
      val fullName = avroEnum.fullName
      memo(fullName) = this
      val symbols = avroEnum.symbols
      def toScala(datum: AnyRef): String = datum.asInstanceOf[AnyPFAEnumSymbol].toString
      def fromScala(datum: Any): AnyRef = new GenericPFAEnumSymbol(avroEnum.schema, datum.asInstanceOf[String])
      val scalaClass = classOf[String]
    }

    class TranslateArray[X <: Seq[_]](items: Translator[_]) extends Translator[Seq[_]] {
      def toScala(datum: AnyRef): Seq[_] =
        datum.asInstanceOf[PFAArray[Any]].toVector map {x => items.toScala(box(x))}
      def fromScala(datum: Any): AnyRef =
        PFAArray.fromVector(datum.asInstanceOf[Seq[Any]] map {x => unbox(items.fromScala(x))} toVector)
      val scalaClass = classOf[Seq[_]]
    }

    class TranslateMap[X <: Map[String, _]](values: Translator[_]) extends Translator[Map[String, _]] {
      def toScala(datum: AnyRef): Map[String, _] =
        datum.asInstanceOf[PFAMap[AnyRef]].toMap map {case (k, v) => (k, values.toScala(v))}
      def fromScala(datum: Any): AnyRef =
        PFAMap.fromMap(datum.asInstanceOf[Map[String, Any]] map {case (k, v) => (k, values.fromScala(v))})
      val scalaClass = classOf[Map[String, _]]
    }

    class TranslateRecord[X](avroRecord: AvroRecord, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator[_]]) extends Translator[X] {
      val fullName = avroRecord.fullName
      memo(fullName) = this
      val fieldTranslators = avroRecord.fields map {field => getTranslator(field.avroType, classLoader, memo)}

      val constructor = classLoader.loadClass(fullName).getConstructor(fieldTranslators map {_.scalaClass}: _*)
      constructor.setAccessible(true)

      def toScala(datum: AnyRef): X = {
        val fieldData = datum.asInstanceOf[AnyPFARecord].getAll
        val convertedFields = fieldTranslators zip fieldData map {case (t, d) => box(t.toScala(d))}
        constructor.newInstance(convertedFields: _*).asInstanceOf[X]
      }

      def fromScala(datum: Any): AnyRef = {
        val fieldData = datum.asInstanceOf[Product].productIterator.toSeq
        val out = new GenericPFARecord(avroRecord.schema)
        for (((t, d), i) <- fieldTranslators zip fieldData zipWithIndex)
          out.put(i, t.fromScala(d))
        out
      }
      val scalaClass = classLoader.loadClass(fullName)
    }

    class TranslateUnion(translateToScala: PartialFunction[AnyRef, Any], translateFromScala: PartialFunction[Any, AnyRef]) extends Translator[Any] {
      def toScala(datum: AnyRef): Any = translateToScala(datum)
      def fromScala(datum: Any): AnyRef = translateFromScala(datum)
      val scalaClass = classOf[AnyRef]
    }

    def getTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader, memo: mutable.Map[String, Translator[_]]): Translator[_] =
      memo.getOrElse(avroType.fullName, avroType match {
        case _: AvroNull => new TranslateNull
        case _: AvroBoolean => new TranslateBoolean
        case _: AvroInt => new TranslateInt
        case _: AvroLong => new TranslateLong
        case _: AvroFloat => new TranslateFloat
        case _: AvroDouble => new TranslateDouble
        case _: AvroBytes => new TranslateBytes
        case _: AvroString => new TranslateString

        case x: AvroFixed => new TranslateFixed(x, memo)
        case x: AvroEnum => new TranslateEnum(x, memo)

        case x: AvroArray => new TranslateArray(getTranslator(x.items, classLoader, memo))
        case x: AvroMap => new TranslateMap(getTranslator(x.values, classLoader, memo))

        case x: AvroRecord => new TranslateRecord(x, classLoader, memo)

        case x: AvroUnion =>
          val translators = x.types map {y => getTranslator(y, classLoader, memo)}

          val translateToScala = translators map {
            case y: TranslateNull =>      {case null                     => y.toScala(null)}:  PartialFunction[AnyRef, Any]
            case y: TranslateBoolean =>   {case datum: java.lang.Boolean => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateInt =>       {case datum: java.lang.Integer => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateLong =>      {case datum: java.lang.Long    => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateFloat =>     {case datum: java.lang.Float   => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateDouble =>    {case datum: java.lang.Double  => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateBytes =>     {case datum: Array[_]          => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateString =>    {case datum: String            => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateArray[_] =>  {case datum: PFAArray[_]       => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateMap[_] =>    {case datum: PFAMap[_]         => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateFixed =>     {case datum: AnyPFAFixed if (datum.getSchema.getFullName == y.fullName)      => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateEnum =>      {case datum: AnyPFAEnumSymbol if (datum.getSchema.getFullName == y.fullName) => y.toScala(datum)}: PartialFunction[AnyRef, Any]
            case y: TranslateRecord[_] => {case datum: AnyPFARecord if (datum.getSchema.getFullName == y.fullName)     => y.toScala(datum)}: PartialFunction[AnyRef, Any]
          }

          val translateFromScala = translators map {
            case y: TranslateNull =>      {case null             => y.fromScala(null)}: PartialFunction[Any, AnyRef]
            case y: TranslateBoolean =>   {case datum: Boolean   => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateInt =>       {case datum: Int       => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateLong =>      {case datum: Long      => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateFloat =>     {case datum: Float     => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateDouble =>    {case datum: Double    => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateBytes =>     {case datum: Array[_]  => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateString =>    {case datum: String    => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateArray[_] =>  {case datum: Seq[_]    => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateMap[_] =>    {case datum: Map[_, _] => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateFixed =>     {case datum: Array[_] if (y.size == datum.size)                          => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateEnum =>      {case datum: String if (y.symbols.contains(datum))                       => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
            case y: TranslateRecord[_] => {case datum: AnyPFARecord if (datum.getSchema.getFullName == y.fullName) => y.fromScala(datum)}: PartialFunction[Any, AnyRef]
          }

          new TranslateUnion(translateToScala reduce {_ orElse _}, translateFromScala reduce {_ orElse _})
      })
  }
  /** Translates Scala data into a form usable by a [[com.opendatagroup.hadrian.jvmcompiler.PFAEngine PFAEngine]].
    * 
    *  - Any Scala `Seq` is converted to a [[com.opendatagroup.hadrian.data.PFAArray PFAArray]].
    *  - Any Scala `Map` is converted to a [[com.opendatagroup.hadrian.data.PFAMap PFAMap]].
    *  - Any Scala case class is converted to a [[com.opendatagroup.hadrian.data.PFARecord PFARecord]].
    * 
    * @param avroType type of the datum
    * @param classLoader `ClassLoader` of the destination engine
    */
  class ScalaDataTranslator[X](avroType: AvroType, classLoader: java.lang.ClassLoader) {
    /** Translator object built once with care take to avoid unnecessary conversions at runtime.
      */
    val translator = ScalaDataTranslator.getTranslator(avroType, classLoader, mutable.Map[String, ScalaDataTranslator.Translator[_]]())
    /** Runtime conversion of `datum` from PFA to Scala.
      */
    def toScala(datum: AnyRef): X = translator.toScala(datum).asInstanceOf[X]
    /** Runtime conversion of `datum` from Scala to PFA.
      */
    def fromScala(datum: Any): AnyRef = translator.fromScala(datum)
  }
}

package object data {
  val genericData = new PFAGenericData(getClass.getClassLoader)

  /** Convert data from JSON to a generic PFA object.
    * 
    * Needs to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param json JSON data
    * @param schema Avro schema
    * @return PFA data
    */
  def fromJson(json: String, schema: Schema): AnyRef = {
    val reader = new GenericPFADatumReader[AnyRef](schema, genericData)
    // reader.setSchema(schema)
    val decoder = DecoderFactory.get.jsonDecoder(schema, json)
    reader.read(null.asInstanceOf[AnyRef], decoder)
  }

  /** Convert data from Avro to a generic PFA object.
    * 
    * Needs to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param avro Avro data
    * @param schema Avro schema
    * @return PFA data
    */
  def fromAvro(avro: Array[Byte], schema: Schema): AnyRef = {
    val reader = new GenericPFADatumReader[AnyRef](schema, genericData)
    val decoder = DecoderFactory.get.validatingDecoder(schema, DecoderFactory.get.binaryDecoder(avro, null))
    reader.read(null.asInstanceOf[AnyRef], decoder)
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
    val writer = new GenericPFADatumWriter[AnyRef](schema, genericData)
    writer.write(obj, encoder)
    encoder.flush()
    out.toString
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
    val writer = new GenericPFADatumWriter[AnyRef](schema, genericData)
    writer.write(obj, encoder)
    encoder.flush()
    out.toByteArray
  }

  /** Convert data from JSON to a generic PFA object.
    * 
    * Needs to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param json JSON data
    * @param avroType data type
    * @return PFA data
    */
  def fromJson(json: String, avroType: AvroType): AnyRef      = fromJson(json, avroType.schema)
  /** Convert data from Avro to a generic PFA object.
    * 
    * Needs to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param avro Avro data
    * @param avroType data type
    * @return PFA data
    */
  def fromAvro(avro: Array[Byte], avroType: AvroType): AnyRef = fromAvro(avro, avroType.schema)
  /** Convert data to JSON.
    * 
    * @param obj object reference
    * @param schema Avro schema
    * @return JSON string
    */
  def toJson(obj: AnyRef, avroType: AvroType): String         = toJson(obj, avroType.schema)
  /** Convert data to Avro.
    * 
    * @param obj object reference
    * @param schema Avro schema
    * @return Avro bytes
    */
  def toAvro(obj: AnyRef, avroType: AvroType): Array[Byte]    = toAvro(obj, avroType.schema)

  /** Create an Avro iterator (subclass of `java.util.Iterator`) over Avro-serialized input data.
    * 
    * The objects produced by this iterator need to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param inputStream serialized data
    * @param inputType input type
    * @return unserialized data
    */
  def avroInputIterator[X](inputStream: InputStream, inputType: AvroType): DataFileStream[X] = {    // DataFileStream is a java.util.Iterator
    val schema = inputType.schema
    val reader = new GenericPFADatumReader[X](schema, genericData)
    val out = new DataFileStream[X](inputStream, reader)
    
    val fileSchema = AvroConversions.schemaToAvroType(out.getSchema)
    if (!inputType.accepts(fileSchema))
      throw new IllegalArgumentException("InputStream has schema %s\nbut expecting schema %s".format(fileSchema, inputType))
    out
  }

  /** Create an iterator over JSON-serialized input data.
    * 
    * The objects produced by this iterator need to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param inputStream serialized data
    * @param inputType input type
    * @return unserialized data
    */
  def jsonInputIterator[X](inputStream: InputStream, inputType: AvroType): java.util.Iterator[X] = {
    val schema = inputType.schema
    val reader = new GenericPFADatumReader[X](schema, genericData)
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
    * The objects produced by this iterator need to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param inputIterator iterator of `Strings`, each containing a JSON object
    * @param inputType input type
    * @return unserialized data
    */
  def jsonInputIterator[X](inputIterator: java.util.Iterator[String], inputType: AvroType): java.util.Iterator[X] = {
    val schema = inputType.schema
    val reader = new GenericPFADatumReader[X](schema, genericData)

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
    * The objects produced by this iterator need to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * @param inputIterator iterator of `Strings`, each containing a JSON object
    * @param inputType input type
    * @return unserialized data
    */
  def jsonInputIterator[X](inputIterator: scala.collection.Iterator[String], inputType: AvroType): scala.collection.Iterator[X] = {
    val schema = inputType.schema
    val reader = new GenericPFADatumReader[X](schema, genericData)

    new scala.collection.Iterator[X] {
      override def hasNext: Boolean = inputIterator.hasNext
      override def next(): X = {
        val json = inputIterator.next()
        val decoder = DecoderFactory.get.jsonDecoder(inputType.schema, json)
        reader.read(null.asInstanceOf[X], decoder)
      }
    }
  }

  private def javaFieldReaders(inputType: AvroType, csvIndexLookup: Map[String, Int]): Seq[(Int, String => AnyRef)] = inputType match {
    case AvroRecord(fields, _, _, _, _) =>
      val fieldNames = fields.map(_.name)
      fields map {field =>
        csvIndexLookup.get(field.name) match {
          case None => throw new IllegalArgumentException(s"CSV file has no field named ${field.name}")
          case Some(i) => (i, field.avroType match {
            case AvroNull() => {x: String => null.asInstanceOf[AnyRef]}
            case AvroBoolean() => {x: String => if (x.toLowerCase == "true") java.lang.Boolean.TRUE else if (x.toLowerCase == "false") java.lang.Boolean.FALSE else throw new IllegalArgumentException(s"""field ${field.name} declared boolean but found "$x"""")}
            case AvroInt() => {x: String => try {java.lang.Integer.valueOf(x.toInt)} catch {case _: java.lang.NumberFormatException => throw new IllegalArgumentException(s"""field ${field.name} declared int but found "$x"""")}}
            case AvroLong() => {x: String => try {java.lang.Long.valueOf(x.toLong)} catch {case _: java.lang.NumberFormatException => throw new IllegalArgumentException(s"""field ${field.name} declared long but found "$x"""")}}
            case AvroFloat() => {x: String => try {java.lang.Float.valueOf(x.toFloat)} catch {case _: java.lang.NumberFormatException => throw new IllegalArgumentException(s"""field ${field.name} declared float but found "$x"""")}}
            case AvroDouble() => {x: String => try {java.lang.Double.valueOf(x.toDouble)} catch {case _: java.lang.NumberFormatException => throw new IllegalArgumentException(s"""field ${field.name} declared double but found "$x"""")}}
            case AvroBytes() => {x: String => x.getBytes}
            case AvroString() => {x: String => x}
            case _ => throw new IllegalArgumentException("CSV input only allowed for records of non-named, non-container types (not enum, fixed, record, array, map, or union)")
          })
        }
      }
    case _ => throw new IllegalArgumentException("CSV input only allowed for records")
  }

  /** Create an iterator over CSV-serialized input data.
    * 
    * The objects produced by this iterator are need to be translated to a specific engine with a [[com.opendatagroup.hadrian.data.PFADataTranslator PFADataTranslator]] or the engine's `fromPFAData` method.
    * 
    * Note that only records of primitives can be read from CSV because of the nature of the CSV format.
    * 
    * @param inputStream serialized data
    * @param inputType input type
    * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
    * @return unserialized data
    */
  def csvInputIterator[X](inputStream: InputStream, inputType: AvroType, csvFormat: CSVFormat = CSVFormat.DEFAULT.withHeader(), makeFieldReaders: Option[(AvroType, Map[String, Int]) => Seq[(Int, String => AnyRef)]] = None, makeRecord: Option[Array[AnyRef] => X] = None): java.util.Iterator[X] = {
    val q = csvFormat.getQuoteCharacter
    val Quoted = ("""^\s*""" + q + """(.*)""" + q + """\s*$""").r
    val Unquoted = ("""^\s*(.*)\s*$""").r
    def unquote(x: String): String = x match {
      case Quoted(inner) => inner.replaceAll(q.toString + q.toString, q.toString)
      case Unquoted(inner) => inner
      case _ => x
    }

    val csvParser = csvFormat.parse(new java.io.InputStreamReader(inputStream))
    val csvIndexLookup = csvParser.getHeaderMap map {case (csvName, csvIndex) => unquote(csvName) -> csvIndex.intValue} toMap

    val fieldReaders: Seq[(Int, String => AnyRef)] = makeFieldReaders match {
      case Some(f) => f(inputType, csvIndexLookup)
      case None => javaFieldReaders(inputType, csvIndexLookup)
    }

    val myMakeRecord = makeRecord.getOrElse {fieldValues: Array[AnyRef] =>
      val out = new GenericPFARecord(inputType.schema)
      var i = 0
      while (i < fieldValues.size) {
        out.put(i, fieldValues(i))
        i += 1
      }
      out.asInstanceOf[X]
    }

    new java.util.Iterator[X] {
      private val csvIterator = csvParser.iterator
      def hasNext(): Boolean = csvIterator.hasNext
      def next(): X = {
        val csvRecord = csvIterator.next()
        myMakeRecord(fieldReaders map {case (i, r) => r(unquote(csvRecord.get(i)))} toArray)
      }
      override def remove(): Unit = throw new java.lang.UnsupportedOperationException
    }
  }

  /** Output for a stream of data objects.
    */
  trait OutputDataStream {
    /** Add one object to the output stream.
      */
    def append(datum: AnyRef): Unit
    /** Flush the output stream.
      */
    def flush(): Unit
    /** Flush and close the output stream.
      */
    def close(): Unit
  }
  
  /** Output stream for Avro files (including header).
    * 
    * @param writer Avro `org.apache.avro.io.DatumWriter`
    */
  class AvroOutputDataStream(writer: DatumWriter[AnyRef]) extends DataFileWriter[AnyRef](writer) with OutputDataStream

  /** Create an output stream to an Avro file (including header).
    * 
    * @param outputStream stream to write into
    * @param outputType datum type
    */
  def avroOutputDataStream(outputStream: OutputStream, outputType: AvroType): AvroOutputDataStream = {
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new AvroOutputDataStream(writer)
    out.create(outputType.schema, outputStream)
    out
  }

  /** Create an output stream to an Avro file (including header).
    * 
    * @param file file to overwrite
    * @param outputType datum type
    */
  def avroOutputDataStream(file: java.io.File, outputType: AvroType): AvroOutputDataStream = {
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new AvroOutputDataStream(writer)
    out.create(outputType.schema, file)
    out
  }

  /** Create an output stream to an Avro file (including header).
    * 
    * @param fileName name of file to overwrite
    * @param outputType datum type
    */
  def avroOutputDataStream(fileName: String, outputType: AvroType): AvroOutputDataStream = avroOutputDataStream(new java.io.File(fileName), outputType)

  /** Output stream for JSON files (each line of text is one JSON object).
    * 
    * @param writer Avro `org.apache.avro.io.DatumWriter`
    * @param encoder Avro `org.apache.avro.io.JsonEncoder`
    * @param outputStream stream to write into
    */
  class JsonOutputDataStream(writer: DatumWriter[AnyRef], encoder: JsonEncoder, outputStream: OutputStream) extends OutputDataStream {
    def append(obj: AnyRef): Unit = writer.write(obj, encoder)
    def flush(): Unit = encoder.flush()
    def close(): Unit = {
      encoder.flush()
      outputStream.close()
    }
  }

  /** Create an output stream to a JSON file (each line of text is one JSON object).
    * 
    * @param outputStream stream to write into
    * @param outputType datum type
    * @param writeSchema if `true`, write the Avro schema as the first line of the file; if `false`, write only data
    */
  def jsonOutputDataStream(outputStream: OutputStream, outputType: AvroType, writeSchema: Boolean): JsonOutputDataStream = {
    val encoder = EncoderFactory.get.jsonEncoder(outputType.schema, outputStream)
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new JsonOutputDataStream(writer, encoder, outputStream)
    if (writeSchema) {
      outputStream.write(outputType.toJson.getBytes("utf-8"))
      outputStream.write("\n".getBytes("utf-8"))
    }
    out
  }

  /** Create an output stream to a JSON file (each line of text is one JSON object).
    * 
    * @param file file to overwrite
    * @param outputType datum type
    * @param writeSchema if `true`, write the Avro schema as the first line of the file; if `false`, write only data
    */
  def jsonOutputDataStream(file: java.io.File, outputType: AvroType, writeSchema: Boolean): JsonOutputDataStream = {
    val outputStream = new java.io.FileOutputStream(file)
    val encoder = EncoderFactory.get.jsonEncoder(outputType.schema, outputStream)
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new JsonOutputDataStream(writer, encoder, outputStream)
    if (writeSchema) {
      outputStream.write(outputType.toJson.getBytes("utf-8"))
      outputStream.write("\n".getBytes("utf-8"))
    }
    out
  }

  /** Create an output stream to a JSON file (each line of text is one JSON object).
    * 
    * @param fileName name of file to overwrite
    * @param outputType datum type
    * @param writeSchema if `true`, write the Avro schema as the first line of the file; if `false`, write only data
    */
  def jsonOutputDataStream(fileName: String, outputType: AvroType, writeSchema: Boolean): JsonOutputDataStream =
    jsonOutputDataStream(new java.io.File(fileName), outputType, writeSchema)

  /** Output stream for CSV files.
    * 
    * @param outputStream stream to write into
    * @param fieldConverters pairs from field name to a function that converts PFA data into text for the CSV file
    * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
    */
  class CsvOutputDataStream(outputStream: OutputStream, fieldConverters: Seq[(String, PartialFunction[AnyRef, String])], csvFormat: CSVFormat) extends OutputDataStream {
    private val writer = new CSVPrinter(new java.io.PrintStream(outputStream), csvFormat)
    def append(datum: AnyRef) {
      datum.asInstanceOf[AnyPFARecord].getAll.foreach(writer.print)
      writer.println()
    }
    def flush() {
      writer.flush()
    }
    def close() {
      writer.close()
    }
  }

  /** Create an output stream for CSV-serializing scoring engine output.
    * 
    * Return values from the `action` method (or outputs captured by an `emit` callback) are suitable for writing to this stream.
    * 
    * Note that only records of primitives can be written to CSV because of the nature of the CSV format.
    * 
    * @param outputStream the raw output stream onto which CSV bytes will be written.
    * @param csvFormat format description for [[https://commons.apache.org/proper/commons-csv/ Apache `commons-csv`]]
    */
  def csvOutputDataStream(outputStream: OutputStream, outputType: AvroType, csvFormat: CSVFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL), writeHeader: Boolean = true) = {
    val fieldConverters: Seq[(String, PartialFunction[AnyRef, String])] = outputType match {
      case AvroRecord(fields, _, _, _, _) => fields map {field =>
        (field.name, field.avroType match {
          case AvroNull() => {case x: AnyRef => "null"}: PartialFunction[AnyRef, String]
          case AvroBoolean() => {case java.lang.Boolean.TRUE => "true"; case java.lang.Boolean.FALSE => "false"}: PartialFunction[AnyRef, String]
          case AvroInt() => {case x: java.lang.Number => x.intValue.toString}: PartialFunction[AnyRef, String]
          case AvroLong() => {case x: java.lang.Number => x.longValue.toString}: PartialFunction[AnyRef, String]
          case AvroFloat() => {case x: java.lang.Number => x.floatValue.toString}: PartialFunction[AnyRef, String]
          case AvroDouble() => {case x: java.lang.Number => x.doubleValue.toString}: PartialFunction[AnyRef, String]
          case AvroBytes() => {case x: Array[_] => new String(x.asInstanceOf[Array[Byte]])}: PartialFunction[AnyRef, String]
          case AvroString() => {case x: String => x}: PartialFunction[AnyRef, String]
          case _ => throw new IllegalArgumentException("CSV output only allowed for records of non-named, non-container types (not enum, fixed, record, array, map, or union)")
        })
      }
      case _ => throw new IllegalArgumentException("CSV output only allowed for records")
    }

    val fullCsvFormat =
      if (writeHeader)
        csvFormat.withHeader(fieldConverters.map(_._1): _*)
      else
        csvFormat

    new CsvOutputDataStream(outputStream, fieldConverters, fullCsvFormat)
  }

  /** Convert a PFA object into an in-house [[com.opendatagroup.hadrian.reader.JsonDom JsonDom]].
    * 
    * @param obj the object
    * @param avroType its type
    * @return the JSON DOM that can be used in Scala pattern matching
    */
  def toJsonDom(obj: AnyRef, avroType: AvroType): JsonDom = (obj, avroType) match {
    case (null, AvroNull()) => JsonNull
    case (java.lang.Boolean.TRUE, AvroBoolean()) => JsonTrue
    case (java.lang.Boolean.FALSE, AvroBoolean()) => JsonFalse
    case (x: java.lang.Number, AvroInt()) => JsonLong(x.longValue)
    case (x: java.lang.Number, AvroLong()) => JsonLong(x.longValue)
    case (x: java.lang.Number, AvroFloat()) => JsonDouble(x.doubleValue)
    case (x: java.lang.Number, AvroDouble()) => JsonDouble(x.doubleValue)
    case (x: Array[_], AvroBytes()) => JsonString(new String(x.asInstanceOf[Array[Byte]]))
    case (x: AnyPFAFixed, AvroFixed(_, _, _, _, _)) => JsonString(new String(x.bytes))
    case (x: String, AvroString()) => JsonString(x)
    case (x: AnyPFAEnumSymbol, AvroEnum(_, _, _, _, _)) => JsonString(x.toString)
    case (x: PFAArray[_], AvroArray(items)) => JsonArray(x.asInstanceOf[PFAArray[AnyRef]].toVector map {v => toJsonDom(v, items)})
    case (x: PFAMap[_], AvroMap(values)) => JsonObject(x.asInstanceOf[PFAMap[AnyRef]].toMap map {case (k, v) => (JsonString(k), toJsonDom(v, values))})
    case (x: AnyPFARecord, AvroRecord(fields, _, _, _, _)) => JsonObject(fields map {f => (JsonString(f.name), toJsonDom(x.get(f.name), f.avroType))} toMap)
    case (_, AvroUnion(types)) =>
      var t: AvroType = null
      var v: JsonDom = null
      var i = 0
      var done = false
      while (i < types.size  &&  !done) {
        try {
          t = types(i)
          v = toJsonDom(obj, t)
          done = true
        }
        catch {
          case _: IllegalArgumentException =>
        }
        i += 1
      }
      if (done  &&  t.isInstanceOf[AvroNull])
        JsonNull
      else if (done)
        JsonObject(Map(JsonString(t.fullName) -> v))
      else
        throw new IllegalArgumentException("could not resolve union when turning PFA data %s into JSON using schema %s".format(obj.toString, avroType.toString))

    case _ => throw new IllegalArgumentException("could not turn PFA data %s into JSON using schema %s".format(obj.toString, avroType.toString))
  }

  /** Convert a PFA object into a Jackson `JsonNode`.
    * 
    * @param obj the object
    * @param avroType its type
    * @return the JSON DOM used by Jackson
    */
  def toJsonNode(obj: AnyRef, avroType: AvroType): JsonNode = (obj, avroType) match {
    case (null, AvroNull()) => NullNode.getInstance
    case (java.lang.Boolean.TRUE, AvroBoolean()) => BooleanNode.getTrue
    case (java.lang.Boolean.FALSE, AvroBoolean()) => BooleanNode.getFalse
    case (x: java.lang.Number, AvroInt()) => new LongNode(x.longValue)
    case (x: java.lang.Number, AvroLong()) => new LongNode(x.longValue)
    case (x: java.lang.Number, AvroFloat()) => new DoubleNode(x.doubleValue)
    case (x: java.lang.Number, AvroDouble()) => new DoubleNode(x.doubleValue)
    case (x: Array[_], AvroBytes()) => new TextNode(new String(x.asInstanceOf[Array[Byte]]))
    case (x: AnyPFAFixed, AvroFixed(_, _, _, _, _)) => new TextNode(new String(x.bytes))
    case (x: String, AvroString()) => new TextNode(x)
    case (x: AnyPFAEnumSymbol, AvroEnum(_, _, _, _, _)) => new TextNode(x.toString)
    case (x: PFAArray[_], AvroArray(items)) =>
      val out = JsonNodeFactory.instance.arrayNode
      x.asInstanceOf[PFAArray[AnyRef]].toVector foreach {v => toJsonNode(v, items)}
      out
    case (x: PFAMap[_], AvroMap(values)) =>
      val out = JsonNodeFactory.instance.objectNode
      x.asInstanceOf[PFAMap[AnyRef]].toMap foreach {case (k, v) => out.put(k, toJsonNode(v, values))}
      out
    case (x: AnyPFARecord, AvroRecord(fields, _, _, _, _)) =>
      val out = JsonNodeFactory.instance.objectNode
      fields foreach {f => out.put(f.name, toJsonNode(x.get(f.name), f.avroType))}
      out
    case (_, AvroUnion(types)) =>
      var t: AvroType = null
      var v: JsonNode = null
      var i = 0
      var done = false
      while (i < types.size  &&  !done) {
        try {
          t = types(i)
          v = toJsonNode(obj, t)
          done = true
        }
        catch {
          case _: IllegalArgumentException =>
        }
        i += 1
      }
      if (done  &&  t.isInstanceOf[AvroNull])
        NullNode.getInstance
      else if (done) {
        val out = JsonNodeFactory.instance.objectNode
        out.put(t.fullName, v)
        out
      }
      else
        throw new IllegalArgumentException("could not resolve union when turning PFA data %s into JSON using schema %s".format(obj.toString, avroType.toString))

    case _ => throw new IllegalArgumentException("could not turn PFA data %s into JSON using schema %s".format(obj.toString, avroType.toString))
  }
}
