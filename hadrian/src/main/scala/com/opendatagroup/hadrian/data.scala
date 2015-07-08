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

import java.lang.reflect.Method
import java.io.ByteArrayInputStream
import java.io.InputStream

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps

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
import com.opendatagroup.hadrian.errors.PFAInitializationException
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

  class Comparison(schema: Schema) extends Function2[AnyRef, AnyRef, Int] {
    def apply(x: AnyRef, y: AnyRef): Int =
      ReflectData.get().compare(x, y, schema)
  }

  class ComparisonOperator(schema: Schema, operator: Int) extends Function2[AnyRef, AnyRef, Boolean] {
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

  class ComparisonOperatorLT(schema: Schema) extends Function2[AnyRef, AnyRef, Boolean] {
    def apply(x: AnyRef, y: AnyRef): Boolean =
      ReflectData.get().compare(x, y, schema) < 0
  }

  class GenericComparisonMax(cmp: (AnyRef, AnyRef) => java.lang.Integer) {
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (cmp(x, y) >= 0) x else y
  }

  class GenericComparisonMin(cmp: (AnyRef, AnyRef) => java.lang.Integer) {
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (cmp(x, y) < 0) x else y
  }

  class ComparisonMax(schema: Schema) {
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (ReflectData.get().compare(x, y, schema) >= 0) x else y
  }

  class ComparisonMin(schema: Schema) {
    def apply(x: AnyRef, y: AnyRef): AnyRef =
      if (ReflectData.get().compare(x, y, schema) < 0) x else y
  }

  object NumericalComparison {
    def apply(x: Int, y: Int): Int = if (x > y) 1 else if (x < y) -1 else 0
    def apply(x: Long, y: Long): Int = if (x > y) 1 else if (x < y) -1 else 0
    def apply(x: Float, y: Float): Int = if (x > y) 1 else if (x < y) -1 else 0
    def apply(x: Double, y: Double): Int = if (x > y) 1 else if (x < y) -1 else 0
  }

  object NumericalEQ {
    def apply(x: Int, y: Int): Boolean = (x == y)
    def apply(x: Long, y: Long): Boolean = (x == y)
    def apply(x: Float, y: Float): Boolean = (x == y)
    def apply(x: Double, y: Double): Boolean = (x == y)
  }

  object NumericalGE {
    def apply(x: Int, y: Int): Boolean = (x >= y)
    def apply(x: Long, y: Long): Boolean = (x >= y)
    def apply(x: Float, y: Float): Boolean = (x >= y)
    def apply(x: Double, y: Double): Boolean = (x >= y)
  }

  object NumericalGT {
    def apply(x: Int, y: Int): Boolean = (x > y)
    def apply(x: Long, y: Long): Boolean = (x > y)
    def apply(x: Float, y: Float): Boolean = (x > y)
    def apply(x: Double, y: Double): Boolean = (x > y)
  }

  object NumericalNE {
    def apply(x: Int, y: Int): Boolean = (x != y)
    def apply(x: Long, y: Long): Boolean = (x != y)
    def apply(x: Float, y: Float): Boolean = (x != y)
    def apply(x: Double, y: Double): Boolean = (x != y)
  }

  object NumericalLT {
    def apply(x: Int, y: Int): Boolean = (x < y)
    def apply(x: Long, y: Long): Boolean = (x < y)
    def apply(x: Float, y: Float): Boolean = (x < y)
    def apply(x: Double, y: Double): Boolean = (x < y)
  }

  object NumericalLE {
    def apply(x: Int, y: Int): Boolean = (x <= y)
    def apply(x: Long, y: Long): Boolean = (x <= y)
    def apply(x: Float, y: Float): Boolean = (x <= y)
    def apply(x: Double, y: Double): Boolean = (x <= y)
  }

  object NumericalMax {
    def apply(x: Int, y: Int): Int = if (x >= y) x else y
    def apply(x: Long, y: Long): Long = if (x >= y) x else y
    def apply(x: Float, y: Float): Float = if (x >= y) x else y
    def apply(x: Double, y: Double): Double = if (x >= y) x else y
  }

  object NumericalMin {
    def apply(x: Int, y: Int): Int = if (x < y) x else y
    def apply(x: Long, y: Long): Long = if (x < y) x else y
    def apply(x: Float, y: Float): Float = if (x < y) x else y
    def apply(x: Double, y: Double): Double = if (x < y) x else y
  }

  /////////////////////////// replace Avro's in-memory data representation with our own classes

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

  class GenericPFADatumWriter[X](schema: Schema, pfaGenericData: PFAGenericData) extends GenericDatumWriter[X](schema, pfaGenericData) {
    // write bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def writeBytes(datum: AnyRef, out: Encoder): Unit =
      out.writeBytes(java.nio.ByteBuffer.wrap(datum.asInstanceOf[Array[Byte]]))
  }

  class PFADatumWriter[X](schema: Schema, pfaSpecificData: PFASpecificData) extends SpecificDatumWriter[X](schema, pfaSpecificData) {
    // write bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def writeBytes(datum: AnyRef, out: Encoder): Unit =
      out.writeBytes(java.nio.ByteBuffer.wrap(datum.asInstanceOf[Array[Byte]]))
  }

  trait AnyPFARecord {
    def getSchema: Schema
    def getAll(): Array[AnyRef]
    def get(fieldName: String): AnyRef
  }

  class GenericPFARecord(schema: Schema) extends org.apache.avro.generic.GenericData.Record(schema) with AnyPFARecord {
    private val fieldNumbers = 0 until schema.getFields.size toArray
    def getAll() = fieldNumbers.map(get)
  }

  abstract class PFARecord extends SpecificRecordBase with AnyPFARecord {
    def getClassSchema: Schema
    def numFields: Int
    def fieldNames: Array[String]
    def fieldIndex(name: String): Int
    def fieldTypes: Array[Schema]
    def get(field: Int): AnyRef
    def get(field: String): AnyRef
    def put(field: Int, value: Any): Unit
    def put(field: String, value: Any): Unit
    def internalUpdate(field: Int, value: Any): PFARecord
    def internalUpdate(field: String, value: Any): PFARecord

    def multiUpdate(fields: Array[String], values: Array[Any]): PFARecord

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

    def apply(i: String): AnyRef = get(i)
    def updated[X](i: Int, elem: X, schema: Schema): PFARecord = internalUpdate(i, converted(elem, schema))
    def updated[X](i: String, elem: X, schema: Schema): PFARecord = internalUpdate(i, converted(elem, schema))

    def updated(path: Array[PathIndex], elem: AnyRef, schema: Schema): PFARecord = updated(path.toList, (dummy: AnyRef) => elem, schema)
    def updated(path: List[PathIndex], elem: AnyRef, schema: Schema): PFARecord = updated(path, (dummy: AnyRef) => elem, schema)
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema): PFARecord = updated(path.toList, updator, schema)
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

    def fieldNameExists(x: String): Boolean = fieldNames.contains(x)
  }

  trait AnyPFAFixed {
    def getSchema: Schema
    def bytes(): Array[Byte]
  }

  class GenericPFAFixed(schema: Schema, b: Array[Byte]) extends org.apache.avro.generic.GenericData.Fixed(schema, b) with AnyPFAFixed

  abstract class PFAFixed extends SpecificFixed with AnyPFAFixed {
    def getClassSchema: Schema
    def size: Int
    def overlay(replacement: Array[Byte]): PFAFixed
  }

  trait AnyPFAEnumSymbol {
    def getSchema: Schema
    def toString(): String
  }

  class GenericPFAEnumSymbol(schema: Schema, symbol: String) extends org.apache.avro.generic.GenericData.EnumSymbol(schema, symbol) with AnyPFAEnumSymbol

  abstract class PFAEnumSymbol extends AnyPFAEnumSymbol {
    def getClassSchema: Schema
    def value: Int
    def numSymbols: Int
    def intToStr(i: Int): String
    def strToInt(x: String): Int
  }

  object PFAArray {
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
    def empty[X](sizeHint: Int) = {
      val builder = Vector.newBuilder[X]
      builder.sizeHint(sizeHint)
      new PFAArray[X](builder, null)
    }
    def empty[X]() = new PFAArray[X](Vector.newBuilder[X], null)
    def fromVector[X](x: Vector[X]) = new PFAArray[X](null, x)
  }

  class PFAArray[X](private val builder: mutable.Builder[X, Vector[X]], private var vector: Vector[X]) extends java.util.List[X] {
    // PFAArray has two states:
    //    1. vector == null during the filling stage (Avro's code calls java.util.List.add())
    //    2. vector != null during the PFA stage (all calls are immutable)
    // Use of the java.util.List interface after it has been sealed by a PFA call causes a RuntimeException.
    // Most of the java.util.List interface causes NotImplementedErrors, since this is just to satisfy Avro.

    def toVector: Vector[X] = {
      if (vector == null)
        vector = builder.result
      vector
    }

    private var _metadata: Map[String, Any] = null
    def metadata = {
      if (_metadata == null)
        _metadata = Map[String, Any]()
      _metadata
    }
    def metadata_=(x: Map[String, Any]) {
      _metadata = x
    }

    override def toString(): String = "[" + toVector.map(x => if (x == null) "null" else x).mkString(", ") + "]"
    override def equals(that: Any): Boolean = that match {
      case thatArray: PFAArray[_] => this.toVector == thatArray.toVector
      case _ => false
    }
    override def hashCode(): Int = toVector.hashCode

    // mutable java.util.List methods
    override def add(obj: X): Boolean = {
      if (vector != null)
        throw new RuntimeException("PFAArray.add should not be used outside of Avro")
      builder += obj
      true
    }
    override def add(index: Int, obj: X): Unit = throw new NotImplementedError
    override def addAll(that: java.util.Collection[_ <: X]): Boolean = throw new NotImplementedError
    override def addAll(index: Int, that: java.util.Collection[_ <: X]): Boolean = throw new NotImplementedError
    override def clear(): Unit = throw new NotImplementedError
    override def remove(index: Int): X = throw new NotImplementedError
    override def remove(obj: AnyRef): Boolean = throw new NotImplementedError
    override def removeAll(that: java.util.Collection[_]): Boolean = throw new NotImplementedError
    override def retainAll(that: java.util.Collection[_]): Boolean = throw new NotImplementedError
    override def set(index: Int, element: X): X = throw new NotImplementedError

    // immutable java.util.List methods
    override def contains(obj: AnyRef): Boolean = throw new NotImplementedError
    override def containsAll(that: java.util.Collection[_]): Boolean = throw new NotImplementedError
    override def get(index: Int): X = apply(index)
    override def indexOf(obj: AnyRef): Int = throw new NotImplementedError
    override def isEmpty(): Boolean = throw new NotImplementedError
    override def iterator(): java.util.Iterator[X] = toVector.iterator
    override def lastIndexOf(obj: AnyRef): Int = throw new NotImplementedError
    override def listIterator(): java.util.ListIterator[X] = throw new NotImplementedError
    override def listIterator(index: Int): java.util.ListIterator[X] = throw new NotImplementedError
    override def size(): Int = toVector.size
    override def subList(fromIndex: Int, toIndex: Int): java.util.List[X] = throw new NotImplementedError
    override def toArray(): Array[AnyRef] = throw new NotImplementedError
    override def toArray[Y](y: Array[Y with AnyRef]): Array[Y with AnyRef] = throw new NotImplementedError

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

    def apply(i: Int): X = {
      val vec = toVector
      try {
        vec(i)
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException("index %d out of bounds for array with size %d".format(i, vec.size))
      }
    }
    def updated(i: Int, elem: X, schema: Schema): PFAArray[X] = {
      val vec = toVector
      try {
        PFAArray.fromVector(vec.updated(i, converted(elem, schema)))
      }
      catch {
        case err: java.lang.IndexOutOfBoundsException => throw new PFARuntimeException("index %d out of bounds for array with size %d".format(i, vec.size))
      }
    }
    def updated(path: Array[PathIndex], elem: X, schema: Schema): PFAArray[X] = updated(path.toList, (dummy: X) => elem, schema)
    def updated(path: List[PathIndex], elem: X, schema: Schema): PFAArray[X] = updated(path, (dummy: X) => elem, schema)
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema): PFAArray[X] = updated(path.toList, updator, schema)
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
    def empty[X <: AnyRef](sizeHint: Int) = {
      val builder = Map.newBuilder[String, X]
      builder.sizeHint(sizeHint)
      new PFAMap[X](builder, null)
    }
    def empty[X <: AnyRef]() = new PFAMap[X](Map.newBuilder[String, X], null)
    def fromMap[X <: AnyRef](x: Map[String, X]) = new PFAMap[X](null, x)
  }

  class PFAMap[X <: AnyRef](private val builder: mutable.Builder[(String, X), Map[String, X]], private var map: Map[String, X]) extends java.util.Map[String, X] {
    // PFAMap has two states:
    //    1. map == null during the filling stage (Avro's code calls java.util.Map.add())
    //    2. map != null during the PFA stage (all calls are immutable)
    // Use of the java.util.Map interface after it has been sealed by a PFA call causes a RuntimeException.
    // Most of the java.util.Map interface causes NotImplementedErrors, since this is just to satisfy Avro.

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
    override def clear(): Unit = throw new NotImplementedError
    override def put(key: String, value: X): X = {
      if (map != null)
        throw new RuntimeException("PFAMap.add should not be used outside of Avro")
      builder += (key -> value)
      null.asInstanceOf[X]
    }
    override def putAll(that: java.util.Map[_ <: String, _ <: X]): Unit = throw new NotImplementedError
    override def remove(key: AnyRef): X = throw new NotImplementedError

    //////////// immutable java.util.Map contract
    override def containsKey(key: AnyRef): Boolean = throw new NotImplementedError
    override def containsValue(value: AnyRef): Boolean = throw new NotImplementedError
    override def entrySet(): java.util.Set[java.util.Map.Entry[String, X]] = {
      val theMap = toMap
      val out = new java.util.HashSet[java.util.Map.Entry[String, X]](theMap.size)
      for ((k, v) <- theMap)
        out.add(new java.util.AbstractMap.SimpleEntry[String, X](k, v))
      out
    }
    override def get(key: AnyRef): X = key match {
      case i: String => apply(i)
      case _ => throw new java.util.NoSuchElementException("key not found: " + key.toString)
    }
    override def isEmpty(): Boolean = throw new NotImplementedError
    override def keySet(): java.util.Set[String] = throw new NotImplementedError
    override def size(): Int = toMap.size
    override def values(): java.util.Collection[X] = throw new NotImplementedError

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

    def apply(i: String): X = {
      val map = toMap
      try {
        map(i)
      }
      catch {
        case err: java.util.NoSuchElementException => throw new PFARuntimeException("key \"%s\" not found in map with size %d".format(i, map.size))
      }
    }
    def updated(i: String, elem: X, schema: Schema): PFAMap[X] = PFAMap.fromMap(toMap.updated(i, converted(elem, schema)))
    def updated(path: Array[PathIndex], elem: X, schema: Schema): PFAMap[X] = updated(path.toList, (dummy: X) => elem, schema)
    def updated(path: List[PathIndex], elem: X, schema: Schema): PFAMap[X] = updated(path, (dummy: X) => elem, schema)
    def updated[Y](path: Array[PathIndex], updator: Y => Y, schema: Schema): PFAMap[X] = updated(path.toList, updator, schema)
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

    class TranslateFixed(avroFixed: AvroFixed, classLoader: java.lang.ClassLoader) extends Translator {
      val fullName = avroFixed.fullName
      val constructor = classLoader.loadClass(avroFixed.fullName).getConstructor(classOf[AnyPFAFixed])
      constructor.setAccessible(true)
      def translate(datum: AnyRef): AnyRef = constructor.newInstance(datum).asInstanceOf[AnyRef]
    }

    class TranslateEnum(avroEnum: AvroEnum, classLoader: java.lang.ClassLoader) extends Translator {
      val fullName = avroEnum.fullName
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

    class TranslateRecord(avroRecord: AvroRecord, classLoader: java.lang.ClassLoader) extends Translator {
      val fullName = avroRecord.fullName
      val fieldTranslators = avroRecord.fields map {field => getTranslator(field.avroType, classLoader)} toArray

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

    def getTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader): Translator = avroType match {
      case _: AvroNull => new PassThrough
      case _: AvroBoolean => new PassThrough
      case _: AvroInt => new PassThrough
      case _: AvroLong => new PassThrough
      case _: AvroFloat => new PassThrough
      case _: AvroDouble => new PassThrough
      case _: AvroBytes => new PassThrough
      case _: AvroString => new PassThrough

      case x: AvroFixed => new TranslateFixed(x, classLoader)
      case x: AvroEnum => new TranslateEnum(x, classLoader)

      case x: AvroArray =>
        val items = getTranslator(x.items, classLoader)
        items match {
          case _: PassThrough => new PassThrough
          case _ => new TranslateArray(items)
        }

      case x: AvroMap =>
        val values = getTranslator(x.values, classLoader)
        values match {
          case _: PassThrough => new PassThrough
          case _ => new TranslateMap(values)
        }

      case x: AvroRecord => new TranslateRecord(x, classLoader)

      case x: AvroUnion =>
        val types = x.types map {y => getTranslator(y, classLoader)}
        val translators =
          types collect {
            case y: TranslateArray =>  {case null => null; case datum: PFAArray[_] => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateMap =>    {case null => null; case datum: PFAMap[_]   => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateFixed =>  {case null => null; case datum: AnyPFAFixed if (datum.getSchema.getFullName == y.fullName)      => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateEnum =>   {case null => null; case datum: AnyPFAEnumSymbol if (datum.getSchema.getFullName == y.fullName) => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateRecord => {case null => null; case datum: AnyPFARecord if (datum.getSchema.getFullName == y.fullName)     => y.translate(datum)}: PartialFunction[AnyRef, AnyRef]
          }
        new TranslateUnion((translators :+ ({case y => y}: PartialFunction[AnyRef, AnyRef])) reduce {_ orElse _})
    }
  }
  class PFADataTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader) {
    val translator = PFADataTranslator.getTranslator(avroType, classLoader)
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

    class TranslateFixed(avroFixed: AvroFixed, classLoader: java.lang.ClassLoader) extends Translator {
      val fullName = avroFixed.fullName
      val constructor = classLoader.loadClass(avroFixed.fullName).getConstructor(classOf[Array[Byte]])
      constructor.setAccessible(true)
      def translate(datum: AnyRef): AnyRef = {
        val avroDatum = datum.asInstanceOf[org.apache.avro.generic.GenericFixed]
        constructor.newInstance(avroDatum.bytes).asInstanceOf[AnyRef]
      }
    }

    class TranslateEnum(avroEnum: AvroEnum, classLoader: java.lang.ClassLoader) extends Translator {
      val fullName = avroEnum.fullName
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

    class TranslateRecord(avroRecord: AvroRecord, classLoader: java.lang.ClassLoader) extends Translator {
      val fullName = avroRecord.fullName
      val fieldNames = avroRecord.fields map {field => field.name} toArray
      val fieldTranslators = avroRecord.fields map {field => getTranslator(field.avroType, classLoader)} toArray

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

    def getTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader): Translator = avroType match {
      case _: AvroNull => new PassThrough
      case _: AvroBoolean => new PassThrough
      case _: AvroInt => new PassThrough
      case _: AvroLong => new PassThrough
      case _: AvroFloat => new PassThrough
      case _: AvroDouble => new PassThrough

      case _: AvroBytes => new TranslateBytes
      case _: AvroString => new TranslateString

      case x: AvroFixed => new TranslateFixed(x, classLoader)
      case x: AvroEnum => new TranslateEnum(x, classLoader)

      case x: AvroArray =>
        val items = getTranslator(x.items, classLoader)
        new TranslateArray(items)

      case x: AvroMap =>
        val values = getTranslator(x.values, classLoader)
        new TranslateMap(values)

      case x: AvroRecord => new TranslateRecord(x, classLoader)

      case x: AvroUnion =>
        val types = x.types map {y => getTranslator(y, classLoader)}
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
    }
  }
  class AvroDataTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader) {
    val translator = AvroDataTranslator.getTranslator(avroType, classLoader)
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

    class TranslateFixed(avroFixed: AvroFixed) extends Translator[Array[Byte]] {
      val fullName = avroFixed.fullName
      val size = avroFixed.size
      def toScala(datum: AnyRef): Array[Byte] = datum.asInstanceOf[AnyPFAFixed].bytes
      def fromScala(datum: Any): AnyRef = new GenericPFAFixed(avroFixed.schema, datum.asInstanceOf[Array[Byte]])
      val scalaClass = classOf[Array[Byte]]
    }

    class TranslateEnum(avroEnum: AvroEnum) extends Translator[String] {
      val fullName = avroEnum.fullName
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

    class TranslateRecord[X](avroRecord: AvroRecord, classLoader: java.lang.ClassLoader) extends Translator[X] {
      val fullName = avroRecord.fullName
      val fieldTranslators = avroRecord.fields map {field => getTranslator(field.avroType, classLoader)}
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

    def getTranslator(avroType: AvroType, classLoader: java.lang.ClassLoader): Translator[_] = avroType match {
      case _: AvroNull => new TranslateNull
      case _: AvroBoolean => new TranslateBoolean
      case _: AvroInt => new TranslateInt
      case _: AvroLong => new TranslateLong
      case _: AvroFloat => new TranslateFloat
      case _: AvroDouble => new TranslateDouble
      case _: AvroBytes => new TranslateBytes
      case _: AvroString => new TranslateString

      case x: AvroFixed => new TranslateFixed(x)
      case x: AvroEnum => new TranslateEnum(x)

      case x: AvroArray => new TranslateArray(getTranslator(x.items, classLoader))
      case x: AvroMap => new TranslateMap(getTranslator(x.values, classLoader))

      case x: AvroRecord => new TranslateRecord(x, classLoader)

      case x: AvroUnion =>
        val translators = x.types map {y => getTranslator(y, classLoader)}

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
    }
  }
  class ScalaDataTranslator[X](avroType: AvroType, classLoader: java.lang.ClassLoader) {
    val translator = ScalaDataTranslator.getTranslator(avroType, classLoader)
    def toScala(datum: AnyRef): X = translator.toScala(datum).asInstanceOf[X]
    def fromScala(datum: Any): AnyRef = translator.fromScala(datum)
  }
}

package object data {
  val genericData = new PFAGenericData(getClass.getClassLoader)

  def fromJson(json: String, schema: Schema): AnyRef = {
    val reader = new GenericPFADatumReader[AnyRef](schema, genericData)
    // reader.setSchema(schema)
    val decoder = DecoderFactory.get.jsonDecoder(schema, json)
    reader.read(null.asInstanceOf[AnyRef], decoder)
  }

  def fromAvro(avro: Array[Byte], schema: Schema): AnyRef = {
    val reader = new GenericPFADatumReader[AnyRef](schema, genericData)
    val decoder = DecoderFactory.get.validatingDecoder(schema, DecoderFactory.get.binaryDecoder(avro, null))
    reader.read(null.asInstanceOf[AnyRef], decoder)
  }

  def toJson(obj: AnyRef, schema: Schema): String = {
    val out = new java.io.ByteArrayOutputStream
    val encoder = EncoderFactory.get.jsonEncoder(schema, out)
    val writer = new GenericPFADatumWriter[AnyRef](schema, genericData)
    writer.write(obj, encoder)
    encoder.flush()
    out.toString
  }

  def toAvro(obj: AnyRef, schema: Schema): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream
    val encoder = EncoderFactory.get.validatingEncoder(schema, EncoderFactory.get.binaryEncoder(out, null))
    val writer = new GenericPFADatumWriter[AnyRef](schema, genericData)
    writer.write(obj, encoder)
    encoder.flush()
    out.toByteArray
  }

  def fromJson(json: String, avroType: AvroType): AnyRef      = fromJson(json, avroType.schema)
  def fromAvro(avro: Array[Byte], avroType: AvroType): AnyRef = fromAvro(avro, avroType.schema)
  def toJson(obj: AnyRef, avroType: AvroType): String         = toJson(obj, avroType.schema)
  def toAvro(obj: AnyRef, avroType: AvroType): Array[Byte]    = toAvro(obj, avroType.schema)

  def avroInputIterator[X](inputStream: InputStream, inputType: AvroType): DataFileStream[X] = {    // DataFileStream is a java.util.Iterator
    val schema = inputType.schema
    val reader = new GenericPFADatumReader[X](schema, genericData)
    val out = new DataFileStream[X](inputStream, reader)
    
    val fileSchema = AvroConversions.schemaToAvroType(out.getSchema)
    if (!inputType.accepts(fileSchema))
      throw new PFAInitializationException("InputStream has schema %s\nbut expecting schema %s".format(fileSchema, inputType))
    out
  }

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
      def remove(): Unit = throw new java.lang.UnsupportedOperationException
    }
  }

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
      def remove(): Unit = throw new java.lang.UnsupportedOperationException
    }
  }

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

  trait OutputDataStream {
    def append(datum: AnyRef): Unit
    def flush(): Unit
    def close(): Unit
  }
  
  class AvroOutputDataStream(writer: DatumWriter[AnyRef]) extends DataFileWriter[AnyRef](writer) with OutputDataStream

  def avroOutputDataStream(outputStream: java.io.OutputStream, outputType: AvroType): AvroOutputDataStream = {
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new AvroOutputDataStream(writer)
    out.create(outputType.schema, outputStream)
    out
  }

  def avroOutputDataStream(file: java.io.File, outputType: AvroType): AvroOutputDataStream = {
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new AvroOutputDataStream(writer)
    out.create(outputType.schema, file)
    out
  }

  def avroOutputDataStream(fileName: String, outputType: AvroType): AvroOutputDataStream = avroOutputDataStream(new java.io.File(fileName), outputType)

  class JsonOutputDataStream(writer: DatumWriter[AnyRef], encoder: JsonEncoder, outputStream: java.io.OutputStream) extends OutputDataStream {
    def append(obj: AnyRef): Unit = writer.write(obj, encoder)
    def flush(): Unit = encoder.flush()
    def close(): Unit = {
      encoder.flush()
      outputStream.close()
    }
  }

  def jsonOutputDataStream(outputStream: java.io.OutputStream, outputType: AvroType, writeSchema: Boolean): JsonOutputDataStream = {
    val encoder = EncoderFactory.get.jsonEncoder(outputType.schema, outputStream)
    val writer = new GenericPFADatumWriter[AnyRef](outputType.schema, genericData)
    val out = new JsonOutputDataStream(writer, encoder, outputStream)
    if (writeSchema) {
      outputStream.write(outputType.toJson.getBytes("utf-8"))
      outputStream.write("\n".getBytes("utf-8"))
    }
    out
  }

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

  def jsonOutputDataStream(fileName: String, outputType: AvroType, writeSchema: Boolean): JsonOutputDataStream = jsonOutputDataStream(new java.io.File(fileName), outputType, writeSchema)

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

  // def toJacksonTree(obj: AnyRef, avroType: AvroType): JsonNode = (obj, avroType) match {
  //   case (null, AvroNull()) => NullNode.getInstance
  //   case (java.lang.Boolean.TRUE, AvroBoolean()) => BooleanNode.getTrue
  //   case (java.lang.Boolean.FALSE, AvroBoolean()) => BooleanNode.getFalse
  //   case (x: java.lang.Number, AvroInt()) => new LongNode(value)
  //   case (x: java.lang.Number, AvroLong()) => new LongNode(value)
  //   case (x: java.lang.Number, AvroFloat()) => new DoubleNode(value)
  //   case (x: java.lang.Number, AvroDouble()) => new DoubleNode(value)
  //   case (x: Array[_], AvroBytes()) => new TextNode(new String(x.asInstanceOf[Array[Byte]]))
  //   case (x: AnyPFAFixed, AvroFixed(_, _, _, _, _)) => new TextNode(new String(x.bytes))
  //   case (x: String, AvroString()) => new TextNode(x)
  //   case (x: AnyPFAEnumSymbol, AvroEnum(_, _, _, _, _)) => new TextNode(x.toString)
  //   case (x: PFAArray[_], AvroArray(items)) =>
  //     val out = JsonNodeFactory.instance.arrayNode
  //     x.asInstanceOf[PFAArray[AnyRef]].toVector foreach {v => toJacksonTree(v, items)}
  //     out
  //   case (x: PFAMap[_], AvroMap(values)) =>
  //     val out = JsonNodeFactory.instance.objectNode
  //     x.asInstanceOf[PFAMap[AnyRef]].toMap foreach {case (k, v) => out.put(k, toJacksonTree(v, values))}
  //     out
  //   case (x: AnyPFARecord, AvroRecord(fields, _, _, _, _)) =>
  //     val out = JsonNodeFactory.instance.objectNode
  //     fields foreach {f => out.put(f.name, toJacksonTree(x.get(f.name), f.avroType))}
  //   case (_, AvroUnion(types)) =>
  //     var t: AvroType = null
  //     var v: JsonDom = null
  //     var i = 0
  //     var done = false
  //     while (i < types.size  &&  !done) {
  //       try {
  //         t = types(i)
  //         v = toJacksonTree(obj, t)
  //         done = true
  //       }
  //       catch {
  //         case _: IllegalArgumentException =>
  //       }
  //       i += 1
  //     }
  //     if (done  &&  t.isInstanceOf[AvroNull])
  //       NullNode.getInstance
  //     else if (done) {
  //       val out = JsonNodeFactory.instance.objectNode
  //       out.put(t.fullName, v)
  //       out
  //     }
  //     else
  //       throw new IllegalArgumentException("could not resolve union when turning PFA data %s into JSON using schema %s".format(obj.toString, avroType.toString))

  //   case _ => throw new IllegalArgumentException("could not turn PFA data %s into JSON using schema %s".format(obj.toString, avroType.toString))
  // }
}
