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

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps

import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
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

  class PFASpecificData(classLoader: java.lang.ClassLoader) extends SpecificData(classLoader) {
    override def isBytes(datum: AnyRef): Boolean = datum.isInstanceOf[Array[Byte]]

    override def createEnum(symbol: String, schema: Schema): AnyRef =
      getClass(schema).getConstructor(classOf[Schema], classOf[String]).newInstance(schema, symbol).asInstanceOf[AnyRef]

    override def deepCopy[X](schema: Schema, value: X): X = {
      if (value == null)
        null.asInstanceOf[X]
      else
        value
    }
  }

  class PFADatumReader[X](pfaSpecificData: PFASpecificData, var asJavaHashMap: Boolean = false) extends SpecificDatumReader[X](pfaSpecificData) {
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

    // load arrays as PFAMap[_], rather than java.util.Map[Utf8, _] (unless we ask for it at top-level)
    override def newMap(old: AnyRef, size: Int): AnyRef =
      if (asJavaHashMap) {
        asJavaHashMap = false
        new java.util.HashMap[String, AnyRef](size)
      }
      else
        PFAMap.empty[AnyRef](size)
  }

  class PFADatumWriter[X](schema: Schema, pfaSpecificData: PFASpecificData) extends SpecificDatumWriter[X](schema, pfaSpecificData) {
    // write bytes as simple Array[Byte], rather than java.nio.ByteBuffer
    override def writeBytes(datum: AnyRef, out: Encoder): Unit =
      out.writeBytes(java.nio.ByteBuffer.wrap(datum.asInstanceOf[Array[Byte]]))
  }

  abstract class PFARecord extends SpecificRecordBase {
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

  abstract class PFAFixed extends SpecificFixed {
    def getClassSchema: Schema
    def size: Int
  }

  abstract class PFAEnumSymbol {
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

}
