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

package com.opendatagroup.antinous

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps

import org.python.core.Py
import org.python.core.PyDictionary
import org.python.core.PyException
import org.python.core.PyFloat
import org.python.core.PyInstance
import org.python.core.PyInteger
import org.python.core.PyList
import org.python.core.PyLong
import org.python.core.PyObject
import org.python.core.PyObjectDerived
import org.python.core.PyString
import org.python.core.PyTuple
import org.python.core.PyType
import org.python.core.PyUnicode

import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.Encoder
import org.apache.avro.io.ResolvingDecoder
import org.apache.avro.Schema

import org.python.util.PythonInterpreter
import org.python.core.PyList
import org.python.core.PyDictionary
import org.python.core.PyString
import org.python.core.PyInteger
import org.python.core.PyFunction

import com.opendatagroup.hadrian.data.AnyPFAEnumSymbol
import com.opendatagroup.hadrian.data.AnyPFAFixed
import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.data.PFADatumReader
import com.opendatagroup.hadrian.data.PFADatumWriter
import com.opendatagroup.hadrian.data.PFASpecificData

package data {
  object jythonByteString extends Function1[Array[Byte], PyString] {
    private val pythonInterpreter = PythonInterpreter.threadLocalStateInterpreter(new PyDictionary)
    pythonInterpreter.exec("import array")
    private val f = pythonInterpreter.eval("""lambda x: array.array("B", x).tostring()""").asInstanceOf[PyFunction]

    def apply(x: Array[Byte]): PyString = {
      val list = new PyList()
      x foreach {xi => list.append(new PyInteger(if (xi >= 0) xi else xi + 256))}
      f.__call__(list).asInstanceOf[PyString]
    }
  }

  class JythonFixed(val getSchema: Schema, val bytes: Array[Byte], pyType: PyType) extends PyObject(pyType) with AnyPFAFixed with GenericFixed {
    if (getSchema.getType != Schema.Type.FIXED)
      throw new IllegalArgumentException("non-fixed schema passed to JythonFixed constructor")
    if (bytes.size != getSchema.getFixedSize)
      throw new IllegalArgumentException("byte array of the wrong size passed to JythonFixed constructor")

    override def __str__(): PyString = jythonByteString(bytes)

    override def equals(that: Any): Boolean = that match {
      case other: PyObject => __eq__(other) == Py.True
      case _ => false
    }
    override def __eq__(other: PyObject): PyObject =
      if (getType == other.getType)
        __str__.__eq__(other.__str__)
      else
        Py.False
  }

  class JythonEnumSymbol(val getSchema: Schema, val symbol: String, pyType: PyType) extends PyObject(pyType) with AnyPFAEnumSymbol {
    if (getSchema.getType != Schema.Type.ENUM)
      throw new IllegalArgumentException("non-enum schema passed to JythonEnumSymbol constructor")
    if (!getSchema.hasEnumSymbol(symbol))
      throw new IllegalArgumentException("unrecognized symbol passed to JythonEnumSymbol constructor")

    override def __str__() = new PyString(symbol)
    override def __unicode__() = new PyUnicode(symbol)

    override def equals(that: Any): Boolean = that match {
      case other: PyObject => __eq__(other) == Py.True
      case _ => false
    }
    override def __eq__(other: PyObject): PyObject =
      if (getType == other.getType)
        __str__.__eq__(other.__str__)
      else
        Py.False
  }

  class JythonRecord(val getSchema: Schema, pyType: PyType) extends PyObject(pyType) with AnyPFARecord with IndexedRecord {
    def nameToIndex(n: String) = getSchema.getFields.indexWhere(_.name == n)

    val getAll = Array.fill[AnyRef](getSchema.getFields.size)(null)
    def get(i: Int): AnyRef = getAll(i)
    def put(i: Int, v: AnyRef): Unit = getAll(i) = v
    def get(n: String): AnyRef = nameToIndex(n) match {
      case -1 => throw Py.AttributeError(s"${getSchema.getFullName} has no attribute $n")
      case i => getAll(i)
    }
    def put(n: String, v: AnyRef): Unit = nameToIndex(n) match {
      case -1 => throw Py.AttributeError(s"${getSchema.getFullName} has no attribute $n")
      case i => getAll(i) = v
    }

    override def __findattr_ex__(n: String): PyObject = get(n).asInstanceOf[PyObject]
    override def __setattr__(n: String, v: PyObject): Unit = put(n, v)

    override def equals(that: Any): Boolean = that match {
      case other: PyObject => __eq__(other) == Py.True
      case _ => false
    }
    override def __eq__(other: PyObject): PyObject =
      if (getType == other.getType  &&  getSchema.getFields.forall(f => __getattr__(f.name) == other.__getattr__(f.name)))
        Py.True
      else
        Py.False

    if (getSchema.getType != Schema.Type.RECORD)
      throw new IllegalArgumentException("non-record schema passed to JythonRecord constructor")
  }

  class JythonSpecificData(classLoader: java.lang.ClassLoader) extends PFASpecificData(classLoader) {
    val pyTypes = mutable.Map[String, PyType]()

    def pyType(fullName: String) = pyTypes.get(fullName) match {
      case None =>
        pyTypes(fullName) = Py.makeClass(fullName, PyObject.TYPE, new PyDictionary()).asInstanceOf[PyType]
        pyTypes(fullName)
      case Some(t) => t
    }

    override def createFixed(old: AnyRef, bytes: Array[Byte], schema: Schema): AnyRef =
      new JythonFixed(schema, bytes, pyType(schema.getFullName))

    override def createFixed(old: AnyRef, schema: Schema): AnyRef =
      new JythonFixed(schema, Array.fill[Byte](schema.getFixedSize)(0), pyType(schema.getFullName))

    override def createEnum(symbol: String, schema: Schema): AnyRef =
      new JythonEnumSymbol(schema, symbol, pyType(schema.getFullName))

    override def newRecord(old: AnyRef, schema: Schema): AnyRef =
      new JythonRecord(schema, pyType(schema.getFullName))

    override def getSchemaName(datum: AnyRef): String =
      if (datum == Py.None)
        Schema.Type.NULL.getName
      else
        super.getSchemaName(datum)

    override def instanceOf(schema: Schema, datum: AnyRef): Boolean =
      if (datum == Py.None  &&  schema.getType == Schema.Type.NULL)
        true
      else
        super.instanceOf(schema, datum)

    override def validate(schema: Schema, datum: AnyRef): Boolean =
      if (datum == Py.None  &&  schema.getType == Schema.Type.NULL)
        true
      else
        super.validate(schema, datum)

    override def induce(datum: AnyRef): Schema =
      if (datum == Py.None)
        Schema.create(Schema.Type.NULL)
      else
        super.induce(datum)

    override def isBoolean(datum: AnyRef) = datum.isInstanceOf[java.lang.Boolean]  ||  datum == Py.True  ||  datum == Py.False
    override def isInteger(datum: AnyRef) = datum.isInstanceOf[java.lang.Number]  ||  datum.isInstanceOf[PyInteger]  ||  datum.isInstanceOf[PyLong]  ||  datum.isInstanceOf[PyFloat]
    override def isLong(datum: AnyRef) = datum.isInstanceOf[java.lang.Number]  ||  datum.isInstanceOf[PyInteger]  ||  datum.isInstanceOf[PyLong]  ||  datum.isInstanceOf[PyFloat]
    override def isFloat(datum: AnyRef) = datum.isInstanceOf[java.lang.Number]  ||  datum.isInstanceOf[PyInteger]  ||  datum.isInstanceOf[PyLong]  ||  datum.isInstanceOf[PyFloat]
    override def isDouble(datum: AnyRef) = datum.isInstanceOf[java.lang.Number]  ||  datum.isInstanceOf[PyInteger]  ||  datum.isInstanceOf[PyLong]  ||  datum.isInstanceOf[PyFloat]
    override def isString(datum: AnyRef) = datum.isInstanceOf[PyString]  ||  datum.isInstanceOf[String]
    override def isBytes(datum: AnyRef) = datum.isInstanceOf[PyString]  ||  datum.isInstanceOf[String]
    override def isArray(datum: AnyRef) = datum.isInstanceOf[PyList]  ||  datum.isInstanceOf[PyTuple]
    override def isMap(datum: AnyRef) = datum.isInstanceOf[PyDictionary]
    override def isFixed(datum: AnyRef) = datum.isInstanceOf[PyString]  ||  datum.isInstanceOf[String]  ||  datum.isInstanceOf[JythonFixed]
    override def isEnum(datum: AnyRef) = datum.isInstanceOf[PyString]  ||  datum.isInstanceOf[String]  ||  datum.isInstanceOf[JythonEnumSymbol]
    override def isRecord(datum: AnyRef) = datum.isInstanceOf[java.util.Map[_, _]]  ||  datum.isInstanceOf[PyInstance]  ||  datum.isInstanceOf[PyObjectDerived]  ||  datum.isInstanceOf[JythonRecord]

    override def resolveUnion(union: Schema, datum: AnyRef): Int = {
      val i = union.getTypes indexWhere {schema =>
        (schema.getType, datum) match {
          case (Schema.Type.NULL, null) => true
          case (Schema.Type.NULL, Py.None) => true

          case (Schema.Type.BOOLEAN, x: java.lang.Boolean) => true
          case (Schema.Type.BOOLEAN, Py.True) => true
          case (Schema.Type.BOOLEAN, Py.False) => true

          case (Schema.Type.DOUBLE, x: java.lang.Number) => true
          case (Schema.Type.DOUBLE, x: PyInteger) => true
          case (Schema.Type.DOUBLE, x: PyLong) => true
          case (Schema.Type.DOUBLE, x: PyFloat) => true

          case (Schema.Type.FLOAT, x: java.lang.Number) => true
          case (Schema.Type.FLOAT, x: PyInteger) => true
          case (Schema.Type.FLOAT, x: PyLong) => true
          case (Schema.Type.FLOAT, x: PyFloat) => true

          case (Schema.Type.LONG, x: java.lang.Number) => true
          case (Schema.Type.LONG, x: PyInteger) => true
          case (Schema.Type.LONG, x: PyLong) => true

          case (Schema.Type.INT, x: java.lang.Number) => true
          case (Schema.Type.INT, x: PyInteger) => true
          case (Schema.Type.INT, x: PyLong) => true

          case (Schema.Type.STRING, x: String) => true
          case (Schema.Type.STRING, x: PyString) => true

          case (Schema.Type.BYTES, x: String) => true
          case (Schema.Type.BYTES, x: PyString) => true

          case (Schema.Type.ARRAY, x: PyList) => true
          case (Schema.Type.ARRAY, x: PyTuple) => true

          case (Schema.Type.MAP, x: PyDictionary) => true

          case (Schema.Type.FIXED, x: JythonFixed) => true
          case (Schema.Type.FIXED, x: PyString) if (x.toString.getBytes.size == schema.getFixedSize) => true

          case (Schema.Type.ENUM, x: JythonEnumSymbol) => true
          case (Schema.Type.ENUM, x: PyString) if (schema.hasEnumSymbol(x.toString)) => true

          case (Schema.Type.RECORD, x: JythonRecord) => true
          case (Schema.Type.RECORD, x: java.util.Map[_, _]) => schema.getFields forall {f => x.containsKey(f.name)}
          case (Schema.Type.RECORD, x: PyObject) => schema.getFields forall {f =>
            try {
              x.__getattr__(f.name)
              true
            }
            catch {
              case _: PyException => false
            }
          }
            
          case _ => false
        }
      }
      if (i == -1)
        throw new org.apache.avro.UnresolvedUnionException(union, datum)
      i
    }
  }

  class JythonDatumReader[X](pfaSpecificData: PFASpecificData) extends PFADatumReader[X](pfaSpecificData) {
    override def newArray(old: AnyRef, size: Int, schema: Schema): AnyRef = new PyList()
    override def newMap(old: AnyRef, size: Int): AnyRef = new PyDictionary()
    override def read(old: AnyRef, expected: Schema, in: ResolvingDecoder): AnyRef = expected.getType match {
      case Schema.Type.NULL => in.readNull(); Py.None
      case Schema.Type.BOOLEAN => if (in.readBoolean()) Py.True else Py.False
      case Schema.Type.INT => new PyInteger(in.readInt())
      case Schema.Type.LONG => new PyLong(in.readLong())
      case Schema.Type.FLOAT => new PyFloat(in.readFloat())
      case Schema.Type.DOUBLE => new PyFloat(in.readDouble())
      case Schema.Type.STRING => new PyUnicode(readString(old, expected, in).asInstanceOf[String])
      case Schema.Type.BYTES => jythonByteString(readBytes(old, in).asInstanceOf[Array[Byte]])
      case Schema.Type.ARRAY => readArray(old, expected, in)
      case Schema.Type.MAP => readMap(old, expected, in)
      case Schema.Type.FIXED => readFixed(old, expected, in)
      case Schema.Type.ENUM => readEnum(expected, in)
      case Schema.Type.RECORD => readRecord(old, expected, in)
      case Schema.Type.UNION => read(old, expected.getTypes().get(in.readIndex()), in)
    }
  }

  class JythonDatumWriter[X](schema: Schema, pfaSpecificData: PFASpecificData) extends PFADatumWriter[X](schema, pfaSpecificData) {
    override def write(schema: Schema, datum: AnyRef, out: Encoder): Unit = {
      (schema.getType, datum) match {
        case (Schema.Type.NULL, null) => out.writeNull()
        case (Schema.Type.NULL, Py.None) => out.writeNull()

        case (Schema.Type.BOOLEAN, x: java.lang.Boolean) => out.writeBoolean(x.booleanValue)
        case (Schema.Type.BOOLEAN, Py.True) => out.writeBoolean(true)
        case (Schema.Type.BOOLEAN, Py.False) => out.writeBoolean(false)

        case (Schema.Type.DOUBLE, x: java.lang.Number) => out.writeDouble(x.doubleValue)
        case (Schema.Type.DOUBLE, x: PyInteger) => out.writeDouble(x.asLong.toDouble)
        case (Schema.Type.DOUBLE, x: PyLong) => out.writeDouble(x.asLong.toDouble)
        case (Schema.Type.DOUBLE, x: PyFloat) => out.writeDouble(x.asDouble)

        case (Schema.Type.FLOAT, x: java.lang.Number) => out.writeFloat(x.floatValue)
        case (Schema.Type.FLOAT, x: PyInteger) => out.writeFloat(x.asLong.toFloat)
        case (Schema.Type.FLOAT, x: PyLong) => out.writeFloat(x.asLong.toFloat)
        case (Schema.Type.FLOAT, x: PyFloat) => out.writeFloat(x.asDouble.toFloat)

        case (Schema.Type.LONG, x: java.lang.Number) => out.writeLong(x.longValue)
        case (Schema.Type.LONG, x: PyInteger) => out.writeLong(x.asLong)
        case (Schema.Type.LONG, x: PyLong) => out.writeLong(x.asLong)

        case (Schema.Type.INT, x: java.lang.Number) => out.writeInt(x.intValue)
        case (Schema.Type.INT, x: PyInteger) => out.writeInt(x.asInt)
        case (Schema.Type.INT, x: PyLong) => out.writeInt(x.asInt)

        case (Schema.Type.STRING, x: String) => writeString(schema, x, out)
        case (Schema.Type.STRING, x: PyString) => writeString(schema, x.toString, out)

        case (Schema.Type.BYTES, x: String) => writeBytes(x.getBytes, out)
        case (Schema.Type.BYTES, x: PyString) => writeBytes(x.toString.getBytes, out)

        case (Schema.Type.ARRAY, x: PyList) => writeArray(schema, asJavaIterable(0 until x.size map {i: Int => x.pyget(i)}), out)
        case (Schema.Type.ARRAY, x: PyTuple) => writeArray(schema, asJavaIterable(0 until x.size map {i: Int => x.pyget(i)}), out)

        case (Schema.Type.MAP, x: PyDictionary) =>
          val keys = x.keys
          if (!(0 until keys.size forall {i => keys.pyget(i).isInstanceOf[PyString]}))
            throw Py.TypeError("output dictionary keys must be strings")
          writeMap(schema, mapAsJavaMap(0 until keys.size map {i: Int => (keys.get(i).asInstanceOf[String], x.get(keys.pyget(i)))} toMap), out)

        case (Schema.Type.FIXED, x: JythonFixed) => writeFixed(schema, x, out)
        case (Schema.Type.FIXED, x: PyString) if (x.toString.getBytes.size == schema.getFixedSize) =>
          writeFixed(schema, new JythonFixed(schema, x.toString.getBytes, PyObject.TYPE), out)

        case (Schema.Type.ENUM, x: JythonEnumSymbol) => writeEnum(schema, x.symbol, out)
        case (Schema.Type.ENUM, x: PyString) if (schema.hasEnumSymbol(x.toString)) =>
          writeEnum(schema, x.toString, out)

        case (Schema.Type.RECORD, x: JythonRecord) => writeRecord(schema, x, out)
        case (Schema.Type.RECORD, x: java.util.Map[_, _]) =>
          val y = new JythonRecord(schema, PyObject.TYPE)
          schema.getFields foreach {f =>
            if (x.containsKey(f.name))
              y.put(f.name, x.get(f.name).asInstanceOf[AnyRef])
            else
              throw Py.KeyError(s"schema for ${schema.getFullName} requires field ${f.name}, but it is not in the dictionary")
          }
          writeRecord(schema, y, out)
        case (Schema.Type.RECORD, x: PyObject) =>
          val y = new JythonRecord(schema, PyObject.TYPE)
          schema.getFields.foreach(f => y.put(f.name, x.__getattr__(f.name)))
          writeRecord(schema, y, out)

        case (Schema.Type.UNION, _) =>
          val index = resolveUnion(schema, datum)
          out.writeIndex(index)
          write(schema.getTypes().get(index), datum, out)

        case (_, x: PyObject) =>
          throw Py.TypeError(s"schema $schema not satisfied by ${x.__repr__}")

        case _ =>
          throw Py.TypeError(s"schema $schema not satisfied by $datum")
      }}
  }
}
