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
import org.python.core.PyFloat
import org.python.core.PyInteger
import org.python.core.PyList
import org.python.core.PyLong
import org.python.core.PyObject
import org.python.core.PyObjectDerived
import org.python.core.PyString
import org.python.core.PyTuple
import org.python.core.PyUnicode

import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord

import com.opendatagroup.hadrian.data.AnyPFAEnumSymbol
import com.opendatagroup.hadrian.data.AnyPFAFixed
import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.data.GenericPFAEnumSymbol
import com.opendatagroup.hadrian.data.GenericPFAFixed
import com.opendatagroup.hadrian.data.GenericPFARecord
import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap

import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroNull
import com.opendatagroup.hadrian.datatype.AvroBoolean
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroBytes
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroFixed
import com.opendatagroup.hadrian.datatype.AvroEnum
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroUnion

import com.opendatagroup.antinous.data.JythonFixed
import com.opendatagroup.antinous.data.JythonEnumSymbol
import com.opendatagroup.antinous.data.JythonRecord
import com.opendatagroup.antinous.data.JythonSpecificData

package translate {
  object PFAToJythonDataTranslator {
    trait Translator {
      def toJython(datum: AnyRef): PyObject
    }

    def box(value: Any): AnyRef = value match {
      case null => null
      case x: Boolean => java.lang.Boolean.valueOf(x)
      case x: Int => java.lang.Integer.valueOf(x)
      case x: Long => java.lang.Long.valueOf(x)
      case x: Float => java.lang.Float.valueOf(x)
      case x: Double => java.lang.Double.valueOf(x)
      case x: AnyRef => x
    }

    class TranslateNull extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case null => Py.None
        case x => throw new IllegalArgumentException(s"""schema "null" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateBoolean extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case java.lang.Boolean.TRUE => Py.True
        case java.lang.Boolean.FALSE => Py.False
        case x => throw new IllegalArgumentException(s"""schema "boolean" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateInt extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyInteger(x.intValue)
        case x => throw new IllegalArgumentException(s"""schema "int" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateLong extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyLong(x.longValue)
        case x => throw new IllegalArgumentException(s"""schema "long" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateFloat extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyFloat(x.doubleValue)
        case x => throw new IllegalArgumentException(s"""schema "float" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateDouble extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyFloat(x.doubleValue)
        case x => throw new IllegalArgumentException(s"""schema "double" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateBytes extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: Array[Byte] => new PyString(new String(x))
        case x => throw new IllegalArgumentException(s"""schema "bytes" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateString extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: String => new PyUnicode(x)
        case x => throw new IllegalArgumentException(s"""schema "string" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateFixed(avroFixed: AvroFixed, jythonSpecificData: JythonSpecificData) extends Translator {
      val size = avroFixed.size
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: AnyPFAFixed => jythonSpecificData.createFixed(null, x.bytes, avroFixed.schema).asInstanceOf[PyObject]
        case x => throw new IllegalArgumentException(s"""schema "${avroFixed.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateEnum(avroEnum: AvroEnum, jythonSpecificData: JythonSpecificData) extends Translator {
      val symbols = avroEnum.symbols.toSet
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: AnyPFAEnumSymbol => jythonSpecificData.createEnum(x.toString, avroEnum.schema).asInstanceOf[PyObject]
        case x => throw new IllegalArgumentException(s"""schema "${avroEnum.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateArray(avroArray: AvroArray, jythonSpecificData: JythonSpecificData) extends Translator {
      val itemsTranslator = getTranslator(avroArray.items, jythonSpecificData)
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: PFAArray[_] =>
          val out = new PyList()
          x.toVector foreach {y: Any => out.add(itemsTranslator.toJython(box(y)))}
          out
        case x => throw new IllegalArgumentException(s"""schema "${avroArray.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateMap(avroMap: AvroMap, jythonSpecificData: JythonSpecificData) extends Translator {
      val valuesTranslator = getTranslator(avroMap.values, jythonSpecificData)
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: PFAMap[_] =>
          val out = new PyDictionary()
          x.toMap foreach {case (k, v) => out.put(new PyString(k), valuesTranslator.toJython(v))}
          out
        case x => throw new IllegalArgumentException(s"""schema "${avroMap.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateRecord(avroRecord: AvroRecord, jythonSpecificData: JythonSpecificData) extends Translator {
      val fieldNames = avroRecord.fieldNames.toArray
      val fieldTranslators = fieldNames map {x => getTranslator(avroRecord.field(x).avroType, jythonSpecificData)} toArray
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: AnyPFARecord =>
          val out = jythonSpecificData.newRecord(null, avroRecord.schema).asInstanceOf[JythonRecord]
          var i = 0
          while (i < fieldNames.size) {
            out.put(fieldNames(i), fieldTranslators(i).toJython(x.get(fieldNames(i))))
            i += 1
          }
          out.asInstanceOf[PyObject]
        case x => throw new IllegalArgumentException(s"""schema "${avroRecord.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateUnion(toJythonTranslate: PartialFunction[AnyRef, PyObject]) extends Translator {
      def toJython(datum: AnyRef): PyObject = toJythonTranslate(datum)
    }

    def getTranslator(avroType: AvroType, jythonSpecificData: JythonSpecificData): Translator = avroType match {
      case _: AvroNull => new TranslateNull
      case _: AvroBoolean => new TranslateBoolean
      case _: AvroInt => new TranslateInt
      case _: AvroLong => new TranslateLong
      case _: AvroFloat => new TranslateFloat
      case _: AvroDouble => new TranslateDouble
      case _: AvroBytes => new TranslateBytes
      case _: AvroString => new TranslateString
      case x: AvroFixed => new TranslateFixed(x, jythonSpecificData)
      case x: AvroEnum => new TranslateEnum(x, jythonSpecificData)
      case x: AvroArray => new TranslateArray(x, jythonSpecificData)
      case x: AvroMap => new TranslateMap(x, jythonSpecificData)
      case x: AvroRecord => new TranslateRecord(x, jythonSpecificData)
      case x: AvroUnion =>
        val types = x.types map {y => getTranslator(y, jythonSpecificData)}
        val toJythonTranslate =
          types collect {
            case y: TranslateNull =>    {case null                                        => y.toJython(null)}:  PartialFunction[AnyRef, PyObject]
            case y: TranslateBoolean => {case datum: java.lang.Boolean if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateInt =>     {case datum: java.lang.Integer if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateLong =>    {case datum: java.lang.Long    if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateFloat =>   {case datum: java.lang.Float   if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateDouble =>  {case datum: java.lang.Double  if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateBytes =>   {case datum: Array[_]          if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateString =>  {case datum: String            if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateFixed =>   {case datum: AnyPFAFixed       if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateEnum =>    {case datum: AnyPFAEnumSymbol  if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateArray =>   {case datum: PFAArray[_]       if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateMap =>     {case datum: PFAMap[_]         if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateRecord =>  {case datum: AnyPFARecord      if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
          }
        new TranslateUnion(toJythonTranslate reduce {_ orElse _})
    }
  }
  class PFAToJythonDataTranslator(avroType: AvroType, jythonSpecificData: JythonSpecificData) {
    val translator = PFAToJythonDataTranslator.getTranslator(avroType, jythonSpecificData)
    def translate(datum: AnyRef): PyObject = translator.toJython(datum)
  }

  object AvroToJythonDataTranslator {
    trait Translator {
      def toJython(datum: AnyRef): PyObject
    }

    class TranslateNull extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case null => Py.None
        case x => throw new IllegalArgumentException(s"""schema "null" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateBoolean extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case java.lang.Boolean.TRUE => Py.True
        case java.lang.Boolean.FALSE => Py.False
        case x => throw new IllegalArgumentException(s"""schema "boolean" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateInt extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyInteger(x.intValue)
        case x => throw new IllegalArgumentException(s"""schema "int" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateLong extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyLong(x.longValue)
        case x => throw new IllegalArgumentException(s"""schema "long" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateFloat extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyFloat(x.doubleValue)
        case x => throw new IllegalArgumentException(s"""schema "float" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateDouble extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.lang.Number => new PyFloat(x.doubleValue)
        case x => throw new IllegalArgumentException(s"""schema "double" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateBytes extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.nio.ByteBuffer => new PyString(new String(x.array))
        case x => throw new IllegalArgumentException(s"""schema "bytes" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateString extends Translator {
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: String => new PyUnicode(x)
        case x: java.lang.CharSequence => new PyUnicode(x.toString)
        case x => throw new IllegalArgumentException(s"""schema "string" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateFixed(avroFixed: AvroFixed, jythonSpecificData: JythonSpecificData) extends Translator {
      val size = avroFixed.size
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: GenericFixed => jythonSpecificData.createFixed(null, x.bytes, avroFixed.schema).asInstanceOf[PyObject]
        case x => throw new IllegalArgumentException(s"""schema "${avroFixed.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateEnum(avroEnum: AvroEnum, jythonSpecificData: JythonSpecificData) extends Translator {
      val symbols = avroEnum.symbols.toSet
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: GenericEnumSymbol => jythonSpecificData.createEnum(x.toString, avroEnum.schema).asInstanceOf[PyObject]
        case x => throw new IllegalArgumentException(s"""schema "${avroEnum.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateArray(avroArray: AvroArray, jythonSpecificData: JythonSpecificData) extends Translator {
      val itemsTranslator = getTranslator(avroArray.items, jythonSpecificData)
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.util.List[_] =>
          val out = new PyList()
          x.asInstanceOf[java.util.List[AnyRef]] foreach {y: AnyRef => out.add(itemsTranslator.toJython(y))}
          out
        case x => throw new IllegalArgumentException(s"""schema "${avroArray.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateMap(avroMap: AvroMap, jythonSpecificData: JythonSpecificData) extends Translator {
      val valuesTranslator = getTranslator(avroMap.values, jythonSpecificData)
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: java.util.Map[_, _] =>
          val out = new PyDictionary()
          x.asInstanceOf[java.util.Map[String, AnyRef]] foreach {case (k, v) =>
            out.put(new PyString(k), valuesTranslator.toJython(v))
          }
          out
        case x => throw new IllegalArgumentException(s"""schema "${avroMap.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateRecord(avroRecord: AvroRecord, jythonSpecificData: JythonSpecificData) extends Translator {
      val fieldNames = avroRecord.fieldNames.toArray
      val fieldTranslators = fieldNames map {x => getTranslator(avroRecord.field(x).avroType, jythonSpecificData)} toArray
      def toJython(datum: AnyRef): PyObject = datum match {
        case x: GenericRecord =>
          val out = jythonSpecificData.newRecord(null, avroRecord.schema).asInstanceOf[JythonRecord]
          var i = 0
          while (i < fieldNames.size) {
            out.put(fieldNames(i), fieldTranslators(i).toJython(x.get(fieldNames(i))))
            i += 1
          }
          out.asInstanceOf[PyObject]
        case x => throw new IllegalArgumentException(s"""schema "${avroRecord.schema}" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateUnion(toJythonTranslate: PartialFunction[AnyRef, PyObject]) extends Translator {
      def toJython(datum: AnyRef): PyObject = toJythonTranslate(datum)
    }

    def getTranslator(avroType: AvroType, jythonSpecificData: JythonSpecificData): Translator = avroType match {
      case _: AvroNull => new TranslateNull
      case _: AvroBoolean => new TranslateBoolean
      case _: AvroInt => new TranslateInt
      case _: AvroLong => new TranslateLong
      case _: AvroFloat => new TranslateFloat
      case _: AvroDouble => new TranslateDouble
      case _: AvroBytes => new TranslateBytes
      case _: AvroString => new TranslateString
      case x: AvroFixed => new TranslateFixed(x, jythonSpecificData)
      case x: AvroEnum => new TranslateEnum(x, jythonSpecificData)
      case x: AvroArray => new TranslateArray(x, jythonSpecificData)
      case x: AvroMap => new TranslateMap(x, jythonSpecificData)
      case x: AvroRecord => new TranslateRecord(x, jythonSpecificData)
      case x: AvroUnion =>
        val types = x.types map {y => getTranslator(y, jythonSpecificData)}
        val toJythonTranslate =
          types collect {
            case y: TranslateNull =>    {case null                                             => y.toJython(null)}:  PartialFunction[AnyRef, PyObject]
            case y: TranslateBoolean => {case datum: java.lang.Boolean      if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateInt =>     {case datum: java.lang.Integer      if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateLong =>    {case datum: java.lang.Long         if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateFloat =>   {case datum: java.lang.Float        if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateDouble =>  {case datum: java.lang.Double       if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateBytes =>   {case datum: java.nio.ByteBuffer    if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateString =>  {case datum: java.lang.CharSequence if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateFixed =>   {case datum: GenericFixed           if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateEnum =>    {case datum: GenericEnumSymbol      if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateArray =>   {case datum: java.util.List[_]      if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateMap =>     {case datum: java.util.Map[_, _]    if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
            case y: TranslateRecord =>  {case datum: GenericRecord          if (datum != null) => y.toJython(datum)}: PartialFunction[AnyRef, PyObject]
          }
        new TranslateUnion(toJythonTranslate reduce {_ orElse _})
    }
  }
  class AvroToJythonDataTranslator(avroType: AvroType, jythonSpecificData: JythonSpecificData) {
    val translator = AvroToJythonDataTranslator.getTranslator(avroType, jythonSpecificData)
    def translate(datum: AnyRef): PyObject = translator.toJython(datum)
  }

  object JythonToPFADataTranslator {
    trait Translator {
      def toPFA(datum: AnyRef): AnyRef
    }

    def unbox(value: AnyRef): Any = value match {
      case null => null
      case x: java.lang.Boolean => x.booleanValue
      case x: java.lang.Integer => x.intValue
      case x: java.lang.Long => x.longValue
      case x: java.lang.Float => x.floatValue
      case x: java.lang.Double => x.doubleValue
      case x => x
    }

    def stripDerived(value: AnyRef) = value match {
      case null => null
      case x: PyObjectDerived => x.__tojava__(classOf[AnyRef]) match {
        case Py.NoConversion => value  // go back to the original value if __tojava_ gave you NoConversion
        case y => y
      }
      case x => x
    }

    class TranslateNull extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case null => null.asInstanceOf[AnyRef]
        case Py.None => null.asInstanceOf[AnyRef]
        case x: PyObject => throw Py.TypeError(s"""schema "null" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "null" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateBoolean extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case Py.True => java.lang.Boolean.TRUE
        case Py.False => java.lang.Boolean.FALSE
        case x: java.lang.Boolean => x
        case x: PyObject => throw Py.TypeError(s"""schema "boolean" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "boolean" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateInt extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyInteger => java.lang.Integer.valueOf(x.asInt)
        case x: PyLong => java.lang.Integer.valueOf(x.asInt)
        case x: java.lang.Integer => x
        case x: java.lang.Long => java.lang.Integer.valueOf(x.intValue)
        case x: PyObject => throw Py.TypeError(s"""schema "int" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "int" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateLong extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyInteger => java.lang.Long.valueOf(x.asLong)
        case x: PyLong => java.lang.Long.valueOf(x.asLong)
        case x: java.lang.Integer => java.lang.Long.valueOf(x.longValue)
        case x: java.lang.Long => x
        case x: PyObject => throw Py.TypeError(s"""schema "long" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "long" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateFloat extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyInteger => java.lang.Float.valueOf(x.asLong.toFloat)
        case x: PyLong => java.lang.Float.valueOf(x.asLong.toFloat)
        case x: PyFloat => java.lang.Float.valueOf(x.asDouble.toFloat)
        case x: java.lang.Float => x
        case x: java.lang.Number => java.lang.Float.valueOf(x.floatValue)
        case x: PyObject => throw Py.TypeError(s"""schema "float" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "float" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateDouble extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyInteger => java.lang.Double.valueOf(x.asLong.toDouble)
        case x: PyLong => java.lang.Double.valueOf(x.asLong.toDouble)
        case x: PyFloat => java.lang.Double.valueOf(x.asDouble)
        case x: java.lang.Double => x
        case x: java.lang.Number => java.lang.Double.valueOf(x.doubleValue)
        case x: PyObject => throw Py.TypeError(s"""schema "double" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "double" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateBytes extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyString => x.toString.getBytes
        case x: Array[_] => x
        case x: String => x.getBytes
        case x: PyObject => throw Py.TypeError(s"""schema "bytes" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "bytes" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateString extends Translator {
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyString => x.toString
        case x: String => x
        case x: PyObject => throw Py.TypeError(s"""schema "string" not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")
        case x => throw Py.TypeError(s"""schema "string" not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateFixed(avroFixed: AvroFixed) extends Translator {
      val fullName = avroFixed.fullName
      val size = avroFixed.size
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: JythonFixed =>
          new GenericPFAFixed(avroFixed.schema, x.bytes)

        case x: PyString if (x.toString.getBytes.size == size) =>
          new GenericPFAFixed(avroFixed.schema, x.toString.getBytes)

        case x: Array[_] if (x.size == size) =>
          new GenericPFAFixed(avroFixed.schema, x.asInstanceOf[Array[Byte]])

        case x: String if (x.getBytes.size == size) =>
          new GenericPFAFixed(avroFixed.schema, x.getBytes)

        case x: PyObject => throw Py.TypeError(s"""schema ${avroFixed.schema} not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")

        case x => throw Py.TypeError(s"""schema ${avroFixed.schema} not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateEnum(avroEnum: AvroEnum) extends Translator {
      val fullName = avroEnum.fullName
      val symbols = avroEnum.symbols.toSet
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: JythonEnumSymbol =>
          new GenericPFAEnumSymbol(avroEnum.schema, x.__str__.toString)

        case x: PyString if (symbols.contains(x.toString)) =>
          new GenericPFAEnumSymbol(avroEnum.schema, x.toString)

        case x: String if (symbols.contains(x)) =>
          new GenericPFAEnumSymbol(avroEnum.schema, x)

        case x: PyObject => throw Py.TypeError(s"""schema ${avroEnum.schema} not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")

        case x => throw Py.TypeError(s"""schema ${avroEnum.schema} not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateArray(avroArray: AvroArray) extends Translator {
      val itemsTranslator = getTranslator(avroArray.items)
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyList => PFAArray.fromVector(0 until x.size map {i => unbox(itemsTranslator.toPFA(x.pyget(i)))} toVector)

        case x: PyTuple => PFAArray.fromVector(0 until x.size map {i => unbox(itemsTranslator.toPFA(x.pyget(i)))} toVector)

        case x: java.util.List[_] => PFAArray.fromVector(x map {v => unbox(itemsTranslator.toPFA(v.asInstanceOf[AnyRef]))} toVector)

        case x: PyObject => throw Py.TypeError(s"""schema ${avroArray.schema} not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")

        case x => throw Py.TypeError(s"""schema ${avroArray.schema} not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateMap(avroMap: AvroMap) extends Translator {
      val valuesTranslator = getTranslator(avroMap.values)
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyDictionary =>
          val keys = x.keys
          if (!(0 until keys.size forall {i => keys.pyget(i).isInstanceOf[PyString]}))
            throw Py.TypeError("output dictionary keys must be strings")
          PFAMap.fromMap(0 until keys.size map {i => (keys.get(i).asInstanceOf[String], valuesTranslator.toPFA(x.get(keys.pyget(i))))} toMap)

        case x: java.util.Map[_, _] =>
          val keys = x.keys
          if (!(keys forall {_.isInstanceOf[String]}))
            throw Py.TypeError("output java.util.Map keys must be strings")
          PFAMap.fromMap(keys map {k => (k.asInstanceOf[String], valuesTranslator.toPFA(x.get(k).asInstanceOf[AnyRef]))} toMap)

        case x: PyObject => throw Py.TypeError(s"""schema ${avroMap.schema} not satisfied by ${x.getType.__repr__} of ${x.getClass.getName} (${x.__repr__})""")

        case x => throw Py.TypeError(s"""schema ${avroMap.schema} not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateRecord(avroRecord: AvroRecord) extends Translator {
      val fullName = avroRecord.fullName
      val fieldNames = avroRecord.fieldNames.toArray
      val fieldTranslators = fieldNames map {x => getTranslator(avroRecord.field(x).avroType)} toArray
      def toPFA(datum: AnyRef): AnyRef = stripDerived(datum) match {
        case x: PyDictionary =>
          val out = new GenericPFARecord(avroRecord.schema)
          var i = 0
          while (i < fieldNames.size) {
            if (x.containsKey(fieldNames(i)))
              out.put(fieldNames(i), fieldTranslators(i).toPFA(x.get(new PyString(fieldNames(i)))))
            else
              throw Py.KeyError(s"schema for ${avroRecord.fullName} requires field ${fieldNames(i)}, but it is not in the dictionary")
            i += 1
          }
          out

        case x: java.util.Map[_, _] =>
          val out = new GenericPFARecord(avroRecord.schema)
          var i = 0
          while (i < fieldNames.size) {
            if (x.containsKey(fieldNames(i)))
              out.put(fieldNames(i), fieldTranslators(i).toPFA(x.get(fieldNames(i)).asInstanceOf[AnyRef]))
            else
              throw Py.KeyError(s"schema for ${avroRecord.fullName} requires field ${fieldNames(i)}, but it is not in the java.util.Map")
            i += 1
          }
          out

        case x: PyObject =>
          val out = new GenericPFARecord(avroRecord.schema)
          var i = 0
          while (i < fieldNames.size) {
            out.put(fieldNames(i), fieldTranslators(i).toPFA(x.__getattr__(new PyString(fieldNames(i)))))
            i += 1
          }
          out
        case x => throw Py.TypeError(s"""schema ${avroRecord.schema} not satisfied by ${x.getClass.getName} ($x)""")
      }
    }

    class TranslateUnion(toPFATranslate: PartialFunction[AnyRef, AnyRef]) extends Translator {
      def toPFA(datum: AnyRef): AnyRef = toPFATranslate(datum)
    }

    def getTranslator(avroType: AvroType): Translator = avroType match {
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
      case x: AvroArray => new TranslateArray(x)
      case x: AvroMap => new TranslateMap(x)
      case x: AvroRecord => new TranslateRecord(x)
      case x: AvroUnion =>
        val types = x.types map {y => getTranslator(y)}
        val toPFATranslate =
          types collect {
            case y: TranslateNull =>    {case null      => y.toPFA(null)
                                         case Py.None   => y.toPFA(Py.None)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateBoolean => {case Py.True   => y.toPFA(Py.True)
                                         case Py.False  => y.toPFA(Py.False)
                                         case datum: java.lang.Boolean if (datum != null)   => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateDouble =>  {case datum: PyInteger if (datum != null)           => y.toPFA(datum)
                                         case datum: PyLong if (datum != null)              => y.toPFA(datum)
                                         case datum: PyFloat if (datum != null)             => y.toPFA(datum)
                                         case datum: java.lang.Number if (datum != null)    => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateFloat =>   {case datum: PyInteger if (datum != null)           => y.toPFA(datum)
                                         case datum: PyLong if (datum != null)              => y.toPFA(datum)
                                         case datum: PyFloat if (datum != null)             => y.toPFA(datum)
                                         case datum: java.lang.Number if (datum != null)    => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateLong =>    {case datum: PyInteger if (datum != null)           => y.toPFA(datum)
                                         case datum: PyLong if (datum != null)              => y.toPFA(datum)
                                         case datum: java.lang.Long if (datum != null)      => y.toPFA(datum)
                                         case datum: java.lang.Integer if (datum != null)   => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateInt =>     {case datum: PyInteger if (datum != null)           => y.toPFA(datum)
                                         case datum: PyLong if (datum != null)              => y.toPFA(datum)
                                         case datum: java.lang.Long if (datum != null)      => y.toPFA(datum)
                                         case datum: java.lang.Integer if (datum != null)   => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateString =>  {case datum: PyString if (datum != null)            => y.toPFA(datum)
                                         case datum: String if (datum != null)              => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateBytes =>   {case datum: PyString if (datum != null)            => y.toPFA(datum)
                                         case datum: Array[_] if (datum != null)            => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateArray =>   {case datum: PyList if (datum != null)              => y.toPFA(datum)
                                         case datum: PyTuple if (datum != null)             => y.toPFA(datum)
                                         case datum: java.util.List[_] if (datum != null)   => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateMap =>     {case datum: PyDictionary if (datum != null)        => y.toPFA(datum)
                                         case datum: java.util.Map[_, _] if (datum != null) => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateFixed =>   {case datum: JythonFixed if (datum != null  &&  datum.getSchema.getFullName == y.fullName)      => y.toPFA(datum)
                                         case datum: PyString if (datum != null  &&  datum.toString.getBytes.size == y.size)            => y.toPFA(datum)
                                         case datum: Array[_] if (datum != null  &&  datum.size == y.size)                              => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateEnum =>    {case datum: JythonEnumSymbol if (datum != null  &&  datum.getSchema.getFullName == y.fullName) => y.toPFA(datum)
                                         case datum: PyString if (datum != null  &&  y.symbols.contains(datum.toString))                => y.toPFA(datum)
                                         case datum: String if (datum != null  &&  y.symbols.contains(datum))                           => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case y: TranslateRecord =>  {case datum: JythonRecord if (datum != null  &&  datum.getSchema.getFullName == y.fullName)     => y.toPFA(datum)
                                         case datum: java.util.Map[_, _] if (datum != null  &&  datum.keys.toSet == y.fieldNames.toSet) => y.toPFA(datum)
                                         case datum: PyDictionary if (datum != null  &&  datum.keys.toSet == y.fieldNames.toSet)        => y.toPFA(datum)
                                         case datum: PyObject if (datum != null  &&  datum.getDict.asInstanceOf[PyDictionary].keys.toSet == y.fieldNames.toSet) => y.toPFA(datum)}: PartialFunction[AnyRef, AnyRef]
            case _ =>                   {case y: PyObject => throw Py.TypeError(s"""schema ${x.schema} not satisfied by ${y.getType.__repr__} (${y.__repr__})""")
                                         case y => throw Py.TypeError(s"""schema ${x.schema} not satisfied by ${y.getClass.getName} ($y)""")}: PartialFunction[AnyRef, AnyRef]
          }
        new TranslateUnion(toPFATranslate reduce {_ orElse _})
    }
  }
  class JythonToPFADataTranslator(avroType: AvroType) {
    val translator = JythonToPFADataTranslator.getTranslator(avroType)
    def translate(datum: AnyRef): AnyRef = datum match {
      case x: producer.Model => translator.toPFA(x.pfa)
      case x => translator.toPFA(x)
    }
  }
}
