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

import java.util.Base64

import scala.language.postfixOps

import org.python.core.Py
import org.python.core.PyDictionary
import org.python.core.PyFloat
import org.python.core.PyInteger
import org.python.core.PyList
import org.python.core.PyLong
import org.python.core.PyObject
import org.python.core.PyString
import org.python.core.PyTuple
import org.python.core.PyType
import org.python.core.PyUnicode

import org.apache.avro.Schema

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.data._

package pfainterface {
  class PFAEngineFactory {
    private var debug = false
    def setDebug(x: Boolean) {debug = x}

    def engineFromJson(src: String) = PFAEngine.fromJson(src, debug = debug).head
    def engineFromYaml(src: String) = PFAEngine.fromYaml(src, debug = debug).head

    def action(pfaEngine: PFAEngine[AnyRef, AnyRef], input: PyObject): PyObject =
      dataToPython(pfaEngine.action(dataFromPython(input, pfaEngine)), pfaEngine)

    def dataToPython(data: AnyRef, pfaEngine: PFAEngine[_, _]) = pfaToPython(data, pfaEngine.outputType)
    def dataFromPython(data: PyObject, pfaEngine: PFAEngine[_, _]) = pythonToPFA(data, pfaEngine.inputType, pfaEngine.classLoader)

    private def box(value: Any): AnyRef = value match {
      case null => null
      case x: Boolean => java.lang.Boolean.valueOf(x)
      case x: Int => java.lang.Integer.valueOf(x)
      case x: Long => java.lang.Long.valueOf(x)
      case x: Float => java.lang.Float.valueOf(x)
      case x: Double => java.lang.Double.valueOf(x)
      case x: AnyRef => x
    }

    private def unbox(value: AnyRef): Any = value match {
      case null => null
      case x: java.lang.Boolean => x.booleanValue
      case x: java.lang.Integer => x.intValue
      case x: java.lang.Long => x.longValue
      case x: java.lang.Float => x.floatValue
      case x: java.lang.Double => x.doubleValue
      case x => x
    }

    def pfaToPython(data: AnyRef, avroType: AvroType): PyObject = avroType match {
      case AvroNull() =>
        if (data == null)
          Py.None
        else
          throw Py.TypeError(s"expecting PFA null ($avroType), not $data (${data.getClass.getName})")

      case AvroBoolean() =>
        if (data == java.lang.Boolean.TRUE)
          Py.True
        else if (data == java.lang.Boolean.FALSE)
          Py.False
        else
          throw Py.TypeError(s"expecting PFA boolean ($avroType), not $data (${data.getClass.getName})")

      case AvroInt() => data match {
        case x: java.lang.Integer => new PyInteger(x.intValue)
        case _ => throw Py.TypeError(s"expecting PFA int ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroLong() => data match {
        case x: java.lang.Long => new PyLong(x.longValue)
        case _ => throw Py.TypeError(s"expecting PFA long ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroFloat() => data match {
        case x: java.lang.Float => new PyFloat(x.floatValue)
        case _ => throw Py.TypeError(s"expecting PFA float ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroDouble() => data match {
        case x: java.lang.Double => new PyFloat(x.doubleValue)
        case _ => throw Py.TypeError(s"expecting PFA double ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroString() => data match {
        case x: String => new PyUnicode(x)
        case _ => throw Py.TypeError(s"expecting PFA string ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroBytes() => data match {
        case x: Array[Byte] => new PyString(Base64.getEncoder.encodeToString(x))
        case _ => throw Py.TypeError(s"expecting PFA bytes ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroArray(items) => data match {
        case x: PFAArray[_] =>
          val out = new PyList
          x.toVector foreach {y: Any => out.add(pfaToPython(box(y), items))}
          out
        case _ => throw Py.TypeError(s"expecting PFA array ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroMap(values) => data match {
        case x: PFAMap[_] =>
          val out = new PyDictionary()
          x.toMap foreach {case (k, v) => out.put(new PyUnicode(k), pfaToPython(v, values))}
          out
        case _ => throw Py.TypeError(s"expecting PFA map ($avroType), not $data (${data.getClass.getName})")
      }

      case record: AvroRecord => data match {
        case x: PFARecord =>
          val out = new PyDictionary()
          record.fields map {field => out.put(new PyUnicode(field.name), pfaToPython(x.get(field.name), field.avroType))}
          out
        case _ => throw Py.TypeError(s"expecting PFA record ($avroType), not $data (${data.getClass.getName})")
      }

      case fixed: AvroFixed => data match {
        case x: PFAFixed =>
          new PyString(Base64.getEncoder.encodeToString(x.bytes))
        case _ => throw Py.TypeError(s"expecting PFA fixed ($avroType), not $data (${data.getClass.getName})")
      }

      case enum: AvroEnum => data match {
        case x: PFAEnumSymbol =>
          new PyUnicode(x.toString)
        case _ => throw Py.TypeError(s"expecting PFA enum ($avroType), not $data (${data.getClass.getName})")
      }

      case AvroUnion(types) =>
        if (data == null) {
          if (types.contains(AvroNull()))
            Py.None
          else
            throw Py.TypeError(s"PFA union ($avroType) for $data does not include null")
        }
        else {
          val (tag, value) =
            data match {
              case x: java.lang.Boolean => types.find(_.isInstanceOf[AvroBoolean]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include boolean")
              }

              case x: java.lang.Integer => types.find(_.isInstanceOf[AvroInt]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include int")
              }

              case x: java.lang.Long => types.find(_.isInstanceOf[AvroLong]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include long")
              }

              case x: java.lang.Float => types.find(_.isInstanceOf[AvroFloat]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include float")
              }

              case x: java.lang.Double => types.find(_.isInstanceOf[AvroDouble]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include double")
              }

              case x: String => types.find(_.isInstanceOf[AvroString]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include string")
              }

              case x: Array[Byte] => types.find(_.isInstanceOf[AvroBytes]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include bytes")
              }

              case x: PFAArray[_] => types.find(_.isInstanceOf[AvroArray]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include array")
              }

              case x: PFAMap[_] => types.find(_.isInstanceOf[AvroMap]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include map")
              }

              case x: PFARecord => types.find(_.isInstanceOf[AvroRecord]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include record")
              }

              case x: PFAFixed => types.find(_.isInstanceOf[AvroFixed]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include fixed")
              }

              case x: PFAEnumSymbol => types.find(_.isInstanceOf[AvroEnum]) match {
                case Some(ti) => (ti.name, pfaToPython(data, ti))
                case None => throw Py.TypeError(s"PFA union ($avroType) for $data does not include enum")
              }
            }

          val out = new PyDictionary
          out.put(new PyUnicode(tag), value)
          out
        }
    }

    def pythonToPFA(data: PyObject, avroType: AvroType, classLoader: java.lang.ClassLoader): AnyRef = avroType match {
      case AvroNull() =>
        if (data != Py.None)
          throw Py.TypeError(s"PFA null ($avroType) must be Python None, not $data (${data.getClass.getName})")
        else
          null.asInstanceOf[AnyRef]

      case AvroBoolean() =>
        if (data != Py.True  &&  data != Py.False)
          throw Py.TypeError(s"PFA boolean ($avroType) must be Python bool, not $data (${data.getClass.getName})")
        if (data == Py.True)
          java.lang.Boolean.TRUE
        else
          java.lang.Boolean.FALSE

      case AvroInt() => data match {
        case x: PyInteger => java.lang.Integer.valueOf(x.getValue)
        case x: PyLong => java.lang.Integer.valueOf(x.getValue.intValue)
        case _ => throw Py.TypeError(s"PFA int ($avroType) must be Python int or long, not $data (${data.getClass.getName})")
      }

      case AvroLong() => data match {
        case x: PyInteger => java.lang.Long.valueOf(x.getValue)
        case x: PyLong => java.lang.Long.valueOf(x.getValue.longValue)
        case _ => throw Py.TypeError(s"PFA long ($avroType) must be Python int or long, not $data (${data.getClass.getName})")
      }

      case AvroFloat() => data match {
        case x: PyInteger => java.lang.Float.valueOf(x.getValue)
        case x: PyLong => java.lang.Float.valueOf(x.getValue.floatValue)
        case x: PyFloat => java.lang.Float.valueOf(x.getValue.toFloat)
        case _ => throw Py.TypeError(s"PFA float ($avroType) must be Python int, long, or float, not $data (${data.getClass.getName})")
      }

      case AvroDouble() => data match {
        case x: PyInteger => java.lang.Double.valueOf(x.getValue)
        case x: PyLong => java.lang.Double.valueOf(x.getValue.doubleValue)
        case x: PyFloat => java.lang.Double.valueOf(x.getValue)
        case _ => throw Py.TypeError(s"PFA double ($avroType) must be Python int, long, or float, not $data (${data.getClass.getName})")
      }

      case AvroString() => data match {
        case x: PyString => x.toString
        case _ => throw Py.TypeError(s"PFA string ($avroType) must be Python str or unicode, not $data (${data.getClass.getName})")
      }

      case AvroBytes() => data match {
        case x: PyString => x.toBytes
        case _ => throw Py.TypeError(s"PFA bytes ($avroType) must be Python str or unicode, not $data (${data.getClass.getName})")
      }

      case AvroArray(items) => data match {
        case x: PyList => PFAArray.fromVector(0 until x.size map {i => unbox(pythonToPFA(x.pyget(i), items, classLoader))} toVector)
        case x: PyTuple => PFAArray.fromVector(0 until x.size map {i => unbox(pythonToPFA(x.pyget(i), items, classLoader))} toVector)
        case _ => throw Py.TypeError(s"PFA array ($avroType) must be Python list or tuple, not $data (${data.getClass.getName})")
      }

      case AvroMap(values) => data match {
        case x: PyDictionary =>
          val keys = x.keys
          if (!(0 until keys.size forall {i => keys.pyget(i).isInstanceOf[PyString]}))
            throw Py.TypeError(s"PFA map ($avroType) must have Python str keys, not $data (${data.getClass.getName})")
          PFAMap.fromMap(0 until keys.size map {i => (keys.get(i).asInstanceOf[String], pythonToPFA(x.get(keys.pyget(i)), values, classLoader))} toMap)
        case _ => throw Py.TypeError(s"PFA map ($avroType) must be Python dict, not $data (${data.getClass.getName})")
      }

      case record: AvroRecord => data match {
        case x: PyDictionary =>
          val keys = x.keys
          if (!(0 until keys.size forall {i => keys.pyget(i).isInstanceOf[PyString]}))
            throw Py.TypeError(s"PFA record ($avroType) must have Python str keys, not $data (${data.getClass.getName})")
          
          val constructor = classLoader.loadClass(record.fullName).getConstructor(classOf[Array[AnyRef]])
          val convertedFields = record.fields map {field => pythonToPFA(x.get(new PyString(field.name)), field.avroType, classLoader)} toArray

          constructor.newInstance(convertedFields).asInstanceOf[AnyRef]

        case _ => throw Py.TypeError(s"PFA record ($avroType) must be Python dict, not $data (${data.getClass.getName})")
      }

      case fixed: AvroFixed => 
        val bytes = data match {
          case x: PyString => x.toBytes
          case _ => throw Py.TypeError(s"PFA fixed ($avroType) must be Python str or unicode, not $data (${data.getClass.getName})")
        }

        val constructor = classLoader.loadClass(fixed.fullName).getConstructor(classOf[Array[Byte]])
        constructor.newInstance(bytes).asInstanceOf[AnyRef]

      case enum: AvroEnum =>
        val string = data match {
          case x: PyString => x.toString
          case _ => throw Py.TypeError(s"PFA enum ($avroType) must be Python str or unicode, not $data (${data.getClass.getName})")
        }

        val constructor = classLoader.loadClass(enum.fullName).getConstructor(classOf[Schema], classOf[String])
        constructor.newInstance(enum.schema, string).asInstanceOf[AnyRef]

      case AvroUnion(types) => data match {
        case Py.None =>
          if (types.contains(AvroNull()))
            null.asInstanceOf[AnyRef]
          else
            throw Py.TypeError(s"PFA union ($avroType) for $data does not include null")

        case x: PyDictionary =>
          if (x.__len__ != 1)
            throw Py.TypeError(s"PFA union ($avroType) must be a singleton Python dict, not $data (${data.getClass.getName})")

          val tag = x.keys.pyget(0)
          if (!tag.isInstanceOf[PyString])
            throw Py.TypeError(s"PFA union ($avroType) must be a singleton Python dict with a str key, not $data (${data.getClass.getName})")

          val value = x.get(tag)

          types find {t => t.name == tag.toString} match {
            case Some(ti) => pythonToPFA(value, ti, classLoader)
            case None => throw Py.TypeError(s"""PFA union ($avroType) tag "${tag.toString}" not found in Python dict, not $data (${data.getClass.getName})""")
          }

        case _ => throw Py.TypeError(s"PFA union ($avroType) must be Python None or dict, not $data (${data.getClass.getName})")
      }
    }

  }
}
