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

package test.scala.data

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.python.core.Py
import org.python.core.PyObject
import org.python.core.PyString
import org.python.core.PyInteger

import com.opendatagroup.hadrian.data.fromJson
import com.opendatagroup.hadrian.data.AnyPFAEnumSymbol
import com.opendatagroup.hadrian.data.AnyPFAFixed
import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.data.GenericPFAEnumSymbol
import com.opendatagroup.hadrian.data.GenericPFAFixed
import com.opendatagroup.hadrian.data.GenericPFARecord
import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine

import com.opendatagroup.antinous.engine.JythonEngine

@RunWith(classOf[JUnitRunner])
class DataSuite extends FlatSpec with Matchers {
  "JythonEngine" must "basically work" in {
    val engine: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = double
output = double
def action(input):
  emit(input + 100)
""").head
    engine.emit = {x => x should be (java.lang.Double.valueOf(113.0))}
    engine.action(engine.fromPFAData(java.lang.Double.valueOf(13)))
    engine.action(engine.fromGenericAvroData(java.lang.Double.valueOf(13)))
    engine.action(engine.jsonInput("13"))
  }

  "JythonEngine input" must "input null" in {
    val engineNull: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = null
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineNull.emit = {x => x should be ("<type 'NoneType'>")}
    engineNull.action(engineNull.fromPFAData(null))
    engineNull.action(engineNull.fromGenericAvroData(null))
    engineNull.action(engineNull.jsonInput("null"))
  }

  it must "input boolean" in {
    val engineBoolean: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = bool
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineBoolean.emit = {x => x should be ("<type 'bool'>")}
    engineBoolean.action(engineBoolean.fromPFAData(java.lang.Boolean.TRUE))
    engineBoolean.action(engineBoolean.fromPFAData(java.lang.Boolean.FALSE))
    engineBoolean.action(engineBoolean.fromGenericAvroData(java.lang.Boolean.TRUE))
    engineBoolean.action(engineBoolean.fromGenericAvroData(java.lang.Boolean.FALSE))
    engineBoolean.action(engineBoolean.jsonInput("true"))
    engineBoolean.action(engineBoolean.jsonInput("false"))
  }

  it must "input int" in {
    val engineInt: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = int
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineInt.emit = {x => x should be ("<type 'int'>")}
    engineInt.action(engineInt.fromPFAData(java.lang.Integer.valueOf(12)))
    engineInt.action(engineInt.fromGenericAvroData(java.lang.Integer.valueOf(12)))
    engineInt.action(engineInt.jsonInput("12"))
  }

  it must "input long" in {
    val engineLong: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = long
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineLong.emit = {x => x should be ("<type 'long'>")}
    engineLong.action(engineLong.fromPFAData(java.lang.Long.valueOf(234524523453245L)))
    engineLong.action(engineLong.fromGenericAvroData(java.lang.Long.valueOf(234524523453245L)))
    engineLong.action(engineLong.jsonInput("234524523453245"))
  }

  it must "input float" in {
    val engineFloat: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = float
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineFloat.emit = {x => x should be ("<type 'float'>")}
    engineFloat.action(engineFloat.fromPFAData(java.lang.Float.valueOf(3.14F)))
    engineFloat.action(engineFloat.fromGenericAvroData(java.lang.Float.valueOf(3.14F)))
    engineFloat.action(engineFloat.jsonInput("3.14"))
  }

  it must "input double" in {
    val engineDouble: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = double
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineDouble.emit = {x => x should be ("<type 'float'>")}
    engineDouble.action(engineDouble.fromPFAData(java.lang.Double.valueOf(3.14)))
    engineDouble.action(engineDouble.fromGenericAvroData(java.lang.Double.valueOf(3.14)))
    engineDouble.action(engineDouble.jsonInput("3.14"))
  }

  it must "input string" in {
    val engineString: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = string
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineString.emit = {x => x should be ("<type 'unicode'>")}
    engineString.action(engineString.fromPFAData("hello"))
    engineString.action(engineString.fromGenericAvroData("hello"))
    engineString.action(engineString.jsonInput(""""hello""""))
  }

  it must "input bytes" in {
    val engineBytes: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = bytes
output = string
def action(input):
  emit(repr(type(input)))
""").head
    engineBytes.emit = {x => x should be ("<type 'str'>")}
    engineBytes.action(engineBytes.fromPFAData("hello".getBytes))
    engineBytes.action(engineBytes.fromGenericAvroData(java.nio.ByteBuffer.wrap("hello".getBytes)))
    engineBytes.action(engineBytes.jsonInput(""""hello""""))
  }

  it must "input array" in {
    val engineArray1: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = array(int)
output = string
def action(input):
  emit(repr(type(input)) + " " + " ".join(repr(type(x)) for x in input))
""").head
    engineArray1.emit = {x => x should be ("<type 'list'> <type 'int'> <type 'int'> <type 'int'>")}
    engineArray1.action(engineArray1.fromPFAData(PFAArray.fromVector(Vector(1, 2, 3))))
    engineArray1.action(engineArray1.fromGenericAvroData(seqAsJavaList(Vector(1, 2, 3))))
    engineArray1.action(engineArray1.jsonInput("""[1, 2, 3]"""))

    val engineArray2: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = array(union(int, null))
output = string
def action(input):
  emit(repr(type(input)) + " " + " ".join(repr(type(x)) for x in input))
""").head
    engineArray2.emit = {x => x should be ("<type 'list'> <type 'int'> <type 'NoneType'> <type 'int'>")}
    engineArray2.action(engineArray2.fromPFAData(PFAArray.fromVector(Vector(1, null, 3))))
    engineArray2.action(engineArray2.fromGenericAvroData(seqAsJavaList(Vector(1, null, 3))))
    engineArray2.action(engineArray2.jsonInput("""[{"int": 1}, null, {"int": 3}]"""))
  }

  it must "input map" in {
    val engineMap1: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = map(int)
output = string
def action(input):
  emit(repr(type(input)) + " " + " ".join(repr(type(v)) for k, v in input.items()))
""").head
    engineMap1.emit = {x => x should be ("<type 'dict'> <type 'int'> <type 'int'> <type 'int'>")}
    engineMap1.action(engineMap1.fromPFAData(PFAMap.fromMap(Map("one" -> java.lang.Integer.valueOf(1), "two" -> java.lang.Integer.valueOf(2), "three" -> java.lang.Integer.valueOf(3)))))
    engineMap1.action(engineMap1.fromGenericAvroData(mapAsJavaMap(Map("one" -> java.lang.Integer.valueOf(1), "two" -> java.lang.Integer.valueOf(2), "three" -> java.lang.Integer.valueOf(3)))))
    engineMap1.action(engineMap1.jsonInput("""{"one": 1, "two": 2, "three": 3}"""))

    val engineMap2: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = map(union(int, null))
output = string
def action(input):
  emit(repr(type(input)) + " " + " ".join(repr(type(v)) for k, v in input.items()))
""").head
    engineMap2.emit = {x => x should be ("<type 'dict'> <type 'int'> <type 'NoneType'> <type 'int'>")}
    engineMap2.action(engineMap2.fromPFAData(PFAMap.fromMap(Map("one" -> java.lang.Integer.valueOf(1), "two" -> null, "three" -> java.lang.Integer.valueOf(3)))))
    engineMap2.action(engineMap2.fromGenericAvroData(mapAsJavaMap(Map("one" -> java.lang.Integer.valueOf(1), "two" -> null, "three" -> java.lang.Integer.valueOf(3)))))
    engineMap2.action(engineMap2.jsonInput("""{"one": {"int": 1}, "two": null, "three": {"int": 3}}"""))
  }

  it must "input fixed" in {
    val engineFixed: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = fixed(5, "MyFixed")
output = string
def action(input):
  emit(repr(type(input)) + " " + str(input))
""").head
    engineFixed.emit = {x => x should be ("<class 'MyFixed'> hello")}
    val fixed = new GenericPFAFixed(AvroFixed(5).schema, "hello".getBytes)
    engineFixed.action(engineFixed.fromPFAData(fixed))
    engineFixed.action(engineFixed.fromGenericAvroData(fixed))
    engineFixed.action(engineFixed.jsonInput(""""hello""""))
  }

  it must "input enum" in {
    val engineEnum: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = enum(["one", "two", "three"], "MyEnum")
output = string
def action(input):
  emit(repr(type(input)) + " " + str(input))
""").head
    engineEnum.emit = {x => x should be ("<class 'MyEnum'> one")}
    val enum = new GenericPFAEnumSymbol(AvroEnum(List("one", "two", "three")).schema, "one")
    engineEnum.action(engineEnum.fromPFAData(enum))
    engineEnum.action(engineEnum.fromGenericAvroData(enum))
    engineEnum.action(engineEnum.jsonInput(""""one""""))
  }

  it must "input record" in {
    val engineRecord: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = record("MyRecord", one=int, two=double, three=string)
output = string
def action(input):
  emit(repr(type(input)) + " " + str(input.one) + " " + str(input.two) + " " + input.three)
""").head
    engineRecord.emit = {x => x should be ("<class 'MyRecord'> 1 2.2 THREE")}
    val rec = new GenericPFARecord(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyRecord").schema)
    rec.put("one", java.lang.Integer.valueOf(1))
    rec.put("two", java.lang.Double.valueOf(2.2))
    rec.put("three", "THREE")
    engineRecord.action(engineRecord.fromPFAData(rec))
    engineRecord.action(engineRecord.fromGenericAvroData(rec))
    engineRecord.action(engineRecord.jsonInput("""{"one": 1, "two": 2.2, "three": "THREE"}"""))
  }

  it must "input union" in {
    val engineRecordUnion: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = union(null, record("MyRecord", one=int, two=double, three=string))
output = string
def action(input):
  if input is None:
    emit(repr(type(input)))
  else:
    emit(repr(type(input)) + " " + str(input.one) + " " + str(input.two) + " " + input.three)
""").head
    engineRecordUnion.emit = {x => x should be ("<class 'MyRecord'> 1 2.2 THREE")}
    val rec = new GenericPFARecord(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyRecord").schema)
    rec.put("one", java.lang.Integer.valueOf(1))
    rec.put("two", java.lang.Double.valueOf(2.2))
    rec.put("three", "THREE")
    engineRecordUnion.action(engineRecordUnion.fromPFAData(rec))
    engineRecordUnion.action(engineRecordUnion.fromGenericAvroData(rec))
    engineRecordUnion.action(engineRecordUnion.jsonInput("""{"MyRecord": {"one": 1, "two": 2.2, "three": "THREE"}}"""))

    engineRecordUnion.emit = {x => x should be ("<type 'NoneType'>")}
    engineRecordUnion.action(engineRecordUnion.fromPFAData(null))
    engineRecordUnion.action(engineRecordUnion.fromGenericAvroData(null))
    engineRecordUnion.action(engineRecordUnion.jsonInput("""null"""))

    val enginePrimitivesUnion: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = union(int, string)
output = string
def action(input):
  emit(repr(type(input)))
""").head
    enginePrimitivesUnion.emit = {x => x should be ("<type 'int'>")}
    enginePrimitivesUnion.action(enginePrimitivesUnion.fromPFAData(java.lang.Integer.valueOf(1)))
    enginePrimitivesUnion.action(enginePrimitivesUnion.fromGenericAvroData(java.lang.Integer.valueOf(1)))
    enginePrimitivesUnion.action(enginePrimitivesUnion.jsonInput("""{"int": 1}"""))

    enginePrimitivesUnion.emit = {x => x should be ("<type 'unicode'>")}
    enginePrimitivesUnion.action(enginePrimitivesUnion.fromPFAData("hello"))
    enginePrimitivesUnion.action(enginePrimitivesUnion.fromGenericAvroData("hello"))
    enginePrimitivesUnion.action(enginePrimitivesUnion.jsonInput("""{"string": "hello"}"""))
  }

  "JythonEngine output" must "output null" in {
    val engineNull = JythonEngine.fromPython("""
from antinous import *
input = null
output = null
def action(input):
  emit(None)
""").head
    engineNull.emit = {x => x should be (null)}
    engineNull.action(Py.None)
    engineNull.emit = {x => engineNull.jsonOutput(x) should be ("null")}
    engineNull.action(Py.None)
  }

  it must "output boolean" in {
    val engineTrue = JythonEngine.fromPython("""
from antinous import *
input = null
output = boolean
def action(input):
  emit(True)
""").head
    engineTrue.emit = {x => x should be (java.lang.Boolean.TRUE)}
    engineTrue.action(Py.None)
    engineTrue.emit = {x => engineTrue.jsonOutput(x) should be ("true")}
    engineTrue.action(Py.None)

    val engineFalse = JythonEngine.fromPython("""
from antinous import *
input = null
output = boolean
def action(input):
  emit(False)
""").head
    engineFalse.emit = {x => x should be (java.lang.Boolean.FALSE)}
    engineFalse.action(Py.None)
    engineFalse.emit = {x => engineFalse.jsonOutput(x) should be ("false")}
    engineFalse.action(Py.None)
  }

  it must "output int" in {
    val engineInt = JythonEngine.fromPython("""
from antinous import *
input = null
output = int
def action(input):
  emit(12)
""").head
    engineInt.emit = {x => x should be (java.lang.Integer.valueOf(12))}
    engineInt.action(Py.None)
    engineInt.emit = {x => engineInt.jsonOutput(x) should be ("12")}
    engineInt.action(Py.None)
  }

  it must "output long" in {
    val engineLong = JythonEngine.fromPython("""
from antinous import *
input = null
output = long
def action(input):
  emit(12)
""").head
    engineLong.emit = {x => x should be (java.lang.Long.valueOf(12))}
    engineLong.action(Py.None)
    engineLong.emit = {x => engineLong.jsonOutput(x) should be ("12")}
    engineLong.action(Py.None)
  }

  it must "output float" in {
    val engineFloat = JythonEngine.fromPython("""
from antinous import *
input = null
output = float
def action(input):
  emit(12)
  emit(12.0)
""").head
    engineFloat.emit = {x => x should be (java.lang.Float.valueOf(12))}
    engineFloat.action(Py.None)
    engineFloat.emit = {x => engineFloat.jsonOutput(x) should be ("12.0")}
    engineFloat.action(Py.None)
  }

  it must "output double" in {
    val engineDouble = JythonEngine.fromPython("""
from antinous import *
input = null
output = double
def action(input):
  emit(12)
  emit(12.0)
""").head
    engineDouble.emit = {x => x should be (java.lang.Double.valueOf(12))}
    engineDouble.action(Py.None)
    engineDouble.emit = {x => engineDouble.jsonOutput(x) should be ("12.0")}
    engineDouble.action(Py.None)
  }

  it must "output string" in {
    val engineString = JythonEngine.fromPython("""
from antinous import *
input = null
output = string
def action(input):
  emit("hello")
  emit(u"hello")
""").head
    engineString.emit = {x => x should be ("hello")}
    engineString.action(Py.None)
    engineString.emit = {x => engineString.jsonOutput(x) should be (""""hello"""")}
    engineString.action(Py.None)
  }

  it must "output bytes" in {
    val engineBytes = JythonEngine.fromPython("""
from antinous import *
input = null
output = bytes
def action(input):
  emit("hello")
  emit(u"hello")
""").head
    engineBytes.emit = {x => x.asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))}
    engineBytes.action(Py.None)
    engineBytes.emit = {x => engineBytes.jsonOutput(x) should be (""""hello"""")}
    engineBytes.action(Py.None)
  }

  it must "output array" in {
    val engineArray1 = JythonEngine.fromPython("""
from antinous import *
input = null
output = array(string)
def action(input):
  emit(["one", "two", "three"])
""").head
    engineArray1.emit = {x => x.asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three"))}
    engineArray1.action(Py.None)
    engineArray1.emit = {x => engineArray1.jsonOutput(x) should be ("""["one","two","three"]""")}
    engineArray1.action(Py.None)

    val engineArray2 = JythonEngine.fromPython("""
from antinous import *
input = null
output = array(int)
def action(input):
  emit([1, 2, 3])
""").head
    engineArray2.emit = {x => x.asInstanceOf[PFAArray[Int]].toVector should be (Vector(1, 2, 3))}
    engineArray2.action(Py.None)
    engineArray2.emit = {x => engineArray2.jsonOutput(x) should be ("""[1,2,3]""")}
    engineArray2.action(Py.None)
  }

  it must "output map" in {
    val engineMap1 = JythonEngine.fromPython("""
from antinous import *
input = null
output = map(string)
def action(input):
  emit({"one": "uno", "two": "dos", "three": "tres"})
""").head
    engineMap1.emit = {x => x.asInstanceOf[PFAMap[String]].toMap should be (Map("one" -> "uno", "two" -> "dos", "three" -> "tres"))}
    engineMap1.action(Py.None)
    engineMap1.emit = {x => engineMap1.jsonOutput(x) should be ("""{"one":"uno","two":"dos","three":"tres"}""")}
    engineMap1.action(Py.None)

    val engineMap2 = JythonEngine.fromPython("""
from antinous import *
input = null
output = map(int)
def action(input):
  emit({"one": 1, "two": 2, "three": 3})
""").head
    engineMap2.emit = {x => x.asInstanceOf[PFAMap[java.lang.Integer]].toMap should be (Map("one" -> java.lang.Integer.valueOf(1), "two" -> java.lang.Integer.valueOf(2), "three" -> java.lang.Integer.valueOf(3)))}
    engineMap2.action(Py.None)
    engineMap2.emit = {x => engineMap2.jsonOutput(x) should be ("""{"one":1,"three":3,"two":2}""")}
    engineMap2.action(Py.None)
  }

  it must "output fixed" in {
    val engineFixed1 = JythonEngine.fromPython("""
from antinous import *
input = null
output = fixed(5)
def action(input):
  emit("hello")
""").head
    engineFixed1.emit = {x => x.asInstanceOf[AnyPFAFixed].bytes should be (List(104, 101, 108, 108, 111))}
    engineFixed1.action(Py.None)
    engineFixed1.emit = {x => engineFixed1.jsonOutput(x) should be (""""hello"""")}
    engineFixed1.action(Py.None)

    val engineFixed2 = JythonEngine.fromPython("""
from antinous import *
input = fixed(5)
output = input
def action(input):
  emit(input)
""").head
    engineFixed2.emit = {x => x.asInstanceOf[AnyPFAFixed].bytes should be (List(104, 101, 108, 108, 111))}
    engineFixed2.action(engineFixed2.jsonInput(""""hello""""))
    engineFixed2.emit = {x => engineFixed2.jsonOutput(x) should be (""""hello"""")}
    engineFixed2.action(engineFixed2.jsonInput(""""hello""""))
  }

  it must "output enum" in {
    val engineEnum1 = JythonEngine.fromPython("""
from antinous import *
input = null
output = enum(["one", "two", "three"])
def action(input):
  emit("one")
""").head
    engineEnum1.emit = {x => x.asInstanceOf[AnyPFAEnumSymbol].toString should be ("one")}
    engineEnum1.action(Py.None)
    engineEnum1.emit = {x => engineEnum1.jsonOutput(x) should be (""""one"""")}
    engineEnum1.action(Py.None)

    val engineEnum2 = JythonEngine.fromPython("""
from antinous import *
input = enum(["one", "two", "three"])
output = input
def action(input):
  emit(input)
""").head
    engineEnum2.emit = {x => x.asInstanceOf[AnyPFAEnumSymbol].toString should be ("one")}
    engineEnum2.action(engineEnum2.jsonInput(""""one""""))
    engineEnum2.emit = {x => engineEnum2.jsonOutput(x) should be (""""one"""")}
    engineEnum2.action(engineEnum2.jsonInput(""""one""""))
  }

  it must "output record" in {
    val engineRecord1 = JythonEngine.fromPython("""
from antinous import *
input = null
output = record(one=int, two=double, three=string)
def action(input):
  emit({"one": 1, "two": 2.2, "three": "THREE"})
""").head
    engineRecord1.emit = {x => x.asInstanceOf[AnyPFARecord].get("one") should be (java.lang.Integer.valueOf(1))
                               x.asInstanceOf[AnyPFARecord].get("two") should be (java.lang.Double.valueOf(2.2))
                               x.asInstanceOf[AnyPFARecord].get("three") should be ("THREE")}
    engineRecord1.action(Py.None)
    engineRecord1.emit = {x => engineRecord1.jsonOutput(x) should be ("""{"one":1,"three":"THREE","two":2.2}""")}
    engineRecord1.action(Py.None)

    val engineRecord2 = JythonEngine.fromPython("""
from antinous import *
input = null
output = record(one=int, two=double, three=string)
class Stuff:
  def __init__(self, one, two, three):
    self.one = one
    self.two = two
    self.three = three
def action(input):
  emit(Stuff(1, 2.2, "THREE"))
""").head
    engineRecord2.emit = {x => x.asInstanceOf[AnyPFARecord].get("one") should be (java.lang.Integer.valueOf(1))
                               x.asInstanceOf[AnyPFARecord].get("two") should be (java.lang.Double.valueOf(2.2))
                               x.asInstanceOf[AnyPFARecord].get("three") should be ("THREE")}
    engineRecord2.action(Py.None)
    engineRecord2.emit = {x => engineRecord2.jsonOutput(x) should be ("""{"one":1,"three":"THREE","two":2.2}""")}
    engineRecord2.action(Py.None)

    val engineRecord3 = JythonEngine.fromPython("""
from antinous import *
input = null
output = record(one=int, two=double, three=string)
class Stuff(object):
  def __init__(self, one, two, three):
    self.one = one
    self.two = two
    self.three = three
def action(input):
  emit(Stuff(1, 2.2, "THREE"))
""").head
    engineRecord3.emit = {x => x.asInstanceOf[AnyPFARecord].get("one") should be (java.lang.Integer.valueOf(1))
                               x.asInstanceOf[AnyPFARecord].get("two") should be (java.lang.Double.valueOf(2.2))
                               x.asInstanceOf[AnyPFARecord].get("three") should be ("THREE")}
    engineRecord3.action(Py.None)
    engineRecord3.emit = {x => engineRecord3.jsonOutput(x) should be ("""{"one":1,"three":"THREE","two":2.2}""")}
    engineRecord3.action(Py.None)

    val engineRecord4 = JythonEngine.fromPython("""
from antinous import *
input = record(one=int, two=double, three=string)
output = input
def action(input):
  emit(input)
""").head
    engineRecord4.emit = {x => x.asInstanceOf[AnyPFARecord].get("one") should be (java.lang.Integer.valueOf(1))
                               x.asInstanceOf[AnyPFARecord].get("two") should be (java.lang.Double.valueOf(2.2))
                               x.asInstanceOf[AnyPFARecord].get("three") should be ("THREE")}
    engineRecord4.action(engineRecord4.jsonInput("""{"one": 1, "two": 2.2, "three": "THREE"}"""))
    engineRecord4.emit = {x => engineRecord4.jsonOutput(x) should be ("""{"one":1,"three":"THREE","two":2.2}""")}
    engineRecord4.action(engineRecord4.jsonInput("""{"one": 1, "two": 2.2, "three": "THREE"}"""))
  }

  it must "output union" in {
    val engineRecordUnion: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = boolean
output = union(null, record("MyRecord", one=int, two=double, three=string))
def action(input):
  if input:
    emit({"one": 1, "two": 2.2, "three": "THREE"})
  else:
    emit(None)
""").head
    engineRecordUnion.emit = {x => x.asInstanceOf[AnyPFARecord].get("one") should be (java.lang.Integer.valueOf(1))
                                   x.asInstanceOf[AnyPFARecord].get("two") should be (java.lang.Double.valueOf(2.2))
                                   x.asInstanceOf[AnyPFARecord].get("three") should be ("THREE")}
    engineRecordUnion.action(engineRecordUnion.jsonInput("true"))
    engineRecordUnion.emit = {x => engineRecordUnion.jsonOutput(x) should be ("""{"MyRecord":{"one":1,"three":"THREE","two":2.2}}""")}
    engineRecordUnion.action(engineRecordUnion.jsonInput("true"))

    engineRecordUnion.emit = {x => x should be (null)}
    engineRecordUnion.action(engineRecordUnion.jsonInput("false"))
    engineRecordUnion.emit = {x => engineRecordUnion.jsonOutput(x) should be ("""null""")}
    engineRecordUnion.action(engineRecordUnion.jsonInput("false"))

    val enginePrimitiveUnion: PFAEmitEngine[PyObject, AnyRef] = JythonEngine.fromPython("""
from antinous import *
input = boolean
output = union(int, string)
def action(input):
  if input:
    emit(12)
  else:
    emit("hey")
""").head
    enginePrimitiveUnion.emit = {x => x.asInstanceOf[java.lang.Integer] should be (java.lang.Integer.valueOf(12))}
    enginePrimitiveUnion.action(enginePrimitiveUnion.jsonInput("true"))
    enginePrimitiveUnion.emit = {x => enginePrimitiveUnion.jsonOutput(x) should be ("""{"int":12}""")}
    enginePrimitiveUnion.action(enginePrimitiveUnion.jsonInput("true"))

    enginePrimitiveUnion.emit = {x => x.asInstanceOf[String] should be ("hey")}
    enginePrimitiveUnion.action(enginePrimitiveUnion.jsonInput("false"))
    enginePrimitiveUnion.emit = {x => enginePrimitiveUnion.jsonOutput(x) should be ("""{"string":"hey"}""")}
    enginePrimitiveUnion.action(enginePrimitiveUnion.jsonInput("false"))
  }

  "JythonEngine" must "do a complex example" in {
    val avsc = new java.util.Scanner(getClass.getResourceAsStream("/resources/exoplanets.avsc")).useDelimiter("\\A").next
    val engine = JythonEngine.fromPython("""
from antinous import *
input = <<INPUT>>
output = array(double)

def handlePlanet(star, planet):
  if planet.radius is not None and \
     planet.mass is not None and \
     star.age is not None and \
     star.temp is not None and \
     star.dist is not None:
    emit([planet.radius, planet.mass, star.age, star.temp, star.dist])

def action(input):
  star = input
  for planet in star.planets:
    handlePlanet(star, planet)

""".replace("<<INPUT>>", avsc)).head

    val inputStream = engine.avroInputIterator(getClass.getResourceAsStream("/resources/exoplanets.avro"))
    while (inputStream.hasNext)
      engine.action(inputStream.next())
  }

  it must "restore state" in {
    val engine = JythonEngine.fromPython("""
from antinous import *
input = int
output = record(a=array(int), b=int, c=int)

cumulative = []
last = 999

def action(input):
  global last
  emit({"a": cumulative, "b": last, "c": input})
  cumulative.append(input)
  last = input
""").head

    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int]())
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (999)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (1)}
    engine.action(new PyInteger(1))
    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int](1))
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (1)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (2)}
    engine.action(new PyInteger(2))
    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int](1, 2))
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (2)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (3)}
    engine.action(new PyInteger(3))

    engine.revert()

    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int]())
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (999)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (4)}
    engine.action(new PyInteger(4))
    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int](4))
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (4)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (5)}
    engine.action(new PyInteger(5))
    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int](4, 5))
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (5)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (6)}
    engine.action(new PyInteger(6))

    engine.revert()

    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int]())
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (999)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (7)}
    engine.action(new PyInteger(7))
    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int](7))
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (7)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (8)}
    engine.action(new PyInteger(8))
    engine.emit = {x => x.asInstanceOf[AnyPFARecord].get("a").asInstanceOf[PFAArray[Int]].toVector should be (Vector[Int](7, 8))
                        x.asInstanceOf[AnyPFARecord].get("b").asInstanceOf[java.lang.Integer].intValue should be (8)
                        x.asInstanceOf[AnyPFARecord].get("c").asInstanceOf[java.lang.Integer].intValue should be (9)}
    engine.action(new PyInteger(9))
  }
}
