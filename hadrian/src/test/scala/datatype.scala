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

package test.scala.datatype

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.map.ObjectMapper
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder
import org.apache.avro.Schema

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import test.scala._

@RunWith(classOf[JUnitRunner])
class DataTypeSuite extends FlatSpec with Matchers {
  val objectMapper = new ObjectMapper

  "Avro type comparison" must "promote numbers int -> long -> float -> double" taggedAs(DataType) in {
    AvroInt().accepts(AvroInt()) should be (true)
    AvroLong().accepts(AvroInt()) should be (true)
    AvroFloat().accepts(AvroInt()) should be (true)
    AvroDouble().accepts(AvroInt()) should be (true)

    AvroLong().accepts(AvroLong()) should be (true)
    AvroFloat().accepts(AvroLong()) should be (true)
    AvroDouble().accepts(AvroLong()) should be (true)

    AvroFloat().accepts(AvroFloat()) should be (true)
    AvroDouble().accepts(AvroFloat()) should be (true)

    AvroDouble().accepts(AvroDouble()) should be (true)

    AvroInt().accepts(AvroLong()) should be (false)
    AvroInt().accepts(AvroFloat()) should be (false)
    AvroInt().accepts(AvroDouble()) should be (false)

    AvroLong().accepts(AvroFloat()) should be (false)
    AvroLong().accepts(AvroDouble()) should be (false)

    AvroFloat().accepts(AvroDouble()) should be (false)
  }

  it must "promote numbers in type-variant array" taggedAs(DataType) in {
    AvroArray(AvroInt()).accepts(AvroArray(AvroInt())) should be (true)
    AvroArray(AvroLong()).accepts(AvroArray(AvroInt())) should be (true)
    AvroArray(AvroFloat()).accepts(AvroArray(AvroInt())) should be (true)
    AvroArray(AvroDouble()).accepts(AvroArray(AvroInt())) should be (true)

    AvroArray(AvroLong()).accepts(AvroArray(AvroLong())) should be (true)
    AvroArray(AvroFloat()).accepts(AvroArray(AvroLong())) should be (true)
    AvroArray(AvroDouble()).accepts(AvroArray(AvroLong())) should be (true)

    AvroArray(AvroFloat()).accepts(AvroArray(AvroFloat())) should be (true)
    AvroArray(AvroDouble()).accepts(AvroArray(AvroFloat())) should be (true)

    AvroArray(AvroDouble()).accepts(AvroArray(AvroDouble())) should be (true)

    AvroArray(AvroInt()).accepts(AvroArray(AvroLong())) should be (false)
    AvroArray(AvroInt()).accepts(AvroArray(AvroFloat())) should be (false)
    AvroArray(AvroInt()).accepts(AvroArray(AvroDouble())) should be (false)

    AvroArray(AvroLong()).accepts(AvroArray(AvroFloat())) should be (false)
    AvroArray(AvroLong()).accepts(AvroArray(AvroDouble())) should be (false)

    AvroArray(AvroFloat()).accepts(AvroArray(AvroDouble())) should be (false)
  }

  it must "promote numbers in type-variant map" taggedAs(DataType) in {
    AvroMap(AvroInt()).accepts(AvroMap(AvroInt())) should be (true)
    AvroMap(AvroLong()).accepts(AvroMap(AvroInt())) should be (true)
    AvroMap(AvroFloat()).accepts(AvroMap(AvroInt())) should be (true)
    AvroMap(AvroDouble()).accepts(AvroMap(AvroInt())) should be (true)

    AvroMap(AvroLong()).accepts(AvroMap(AvroLong())) should be (true)
    AvroMap(AvroFloat()).accepts(AvroMap(AvroLong())) should be (true)
    AvroMap(AvroDouble()).accepts(AvroMap(AvroLong())) should be (true)

    AvroMap(AvroFloat()).accepts(AvroMap(AvroFloat())) should be (true)
    AvroMap(AvroDouble()).accepts(AvroMap(AvroFloat())) should be (true)

    AvroMap(AvroDouble()).accepts(AvroMap(AvroDouble())) should be (true)

    AvroMap(AvroInt()).accepts(AvroMap(AvroLong())) should be (false)
    AvroMap(AvroInt()).accepts(AvroMap(AvroFloat())) should be (false)
    AvroMap(AvroInt()).accepts(AvroMap(AvroDouble())) should be (false)

    AvroMap(AvroLong()).accepts(AvroMap(AvroFloat())) should be (false)
    AvroMap(AvroLong()).accepts(AvroMap(AvroDouble())) should be (false)

    AvroMap(AvroFloat()).accepts(AvroMap(AvroDouble())) should be (false)
  }

  it must "promote numbers in record" taggedAs(DataType) in {
    AvroRecord(List(AvroField("one", AvroInt())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroInt())), name = "One")) should be (true)
    AvroRecord(List(AvroField("one", AvroLong())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroInt())), name = "One")) should be (true)
    AvroRecord(List(AvroField("one", AvroFloat())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroInt())), name = "One")) should be (true)
    AvroRecord(List(AvroField("one", AvroDouble())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroInt())), name = "One")) should be (true)

    AvroRecord(List(AvroField("one", AvroLong())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroLong())), name = "One")) should be (true)
    AvroRecord(List(AvroField("one", AvroFloat())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroLong())), name = "One")) should be (true)
    AvroRecord(List(AvroField("one", AvroDouble())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroLong())), name = "One")) should be (true)

    AvroRecord(List(AvroField("one", AvroFloat())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroFloat())), name = "One")) should be (true)
    AvroRecord(List(AvroField("one", AvroDouble())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroFloat())), name = "One")) should be (true)

    AvroRecord(List(AvroField("one", AvroDouble())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroDouble())), name = "One")) should be (true)

    AvroRecord(List(AvroField("one", AvroInt())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroLong())), name = "One")) should be (false)
    AvroRecord(List(AvroField("one", AvroInt())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroFloat())), name = "One")) should be (false)
    AvroRecord(List(AvroField("one", AvroInt())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroDouble())), name = "One")) should be (false)

    AvroRecord(List(AvroField("one", AvroLong())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroFloat())), name = "One")) should be (false)
    AvroRecord(List(AvroField("one", AvroLong())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroDouble())), name = "One")) should be (false)

    AvroRecord(List(AvroField("one", AvroFloat())), name = "One").accepts(AvroRecord(List(AvroField("one", AvroDouble())), name = "One")) should be (false)
  }

  it must "have a type-safe null" taggedAs(DataType) in {
    AvroNull().accepts(AvroString()) should be (false)
    AvroString().accepts(AvroNull()) should be (false)
  }

  it must "distinguish boolean from numbers" taggedAs(DataType) in {
    AvroBoolean().accepts(AvroInt()) should be (false)
    AvroBoolean().accepts(AvroLong()) should be (false)
    AvroBoolean().accepts(AvroFloat()) should be (false)
    AvroBoolean().accepts(AvroDouble()) should be (false)

    AvroInt().accepts(AvroBoolean()) should be (false)
    AvroLong().accepts(AvroBoolean()) should be (false)
    AvroFloat().accepts(AvroBoolean()) should be (false)
    AvroDouble().accepts(AvroBoolean()) should be (false)
  }

  it must "distinguish bytes, string, enum, fixed" taggedAs(DataType) in {
    AvroBytes().accepts(AvroBytes()) should be (true)
    AvroBytes().accepts(AvroString()) should be (false)
    AvroBytes().accepts(AvroEnum(List("one", "two", "three"), name = "One")) should be (false)
    AvroBytes().accepts(AvroFixed(5, name = "One")) should be (false)

    AvroString().accepts(AvroBytes()) should be (false)
    AvroString().accepts(AvroString()) should be (true)
    AvroString().accepts(AvroEnum(List("one", "two", "three"), name = "One")) should be (false)
    AvroString().accepts(AvroFixed(5, name = "One")) should be (false)

    AvroEnum(List("one", "two", "three"), name = "One").accepts(AvroBytes()) should be (false)
    AvroEnum(List("one", "two", "three"), name = "One").accepts(AvroString()) should be (false)
    AvroEnum(List("one", "two", "three"), name = "One").accepts(AvroEnum(List("one", "two", "three"), name = "One")) should be (true)
    AvroEnum(List("one", "two", "three"), name = "One").accepts(AvroFixed(5, name = "One")) should be (false)

    AvroFixed(5, name = "One").accepts(AvroBytes()) should be (false)
    AvroFixed(5, name = "One").accepts(AvroString()) should be (false)
    AvroFixed(5, name = "One").accepts(AvroEnum(List("one", "two", "three"), name = "One")) should be (false)
    AvroFixed(5, name = "One").accepts(AvroFixed(5, name = "One")) should be (true)
  }

  it must "resolve unions" taggedAs(DataType) in {
    AvroUnion(List(AvroInt(), AvroString())).accepts(AvroInt()) should be (true)
    AvroUnion(List(AvroInt(), AvroString())).accepts(AvroString()) should be (true)
    AvroUnion(List(AvroInt(), AvroString())).accepts(AvroDouble()) should be (false)

    AvroUnion(List(AvroInt(), AvroString(), AvroNull())).accepts(AvroUnion(List(AvroInt(), AvroString()))) should be (true)
    AvroUnion(List(AvroInt(), AvroString(), AvroNull())).accepts(AvroUnion(List(AvroInt(), AvroNull()))) should be (true)
  }

  def testParser(input: String, expected: AvroType): Unit =
    (new ForwardDeclarationParser).parse(List(input)) should be (Map(input -> expected))

  "Avro JSON schema" must "read simple types" taggedAs(DataType) in {
    testParser(""""null"""", AvroNull())
    testParser(""""boolean"""", AvroBoolean())
    testParser(""""int"""", AvroInt())
    testParser(""""long"""", AvroLong())
    testParser(""""float"""", AvroFloat())
    testParser(""""double"""", AvroDouble())
    testParser(""""bytes"""", AvroBytes())
    testParser("""{"type": "fixed", "name": "Test", "size": 5}""", AvroFixed(5, name = "Test"))
    testParser(""""string"""", AvroString())
    testParser("""{"type": "enum", "name": "Test", "symbols": ["one", "two", "three"]}""", AvroEnum(List("one", "two", "three"), name = "Test"))
    testParser("""{"type": "array", "items": "int"}""", AvroArray(AvroInt()))
    testParser("""{"type": "map", "values": "int"}""", AvroMap(AvroInt()))
    testParser("""{"type": "record", "name": "Test", "fields": [{"name": "one", "type": "int"}]}""", AvroRecord(List(AvroField("one", AvroInt())), name = "Test"))
    testParser("""["string", "null"]""", AvroUnion(List(AvroString(), AvroNull())))
  }

  it must "identify nested record definitions" taggedAs(DataType) in {
    testParser(
      """{"type": "record", "name": "Outer", "fields": [{"name": "child", "type": {"type": "record", "name": "Inner", "fields": [{"name": "child", "type": "int"}]}}]}""",
      AvroRecord(List(AvroField("child", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner"))), name = "Outer"))
  }

  it must "resolve dependent records in any order" taggedAs(DataType) in {
    val record1 = """{"type": "record", "name": "Outer", "fields": [{"name": "child", "type": "Inner"}]}"""
    val record2 = """{"type": "record", "name": "Inner", "fields": [{"name": "child", "type": "int"}]}"""

    (new ForwardDeclarationParser).parse(List(record1, record2)) should be (Map(
      record1 -> AvroRecord(List(AvroField("child", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner"))), name = "Outer"),
      record2 -> AvroRecord(List(AvroField("child", AvroInt())), name = "Inner")
    ))

    (new ForwardDeclarationParser).parse(List(record2, record1)) should be (Map(
      record1 -> AvroRecord(List(AvroField("child", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner"))), name = "Outer"),
      record2 -> AvroRecord(List(AvroField("child", AvroInt())), name = "Inner")
    ))
  }

  it must "resolve dependent records with namespaces" taggedAs(DataType) in {
    val record1 = """{"type": "record", "name": "Outer", "namespace": "com.wowie", "fields": [{"name": "child", "type": "Inner"}]}"""
    val record2 = """{"type": "record", "name": "Inner", "namespace": "com.wowie", "fields": [{"name": "child", "type": "int"}]}"""

    (new ForwardDeclarationParser).parse(List(record1, record2)) should be (Map(
      record1 -> AvroRecord(List(AvroField("child", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner", namespace = Some("com.wowie")))), name = "Outer", namespace = Some("com.wowie")),
      record2 -> AvroRecord(List(AvroField("child", AvroInt())), name = "Inner", namespace = Some("com.wowie"))
    ))

    (new ForwardDeclarationParser).parse(List(record2, record1)) should be (Map(
      record1 -> AvroRecord(List(AvroField("child", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner", namespace = Some("com.wowie")))), name = "Outer", namespace = Some("com.wowie")),
      record2 -> AvroRecord(List(AvroField("child", AvroInt())), name = "Inner", namespace = Some("com.wowie"))
    ))
  }

  it must "resolve recursively defined records" taggedAs(DataType) in {
    val input = """{"type": "record", "name": "Recursive", "fields": [{"name": "child", "type": ["null", "Recursive"]}]}"""
    (new ForwardDeclarationParser).parse(List(input))(input) match {
      case x: AvroRecord => x.field("child").avroType should be (AvroUnion(List(AvroNull(), x)))
      case _ => "non-record" should be ("record")
    }
  }

  it must "be re-callable and accumulate types" taggedAs(DataType) in {
    val forwardDeclarationParser = new ForwardDeclarationParser

    val record1 = """{"type": "record", "name": "Outer", "fields": [{"name": "child", "type": "Inner"}]}"""
    val record2 = """{"type": "record", "name": "Inner", "fields": [{"name": "child", "type": "int"}]}"""
    val record3 = """{"type": "record", "name": "Another", "fields": [{"name": "innerChild", "type": "Inner"}]}"""

    val type1 = AvroRecord(List(AvroField("child", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner"))), name = "Outer")
    val type2 = AvroRecord(List(AvroField("child", AvroInt())), name = "Inner")
    val type3 = AvroRecord(List(AvroField("innerChild", AvroRecord(List(AvroField("child", AvroInt())), name = "Inner"))), name = "Another")

    forwardDeclarationParser.parse(List(record1, record2)) should be (Map(record1 -> type1, record2 -> type2))
    forwardDeclarationParser.parse(List(record3)) should be (Map(record3 -> type3))

    forwardDeclarationParser.getAvroType("Outer") should be (Some(type1))
    forwardDeclarationParser.getAvroType("Inner") should be (Some(type2))
    forwardDeclarationParser.getAvroType("Another") should be (Some(type3))
    forwardDeclarationParser.getAvroType(""""Outer"""") should be (Some(type1))
    forwardDeclarationParser.getAvroType("""{"type": "Outer"}""") should be (Some(type1))
    forwardDeclarationParser.getAvroType("""{"type": "array", "items": "Outer"}""") should be (Some(AvroArray(type1)))
    forwardDeclarationParser.getAvroType("""{"type": "array", "items": "int"}""") should be (Some(AvroArray(AvroInt())))
  }

  "Avro type matching" must "work in pattern matching" taggedAs(DataType) in {
    val avroNull = AvroNull()
    val avroBoolean = AvroBoolean()
    val avroInt = AvroInt()
    val avroLong = AvroLong()
    val avroFloat = AvroFloat()
    val avroDouble = AvroDouble()
    val avroBytes = AvroBytes()
    val avroFixed = AvroFixed(10, "fixed", Some("namespace"), Set("one", "two", "three"), "doc")
    val avroString = AvroString()
    val avroEnum = AvroEnum(List("A", "B", "C"), "enum", Some("namespace"), Set("one", "two", "three"), "doc")
    val avroArray1 = AvroArray(AvroInt())
    val avroArray2 = AvroArray(AvroEnum(List("A", "B", "C")))
    val avroArray3 = AvroArray(AvroArray(AvroString()))
    val avroMap1 = AvroMap(AvroInt())
    val avroMap2 = AvroMap(AvroEnum(List("A", "B", "C")))
    val avroMap3 = AvroMap(AvroMap(AvroString()))
    val avroRecord = AvroRecord(List(AvroField("i", AvroInt()), AvroField("j", AvroDouble()), AvroField("k", AvroString())), "record")
    val avroUnion1 = AvroUnion(List(avroNull, avroInt, avroString))
    val avroUnion2 = AvroUnion(List(avroString, avroArray1, avroMap2))
    val avroUnion3 = AvroUnion(List(avroRecord))

    (avroNull match {
      case AvroNull() => "null"
      case _ => ""
    }) should be ("null")

    (avroBoolean match {
      case AvroBoolean() => "boolean"
      case _ => ""
    }) should be ("boolean")

    (avroInt match {
      case AvroInt() => "int"
      case _ => ""
    }) should be ("int")

    (avroLong match {
      case AvroLong() => "long"
      case _ => ""
    }) should be ("long")

    (avroFloat match {
      case AvroFloat() => "float"
      case _ => ""
    }) should be ("float")

    (avroDouble match {
      case AvroDouble() => "double"
      case _ => ""
    }) should be ("double")

    (avroBytes match {
      case AvroBytes() => "bytes"
      case _ => ""
    }) should be ("bytes")

    (avroFixed match {
      case AvroFixed(a, b, c, d, e) => (a, b, c, d, e)
      case _ => ()
    }) should be (10, "fixed", Some("namespace"), Set("namespace.one", "namespace.two", "namespace.three"), "doc")

    (avroString match {
      case AvroString() => "string"
      case _ => ""
    }) should be ("string")

    (avroEnum match {
      case AvroEnum(a1 :: a2 :: a3 :: Nil, b, c, d, e) => (List(a1, a2, a3), b, c, d, e)
      case _ => ()
    }) should be (List("A", "B", "C"), "enum", Some("namespace"), Set("namespace.one", "namespace.two", "namespace.three"), "doc")

    (avroArray1 match {
      case AvroArray(AvroInt()) => "array1"
      case _ => ""
    }) should be ("array1")

    (avroArray2 match {
      case AvroArray(AvroEnum(a1 :: a2 :: a3 :: Nil, _, _, _, _)) => ("array2", List(a1, a2, a3))
      case _ => ""
    }) should be ("array2", (List("A", "B", "C")))

    (avroArray3 match {
      case AvroArray(AvroArray(AvroString())) => "array3"
      case _ => ""
    }) should be ("array3")

    (avroMap1 match {
      case AvroMap(AvroInt()) => "map1"
      case _ => ""
    }) should be ("map1")

    (avroMap2 match {
      case AvroMap(AvroEnum(a1 :: a2 :: a3 :: Nil, _, _, _, _)) => ("map2", List(a1, a2, a3))
      case _ => ""
    }) should be (("map2", List("A", "B", "C")))

    (avroMap3 match {
      case AvroMap(AvroMap(AvroString())) => "map3"
      case _ => ""
    }) should be ("map3")

    (avroRecord match {
      case AvroRecord(AvroField("i", AvroInt(), _, _, _, _) :: AvroField("j", AvroDouble(), _, _, _, _) :: AvroField("k", AvroString(), _, _, _, _) :: Nil, "record", _, _, _) => "record"
      case _ => ""
    }) should be ("record")

    (avroUnion1 match {
      case AvroUnion(AvroNull() :: AvroInt() :: AvroString() :: Nil) => "union1"
      case _ => ""
    }) should be ("union1")

    (avroUnion2 match {
      case AvroUnion(AvroString() :: AvroArray(AvroInt()) :: AvroMap(AvroEnum(a1 :: a2 :: a3 :: Nil, _, _, _, _)) :: Nil) => "union2"
      case _ => ""
    }) should be ("union2")

    (avroUnion3 match {
      case AvroUnion(AvroRecord(AvroField("i", AvroInt(), _, _, _, _) :: AvroField("j", AvroDouble(), _, _, _, _) :: AvroField("k", AvroString(), _, _, _, _) :: Nil, "record", _, _, _) :: Nil) => "union3"
      case _ => ""
    }) should be ("union3")

  }
}
