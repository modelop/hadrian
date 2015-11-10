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

package test.scala.signature

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
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
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion
import com.opendatagroup.hadrian.signature._
import test.scala._

@RunWith(classOf[JUnitRunner])
class SignatureSuite extends FlatSpec with Matchers {
  def dropsig(everything: Option[(Sig, Seq[Type], AvroType)]): Option[(Seq[Type], AvroType)] = everything map {case (x, y, z) => (y, z)}
  val version = PFAVersion(0, 7, 2)

  "signature-based pattern matchers" must "pass through exact parameter matches" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Null), P.Null).accepts(List(AvroNull()), version)) should be (Some(Seq(AvroNull()), AvroNull()))
    dropsig(Sig(List("x" -> P.Int), P.Null).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroInt()), AvroNull()))
    dropsig(Sig(List("x" -> P.Long), P.Null).accepts(List(AvroLong()), version)) should be (Some(Seq(AvroLong()), AvroNull()))
    dropsig(Sig(List("x" -> P.Float), P.Null).accepts(List(AvroFloat()), version)) should be (Some(Seq(AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Double), P.Null).accepts(List(AvroDouble()), version)) should be (Some(Seq(AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Bytes), P.Null).accepts(List(AvroBytes()), version)) should be (Some(Seq(AvroBytes()), AvroNull()))
    dropsig(Sig(List("x" -> P.String), P.Null).accepts(List(AvroString()), version)) should be (Some(Seq(AvroString()), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroInt())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroInt())), AvroNull()))
    dropsig(Sig(List("x" -> P.Union(List(P.Int))), P.Null).accepts(List(AvroUnion(List(AvroInt()))), version)) should be (Some(Seq(AvroUnion(List(AvroInt()))), AvroNull()))
    dropsig(Sig(List("x" -> P.Fixed(10)), P.Null).accepts(List(AvroFixed(10, "MyFixed")), version)) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroNull()))
    dropsig(Sig(List("x" -> P.Fixed(10, Some("MyFixed"))), P.Null).accepts(List(AvroFixed(10, "MyFixed")), version)) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroNull()))
    dropsig(Sig(List("x" -> P.Enum(List("one", "two", "three"))), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum")), version)) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroNull()))
    dropsig(Sig(List("x" -> P.Enum(List("one", "two", "three"), Some("MyEnum"))), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum")), version)) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroNull()))
    dropsig(Sig(List("x" -> P.Record(Map("one" -> P.Int))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroNull()))
    dropsig(Sig(List("x" -> P.Record(Map("one" -> P.Int), Some("MyRecord"))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroNull()))
  }

  it must "not match anti-patterns" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Null), P.Null).accepts(List(AvroInt()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Int), P.Null).accepts(List(AvroLong()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Long), P.Null).accepts(List(AvroFloat()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Float), P.Null).accepts(List(AvroDouble()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Double), P.Null).accepts(List(AvroBytes()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Bytes), P.Null).accepts(List(AvroString()), version)) should be (None)
    dropsig(Sig(List("x" -> P.String), P.Null).accepts(List(AvroArray(AvroInt())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroMap(AvroInt())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroUnion(List(AvroInt()))), version)) should be (None)
    dropsig(Sig(List("x" -> P.Union(List(P.Int))), P.Null).accepts(List(AvroFixed(10, "MyFixed")), version)) should be (None)
    dropsig(Sig(List("x" -> P.Fixed(10, Some("YourFixed"))), P.Null).accepts(List(AvroFixed(10, "MyFixed")), version)) should be (None)
    dropsig(Sig(List("x" -> P.Fixed(10)), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum")), version)) should be (None)
    dropsig(Sig(List("x" -> P.Enum(List("one", "two", "three"), Some("YourEnum"))), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum")), version)) should be (None)
    dropsig(Sig(List("x" -> P.Enum(List("one", "two", "three"))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (None)
    dropsig(Sig(List("x" -> P.Record(Map("one" -> P.Int), Some("YourRecord"))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (None)
    dropsig(Sig(List("x" -> P.Record(Map("one" -> P.Int))), P.Null).accepts(List(AvroNull()), version)) should be (None)
  }

  it must "promote numbers" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Int), P.Null).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroInt()), AvroNull()))
    dropsig(Sig(List("x" -> P.Int), P.Null).accepts(List(AvroLong()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Int), P.Null).accepts(List(AvroFloat()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Int), P.Null).accepts(List(AvroDouble()), version)) should be (None)

    dropsig(Sig(List("x" -> P.Long), P.Null).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroLong()), AvroNull()))
    dropsig(Sig(List("x" -> P.Long), P.Null).accepts(List(AvroLong()), version)) should be (Some(Seq(AvroLong()), AvroNull()))
    dropsig(Sig(List("x" -> P.Long), P.Null).accepts(List(AvroFloat()), version)) should be (None)
    dropsig(Sig(List("x" -> P.Long), P.Null).accepts(List(AvroDouble()), version)) should be (None)

    dropsig(Sig(List("x" -> P.Float), P.Null).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Float), P.Null).accepts(List(AvroLong()), version)) should be (Some(Seq(AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Float), P.Null).accepts(List(AvroFloat()), version)) should be (Some(Seq(AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Float), P.Null).accepts(List(AvroDouble()), version)) should be (None)

    dropsig(Sig(List("x" -> P.Double), P.Null).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Double), P.Null).accepts(List(AvroLong()), version)) should be (Some(Seq(AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Double), P.Null).accepts(List(AvroDouble()), version)) should be (Some(Seq(AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Double), P.Null).accepts(List(AvroDouble()), version)) should be (Some(Seq(AvroDouble()), AvroNull()))

    dropsig(Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroInt())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroLong())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroFloat())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroDouble())), version)) should be (None)

    dropsig(Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroLong())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroLong())), version)) should be (Some(Seq(AvroArray(AvroLong())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroFloat())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroDouble())), version)) should be (None)

    dropsig(Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroFloat())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroLong())), version)) should be (Some(Seq(AvroArray(AvroFloat())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroFloat())), version)) should be (Some(Seq(AvroArray(AvroFloat())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroDouble())), version)) should be (None)

    dropsig(Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroLong())), version)) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroDouble())), version)) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroDouble())), version)) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))

    dropsig(Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroInt())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroLong())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroFloat())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroDouble())), version)) should be (None)

    dropsig(Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroLong())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroLong())), version)) should be (Some(Seq(AvroMap(AvroLong())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroFloat())), version)) should be (None)
    dropsig(Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroDouble())), version)) should be (None)

    dropsig(Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroFloat())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroLong())), version)) should be (Some(Seq(AvroMap(AvroFloat())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroFloat())), version)) should be (Some(Seq(AvroMap(AvroFloat())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroDouble())), version)) should be (None)

    dropsig(Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroLong())), version)) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroDouble())), version)) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
    dropsig(Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroDouble())), version)) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
  }

  it must "loosely match function references" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Fcn(List(P.Long), P.Long)), P.Null).accepts(List(FcnType(List(AvroLong()), AvroLong())), version)) should be (Some(Seq(FcnType(List(AvroLong()), AvroLong())), AvroNull()))
    dropsig(Sig(List("x" -> P.Fcn(List(P.Long), P.Long)), P.Null).accepts(List(FcnType(List(AvroLong()), AvroInt())), version)) should be (Some(Seq(FcnType(List(AvroLong()), AvroInt())), AvroNull()))
    dropsig(Sig(List("x" -> P.Fcn(List(P.Long), P.Long)), P.Null).accepts(List(FcnType(List(AvroDouble()), AvroLong())), version)) should be (Some(Seq(FcnType(List(AvroDouble()), AvroLong())), AvroNull()))
  }

  it must "match wildcards" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroNull()), version)) should be (Some(Seq(AvroNull()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroInt()), AvroInt()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroLong()), version)) should be (Some(Seq(AvroLong()), AvroLong()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroFloat()), version)) should be (Some(Seq(AvroFloat()), AvroFloat()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroDouble()), version)) should be (Some(Seq(AvroDouble()), AvroDouble()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroBytes()), version)) should be (Some(Seq(AvroBytes()), AvroBytes()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroString()), version)) should be (Some(Seq(AvroString()), AvroString()))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroInt())), AvroArray(AvroInt())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroInt())), AvroMap(AvroInt())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroUnion(List(AvroInt()))), version)) should be (Some(Seq(AvroUnion(List(AvroInt()))), AvroUnion(List(AvroInt()))))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroFixed(10, "MyFixed")), version)) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroFixed(10, "MyFixed")))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum")), version)) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroEnum(List("one", "two", "three"), "MyEnum")))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))
  }

  it must "match nested wildcards" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroNull())), version)) should be (Some(Seq(AvroArray(AvroNull())), AvroNull()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroInt())), AvroInt()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroLong())), version)) should be (Some(Seq(AvroArray(AvroLong())), AvroLong()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroFloat())), version)) should be (Some(Seq(AvroArray(AvroFloat())), AvroFloat()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroDouble())), version)) should be (Some(Seq(AvroArray(AvroDouble())), AvroDouble()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroBytes())), version)) should be (Some(Seq(AvroArray(AvroBytes())), AvroBytes()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroString())), version)) should be (Some(Seq(AvroArray(AvroString())), AvroString()))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroArray(AvroInt()))), version)) should be (Some(Seq(AvroArray(AvroArray(AvroInt()))), AvroArray(AvroInt())))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroMap(AvroInt()))), version)) should be (Some(Seq(AvroArray(AvroMap(AvroInt()))), AvroMap(AvroInt())))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroUnion(List(AvroInt())))), version)) should be (Some(Seq(AvroArray(AvroUnion(List(AvroInt())))), AvroUnion(List(AvroInt()))))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroFixed(10, "MyFixed"))), version)) should be (Some(Seq(AvroArray(AvroFixed(10, "MyFixed"))), AvroFixed(10, "MyFixed")))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroEnum(List("one", "two", "three"), "MyEnum"))), version)) should be (Some(Seq(AvroArray(AvroEnum(List("one", "two", "three"), "MyEnum"))), AvroEnum(List("one", "two", "three"), "MyEnum")))
    dropsig(Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))), version)) should be (Some(Seq(AvroArray(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))

    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroNull()), version)) should be (Some(Seq(AvroNull()), AvroArray(AvroNull())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroInt()), version)) should be (Some(Seq(AvroInt()), AvroArray(AvroInt())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroLong()), version)) should be (Some(Seq(AvroLong()), AvroArray(AvroLong())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroFloat()), version)) should be (Some(Seq(AvroFloat()), AvroArray(AvroFloat())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroDouble()), version)) should be (Some(Seq(AvroDouble()), AvroArray(AvroDouble())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroBytes()), version)) should be (Some(Seq(AvroBytes()), AvroArray(AvroBytes())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroString()), version)) should be (Some(Seq(AvroString()), AvroArray(AvroString())))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroArray(AvroInt())), version)) should be (Some(Seq(AvroArray(AvroInt())), AvroArray(AvroArray(AvroInt()))))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroMap(AvroInt())), version)) should be (Some(Seq(AvroMap(AvroInt())), AvroArray(AvroMap(AvroInt()))))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroUnion(List(AvroInt()))), version)) should be (Some(Seq(AvroUnion(List(AvroInt()))), AvroArray(AvroUnion(List(AvroInt())))))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroFixed(10, "MyFixed")), version)) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroArray(AvroFixed(10, "MyFixed"))))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum")), version)) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroArray(AvroEnum(List("one", "two", "three"), "MyEnum"))))
    dropsig(Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroArray(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))))
  }

  it must "use wildcards to normalize numerical types" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroInt()), version)) should be (Some(Seq(AvroInt(), AvroInt()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroLong()), version)) should be (Some(Seq(AvroLong(), AvroLong()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroFloat()), version)) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroDouble()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))

    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroInt()), version)) should be (Some(Seq(AvroLong(), AvroLong()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroLong()), version)) should be (Some(Seq(AvroLong(), AvroLong()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroFloat()), version)) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroDouble()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))

    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroInt()), version)) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroLong()), version)) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroFloat()), version)) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroDouble()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))

    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroInt()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroLong()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroFloat()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
    dropsig(Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroDouble()), version)) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
  }

  it must "match wild records" taggedAs(SignatureMatch) in {
    dropsig(Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Int))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), version)) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))
    dropsig(Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Int))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")), version)) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")), AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")))
    dropsig(Sig(List("x" -> P.WildRecord("A", Map("one" -> P.String))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")), version)) should be (None)
    dropsig(Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Double))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")), version)) should be (None)
    dropsig(Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Int))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("uno", AvroInt()), AvroField("two", AvroString())), "MyRecord")), version)) should be (None)
  }

}
