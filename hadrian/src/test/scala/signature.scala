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
  "signature-based pattern matchers" must "pass through exact parameter matches" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Null), P.Null).accepts(List(AvroNull())) should be (Some(Seq(AvroNull()), AvroNull()))
    Sig(List("x" -> P.Int), P.Null).accepts(List(AvroInt())) should be (Some(Seq(AvroInt()), AvroNull()))
    Sig(List("x" -> P.Long), P.Null).accepts(List(AvroLong())) should be (Some(Seq(AvroLong()), AvroNull()))
    Sig(List("x" -> P.Float), P.Null).accepts(List(AvroFloat())) should be (Some(Seq(AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Double), P.Null).accepts(List(AvroDouble())) should be (Some(Seq(AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Bytes), P.Null).accepts(List(AvroBytes())) should be (Some(Seq(AvroBytes()), AvroNull()))
    Sig(List("x" -> P.String), P.Null).accepts(List(AvroString())) should be (Some(Seq(AvroString()), AvroNull()))
    Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroInt())), AvroNull()))
    Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroInt())), AvroNull()))
    Sig(List("x" -> P.Union(List(P.Int))), P.Null).accepts(List(AvroUnion(List(AvroInt())))) should be (Some(Seq(AvroUnion(List(AvroInt()))), AvroNull()))
    Sig(List("x" -> P.Fixed(10)), P.Null).accepts(List(AvroFixed(10, "MyFixed"))) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroNull()))
    Sig(List("x" -> P.Fixed(10, Some("MyFixed"))), P.Null).accepts(List(AvroFixed(10, "MyFixed"))) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroNull()))
    Sig(List("x" -> P.Enum(List("one", "two", "three"))), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum"))) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroNull()))
    Sig(List("x" -> P.Enum(List("one", "two", "three"), Some("MyEnum"))), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum"))) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroNull()))
    Sig(List("x" -> P.Record(Map("one" -> P.Int))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroNull()))
    Sig(List("x" -> P.Record(Map("one" -> P.Int), Some("MyRecord"))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroNull()))
  }

  it must "not match anti-patterns" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Null), P.Null).accepts(List(AvroInt())) should be (None)
    Sig(List("x" -> P.Int), P.Null).accepts(List(AvroLong())) should be (None)
    Sig(List("x" -> P.Long), P.Null).accepts(List(AvroFloat())) should be (None)
    Sig(List("x" -> P.Float), P.Null).accepts(List(AvroDouble())) should be (None)
    Sig(List("x" -> P.Double), P.Null).accepts(List(AvroBytes())) should be (None)
    Sig(List("x" -> P.Bytes), P.Null).accepts(List(AvroString())) should be (None)
    Sig(List("x" -> P.String), P.Null).accepts(List(AvroArray(AvroInt()))) should be (None)
    Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroMap(AvroInt()))) should be (None)
    Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroUnion(List(AvroInt())))) should be (None)
    Sig(List("x" -> P.Union(List(P.Int))), P.Null).accepts(List(AvroFixed(10, "MyFixed"))) should be (None)
    Sig(List("x" -> P.Fixed(10, Some("YourFixed"))), P.Null).accepts(List(AvroFixed(10, "MyFixed"))) should be (None)
    Sig(List("x" -> P.Fixed(10)), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum"))) should be (None)
    Sig(List("x" -> P.Enum(List("one", "two", "three"), Some("YourEnum"))), P.Null).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum"))) should be (None)
    Sig(List("x" -> P.Enum(List("one", "two", "three"))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (None)
    Sig(List("x" -> P.Record(Map("one" -> P.Int), Some("YourRecord"))), P.Null).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (None)
    Sig(List("x" -> P.Record(Map("one" -> P.Int))), P.Null).accepts(List(AvroNull())) should be (None)
  }

  it must "promote numbers" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Int), P.Null).accepts(List(AvroInt())) should be (Some(Seq(AvroInt()), AvroNull()))
    Sig(List("x" -> P.Int), P.Null).accepts(List(AvroLong())) should be (None)
    Sig(List("x" -> P.Int), P.Null).accepts(List(AvroFloat())) should be (None)
    Sig(List("x" -> P.Int), P.Null).accepts(List(AvroDouble())) should be (None)

    Sig(List("x" -> P.Long), P.Null).accepts(List(AvroInt())) should be (Some(Seq(AvroLong()), AvroNull()))
    Sig(List("x" -> P.Long), P.Null).accepts(List(AvroLong())) should be (Some(Seq(AvroLong()), AvroNull()))
    Sig(List("x" -> P.Long), P.Null).accepts(List(AvroFloat())) should be (None)
    Sig(List("x" -> P.Long), P.Null).accepts(List(AvroDouble())) should be (None)

    Sig(List("x" -> P.Float), P.Null).accepts(List(AvroInt())) should be (Some(Seq(AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Float), P.Null).accepts(List(AvroLong())) should be (Some(Seq(AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Float), P.Null).accepts(List(AvroFloat())) should be (Some(Seq(AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Float), P.Null).accepts(List(AvroDouble())) should be (None)

    Sig(List("x" -> P.Double), P.Null).accepts(List(AvroInt())) should be (Some(Seq(AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Double), P.Null).accepts(List(AvroLong())) should be (Some(Seq(AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Double), P.Null).accepts(List(AvroDouble())) should be (Some(Seq(AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Double), P.Null).accepts(List(AvroDouble())) should be (Some(Seq(AvroDouble()), AvroNull()))

    Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroInt())), AvroNull()))
    Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroLong()))) should be (None)
    Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroFloat()))) should be (None)
    Sig(List("x" -> P.Array(P.Int)), P.Null).accepts(List(AvroArray(AvroDouble()))) should be (None)

    Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroLong())), AvroNull()))
    Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroLong()))) should be (Some(Seq(AvroArray(AvroLong())), AvroNull()))
    Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroFloat()))) should be (None)
    Sig(List("x" -> P.Array(P.Long)), P.Null).accepts(List(AvroArray(AvroDouble()))) should be (None)

    Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroFloat())), AvroNull()))
    Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroLong()))) should be (Some(Seq(AvroArray(AvroFloat())), AvroNull()))
    Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroFloat()))) should be (Some(Seq(AvroArray(AvroFloat())), AvroNull()))
    Sig(List("x" -> P.Array(P.Float)), P.Null).accepts(List(AvroArray(AvroDouble()))) should be (None)

    Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))
    Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroLong()))) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))
    Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroDouble()))) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))
    Sig(List("x" -> P.Array(P.Double)), P.Null).accepts(List(AvroArray(AvroDouble()))) should be (Some(Seq(AvroArray(AvroDouble())), AvroNull()))

    Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroInt())), AvroNull()))
    Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroLong()))) should be (None)
    Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroFloat()))) should be (None)
    Sig(List("x" -> P.Map(P.Int)), P.Null).accepts(List(AvroMap(AvroDouble()))) should be (None)

    Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroLong())), AvroNull()))
    Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroLong()))) should be (Some(Seq(AvroMap(AvroLong())), AvroNull()))
    Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroFloat()))) should be (None)
    Sig(List("x" -> P.Map(P.Long)), P.Null).accepts(List(AvroMap(AvroDouble()))) should be (None)

    Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroFloat())), AvroNull()))
    Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroLong()))) should be (Some(Seq(AvroMap(AvroFloat())), AvroNull()))
    Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroFloat()))) should be (Some(Seq(AvroMap(AvroFloat())), AvroNull()))
    Sig(List("x" -> P.Map(P.Float)), P.Null).accepts(List(AvroMap(AvroDouble()))) should be (None)

    Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
    Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroLong()))) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
    Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroDouble()))) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
    Sig(List("x" -> P.Map(P.Double)), P.Null).accepts(List(AvroMap(AvroDouble()))) should be (Some(Seq(AvroMap(AvroDouble())), AvroNull()))
  }

  it must "loosely match function references" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Fcn(List(P.Long), P.Long)), P.Null).accepts(List(FcnType(List(AvroLong()), AvroLong()))) should be (Some(Seq(FcnType(List(AvroLong()), AvroLong())), AvroNull()))
    Sig(List("x" -> P.Fcn(List(P.Long), P.Long)), P.Null).accepts(List(FcnType(List(AvroLong()), AvroInt()))) should be (Some(Seq(FcnType(List(AvroLong()), AvroInt())), AvroNull()))
    Sig(List("x" -> P.Fcn(List(P.Long), P.Long)), P.Null).accepts(List(FcnType(List(AvroDouble()), AvroLong()))) should be (Some(Seq(FcnType(List(AvroDouble()), AvroLong())), AvroNull()))
  }

  it must "match wildcards" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroNull())) should be (Some(Seq(AvroNull()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroInt())) should be (Some(Seq(AvroInt()), AvroInt()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroLong())) should be (Some(Seq(AvroLong()), AvroLong()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroFloat())) should be (Some(Seq(AvroFloat()), AvroFloat()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroDouble())) should be (Some(Seq(AvroDouble()), AvroDouble()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroBytes())) should be (Some(Seq(AvroBytes()), AvroBytes()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroString())) should be (Some(Seq(AvroString()), AvroString()))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroInt())), AvroArray(AvroInt())))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroInt())), AvroMap(AvroInt())))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroUnion(List(AvroInt())))) should be (Some(Seq(AvroUnion(List(AvroInt()))), AvroUnion(List(AvroInt()))))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroFixed(10, "MyFixed"))) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroFixed(10, "MyFixed")))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum"))) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroEnum(List("one", "two", "three"), "MyEnum")))
    Sig(List("x" -> P.Wildcard("A")), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))
  }

  it must "match nested wildcards" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroNull()))) should be (Some(Seq(AvroArray(AvroNull())), AvroNull()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroInt())), AvroInt()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroLong()))) should be (Some(Seq(AvroArray(AvroLong())), AvroLong()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroFloat()))) should be (Some(Seq(AvroArray(AvroFloat())), AvroFloat()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroDouble()))) should be (Some(Seq(AvroArray(AvroDouble())), AvroDouble()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroBytes()))) should be (Some(Seq(AvroArray(AvroBytes())), AvroBytes()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroString()))) should be (Some(Seq(AvroArray(AvroString())), AvroString()))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroArray(AvroInt())))) should be (Some(Seq(AvroArray(AvroArray(AvroInt()))), AvroArray(AvroInt())))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroMap(AvroInt())))) should be (Some(Seq(AvroArray(AvroMap(AvroInt()))), AvroMap(AvroInt())))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroUnion(List(AvroInt()))))) should be (Some(Seq(AvroArray(AvroUnion(List(AvroInt())))), AvroUnion(List(AvroInt()))))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroFixed(10, "MyFixed")))) should be (Some(Seq(AvroArray(AvroFixed(10, "MyFixed"))), AvroFixed(10, "MyFixed")))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroEnum(List("one", "two", "three"), "MyEnum")))) should be (Some(Seq(AvroArray(AvroEnum(List("one", "two", "three"), "MyEnum"))), AvroEnum(List("one", "two", "three"), "MyEnum")))
    Sig(List("x" -> P.Array(P.Wildcard("A"))), P.Wildcard("A")).accepts(List(AvroArray(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))) should be (Some(Seq(AvroArray(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))

    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroNull())) should be (Some(Seq(AvroNull()), AvroArray(AvroNull())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroInt())) should be (Some(Seq(AvroInt()), AvroArray(AvroInt())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroLong())) should be (Some(Seq(AvroLong()), AvroArray(AvroLong())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroFloat())) should be (Some(Seq(AvroFloat()), AvroArray(AvroFloat())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroDouble())) should be (Some(Seq(AvroDouble()), AvroArray(AvroDouble())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroBytes())) should be (Some(Seq(AvroBytes()), AvroArray(AvroBytes())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroString())) should be (Some(Seq(AvroString()), AvroArray(AvroString())))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroArray(AvroInt()))) should be (Some(Seq(AvroArray(AvroInt())), AvroArray(AvroArray(AvroInt()))))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroMap(AvroInt()))) should be (Some(Seq(AvroMap(AvroInt())), AvroArray(AvroMap(AvroInt()))))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroUnion(List(AvroInt())))) should be (Some(Seq(AvroUnion(List(AvroInt()))), AvroArray(AvroUnion(List(AvroInt())))))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroFixed(10, "MyFixed"))) should be (Some(Seq(AvroFixed(10, "MyFixed")), AvroArray(AvroFixed(10, "MyFixed"))))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroEnum(List("one", "two", "three"), "MyEnum"))) should be (Some(Seq(AvroEnum(List("one", "two", "three"), "MyEnum")), AvroArray(AvroEnum(List("one", "two", "three"), "MyEnum"))))
    Sig(List("x" -> P.Wildcard("A")), P.Array(P.Wildcard("A"))).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroArray(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))))
  }

  it must "use wildcards to normalize numerical types" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroInt())) should be (Some(Seq(AvroInt(), AvroInt()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroLong())) should be (Some(Seq(AvroLong(), AvroLong()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroFloat())) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroInt(), AvroDouble())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))

    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroInt())) should be (Some(Seq(AvroLong(), AvroLong()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroLong())) should be (Some(Seq(AvroLong(), AvroLong()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroFloat())) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroLong(), AvroDouble())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))

    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroInt())) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroLong())) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroFloat())) should be (Some(Seq(AvroFloat(), AvroFloat()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroFloat(), AvroDouble())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))

    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroInt())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroLong())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroFloat())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
    Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong(), AvroFloat(), AvroDouble())), "y" -> P.Wildcard("A")), P.Null).accepts(List(AvroDouble(), AvroDouble())) should be (Some(Seq(AvroDouble(), AvroDouble()), AvroNull()))
  }

  it must "match wild records" taggedAs(SignatureMatch) in {
    Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Int))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord"))) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")), AvroRecord(List(AvroField("one", AvroInt())), "MyRecord")))
    Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Int))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord"))) should be (Some(Seq(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")), AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord")))
    Sig(List("x" -> P.WildRecord("A", Map("one" -> P.String))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord"))) should be (None)
    Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Double))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroString())), "MyRecord"))) should be (None)
    Sig(List("x" -> P.WildRecord("A", Map("one" -> P.Int))), P.Wildcard("A")).accepts(List(AvroRecord(List(AvroField("uno", AvroInt()), AvroField("two", AvroString())), "MyRecord"))) should be (None)
  }

}
