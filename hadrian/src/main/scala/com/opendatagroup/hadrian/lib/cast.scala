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

package com.opendatagroup.hadrian.lib

import scala.language.postfixOps
import scala.collection.immutable.ListMap

import org.apache.avro.Schema
import org.apache.avro.io.EncoderFactory

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

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

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAEnumSymbol

package object cast {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "cast."

  //////////////////////////////////////////////////////////////////// wrap-around arithmetic

  val bitsToMax = Map(
    1 -> 2L,
    2 -> 4L,
    3 -> 8L,
    4 -> 16L,
    5 -> 32L,
    6 -> 64L,
    7 -> 128L,
    8 -> 256L,
    9 -> 512L,
    10 -> 1024L,
    11 -> 2048L,
    12 -> 4096L,
    13 -> 8192L,
    14 -> 16384L,
    15 -> 32768L,
    16 -> 65536L,
    17 -> 131072L,
    18 -> 262144L,
    19 -> 524288L,
    20 -> 1048576L,
    21 -> 2097152L,
    22 -> 4194304L,
    23 -> 8388608L,
    24 -> 16777216L,
    25 -> 33554432L,
    26 -> 67108864L,
    27 -> 134217728L,
    28 -> 268435456L,
    29 -> 536870912L,
    30 -> 1073741824L,
    31 -> 2147483648L,
    32 -> 4294967296L,
    33 -> 8589934592L,
    34 -> 17179869184L,
    35 -> 34359738368L,
    36 -> 68719476736L,
    37 -> 137438953472L,
    38 -> 274877906944L,
    39 -> 549755813888L,
    40 -> 1099511627776L,
    41 -> 2199023255552L,
    42 -> 4398046511104L,
    43 -> 8796093022208L,
    44 -> 17592186044416L,
    45 -> 35184372088832L,
    46 -> 70368744177664L,
    47 -> 140737488355328L,
    48 -> 281474976710656L,
    49 -> 562949953421312L,
    50 -> 1125899906842624L,
    51 -> 2251799813685248L,
    52 -> 4503599627370496L,
    53 -> 9007199254740992L,
    54 -> 18014398509481984L,
    55 -> 36028797018963968L,
    56 -> 72057594037927936L,
    57 -> 144115188075855872L,
    58 -> 288230376151711744L,
    59 -> 576460752303423488L,
    60 -> 1152921504606846976L,
    61 -> 2305843009213693952L,
    62 -> 4611686018427387904L)

  ////   signed (ToSigned)
  class ToSigned(val pos: Option[String] = None) extends LibFcn with Function2[Long, Int, Long] {
    def name = prefix + "signed"
    def sig = Sig(List("x" -> P.Long, "bits" -> P.Int), P.Long)
    def doc =
      <doc>
        <desc>Truncate <p>x</p> as though its signed long two's complement representation were inserted, bit-for-bit, into a signed two's complement representation that is <p>bits</p> wide, removing the most significant bits.</desc>
        <detail>The result of this function may be negative, zero, or positive.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>bits</p> is less than 2 or greater than 64, an "unrepresentable unsigned number" error is raised.</error>
      </doc>
    def errcodeBase = 17000
    def apply(x: Long, bits: Int): Long = {
      if (bits == 64)
        x
      else if (bits < 2  ||  bits > 64)
        throw new PFARuntimeException("unrepresentable unsigned number", errcodeBase + 0, name, pos)
      else {
        val y = new ToUnsigned()(x, bits)
        val maximum = bitsToMax(bits - 1)
        if (y > maximum - 1)
          y - 2*maximum
        else
          y
      }
    }
  }
  provide(new ToSigned)

  ////   unsigned (ToUnsigned)
  class ToUnsigned(val pos: Option[String] = None) extends LibFcn with Function2[Long, Int, Long] {
    def name = prefix + "unsigned"
    def sig = Sig(List("x" -> P.Long, "bits" -> P.Int), P.Long)
    def doc =
      <doc>
        <desc>Truncate <p>x</p> as though its signed long two's complement representation were inserted, bit-for-bit, into an unsigned register that is <p>bits</p> wide, removing the most significant bits.</desc>
        <detail>The result of this function is always nonnegative.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>bits</p> is less than 1 or greater than 63, an "unrepresentable unsigned number" error is raised.</error>
      </doc>
    def errcodeBase = 17010
    def apply(x: Long, bits: Int): Long = {
      if (bits == 63) {
        if (x < 0)
          x + 9223372036854775807L + 1
        else
          x
      }
      else {
        val maximum = try {
          bitsToMax(bits)
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("unrepresentable unsigned number", errcodeBase + 0, name, pos)
        }
        val y =
          if (x < 0)
            x + maximum * Math.ceil(-x.toDouble / maximum).toLong
          else
            x
        y % maximum
      }
    }
  }
  provide(new ToUnsigned)

  //////////////////////////////////////////////////////////////////// number precisions

  ////   int (ToInt)
  class ToInt(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "int"
    def sig = Sigs(List(Sig(List("x" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long), P.Int),
                        Sig(List("x" -> P.Float), P.Int),
                        Sig(List("x" -> P.Double), P.Int)))
    def doc =
      <doc>
        <desc>Cast <p>x</p> to an integer, rounding if necessary.</desc>
        <error code={s"${errcodeBase + 0}"}>Results outside of {java.lang.Integer.MIN_VALUE} and {java.lang.Integer.MAX_VALUE} (inclusive) produce an "int overflow" runtime error.</error>
      </doc>
    def errcodeBase = 17020
    def apply(x: Int): Int = x
    def apply(x: Long): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        x.toInt
    def apply(x: Float): Int =
      if (java.lang.Float.isNaN(x)  ||  x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        java.lang.Math.round(x)
    def apply(x: Double): Int =
      if (java.lang.Double.isNaN(x)  ||  x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        java.lang.Math.round(x).toInt
  }
  provide(new ToInt)

  ////   long (ToLong)
  class ToLong(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "long"
    def sig = Sigs(List(Sig(List("x" -> P.Int), P.Long),
                        Sig(List("x" -> P.Long), P.Long),
                        Sig(List("x" -> P.Float), P.Long),
                        Sig(List("x" -> P.Double), P.Long)))
    def doc =
      <doc>
        <desc>Cast <p>x</p> to a 64-bit integer, rounding if necessary.</desc>
        <error code={s"${errcodeBase + 0}"}>Results outside of {java.lang.Long.MIN_VALUE} and {java.lang.Long.MAX_VALUE} (inclusive) produce a "long overflow" runtime error.</error>
      </doc>
    def errcodeBase = 17030
    def apply(x: Int): Long = x.toLong
    def apply(x: Long): Long = x
    def apply(x: Float): Long =
      if (java.lang.Float.isNaN(x)  ||  x > java.lang.Long.MAX_VALUE  ||  x < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow", errcodeBase + 0, name, pos)
      else
        java.lang.Math.round(x).toLong
    def apply(x: Double): Long =
      if (java.lang.Double.isNaN(x)  ||  x > java.lang.Long.MAX_VALUE  ||  x < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow", errcodeBase + 0, name, pos)
      else
        java.lang.Math.round(x)
  }
  provide(new ToLong)

  ////   float (ToFloat)
  class ToFloat(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "float"
    def sig = Sigs(List(Sig(List("x" -> P.Int), P.Float),
                        Sig(List("x" -> P.Long), P.Float),
                        Sig(List("x" -> P.Float), P.Float),
                        Sig(List("x" -> P.Double), P.Float)))
    def doc =
      <doc>
        <desc>Cast <p>x</p> to a single-precision floating point number, rounding if necessary.</desc>
      </doc>
    def errcodeBase = 17040
    def apply(x: Int): Float = x.toFloat
    def apply(x: Long): Float = x.toFloat
    def apply(x: Float): Float = x
    def apply(x: Double): Float = x.toFloat
  }
  provide(new ToFloat)

  ////   double (ToDouble)
  class ToDouble(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "double"
    def sig = Sigs(List(Sig(List("x" -> P.Int), P.Double),
                        Sig(List("x" -> P.Long), P.Double),
                        Sig(List("x" -> P.Float), P.Double),
                        Sig(List("x" -> P.Double), P.Double)))
    def doc =
      <doc>
        <desc>Cast <p>x</p> to a double-precision floating point number.</desc>
      </doc>
    def errcodeBase = 17050
    def apply(x: Int): Double = x.toDouble
    def apply(x: Long): Double = x.toDouble
    def apply(x: Float): Double = x.toDouble
    def apply(x: Double): Double = x
  }
  provide(new ToDouble)

  //////////////////////////////////////////////////////////////////// fanouts

  object Fanouts {
    def fromEnum[X](x: PFAEnumSymbol, zero: X, one: X): PFAArray[X] =
      PFAArray.fromVector((0 until x.numSymbols) map {i => if (i == x.value) one else zero} toVector)

    def fromString[X](x: String, dictionary: PFAArray[String], outOfRange: Boolean, zero: X, one: X): PFAArray[X] =
      PFAArray.fromVector(dictionary.toVector.indexOf(x) match {
        case -1 =>
          val out = Vector.fill(dictionary.size)(zero)
          if (outOfRange)
            out :+ one
          else
            out
        case i =>
          val out = Vector.fill(dictionary.size)(zero).updated(i, one)
          if (outOfRange)
            out :+ zero
          else
            out
      })

    def fromInt[X](x: Int, minimum: Int, maximum: Int, outOfRange: Boolean, zero: X, one: X): PFAArray[X] =
      PFAArray.fromVector(x - minimum match {
        case i if (i < 0  ||  i >= maximum - minimum) =>
          val out = Vector.fill(maximum - minimum)(zero)
          if (outOfRange)
            out :+ one
          else
            out
        case i =>
          val out = Vector.fill(maximum - minimum)(zero).updated(i, one)
          if (outOfRange)
            out :+ zero
          else
            out
      })
  }

  ////   fanoutBoolean (FanoutBoolean)
  class FanoutBoolean(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fanoutBoolean"
    def sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Boolean)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Boolean)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Boolean))))
    def doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
        <error code={s"${errcodeBase + 0}"}>If not all values in <p>dictionary</p> are unique, this function raises a "non-distinct values in dictionary" runtime error.</error>
      </doc>
    def errcodeBase = 17060
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, false, true)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) = {
      if (dictionary.toVector.size != dictionary.toVector.toSet.size)
        throw new PFARuntimeException("non-distinct values in dictionary", errcodeBase + 0, name, pos)
      Fanouts.fromString(x, dictionary, outOfRange, false, true)
    }
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, false, true)
  }
  provide(new FanoutBoolean)

  ////   fanoutInt (FanoutInt)
  class FanoutInt(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fanoutInt"
    def sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Int)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Int)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Int))))
    def doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
        <error code={s"${errcodeBase + 0}"}>If not all values in <p>dictionary</p> are unique, this function raises a "non-distinct values in dictionary" runtime error.</error>
      </doc>
    def errcodeBase = 17070
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0, 1)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) = {
      if (dictionary.toVector.size != dictionary.toVector.toSet.size)
        throw new PFARuntimeException("non-distinct values in dictionary", errcodeBase + 0, name, pos)
      Fanouts.fromString(x, dictionary, outOfRange, 0, 1)
    }
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0, 1)
  }
  provide(new FanoutInt)

  ////   fanoutLong (FanoutLong)
  class FanoutLong(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fanoutLong"
    def sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Long)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Long)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Long))))
    def doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
        <error code={s"${errcodeBase + 0}"}>If not all values in <p>dictionary</p> are unique, this function raises a "non-distinct values in dictionary" runtime error.</error>
      </doc>
    def errcodeBase = 17080
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0L, 1L)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) = {
      if (dictionary.toVector.size != dictionary.toVector.toSet.size)
        throw new PFARuntimeException("non-distinct values in dictionary", errcodeBase + 0, name, pos)
      Fanouts.fromString(x, dictionary, outOfRange, 0L, 1L)
    }
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0L, 1L)
  }
  provide(new FanoutLong)

  ////   fanoutFloat (FanoutFloat)
  class FanoutFloat(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fanoutFloat"
    def sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Float)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Float)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Float))))
    def doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
        <error code={s"${errcodeBase + 0}"}>If not all values in <p>dictionary</p> are unique, this function raises a "non-distinct values in dictionary" runtime error.</error>
      </doc>
    def errcodeBase = 17090
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0.0F, 1.0F)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) = {
      if (dictionary.toVector.size != dictionary.toVector.toSet.size)
        throw new PFARuntimeException("non-distinct values in dictionary", errcodeBase + 0, name, pos)
      Fanouts.fromString(x, dictionary, outOfRange, 0.0F, 1.0F)
    }
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0.0F, 1.0F)
  }
  provide(new FanoutFloat)

  ////   fanoutDouble (FanoutDouble)
  class FanoutDouble(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fanoutDouble"
    def sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Double)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Double)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Double))))
    def doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
        <error code={s"${errcodeBase + 0}"}>If not all values in <p>dictionary</p> are unique, this function raises a "non-distinct values in dictionary" runtime error.</error>
      </doc>
    def errcodeBase = 17100
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0.0, 1.0)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) = {
      if (dictionary.toVector.size != dictionary.toVector.toSet.size)
        throw new PFARuntimeException("non-distinct values in dictionary", errcodeBase + 0, name, pos)
      Fanouts.fromString(x, dictionary, outOfRange, 0.0, 1.0)
    }
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0.0, 1.0)
  }
  provide(new FanoutDouble)

  //////////////////////////////////////////////////////////////////// serialize

  ////   avro (CastAvro)
  object CastAvro {
    class WithSchema(schema: Schema) {
      val writer = new com.opendatagroup.hadrian.data.GenericPFADatumWriter[AnyRef](schema, com.opendatagroup.hadrian.data.genericData)
      def apply(x: AnyRef): Array[Byte] = {
        val out = new java.io.ByteArrayOutputStream
        val encoder = EncoderFactory.get.binaryEncoder(out, null)
        writer.write(x, encoder)
        encoder.flush()
        out.toByteArray
      }
      def apply(x: Boolean): Array[Byte] = apply(java.lang.Boolean.valueOf(x))
      def apply(x: Int): Array[Byte] = apply(java.lang.Integer.valueOf(x))
      def apply(x: Long): Array[Byte] = apply(java.lang.Long.valueOf(x))
      def apply(x: Float): Array[Byte] = apply(java.lang.Float.valueOf(x))
      def apply(x: Double): Array[Byte] = apply(java.lang.Double.valueOf(x))
    }
  }
  class CastAvro(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "avro"
    def sig = Sig(List("x" -> P.Wildcard("A")), P.Bytes)   // TODO: a signature that takes a compression flag (none, deflate, snappy)
    def doc =
      <doc>
        <desc>Encode an arbitrary object as Avro bytes.</desc>
        <detail>May be composed with <f>bytes.toBase64</f> to get an efficient string representation (e.g. for map keys).</detail>
      </doc>
    def errcodeBase = 17110
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$WithSchema(%s))", javaSchema(P.mustBeAvro(fcnType.params.head), false))
  }
  provide(new CastAvro)

  ////   json (CastJson)
  object CastJson {
    class WithSchema(schema: Schema) {
      val writer = new com.opendatagroup.hadrian.data.GenericPFADatumWriter[AnyRef](schema, com.opendatagroup.hadrian.data.genericData)
      def apply(x: AnyRef): String = {
        val out = new java.io.ByteArrayOutputStream
        val encoder = EncoderFactory.get.jsonEncoder(schema, out)
        writer.write(x, encoder)
        encoder.flush()
        out.toString
      }
      def apply(x: Boolean): String = apply(java.lang.Boolean.valueOf(x))
      def apply(x: Int): String = apply(java.lang.Integer.valueOf(x))
      def apply(x: Long): String = apply(java.lang.Long.valueOf(x))
      def apply(x: Float): String = apply(java.lang.Float.valueOf(x))
      def apply(x: Double): String = apply(java.lang.Double.valueOf(x))
    }
  }
  class CastJson(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "json"
    def sig = Sig(List("x" -> P.Wildcard("A")), P.String)
    def doc =
      <doc>
        <desc>Encode an arbitrary object as a JSON string.</desc>
        <detail>The form of this JSON string (spacing, order of keys in objects, etc.) is not guaranteed from one system to another.</detail>
        <detail>Should exclude unnecessary whitespace.</detail>
        <nondeterministic type="unstable" />
      </doc>
    def errcodeBase = 17120
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$WithSchema(%s))", javaSchema(P.mustBeAvro(fcnType.params.head), false))
  }
  provide(new CastJson)

}
