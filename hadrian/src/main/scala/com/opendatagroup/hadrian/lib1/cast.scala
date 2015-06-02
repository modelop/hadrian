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

package com.opendatagroup.hadrian.lib1

import scala.language.postfixOps

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
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "cast."

  //////////////////////////////////////////////////////////////////// wrap-around arithmetic

  val bitsToMax = Map(1 -> 2L,
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
  object ToSigned extends LibFcn with Function2[Long, Int, Long] {
    val name = prefix + "signed"
    val sig = Sig(List("x" -> P.Long, "bits" -> P.Int), P.Long)
    val doc =
      <doc>
        <desc>Truncate <p>x</p> as though its signed long two's complement representation were inserted, bit-for-bit, into a signed two's complement representation that is <p>bits</p> wide, removing the most significant bits.</desc>
        <detail>The result of this function may be negative, zero, or positive.</detail>
        <error>If <p>bits</p> is less than 2 or greater than 64, an "unrepresentable unsigned number" error is raised.</error>
      </doc>
    def apply(x: Long, bits: Int): Long = {
      if (bits == 64)
        x
      else if (bits == 1)
        throw new PFARuntimeException("unrepresentable unsigned number")
      else {
        val y = ToUnsigned(x, bits)
        val maximum = bitsToMax(bits - 1)
        if (y > maximum - 1)
          y - 2*maximum
        else
          y
      }
    }
  }
  provide(ToSigned)

  ////   unsigned (ToUnsigned)
  object ToUnsigned extends LibFcn with Function2[Long, Int, Long] {
    val name = prefix + "unsigned"
    val sig = Sig(List("x" -> P.Long, "bits" -> P.Int), P.Long)
    val doc =
      <doc>
        <desc>Truncate <p>x</p> as though its signed long two's complement representation were inserted, bit-for-bit, into an unsigned register that is <p>bits</p> wide, removing the most significant bits.</desc>
        <detail>The result of this function is always nonnegative.</detail>
        <error>If <p>bits</p> is less than 1 or greater than 63, an "unrepresentable unsigned number" error is raised.</error>
      </doc>
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
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("unrepresentable unsigned number")
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
  provide(ToUnsigned)

  //////////////////////////////////////////////////////////////////// number precisions

  ////   int (ToInt)
  object ToInt extends LibFcn {
    val name = prefix + "int"
    val sig = Sigs(List(Sig(List("x" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long), P.Int),
                        Sig(List("x" -> P.Float), P.Int),
                        Sig(List("x" -> P.Double), P.Int)))
    val doc =
      <doc>
        <desc>Cast <p>x</p> to an integer, rounding if necessary.</desc>
        <error>Results outside of {java.lang.Integer.MIN_VALUE} and {java.lang.Integer.MAX_VALUE} (inclusive) produce an "int overflow" runtime error.</error>
      </doc>
    def apply(x: Int): Int = x
    def apply(x: Long): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow")
      else
        x.toInt
    def apply(x: Float): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow")
      else
        java.lang.Math.round(x)
    def apply(x: Double): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow")
      else
        java.lang.Math.round(x).toInt
  }
  provide(ToInt)

  ////   long (ToLong)
  object ToLong extends LibFcn {
    val name = prefix + "long"
    val sig = Sigs(List(Sig(List("x" -> P.Int), P.Long),
                        Sig(List("x" -> P.Long), P.Long),
                        Sig(List("x" -> P.Float), P.Long),
                        Sig(List("x" -> P.Double), P.Long)))
    val doc =
      <doc>
        <desc>Cast <p>x</p> to a 64-bit integer, rounding if necessary.</desc>
        <error>Results outside of {java.lang.Long.MIN_VALUE} and {java.lang.Long.MAX_VALUE} (inclusive) produce a "long overflow" runtime error.</error>
      </doc>
    def apply(x: Int): Long = x.toLong
    def apply(x: Long): Long = x
    def apply(x: Float): Long =
      if (x > java.lang.Long.MAX_VALUE  ||  x < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow")
      else
        java.lang.Math.round(x).toLong
    def apply(x: Double): Long =
      if (x > java.lang.Long.MAX_VALUE  ||  x < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow")
      else
        java.lang.Math.round(x)
  }
  provide(ToLong)

  ////   float (ToFloat)
  object ToFloat extends LibFcn {
    val name = prefix + "float"
    val sig = Sigs(List(Sig(List("x" -> P.Int), P.Float),
                        Sig(List("x" -> P.Long), P.Float),
                        Sig(List("x" -> P.Float), P.Float),
                        Sig(List("x" -> P.Double), P.Float)))
    val doc =
      <doc>
        <desc>Cast <p>x</p> to a single-precision floating point number, rounding if necessary.</desc>
      </doc>
    def apply(x: Int): Float = x.toFloat
    def apply(x: Long): Float = x.toFloat
    def apply(x: Float): Float = x
    def apply(x: Double): Float = x.toFloat
  }
  provide(ToFloat)

  ////   double (ToDouble)
  object ToDouble extends LibFcn {
    val name = prefix + "double"
    val sig = Sigs(List(Sig(List("x" -> P.Int), P.Double),
                        Sig(List("x" -> P.Long), P.Double),
                        Sig(List("x" -> P.Float), P.Double),
                        Sig(List("x" -> P.Double), P.Double)))
    val doc =
      <doc>
        <desc>Cast <p>x</p> to a double-precision floating point number.</desc>
      </doc>
    def apply(x: Int): Double = x.toDouble
    def apply(x: Long): Double = x.toDouble
    def apply(x: Float): Double = x.toDouble
    def apply(x: Double): Double = x
  }
  provide(ToDouble)

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
        case i if (i < 0  ||  i > maximum - minimum) =>
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
  object FanoutBoolean extends LibFcn {
    val name = prefix + "fanoutBoolean"
    val sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Boolean)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Boolean)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Boolean))))
    val doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
      </doc>
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, false, true)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) =
      Fanouts.fromString(x, dictionary, outOfRange, false, true)
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, false, true)
  }
  provide(FanoutBoolean)

  ////   fanoutInt (FanoutInt)
  object FanoutInt extends LibFcn {
    val name = prefix + "fanoutInt"
    val sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Int)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Int)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Int))))
    val doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
      </doc>
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0, 1)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) =
      Fanouts.fromString(x, dictionary, outOfRange, 0, 1)
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0, 1)
  }
  provide(FanoutInt)

  ////   fanoutLong (FanoutLong)
  object FanoutLong extends LibFcn {
    val name = prefix + "fanoutLong"
    val sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Long)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Long)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Long))))
    val doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
      </doc>
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0, 1)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) =
      Fanouts.fromString(x, dictionary, outOfRange, 0, 1)
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0, 1)
  }
  provide(FanoutLong)

  ////   fanoutFloat (FanoutFloat)
  object FanoutFloat extends LibFcn {
    val name = prefix + "fanoutFloat"
    val sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Float)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Float)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Float))))
    val doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
      </doc>
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0, 1)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) =
      Fanouts.fromString(x, dictionary, outOfRange, 0, 1)
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0, 1)
  }
  provide(FanoutFloat)

  ////   fanoutDouble (FanoutDouble)
  object FanoutDouble extends LibFcn {
    val name = prefix + "fanoutDouble"
    val sig = Sigs(List(Sig(List("x" -> P.WildEnum("A")), P.Array(P.Double)),
                        Sig(List("x" -> P.String, "dictionary" -> P.Array(P.String), "outOfRange" -> P.Boolean), P.Array(P.Double)),
                        Sig(List("x" -> P.Int, "minimum" -> P.Int, "maximum" -> P.Int, "outOfRange" -> P.Boolean), P.Array(P.Double))))
    val doc =
      <doc>
        <desc>Fanout <p>x</p> to an array of booleans, all <c>false</c> except the matching value.</desc>
        <param name="x">Categorical datum</param>
        <param name="dictionary">Possible values of <p>x</p>, which is needed if <p>x</p> is an arbitrary string.</param>
        <param name="minimum">Inclusive minimum value of <p>x</p>.</param>
        <param name="maximum">Excluded maximum value of <p>x</p>.</param>
        <param name="outOfRange">If <c>true</c>, include an extra item in the output to represent values of <p>x</p> that are outside of the specified range.</param>
      </doc>
    def apply(x: PFAEnumSymbol) =
      Fanouts.fromEnum(x, 0, 1)
    def apply(x: String, dictionary: PFAArray[String], outOfRange: Boolean) =
      Fanouts.fromString(x, dictionary, outOfRange, 0, 1)
    def apply(x: Int, minimum: Int, maximum: Int, outOfRange: Boolean) =
      Fanouts.fromInt(x, minimum, maximum, outOfRange, 0, 1)
  }
  provide(FanoutDouble)

}
