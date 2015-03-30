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
