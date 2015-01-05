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

package object cast {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "cast."

  ////////////////////////////////////////////////////////////////////

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

}
