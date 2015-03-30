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

package object parse {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "parse."

  ////////////////////////////////////////////////////////////////////

  ////   int (ParseInt)
  object ParseInt extends LibFcn {
    val name = prefix + "int"
    val sig = Sig(List("str" -> P.String, "base" -> P.Int), P.Int)
    val doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as an integer with base <p>base</p>, if possible.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <error>Raises "not an integer" if the string does not conform to "<c>[-+]?[0-9a-z]+</c>" or the number it evaluates to is too large to represent as a 32-bit integer or uses characters as large as or larger than <p>base</p> ('0' through '9' encode 0 through 9 and 'a' through 'z' encode 10 through 35).</error>
        <error>Raises "base out of range" if <p>base</p> is less than 2 or greater than 36.</error>
      </doc>
    def apply(str: String, base: Int): Int =
      try {
        if (base < 2  ||  base > 36)
          throw new PFARuntimeException("base out of range")
        var stripped = str.trim.toLowerCase
        if (stripped.startsWith("+"))
          stripped = stripped.tail
        java.lang.Integer.parseInt(stripped, base)
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not an integer")
      }
  }
  provide(ParseInt)

  ////   long (ParseLong)
  object ParseLong extends LibFcn {
    val name = prefix + "long"
    val sig = Sig(List("str" -> P.String, "base" -> P.Int), P.Long)
    val doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as a long integer with base <p>base</p>, if possible.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <error>Raises "not a long integer" if the string does not conform to "<c>[-+]?[0-9a-z]+</c>" or the number it evaluates to is too large to represent as a 64-bit integer or uses characters as large as or larger than <p>base</p> ('0' through '9' encode 0 through 9 and 'a' through 'z' encode 10 through 35).</error>
        <error>Raises "base out of range" if <p>base</p> is less than 2 or greater than 36.</error>
      </doc>
    def apply(str: String, base: Int): Long =
      try {
        if (base < 2  ||  base > 36)
          throw new PFARuntimeException("base out of range")
        var stripped = str.trim.toLowerCase
        if (stripped.startsWith("+"))
          stripped = stripped.tail
        java.lang.Long.parseLong(stripped, base)
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not a long integer")
      }
  }
  provide(ParseLong)

  ////   float (ParseFloat)
  object ParseFloat extends LibFcn {
    val name = prefix + "float"
    val sig = Sig(List("str" -> P.String), P.Float)
    val doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as a single-precision floating point number.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <detail>If the string is "nan", the resulting value is not-a-number and if the string is "inf", "+inf", or "-inf", the resulting value is positive infinity, positive infinity, or negative infinity, respectively (see IEEE 754).</detail>
        <detail>If the number's magnitude is too large to be represented as a single-precision float, the resulting value is positive or negative infinity (depending on the sign).  If the numbers magnitude is too small to be represented as a single-precision float, the resulting value is zero.</detail>
        <error>Raises "not a single-precision float" if the string does not conform to "<c>[-+]?[0-9]+(e[-+]?[0-9]+)?</c>".</error>
      </doc>
    def apply(str: String): Float =
      try {
        val stripped = str.trim.toLowerCase
        if (stripped == "nan")
          java.lang.Float.NaN
        else if (stripped == "inf")
          java.lang.Float.POSITIVE_INFINITY
        else if (stripped == "+inf")
          java.lang.Float.POSITIVE_INFINITY
        else if (stripped == "-inf")
          java.lang.Float.NEGATIVE_INFINITY
        else
          java.lang.Float.parseFloat(stripped)
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not a single-precision float")
      }
  }
  provide(ParseFloat)

  ////   double (ParseDouble)
  object ParseDouble extends LibFcn {
    val name = prefix + "double"
    val sig = Sig(List("str" -> P.String), P.Double)
    val doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as a double-precision floating point number.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <detail>If the string is "nan", the resulting value is not-a-number and if the string is "inf", "+inf", or "-inf", the resulting value is positive infinity, positive infinity, or negative infinity, respectively (see IEEE 754).</detail>
        <detail>If the number's magnitude is too large to be represented as a double-precision float, the resulting value is positive or negative infinity (depending on the sign).  If the numbers magnitude is too small to be represented as a double-precision float, the resulting value is zero.</detail>
        <error>Raises "not a double-precision float" if the string does not conform to "<c>[-+]?[0-9]+(e[-+]?[0-9]+)?</c>".</error>
      </doc>
    def apply(str: String): Double =
      try {
        val stripped = str.trim.toLowerCase
        if (stripped == "nan")
          java.lang.Double.NaN
        else if (stripped == "inf")
          java.lang.Double.POSITIVE_INFINITY
        else if (stripped == "+inf")
          java.lang.Double.POSITIVE_INFINITY
        else if (stripped == "-inf")
          java.lang.Double.NEGATIVE_INFINITY
        else
          java.lang.Double.parseDouble(stripped)
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not a double-precision float")
      }
  }
  provide(ParseDouble)


}
