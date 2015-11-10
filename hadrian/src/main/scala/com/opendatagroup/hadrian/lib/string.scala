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

package com.opendatagroup.hadrian.lib

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs
import com.opendatagroup.hadrian.signature.PFAVersion
import com.opendatagroup.hadrian.signature.Lifespan

import com.opendatagroup.hadrian.lib.array.startEnd
import com.opendatagroup.hadrian.lib.array.describeIndex

package object string {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "s."

  val irrelevantOrder = <detail>The order of characters in <p>chars</p> is irrelevant.</detail>

  //////////////////////////////////////////////////////////////////// basic access

  ////   len (Len)
  class Len(val pos: Option[String] = None) extends LibFcn with Function1[String, Int] {
    def name = prefix + "len"
    def sig = Sig(List("s" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Return the length of string <p>s</p>.</desc>
      </doc>
    def errcodeBase = 39000
    def apply(s: String): Int = s.length
  }
  provide(new Len)

  ////   substr (Substr)
  class Substr(val pos: Option[String] = None) extends LibFcn with Function3[String, Int, Int, String] {
    def name = prefix + "substr"
    def sig = Sig(List("s" -> P.String, "start" -> P.Int, "end" -> P.Int), P.String)
    def doc =
      <doc>
        <desc>Return the substring of <p>s</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive).</desc>{describeIndex}
      </doc>
    def errcodeBase = 39010
    def apply(s: String, start: Int, end: Int): String = {
      val (normStart, normEnd) = startEnd(s.length, start, end)
      s.substring(normStart, normEnd)
    }
  }
  provide(new Substr)

  ////   substrto (SubstrTo)
  class SubstrTo(val pos: Option[String] = None) extends LibFcn with Function4[String, Int, Int, String, String] {
    def name = prefix + "substrto"
    def sig = Sig(List("s" -> P.String, "start" -> P.Int, "end" -> P.Int, "replacement" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Replace <p>s</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) with <p>replacement</p>.</desc>{describeIndex}
      </doc>
    def errcodeBase = 39020
    def apply(s: String, start: Int, end: Int, replacement: String): String = {
      val (normStart, normEnd) = startEnd(s.length, start, end)
      s.substring(0, normStart) + replacement + s.substring(normEnd, s.length)
    }
  }
  provide(new SubstrTo)

  //////////////////////////////////////////////////////////////////// searching

  ////   contains (Contains)
  class Contains(val pos: Option[String] = None) extends LibFcn with Function2[String, String, Boolean] {
    def name = prefix + "contains"
    def sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>haystack</p> contains <p>needle</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 39030
    def apply(haystack: String, needle: String): Boolean =
      haystack.indexOf(needle) != -1
  }
  provide(new Contains)

  ////   count (Count)
  class Count(val pos: Option[String] = None) extends LibFcn with Function2[String, String, Int] {
    def name = prefix + "count"
    def sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Count the number of times <p>needle</p> appears in <p>haystack</p>.</desc>
        <detail>If the <p>needle</p> is an empty string, the result is zero.</detail>
      </doc>
    def errcodeBase = 39040
    def apply(haystack: String, needle: String): Int =
      if (haystack.isEmpty  ||  needle.isEmpty)
        0
      else
        count(haystack, needle, 0)

    @tailrec
    final def count(haystack: String, needle: String, sofar: Int): Int = haystack.indexOf(needle) match {
      case -1 => sofar
      case where => count(haystack.substring(where + needle.length), needle, sofar + 1)
    }
  }
  provide(new Count)

  ////   index (Index)
  class Index(val pos: Option[String] = None) extends LibFcn with Function2[String, String, Int] {
    def name = prefix + "index"
    def sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Return the lowest index where <p>haystack</p> contains <p>needle</p> or -1 if <p>haystack</p> does not contain <p>needle</p>.</desc>
      </doc>
    def errcodeBase = 39050
    def apply(haystack: String, needle: String): Int = haystack.indexOf(needle)
  }
  provide(new Index)

  ////   rindex (RIndex)
  class RIndex(val pos: Option[String] = None) extends LibFcn with Function2[String, String, Int] {
    def name = prefix + "rindex"
    def sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Int)
    def doc =
      <doc>
        <desc>Return the highest index where <p>haystack</p> contains <p>needle</p> or -1 if <p>haystack</p> does not contain <p>needle</p>.</desc>
      </doc>
    def errcodeBase = 39060
    def apply(haystack: String, needle: String): Int = haystack.lastIndexOf(needle)
  }
  provide(new RIndex)

  ////   startswith (StartsWith)
  class StartsWith(val pos: Option[String] = None) extends LibFcn with Function2[String, String, Boolean] {
    def name = prefix + "startswith"
    def sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if the first (leftmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def errcodeBase = 39070
    def apply(haystack: String, needle: String): Boolean = haystack.startsWith(needle)
  }
  provide(new StartsWith)

  ////   endswith (EndsWith)
  class EndsWith(val pos: Option[String] = None) extends LibFcn with Function2[String, String, Boolean] {
    def name = prefix + "endswith"
    def sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if the last (rightmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def errcodeBase = 39080
    def apply(haystack: String, needle: String): Boolean = haystack.endsWith(needle)
  }
  provide(new EndsWith)

  //////////////////////////////////////////////////////////////////// conversions to/from other types

  ////   join (Join)
  class Join(val pos: Option[String] = None) extends LibFcn with Function2[PFAArray[String], String, String] {
    def name = prefix + "join"
    def sig = Sig(List("array" -> P.Array(P.String), "sep" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Combine strings from <p>array</p> into a single string, delimited by <p>sep</p>.</desc>
      </doc>
    def errcodeBase = 39090
    def apply(array: PFAArray[String], sep: String): String = array.toVector.mkString(sep)
  }
  provide(new Join)

  ////   split (Split)
  class Split(val pos: Option[String] = None) extends LibFcn with Function2[String, String, PFAArray[String]] {
    def name = prefix + "split"
    def sig = Sig(List("s" -> P.String, "sep" -> P.String), P.Array(P.String))
    def doc =
      <doc>
        <desc>Divide a string into an array of substrings, splitting at and removing delimiters <p>sep</p>.</desc>
        <detail>If <p>s</p> does not contain <p>sep</p>, this function returns an array whose only element is <p>s</p>.  If <p>sep</p> appears at the beginning or end of <p>s</p>, the array begins with or ends with an empty string.  These conventions match Python's behavior. </detail>
        <detail>If <p>sep</p> is an empty string, this function returns an empty array.</detail>
      </doc>
    def errcodeBase = 39100
    def apply(s: String, sep: String): PFAArray[String] =
      if (sep.isEmpty)
        PFAArray.fromVector(Vector[String]())
      else {
        val out = PFAArray.empty[String](1)
        split(s, sep, out)
        out
      }

    @tailrec
    final def split(remaining: String, sep: String, out: PFAArray[String]): Unit = remaining.indexOf(sep) match {
      case -1 => out.add(remaining)
      case where => {
        out.add(remaining.substring(0, where))
        split(remaining.substring(where + sep.length), sep, out)
      }
    }
  }
  provide(new Split)

  ////   hex (Hex)
  class Hex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "hex"
    def sig = Sigs(List(Sig(List("x" -> P.Long), P.String),
                        Sig(List("x" -> P.Long, "width" -> P.Int, "zeroPad" -> P.Boolean), P.String)))
    def doc =
      <doc>
        <desc>Format an unsigned number as a hexidecimal string.</desc>
        <param name="x">The number.</param>
        <param name="width">Width of the string.  If negative, left-justify.  If omitted, the string will be as wide as it needs to be to provide the precision.</param>
        <param name="zeroPad">If true, pad the integer with zeros to fill up to <p>width</p>.</param>
        <detail>If the <p>precision</p> requires more space than <p>width</p>, the string will be wide enough to accommodate the <p>precision</p>.</detail>
        <detail>Digits "a" (decimal 10) through "f" (decimal 15) are represented by lowercase letters.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>width</p> is negative and <p>zeroPad</p> is <c>true</c>, a "negative width cannot be used with zero-padding" error is raised.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is negative, a "negative number" error is raised.</error>
      </doc>
    def errcodeBase = 39110

    def apply(x: Long): String =
      if (x < 0)
        throw new PFARuntimeException("negative number", errcodeBase + 1, name, pos)
      else
        "%x".format(x)

    def apply(x: Long, width: Int, zeroPad: Boolean): String = {
      if (x < 0)
        throw new PFARuntimeException("negative number", errcodeBase + 1, name, pos)
      val formatStr = (width, zeroPad) match {
        case (w, _) if (w == 0) => ""
        case (w, false) => "%" + w.toString + "x"
        case (w, true) if (w < 0) => throw new PFARuntimeException("negative width cannot be used with zero-padding", errcodeBase + 0, name, pos)
        case (w, true) => "%0" + w.toString + "x"
      }
      formatStr.format(x)
    }
  }
  provide(new Hex)

  ////   int (StringInt)
  class StringInt(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "int"
    def sig = Sigs(List(Sig(List("x" -> P.Long), P.String),
                        Sig(List("x" -> P.Long, "width" -> P.Int, "zeroPad" -> P.Boolean), P.String)))
    def doc =
      <doc>
        <desc>Format an integer as a decimal string.</desc>
        <param name="x">The integer.</param>
        <param name="width">Width of the string.  If negative, left-justify.  If omitted, the string will be as wide as it needs to be to provide enough precision.</param>
        <param name="zeroPad">If true, pad the integer with zeros to fill up to <p>width</p>.</param>
        <error code={s"${errcodeBase + 0}"}>If <p>width</p> is negative and <p>zeroPad</p> is <c>true</c>, a "negative width cannot be used with zero-padding" error is raised.</error>
      </doc>
    def errcodeBase = 39240

    def apply(x: Long): String = "%d".format(x)

    def apply(x: Long, width: Int, zeroPad: Boolean): String = {
      val formatStr = (width, zeroPad) match {
        case (w, _) if (w == 0) => ""
        case (w, false) => "%" + w.toString + "d"
        case (w, true) if (w < 0) => throw new PFARuntimeException("negative width cannot be used with zero-padding", errcodeBase + 0, name, pos)
        case (w, true) => "%0" + w.toString + "d"
      }
      formatStr.format(x)
    }
  }
  provide(new StringInt)

  ////   number (Number)
  class Number(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "number"
    def sig = Sigs(List(Sig(List("x" -> P.Long), P.String, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use s.int for integers"))),
                        Sig(List("x" -> P.Long, "width" -> P.Int, "zeroPad" -> P.Boolean), P.String, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use s.int for integers"))),
                        Sig(List("x" -> P.Double, "width" -> P.Union(List(P.Int, P.Null)), "precision" -> P.Union(List(P.Int, P.Null))), P.String),
                        Sig(List("x" -> P.Double, "width" -> P.Union(List(P.Int, P.Null)), "precision" -> P.Union(List(P.Int, P.Null)), "minNoExp" -> P.Double, "maxNoExp" -> P.Double), P.String)))
    def doc =
      <doc>
        <desc>Format a number as a decimal string.</desc>
        <param name="x">The number.  Note that different signatures apply to integers and floating point numbers.</param>
        <param name="width">Width of the string.  If negative, left-justify.  If omitted, the string will be as wide as it needs to be to provide the precision.</param>
        <param name="zeroPad">If true, pad the integer with zeros to fill up to <p>width</p>.</param>
        <param name="precision">Optional precision with which to represent the number.  If omitted, at most six digits after the decimal point will be shown, unless they are zero.</param>
        <param name="minNoExp">Minimum absolute value that is not presented in scientific notation; 0.0001 if omitted.</param>
        <param name="maxNoExp">Maxiumum absolute value that is not presented in scientific notation; 100000 if omitted.</param>
        <detail>If the <p>precision</p> requires more space than <p>width</p>, the string will be wide enough to accommodate the <p>precision</p>.</detail>
        <detail>Floating point numbers always have a decimal point with at least one digit after the decimal, even if it is zero.</detail>
        <detail>Exponents are represented by a lowercase "e" which is always followed by a sign, whether positive or negative, and an exponent of two or more digits (single-digit exponents are zero-padded).</detail>
        <detail>The base of a number is preceded by a "-" if negative, but not a "+" if positive.</detail>
        <detail>Special floating point values are represented in the following ways: negative zero as zero (no negative sign), not a number as "nan", positive infinity as "inf", and negative infinity as "-inf" (lowercase).  They follow the same precision and width rules as normal numbers, where applicable.</detail>
        <nondeterministic type="unstable" />
        <error code={s"${errcodeBase + 0}"}>If <p>width</p> is negative and <p>zeroPad</p> is <c>true</c>, a "negative width cannot be used with zero-padding" error is raised.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>precision</p> is provided and is less than zero, a "negative precision" error is raised.</error>
      </doc>
    def errcodeBase = 39120

    def apply(x: Long): String = "%d".format(x)

    def apply(x: Long, width: Int, zeroPad: Boolean): String = {
      val formatStr = (width, zeroPad) match {
        case (w, _) if (w == 0) => ""
        case (w, false) => "%" + w.toString + "d"
        case (w, true) if (w < 0) => throw new PFARuntimeException("negative width cannot be used with zero-padding", errcodeBase + 0, name, pos)
        case (w, true) => "%0" + w.toString + "d"
      }
      formatStr.format(x)
    }

    def pad(width: java.lang.Integer, str: String): String = width match {
      case null         => str
      case w if (w < 0) => str + Array.fill(-w.intValue - str.size)(' ').mkString
      case w            => Array.fill(w.intValue - str.size)(' ').mkString + str
    }

    def apply(x: java.lang.Double, width: AnyRef, precision: AnyRef): String =
      apply(x.doubleValue, width.asInstanceOf[java.lang.Integer], precision.asInstanceOf[java.lang.Integer])

    def apply(x: Double, width: java.lang.Integer, precision: java.lang.Integer): String = apply(x, width, precision, 0.0001, 100000)

    def apply(x: java.lang.Double, width: AnyRef, precision: AnyRef, minNoExp: Double, maxNoExp: Double): String =
      apply(x.doubleValue, width.asInstanceOf[java.lang.Integer], precision.asInstanceOf[java.lang.Integer], minNoExp, maxNoExp)

    def apply(x: Double, width: java.lang.Integer, precision: java.lang.Integer, minNoExp: Double, maxNoExp: Double): String = {
      val widthStr = width match {
        case null => ""
        case w if (w < 0) => (w.intValue + 1).toString
        case w => w.intValue.toString
      }

      val precisionStr = precision match {
        case null => ""
        case p if (p < 0) => throw new PFARuntimeException("negative precision", errcodeBase + 1, name, pos)
        case p => "." + p.intValue.toString
      }

      x match {
        case y if (java.lang.Double.isInfinite(y)) => if (y > 0.0) pad(width, "inf") else pad(width, "-inf")
        case y if (java.lang.Double.isNaN(y)) => pad(width, "nan")
        case y => {
          var neg = y < 0.0
          val v = Math.abs(y)

          val conv = if (v >= minNoExp  &&  v <= maxNoExp) "f" else "e"

          if (widthStr == "0")
            ""
          else {
            val raw = ("%" + widthStr + precisionStr + conv).format(v)

            val (wh1, tmp) = raw.span(_ == ' ')
            val (res, wh2) = tmp.span(_ != ' ')

            val (base, exp) =
              if (conv == "f")
                (res, "")
              else
                res.splitAt(res.indexOf('e'))

            var out = base
            if (precisionStr == "") {
              if (out.indexOf('.') < 0)
                out += "."
              out = out.replaceAll("0+$", "")
              if (out.endsWith("."))
                out += "0"
            }
            if (y != 0.0)
              out += exp

            if (neg)
              out = "-" + out

            width match {
              case null => out
              case w if (w < 0) => wh1 + out + Array.fill(Math.abs(w) - wh1.size - out.size)(' ').mkString
              case w => Array.fill(w - out.size - wh2.size)(' ').mkString + out + wh2
            }
          }
        }
      }
    }
  }
  provide(new Number)

  //////////////////////////////////////////////////////////////////// conversions to/from other strings

  ////   concat (Concat)
  class Concat(val pos: Option[String] = None) extends LibFcn with Function2[String, String, String] {
    def name = prefix + "concat"
    def sig = Sig(List("x" -> P.String, "y" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Append <p>y</p> to <p>x</p> to form a single string.</desc>
        <detail>To concatenate an array of strings, use {prefix}join with an empty string as <p>sep</p>.</detail>
      </doc>
    def errcodeBase = 39130
    def apply(x: String, y: String): String = x + y
  }
  provide(new Concat)

  ////   repeat (Repeat)
  class Repeat(val pos: Option[String] = None) extends LibFcn with Function2[String, Int, String] {
    def name = prefix + "repeat"
    def sig = Sig(List("s" -> P.String, "n" -> P.Int), P.String)
    def doc =
      <doc>
        <desc>Create a string by concatenating <p>s</p> with itself <p>n</p> times.</desc>
      </doc>
    def errcodeBase = 39140
    def apply(s: String, n: Int): String = Array.fill(n)(s).mkString
  }
  provide(new Repeat)

  ////   lower (Lower)
  class Lower(val pos: Option[String] = None) extends LibFcn with Function1[String, String] {
    def name = prefix + "lower"
    def sig = Sig(List("s" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Convert <p>s</p> to lower-case.</desc>
      </doc>
    def errcodeBase = 39150
    def apply(s: String): String = s.toLowerCase
  }
  provide(new Lower)

  ////   upper (Upper)
  class Upper(val pos: Option[String] = None) extends LibFcn with Function1[String, String] {
    def name = prefix + "upper"
    def sig = Sig(List("s" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Convert <p>s</p> to upper-case.</desc>
      </doc>
    def errcodeBase = 39160
    def apply(s: String): String = s.toUpperCase
  }
  provide(new Upper)

  ////   lstrip (LStrip)
  class LStrip(val pos: Option[String] = None) extends LibFcn with Function2[String, String, String] {
    def name = prefix + "lstrip"
    def sig = Sig(List("s" -> P.String, "chars" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Remove any characters found in <p>chars</p> from the beginning (left) of <p>s</p>.</desc>{irrelevantOrder}
      </doc>
    def errcodeBase = 39170
    def apply(s: String, chars: String): String = {
      var index = 0
      while (index < s.length  &&  chars.contains(s(index)))
        index += 1
      s.substring(index)
    }
  }
  provide(new LStrip)

  ////   rstrip (RStrip)
  class RStrip(val pos: Option[String] = None) extends LibFcn with Function2[String, String, String] {
    def name = prefix + "rstrip"
    def sig = Sig(List("s" -> P.String, "chars" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Remove any characters found in <p>chars</p> from the end (right) of <p>s</p>.</desc>{irrelevantOrder}
      </doc>
    def errcodeBase = 39180
    def apply(s: String, chars: String): String = {
      var index = s.length
      while (index > 0  &&  chars.contains(s(index - 1)))
        index -= 1
      if (index >= 0)
        s.substring(0, index)
      else
        ""
    }
  }
  provide(new RStrip)

  ////   strip (Strip)
  class Strip(val pos: Option[String] = None) extends LibFcn with Function2[String, String, String] {
    def name = prefix + "strip"
    def sig = Sig(List("s" -> P.String, "chars" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Remove any characters found in <p>chars</p> from the beginning or end of <p>s</p>.</desc>{irrelevantOrder}
      </doc>
    def errcodeBase = 39190
    def apply(s: String, chars: String): String = {
      var startIndex = 0
      while (startIndex < s.length  &&  chars.contains(s(startIndex)))
        startIndex += 1

      var endIndex = s.length
      while (endIndex > 0  &&  chars.contains(s(endIndex - 1)))
        endIndex -= 1

      if (startIndex < s.length  &&  endIndex >= 0)
        s.substring(startIndex, endIndex)
      else
        ""
    }
  }
  provide(new Strip)

  ////   replaceall (ReplaceAll)
  class ReplaceAll(val pos: Option[String] = None) extends LibFcn with Function3[String, String, String, String] {
    def name = prefix + "replaceall"
    def sig = Sig(List("s" -> P.String, "original" -> P.String, "replacement" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Replace every instance of the substring <p>original</p> from <p>s</p> with <p>replacement</p>.</desc>
      </doc>
    def errcodeBase = 39200
    def apply(s: String, original: String, replacement: String): String = s.replace(original, replacement)
  }
  provide(new ReplaceAll)

  ////   replacefirst (ReplaceFirst)
  class ReplaceFirst(val pos: Option[String] = None) extends LibFcn with Function3[String, String, String, String] {
    def name = prefix + "replacefirst"
    def sig = Sig(List("s" -> P.String, "original" -> P.String, "replacement" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Replace the first (leftmost) instance of the substring <p>original</p> from <p>s</p> with <p>replacement</p>.</desc>
      </doc>
    def errcodeBase = 39210
    def apply(s: String, original: String, replacement: String): String = s.indexOf(original) match {
      case -1 => s
      case where => s.substring(0, where) + replacement + s.substring(where + original.length)
    }
  }
  provide(new ReplaceFirst)

  ////   replacelast (ReplaceLast)
  class ReplaceLast(val pos: Option[String] = None) extends LibFcn with Function3[String, String, String, String] {
    def name = prefix + "replacelast"
    def sig = Sig(List("s" -> P.String, "original" -> P.String, "replacement" -> P.String), P.String)
    def doc =
      <doc>
        <desc>Replace the last (rightmost) instance of the substring <p>original</p> from <p>s</p> with <p>replacement</p>.</desc>
      </doc>
    def errcodeBase = 39220
    def apply(s: String, original: String, replacement: String): String = s.lastIndexOf(original) match {
      case -1 => s
      case where => s.substring(0, where) + replacement + s.substring(where + original.length)
    }
  }
  provide(new ReplaceLast)

  ////   translate (Translate)
  class Translate(val pos: Option[String] = None) extends LibFcn with Function3[String, String, String, String] {
    def name = prefix + "translate"
    def sig = Sig(List("s" -> P.String, "oldchars" -> P.String, "newchars" -> P.String), P.String)
    def doc =
      <doc>
        <desc>For each character in <p>s</p> that is also in <p>oldchars</p> with some index <c>i</c>, replace it with the character at index <c>i</c> in <p>newchars</p>.  Any character in <p>s</p> that is not in <p>oldchars</p> is unchanged.  Any index <c>i</c> that is greater than the length of <p>newchars</p> is replaced with nothing.</desc>
        <detail>This is the behavior of the the Posix command <c>tr</c>, where <p>s</p> takes the place of standard input and <p>oldchars</p> and <p>newchars</p> are the <c>tr</c> commandline options.</detail>
      </doc>
    def errcodeBase = 39230
    def apply(s: String, oldchars: String, newchars: String): String = {
      val stringBuilder = new StringBuilder
      for (c <- s) oldchars.indexOf(c) match {
        case -1 => stringBuilder.append(c)
        case i if (i < newchars.length) => stringBuilder.append(newchars(i))
        case _ =>
      }
      stringBuilder.toString
    }
  }
  provide(new Translate)
}
