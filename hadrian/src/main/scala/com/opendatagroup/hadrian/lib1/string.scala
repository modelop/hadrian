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

import scala.annotation.tailrec

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

import com.opendatagroup.hadrian.lib1.array.startEnd
import com.opendatagroup.hadrian.lib1.array.describeIndex

package object string {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "s."

  val irrelevantOrder = <detail>The order of characters in <p>chars</p> is irrelevant.</detail>

  //////////////////////////////////////////////////////////////////// basic access

  ////   len (Len)
  object Len extends LibFcn {
    val name = prefix + "len"
    val sig = Sig(List("s" -> P.String), P.Int)
    val doc =
      <doc>
        <desc>Return the length of string <p>s</p>.</desc>
      </doc>
    def apply(s: String): Int = s.length
  }
  provide(Len)

  ////   substr (Substr)
  object Substr extends LibFcn {
    val name = prefix + "substr"
    val sig = Sig(List("s" -> P.String, "start" -> P.Int, "end" -> P.Int), P.String)
    val doc =
      <doc>
        <desc>Return the substring of <p>s</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive).</desc>{describeIndex}
      </doc>
    def apply(s: String, start: Int, end: Int): String = {
      val (normStart, normEnd) = startEnd(s.length, start, end)
      s.substring(normStart, normEnd)
    }
  }
  provide(Substr)

  ////   substrto (SubstrTo)
  object SubstrTo extends LibFcn {
    val name = prefix + "substrto"
    val sig = Sig(List("s" -> P.String, "start" -> P.Int, "end" -> P.Int, "replacement" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Replace <p>s</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) with <p>replacement</p>.</desc>{describeIndex}
      </doc>
    def apply(s: String, start: Int, end: Int, replacement: String): String = {
      val (normStart, normEnd) = startEnd(s.length, start, end)
      s.substring(0, normStart) + replacement + s.substring(normEnd, s.length)
    }
  }
  provide(SubstrTo)

  //////////////////////////////////////////////////////////////////// searching

  ////   contains (Contains)
  object Contains extends LibFcn {
    val name = prefix + "contains"
    val sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>haystack</p> contains <p>needle</p>, <c>false</c> otherwise.</desc>
      </doc>
    def apply(haystack: String, needle: String): Boolean =
      haystack.indexOf(needle) != -1
  }
  provide(Contains)

  ////   count (Count)
  object Count extends LibFcn {
    val name = prefix + "count"
    val sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Int)
    val doc =
      <doc>
        <desc>Count the number of times <p>needle</p> appears in <p>haystack</p>.</desc>
      </doc>
    def apply(haystack: String, needle: String): Int = count(haystack, needle, 0)

    @tailrec
    def count(haystack: String, needle: String, sofar: Int): Int = haystack.indexOf(needle) match {
      case -1 => sofar
      case where => count(haystack.substring(where + needle.length), needle, sofar + 1)
    }
  }
  provide(Count)

  ////   index (Index)
  object Index extends LibFcn {
    val name = prefix + "index"
    val sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Int)
    val doc =
      <doc>
        <desc>Return the lowest index where <p>haystack</p> contains <p>needle</p> or -1 if <p>haystack</p> does not contain <p>needle</p>.</desc>
      </doc>
    def apply(haystack: String, needle: String): Int = haystack.indexOf(needle)
  }
  provide(Index)

  ////   rindex (RIndex)
  object RIndex extends LibFcn {
    val name = prefix + "rindex"
    val sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Int)
    val doc =
      <doc>
        <desc>Return the highest index where <p>haystack</p> contains <p>needle</p> or -1 if <p>haystack</p> does not contain <p>needle</p>.</desc>
      </doc>
    def apply(haystack: String, needle: String): Int = haystack.lastIndexOf(needle)
  }
  provide(RIndex)

  ////   startswith (StartsWith)
  object StartsWith extends LibFcn {
    val name = prefix + "startswith"
    val sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if the first (leftmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def apply(haystack: String, needle: String): Boolean = haystack.startsWith(needle)
  }
  provide(StartsWith)

  ////   endswith (EndsWith)
  object EndsWith extends LibFcn {
    val name = prefix + "endswith"
    val sig = Sig(List("haystack" -> P.String, "needle" -> P.String), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if the last (rightmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def apply(haystack: String, needle: String): Boolean = haystack.endsWith(needle)
  }
  provide(EndsWith)

  //////////////////////////////////////////////////////////////////// conversions to/from other types

  ////   join (Join)
  object Join extends LibFcn {
    val name = prefix + "join"
    val sig = Sig(List("array" -> P.Array(P.String), "sep" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Combine strings from <p>array</p> into a single string, delimited by <p>sep</p>.</desc>
      </doc>
    def apply(array: PFAArray[String], sep: String): String = array.toVector.mkString(sep)
  }
  provide(Join)

  ////   split (Split)
  object Split extends LibFcn {
    val name = prefix + "split"
    val sig = Sig(List("s" -> P.String, "sep" -> P.String), P.Array(P.String))
    val doc =
      <doc>
        <desc>Divide a string into an array of substrings, splitting at and removing delimiters <p>sep</p>.</desc>
        <detail>If <p>s</p> does not contain <p>sep</p>, this function returns an array whose only element is <p>s</p>.  If <p>sep</p> appears at the beginning or end of <p>s</p>, the array begins with or ends with an empty string.  These conventions match Python's behavior. </detail>
      </doc>
    def apply(s: String, sep: String): PFAArray[String] = {
      val out = PFAArray.empty[String](1)
      split(s, sep, out)
      out
    }

    @tailrec
    def split(remaining: String, sep: String, out: PFAArray[String]): Unit = remaining.indexOf(sep) match {
      case -1 => out.add(remaining)
      case where => {
        out.add(remaining.substring(0, where))
        split(remaining.substring(where + sep.length), sep, out)
      }
    }
  }
  provide(Split)

  ////   number (Number)
  object Number extends LibFcn {
    val name = prefix + "number"
    val sig = Sigs(List(Sig(List("x" -> P.Long), P.String),
                        Sig(List("x" -> P.Long, "width" -> P.Int, "zeroPad" -> P.Boolean), P.String),
                        Sig(List("x" -> P.Double, "width" -> P.Union(List(P.Int, P.Null)), "precision" -> P.Union(List(P.Int, P.Null))), P.String),
                        Sig(List("x" -> P.Double, "width" -> P.Union(List(P.Int, P.Null)), "precision" -> P.Union(List(P.Int, P.Null)), "minNoExp" -> P.Double, "maxNoExp" -> P.Double), P.String)))
    val doc =
      <doc>
        <desc>Format a number as a string.</desc>
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
        <error>If <p>width</p> is negative and <p>zeroPad</p> is <c>true</c>, a "negative width cannot be used with zero-padding" error is raised.</error>
        <error>If <p>precision</p> is provided and is less than zero, a "negative precision" error is raised.</error>
      </doc>

    def apply(x: Long): String = "%d".format(x)

    def apply(x: Long, width: Int, zeroPad: Boolean): String = {
      val formatStr = (width, zeroPad) match {
        case (w, false) => "%" + w.toString + "d"
        case (w, true) if (w < 0) => throw new PFARuntimeException("negative width cannot be used with zero-padding")
        case (w, true) => "%0" + w.toString + "d"
      }
      formatStr.format(x)
    }

    def pad(width: java.lang.Integer, str: String): String = width match {
      case null         => str
      case w if (w < 0) => str + Array.fill(-w.intValue - str.size)(' ').mkString
      case w            => Array.fill(w.intValue - str.size)(' ').mkString + str
    }

    def apply(x: Double, width: java.lang.Integer, precision: java.lang.Integer): String = apply(x, width, precision, 0.0001, 100000)

    def apply(x: Double, width: java.lang.Integer, precision: java.lang.Integer, minNoExp: Double, maxNoExp: Double): String = {
      val widthStr = width match {
        case null => ""
        case w if (w < 0) => (w.intValue + 1).toString
        case w => w.intValue.toString
      }

      val precisionStr = precision match {
        case null => ""
        case p if (p < 0) => throw new PFARuntimeException("negative precision")
        case p => "." + p.intValue.toString
      }

      x match {
        case y if (java.lang.Double.isInfinite(y)) => if (y > 0.0) pad(width, "inf") else pad(width, "-inf")
        case y if (java.lang.Double.isNaN(y)) => pad(width, "nan")
        case y => {
          var neg = y < 0.0
          val v = Math.abs(y)

          val conv = if (v >= minNoExp  &&  v <= maxNoExp) "f" else "e"

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
  provide(Number)

  //////////////////////////////////////////////////////////////////// conversions to/from other strings

  ////   concat (Concat)
  object Concat extends LibFcn {
    val name = prefix + "concat"
    val sig = Sig(List("x" -> P.String, "y" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Append <p>y</p> to <p>x</p> to form a single string.</desc>
        <detail>To concatenate an array of strings, use {prefix}join with an empty string as <p>sep</p>.</detail>
      </doc>
    def apply(x: String, y: String): String = x + y
  }
  provide(Concat)

  ////   repeat (Repeat)
  object Repeat extends LibFcn {
    val name = prefix + "repeat"
    val sig = Sig(List("s" -> P.String, "n" -> P.Int), P.String)
    val doc =
      <doc>
        <desc>Create a string by concatenating <p>s</p> with itself <p>n</p> times.</desc>
      </doc>
    def apply(s: String, n: Int): String = Array.fill(n)(s).mkString
  }
  provide(Repeat)

  ////   lower (Lower)
  object Lower extends LibFcn {
    val name = prefix + "lower"
    val sig = Sig(List("s" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Convert <p>s</p> to lower-case.</desc>
      </doc>
    def apply(s: String): String = s.toLowerCase
  }
  provide(Lower)

  ////   upper (Upper)
  object Upper extends LibFcn {
    val name = prefix + "upper"
    val sig = Sig(List("s" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Convert <p>s</p> to upper-case.</desc>
      </doc>
    def apply(s: String): String = s.toUpperCase
  }
  provide(Upper)

  ////   lstrip (LStrip)
  object LStrip extends LibFcn {
    val name = prefix + "lstrip"
    val sig = Sig(List("s" -> P.String, "chars" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Remove any characters found in <p>chars</p> from the beginning (left) of <p>s</p>.</desc>{irrelevantOrder}
      </doc>
    def apply(s: String, chars: String): String = {
      var index = 0
      while (index < s.length  &&  chars.contains(s(index)))
        index += 1
      s.substring(index)
    }
  }
  provide(LStrip)

  ////   rstrip (RStrip)
  object RStrip extends LibFcn {
    val name = prefix + "rstrip"
    val sig = Sig(List("s" -> P.String, "chars" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Remove any characters found in <p>chars</p> from the end (right) of <p>s</p>.</desc>{irrelevantOrder}
      </doc>
    def apply(s: String, chars: String): String = {
      var index = s.length
      while (index >= 0  &&  chars.contains(s(index - 1)))
        index -= 1
      s.substring(0, index)
    }
  }
  provide(RStrip)

  ////   strip (Strip)
  object Strip extends LibFcn {
    val name = prefix + "strip"
    val sig = Sig(List("s" -> P.String, "chars" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Remove any characters found in <p>chars</p> from the beginning or end of <p>s</p>.</desc>{irrelevantOrder}
      </doc>
    def apply(s: String, chars: String): String = {
      var startIndex = 0
      while (startIndex < s.length  &&  chars.contains(s(startIndex)))
        startIndex += 1

      var endIndex = s.length
      while (endIndex >= 0  &&  chars.contains(s(endIndex - 1)))
        endIndex -= 1
      s.substring(startIndex, endIndex)
    }
  }
  provide(Strip)

  ////   replaceall (ReplaceAll)
  object ReplaceAll extends LibFcn {
    val name = prefix + "replaceall"
    val sig = Sig(List("s" -> P.String, "original" -> P.String, "replacement" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Replace every instance of the substring <p>original</p> from <p>s</p> with <p>replacement</p>.</desc>
      </doc>
    def apply(s: String, original: String, replacement: String): String = s.replace(original, replacement)
  }
  provide(ReplaceAll)

  ////   replacefirst (ReplaceFirst)
  object ReplaceFirst extends LibFcn {
    val name = prefix + "replacefirst"
    val sig = Sig(List("s" -> P.String, "original" -> P.String, "replacement" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Replace the first (leftmost) instance of the substring <p>original</p> from <p>s</p> with <p>replacement</p>.</desc>
      </doc>
    def apply(s: String, original: String, replacement: String): String = s.indexOf(original) match {
      case -1 => s
      case where => s.substring(0, where) + replacement + s.substring(where + original.length)
    }
  }
  provide(ReplaceFirst)

  ////   replacelast (ReplaceLast)
  object ReplaceLast extends LibFcn {
    val name = prefix + "replacelast"
    val sig = Sig(List("s" -> P.String, "original" -> P.String, "replacement" -> P.String), P.String)
    val doc =
      <doc>
        <desc>Replace the last (rightmost) instance of the substring <p>original</p> from <p>s</p> with <p>replacement</p>.</desc>
      </doc>
    def apply(s: String, original: String, replacement: String): String = s.lastIndexOf(original) match {
      case -1 => s
      case where => s.substring(0, where) + replacement + s.substring(where + original.length)
    }
  }
  provide(ReplaceLast)

  ////   translate (Translate)
  object Translate extends LibFcn {
    val name = prefix + "translate"
    val sig = Sig(List("s" -> P.String, "oldchars" -> P.String, "newchars" -> P.String), P.String)
    val doc =
      <doc>
        <desc>For each character in <p>s</p> that is also in <p>oldchars</p> with some index <c>i</c>, replace it with the character at index <c>i</c> in <p>newchars</p>.  Any character in <p>s</p> that is not in <p>oldchars</p> is unchanged.  Any index <c>i</c> that is greater than the length of <p>newchars</p> is replaced with nothing.</desc>
        <detail>This is the behavior of the the Posix command <c>tr</c>, where <p>s</p> takes the place of standard input and <p>oldchars</p> and <p>newchars</p> are the <c>tr</c> commandline options.</detail>
      </doc>
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
  provide(Translate)
}
