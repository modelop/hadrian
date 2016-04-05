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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.language.postfixOps
import scala.util.Random

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.NumericalLT
import com.opendatagroup.hadrian.data.ComparisonOperatorLT

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroArray

package object array {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "a."

  val anyNumber = Set[Type](AvroInt(), AvroLong(), AvroFloat(), AvroDouble())

  def negativeIndex(length: Int, index: Int): Int =
    if (index >= 0) index else length + index

  def checkRange(length: Int, index: Int, code: Int, fcnName: String, pos: Option[String]): Unit =
    if (index < 0  ||  index >= length)
      throw new PFARuntimeException("index out of range", code, fcnName, pos)

  def startEnd(length: Int, start: Int, end: Int): (Int, Int) = {
    var normStart = if (start >= 0) start else length + start
    if (normStart < 0)
      normStart = 0
    if (normStart > length)
      normStart = length

    var normEnd = if (end >= 0) end else length + end
    if (normEnd < 0)
      normEnd = 0
    if (normEnd > length)
      normEnd = length

    if (normEnd < normStart)
      normEnd = normStart

    (normStart, normEnd)
  }

  val describeOneIndex = <detail>Negative indexes count from the right (-1 is just before the last item), following Python's index behavior.</detail>
  val describeIndex = <detail>Negative indexes count from the right (-1 is just before the last item), indexes beyond the legal range are truncated, and <p>end</p> {"\u2264"} <p>start</p> specifies a zero-length subsequence just before the <p>start</p> character.  All of these rules follow Python's slice behavior.</detail>
  def sideEffectFree(exceptions: String = "") = <detail>Note: <p>a</p> is not changed in-place; this is a side-effect-free function{exceptions}.</detail>
  def takesFirst(minmax: String) = <detail>If the {minmax}imum is not unique, this function returns the index of the first {minmax}imal value.</detail>
  val sortsUnique = <detail>If any values are not unique, their indexes will be returned in ascending order.</detail>
  val orderNotGuaranteed = <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it will be called exactly once for each element.</detail>
  val describeTimeout = <detail>This function scales rapidly with the length of the array.  For reasonably large arrays, it will result in timeout exceptions.</detail>

  def errorOnEmpty(code: Int) = <error code={s"${code}"}>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
  def errorOnOutOfRange(code: Int) = <error code={s"${code}"}>If <p>index</p> is beyond the range of <p>a</p>, an "index out of range" runtime error is raised.</error>
  def errorOnNegativeN(code: Int) = <error code={s"${code}"}>If <p>n</p> is negative, an {""""n < 0""""} runtime error is raised.</error>

  //////////////////////////////////////////////////////////////////// basic access

  ////   len (Len)
  class Len(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "len"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Int)
    def doc =
      <doc>
        <desc>Return the length of array <p>a</p>.</desc>
      </doc>
    def errcodeBase = 15000
    def apply[X](a: PFAArray[X]): Int = a.toVector.size
  }
  provide(new Len)

  ////   subseq (Subseq)
  class Subseq(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "subseq"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "start" -> P.Int, "end" -> P.Int), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the subsequence of <p>a</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive).</desc>{describeIndex}
      </doc>
    def errcodeBase = 15010
    def apply[X](a: PFAArray[X], start: Int, end: Int): PFAArray[X] = {
      val (normStart, normEnd) = startEnd(a.toVector.size, start, end)
      PFAArray.fromVector(a.toVector.slice(normStart, normEnd))
    }
  }
  provide(new Subseq)

  ////   head (Head)
  class Head(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "head"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the first item of the array.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def errcodeBase = 15020
    def apply[X](a: PFAArray[X]): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        a.toVector.head
  }
  provide(new Head)

  ////   tail (Tail)
  class Tail(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "tail"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return all but the first item of the array.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def errcodeBase = 15030
    def apply[X](a: PFAArray[X]): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        PFAArray.fromVector(a.toVector.tail)
  }
  provide(new Tail)

  ////   last (Last)
  class Last(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "last"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the last item of the array.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def errcodeBase = 15040
    def apply[X](a: PFAArray[X]): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        a.toVector.last
  }
  provide(new Last)

  ////   init (Init)
  class Init(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "init"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return all but the last item of the array.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def errcodeBase = 15050
    def apply[X](a: PFAArray[X]): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        PFAArray.fromVector(a.toVector.init)
  }
  provide(new Init)

  ////   subseqto (SubseqTo)
  class SubseqTo(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "subseqto"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "start" -> P.Int, "end" -> P.Int, "replacement" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new array by replacing <p>a</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) with <p>replacement</p>.</desc>{describeIndex}{sideEffectFree()}
      </doc>
    def errcodeBase = 15060
    def apply[X](a: PFAArray[X], start: Int, end: Int, replacement: PFAArray[X]): PFAArray[X] = {
      val (normStart, normEnd) = startEnd(a.toVector.size, start, end)
      PFAArray.fromVector(a.toVector.patch(normStart, replacement.toVector, normEnd - normStart))
    }
  }
  provide(new SubseqTo)

  //////////////////////////////////////////////////////////////////// searching

  ////   contains (Contains)
  class Contains(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "contains"
    def sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>haystack</p> contains <p>needle</p> or the <p>needle</p> function evaluates to <c>true</c>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 15070
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Boolean =
      haystack.toVector.indexOfSlice(needle.toVector) != -1
    def apply[X](haystack: PFAArray[X], needle: X): Boolean =
      haystack.toVector.indexOf(needle) != -1
    def apply[X](haystack: PFAArray[X], needle: (X => Boolean)): Boolean =
      haystack.toVector.find(needle) != None
  }
  provide(new Contains)

  ////   count (Count)
  class Count(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "count"
    def sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Int)))
    def doc =
      <doc>
        <desc>Count the number of times <p>needle</p> appears in <p>haystack</p> or the number of times the <p>needle</p> function evaluates to <c>true</c>.</desc>
        <detail>If the <p>needle</p> is an empty array, the result is zero.</detail>
      </doc>
    def errcodeBase = 15080

    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Int =
      if (haystack.toVector.isEmpty)
        0
      else if (needle.toVector.isEmpty)
        0
      else
        count(haystack.toVector, needle.toVector, 0)

    def apply[X](haystack: PFAArray[X], needle: X): Int =
      if (haystack.toVector.isEmpty)
        0
      else
        count(haystack.toVector, needle, 0)

    def apply[X](haystack: PFAArray[X], needle: X => Boolean): Int =
      if (haystack.toVector.isEmpty)
        0
      else
        haystack.toVector.count(needle)

    @tailrec
    final def count[X](haystack: Vector[X], needle: Vector[X], sofar: Int): Int = haystack.indexOfSlice(needle) match {
      case -1 => sofar
      case where => count(haystack.slice(where + needle.size, haystack.size), needle, sofar + 1)
    }
    @tailrec
    final def count[X](haystack: Vector[X], needle: X, sofar: Int): Int = haystack.indexOf(needle) match {
      case -1 => sofar
      case where => count(haystack.slice(where + 1, haystack.size), needle, sofar + 1)
    }
  }
  provide(new Count)

  ////   index (Index)
  class Index(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "index"
    def sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Int)))
    def doc =
      <doc>
        <desc>Return the lowest index where <p>haystack</p> contains <p>needle</p> or the <p>needle</p> function evaluates to <c>true</c>, <m>-1</m> if there is no such element.</desc>
      </doc>
    def errcodeBase = 15090
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Int = haystack.toVector.indexOfSlice(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Int = haystack.toVector.indexOf(needle)
    def apply[X](haystack: PFAArray[X], needle: (X => Boolean)): Int = haystack.toVector.indexWhere(needle)
  }
  provide(new Index)

  ////   rindex (RIndex)
  class RIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "rindex"
    def sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Int)))
    def doc =
      <doc>
        <desc>Return the highest index where <p>haystack</p> contains <p>needle</p> or the <p>needle</p> function evaluates to <c>true</c>, <m>-1</m> if there is no such element.</desc>
      </doc>
    def errcodeBase = 15100
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Int = haystack.toVector.lastIndexOfSlice(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Int = haystack.toVector.lastIndexOf(needle)
    def apply[X](haystack: PFAArray[X], needle: (X => Boolean)): Int = haystack.toVector.lastIndexWhere(needle)
  }
  provide(new RIndex)

  ////   startswith (StartsWith)
  class StartsWith(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "startswith"
    def sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Boolean)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if the first (leftmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def errcodeBase = 15110
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Boolean = haystack.toVector.startsWith(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Boolean = haystack.toVector.headOption match {
      case Some(x) => x == needle
      case None => false
    }
  }
  provide(new StartsWith)

  ////   endswith (EndsWith)
  class EndsWith(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "endswith"
    def sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Boolean)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if the last (rightmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def errcodeBase = 15120
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Boolean = haystack.toVector.endsWith(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Boolean = haystack.toVector.lastOption match {
      case Some(x) => x == needle
      case None => false
    }
  }
  provide(new EndsWith)

  //////////////////////////////////////////////////////////////////// manipulation

  ////   concat (Concat)
  class Concat(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "concat"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Concatenate <p>a</p> and <p>b</p> to make a new array of the same type.</desc>
        <detail>The length of the returned array is the sum of the lengths of <p>a</p> and <p>b</p>.</detail>
      </doc>
    def errcodeBase = 15130
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector ++ b.toVector)
  }
  provide(new Concat)

  ////   append (Append)
  class Append(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "append"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "item" -> P.Wildcard("A")), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new array by adding <p>item</p> at the end of <p>a</p>.</desc>{sideEffectFree()}
        <detail>The length of the returned array is one more than <p>a</p>.</detail>
      </doc>
    def errcodeBase = 15140
    def apply[X](a: PFAArray[X], item: X): PFAArray[X] = PFAArray.fromVector(a.toVector :+ item)
  }
  provide(new Append)

  ////   cycle (Cycle)
  class Cycle(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "cycle"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "item" -> P.Wildcard("A"), "maxLength" -> P.Int), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new array by adding <p>item</p> at the end of <p>a</p>, but keep the length less than or equal to <p>maxLength</p> by removing items from the beginning.</desc>{sideEffectFree()}
        <error code={s"${errcodeBase + 0}"}>If <p>maxLength</p> is less than 0, this function raises a "maxLength out of range" error.</error>
      </doc>
    def errcodeBase = 15150
    def apply[X](a: PFAArray[X], item: X, maxLength: Int): PFAArray[X] = {
      if (maxLength < 0)
        throw new PFARuntimeException("maxLength out of range", errcodeBase + 0, name, pos)
      val out = a.toVector :+ item
      PFAArray.fromVector(out.takeRight(maxLength))
    }
  }
  provide(new Cycle)

  ////   insert (Insert)
  class Insert(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "insert"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "index" -> P.Int, "item" -> P.Wildcard("A")), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new array by inserting <p>item</p> at <p>index</p> of <p>a</p>.</desc>{describeOneIndex}{sideEffectFree()}{errorOnOutOfRange(errcodeBase + 0)}
        <detail>The length of the returned array is one more than <p>a</p>.</detail>
      </doc>
    def errcodeBase = 15160
    def apply[X](a: PFAArray[X], index: Int, item: X): PFAArray[X] = {
      val normIndex = negativeIndex(a.toVector.size, index)
      checkRange(a.toVector.size, normIndex, errcodeBase + 0, name, pos)
      PFAArray.fromVector(a.toVector.patch(normIndex, Vector(item), 0))
    }
  }
  provide(new Insert)

  ////   replace (Replace)
  class Replace(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "replace"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "index" -> P.Int, "item" -> P.Wildcard("A")), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new array by replacing <p>index</p> of <p>a</p> with <p>item</p>.</desc>{describeOneIndex}{sideEffectFree()}{errorOnOutOfRange(errcodeBase + 0)}
        <detail>The length of the returned array is equal to that of <p>a</p>.</detail>
      </doc>
    def errcodeBase = 15170
    def apply[X](a: PFAArray[X], index: Int, item: X): PFAArray[X] = {
      val normIndex = negativeIndex(a.toVector.size, index)
      checkRange(a.toVector.size, normIndex, errcodeBase + 0, name, pos)
      PFAArray.fromVector(a.toVector.updated(normIndex, item))
    }
  }
  provide(new Replace)

  ////   remove (Remove)
  class Remove(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "remove"
    def sig = Sigs(List(Sig(List("a" -> P.Array(P.Wildcard("A")), "start" -> P.Int, "end" -> P.Int), P.Array(P.Wildcard("A"))),
                        Sig(List("a" -> P.Array(P.Wildcard("A")), "index" -> P.Int), P.Array(P.Wildcard("A")))))
    def doc =
      <doc>
        <desc>Return a new array by removing elements from <p>a</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) or just a single <p>index</p>.</desc>{describeIndex}{sideEffectFree()}{errorOnOutOfRange(errcodeBase + 0)}
        <detail>The length of the returned array is one less than <p>a</p>.</detail>
      </doc>
    def errcodeBase = 15180
    def apply[X](a: PFAArray[X], start: Int, end: Int): PFAArray[X] = {
      val (normStart, normEnd) = startEnd(a.toVector.size, start, end)
      PFAArray.fromVector(a.toVector.patch(normStart, Vector[X](), normEnd - normStart))
    }
    def apply[X](a: PFAArray[X], index: Int): PFAArray[X] = {
      val normIndex = negativeIndex(a.toVector.size, index)
      checkRange(a.toVector.size, normIndex, errcodeBase + 0, name, pos)
      PFAArray.fromVector(a.toVector.patch(normIndex, Vector[X](), 1))
    }
  }
  provide(new Remove)

  ////   rotate (Rotate)
  class Rotate(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "rotate"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "steps" -> P.Int), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array formed by rotating <p>a</p> left <p>steps</p> spaces.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>steps</p> is less than zero, a "steps out of range" error is raised.</error>
      </doc>
    def errcodeBase = 15190
    def apply[X](a: PFAArray[X], steps: Int): PFAArray[X] = {
      if (steps < 0)
        throw new PFARuntimeException("steps out of range", errcodeBase + 0, name, pos)
      val vector = a.toVector
      if (vector.isEmpty)
        a
      else {
        val (left, right) = vector.splitAt(steps % vector.size)
        PFAArray.fromVector(right ++ left)
      }
    }
  }
  provide(new Rotate)

  //////////////////////////////////////////////////////////////////// reordering

  ////   sort (Sort)
  object Sort {
    class SorterInt {
      def apply(a: PFAArray[Int]): PFAArray[Int] = PFAArray.fromVector(a.toVector.sorted)
    }
    class SorterLong {
      def apply(a: PFAArray[Long]): PFAArray[Long] = PFAArray.fromVector(a.toVector.sorted)
    }
    class SorterFloat {
      def apply(a: PFAArray[Float]): PFAArray[Float] = PFAArray.fromVector(a.toVector.sortWith((x: Float, y: Float) => NumericalLT.apply(x, y)))
    }
    class SorterDouble {
      def apply(a: PFAArray[Double]): PFAArray[Double] = PFAArray.fromVector(a.toVector.sortWith((x: Double, y: Double) => NumericalLT.apply(x, y)))
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.sortWith(comparisonOperatorLT))
    }
  }
  class Sort(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "sort"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array with the same elements as <p>a</p> but in ascending order (as defined by Avro's sort order).</desc>{sideEffectFree()}
      </doc>
    def errcodeBase = 15200
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Sort)

  ////   sortLT (SortLT)
  class SortLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "sortLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array with the same elements as <p>a</p> but in ascending order as defined by the <p>lessThan</p> function.</desc>{sideEffectFree()}
      </doc>
    def errcodeBase = 15210
    def apply[X](a: PFAArray[X], lt: (X, X) => java.lang.Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.sortWith((x: X, y: X) => lt(x, y).booleanValue))
  }
  provide(new SortLT)

  ////   shuffle (Shuffle)
  object Shuffle {
    class Sorter(randomGenerator: Random) {
      def apply[X](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(randomGenerator.shuffle(a.toVector))
    }
  }
  class Shuffle(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "shuffle"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array with the same elements as <p>a</p> but in a random order.</desc>
        {sideEffectFree(" (except for updating the random number generator)")}
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = 15220
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Sorter(randomGenerator()))")
  }
  provide(new Shuffle)

  ////   reverse (Reverse)
  class Reverse(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "reverse"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the elements of <p>a</p> in reversed order.</desc>
      </doc>
    def errcodeBase = 15230
    def apply[X](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.reverse)
  }
  provide(new Reverse)

  //////////////////////////////////////////////////////////////////// extreme values

  def highestN[X](a: Vector[X], n: Int, lt: (X, X) => Boolean): Vector[X] = {
    var out = mutable.ListBuffer[X]()
    for (x <- a) {
      out.indexWhere(best => lt(best, x)) match {
        case -1 => out.append(x)
        case index => out.insert(index, x)
      }
      if (out.size > n)
        out = out.init
    }
    out.toVector
  }

  def lowestN[X](a: Vector[X], n: Int, lt: (X, X) => Boolean): Vector[X] = {
    var out = mutable.ListBuffer[X]()
    for (x <- a) {
      out.indexWhere(best => lt(x, best)) match {
        case -1 => out.append(x)
        case index => out.insert(index, x)
      }
      if (out.size > n)
        out = out.init
    }
    out.toVector
  }

  def argHighestN[X](a: Vector[X], n: Int, lt: (X, X) => Boolean): Vector[Int] = {
    var out = mutable.ListBuffer[(X, Int)]()
    for ((x, i) <- a.zipWithIndex) {
      out.indexWhere({case (bestx, besti) => lt(bestx, x)}) match {
        case -1 => out.append((x, i))
        case index => {
          var ind = index
          while (ind <= out.size  &&  !lt(out(ind)._1, x)  &&  !lt(x, out(ind)._1)  &&  out(ind)._2 > i)
            ind += 1
          out.insert(ind, (x, i))
        }
      }
      if (out.size > n)
        out = out.init
    }
    out map {_._2} toVector
  }

  def argLowestN[X](a: Vector[X], n: Int, lt: (X, X) => Boolean): Vector[Int] = {
    var out = mutable.ListBuffer[(X, Int)]()
    for ((x, i) <- a.zipWithIndex) {
      out.indexWhere({case (bestx, besti) => lt(x, bestx)}) match {
        case -1 => out.append((x, i))
        case index => {
          var ind = index
          while (ind <= out.size  &&  !lt(out(ind)._1, x)  &&  !lt(x, out(ind)._1)  &&  out(ind)._2 > i)
            ind += 1
          out.insert(ind, (x, i))
        }
      }
      if (out.size > n)
        out = out.init
    }
    out map {_._2} toVector
  }

  ////   max (Max)
  object Max {
    def name = prefix + "max"
    def errcodeBase = 15240
    class MaxInt(pos: Option[String]) {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.max
    }
    class MaxLong(pos: Option[String]) {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.max
    }
    class MaxFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.max
    }
    class MaxDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.max
    }
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          highestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  class Max(val pos: Option[String] = None) extends LibFcn {
    def name = Max.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the maximum value in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Max.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Max)

  ////   min (Min)
  object Min {
    def name = prefix + "min"
    def errcodeBase = 15250
    class MinInt(pos: Option[String]) {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.min
    }
    class MinLong(pos: Option[String]) {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.min
    }
    class MinFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.min
    }
    class MinDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          a.toVector.min
    }
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          lowestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  class Min(val pos: Option[String] = None) extends LibFcn {
    def name = Min.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the minimum value in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Min.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$MinInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$MinLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$MinFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$MinDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Min)

  ////   maxLT (MaxLT)
  class MaxLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "maxLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the maximum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 15260
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        highestN(a.toVector, 1, lt).head
  }
  provide(new MaxLT)

  ////   minLT (MinLT)
  class MinLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "minLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the minimum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 15270
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        lowestN(a.toVector, 1, lt).head
  }
  provide(new MinLT)

  ////   maxN (MaxN)
  object MaxN {
    def name = prefix + "maxN"
    def errcodeBase = 15280
    class MaxInt(pos: Option[String]) {
      def apply(a: PFAArray[Int], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Int, y: Int) => x < y))
    }
    class MaxLong(pos: Option[String]) {
      def apply(a: PFAArray[Long], n: Int): PFAArray[Long] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Long, y: Long) => x < y))
    }
    class MaxFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float], n: Int): PFAArray[Float] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Float, y: Float) => x < y))
    }
    class MaxDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double], n: Int): PFAArray[Double] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Double, y: Double) => x < y))
    }
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[X] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(highestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  class MaxN(val pos: Option[String] = None) extends LibFcn {
    def name = MaxN.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the <p>n</p> highest values in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = MaxN.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$MaxDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new MaxN)

  ////   minN (MinN)
  object MinN {
    def name = prefix + "minN"
    def errcodeBase = 15290
    class MinInt(pos: Option[String]) {
      def apply(a: PFAArray[Int], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Int, y: Int) => x < y))
    }
    class MinLong(pos: Option[String]) {
      def apply(a: PFAArray[Long], n: Int): PFAArray[Long] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Long, y: Long) => x < y))
    }
    class MinFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float], n: Int): PFAArray[Float] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Float, y: Float) => x < y))
    }
    class MinDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double], n: Int): PFAArray[Double] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Double, y: Double) => x < y))
    }
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[X] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(lowestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  class MinN(val pos: Option[String] = None) extends LibFcn {
    def name = MinN.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the <p>n</p> lowest values in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = MinN.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$MinInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$MinLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$MinFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$MinDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new MinN)

  ////   maxNLT (MaxNLT)
  class MaxNLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "maxNLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the <p>n</p> highest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = 15300
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else if (n < 0)
        throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
      else
        PFAArray.fromVector(highestN(a.toVector, n, lt))
  }
  provide(new MaxNLT)

  ////   minNLT (MinNLT)
  class MinNLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "minNLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the <p>n</p> lowest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = 15310
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else if (n < 0)
        throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
      else
        PFAArray.fromVector(lowestN(a.toVector, n, lt))
  }
  provide(new MinNLT)

  ////   argmax (Argmax)
  object Argmax {
    def name = prefix + "argmax"
    def errcodeBase = 15320
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          argHighestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  class Argmax(val pos: Option[String] = None) extends LibFcn {
    def name = Argmax.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Int)
    def doc =
      <doc>
        <desc>Return the index of the maximum value in <p>a</p> (as defined by Avro's sort order).</desc>{takesFirst("max")}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Argmax.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Argmax)

  ////   argmin (Argmin)
  object Argmin {
    def name = prefix + "argmin"
    def errcodeBase = 15330
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          argLowestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  class Argmin(val pos: Option[String] = None) extends LibFcn {
    def name = Argmin.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Int)
    def doc =
      <doc>
        <desc>Return the index of the minimum value in <p>a</p> (as defined by Avro's sort order).</desc>{takesFirst("min")}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Argmin.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Argmin)

  ////   argmaxLT (ArgmaxLT)
  class ArgmaxLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argmaxLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Int)
    def doc =
      <doc>
        <desc>Return the index of the maximum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{takesFirst("max")}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 15340
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): Int =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        argHighestN(a.toVector, 1, lt).head
  }
  provide(new ArgmaxLT)

  ////   argminLT (ArgminLT)
  class ArgminLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argminLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Int)
    def doc =
      <doc>
        <desc>Return the index of the minimum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{takesFirst("min")}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 15350
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): Int =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        argLowestN(a.toVector, 1, lt).head
  }
  provide(new ArgminLT)

  ////   argmaxN (ArgmaxN)
  object ArgmaxN {
    def name = prefix + "argmaxN"
    def errcodeBase = 15360
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(argHighestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  class ArgmaxN(val pos: Option[String] = None) extends LibFcn {
    def name = ArgmaxN.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Int))
    def doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> highest values in <p>a</p> (as defined by Avro's sort order).</desc>{sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = ArgmaxN.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new ArgmaxN)

  ////   argminN (ArgminN)
  object ArgminN {
    def name = prefix + "argminN"
    def errcodeBase = 15370
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(argLowestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  class ArgminN(val pos: Option[String] = None) extends LibFcn {
    def name = ArgminN.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Int))
    def doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> lowest values in <p>a</p> (as defined by Avro's sort order).</desc>{sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = ArgminN.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new ArgminN)

  ////   argmaxNLT (ArgmaxNLT)
  class ArgmaxNLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argmaxNLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Int))
    def doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> highest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = 15380
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[Int] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else if (n < 0)
        throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
      else
        PFAArray.fromVector(argHighestN(a.toVector, n, lt))
  }
  provide(new ArgmaxNLT)

  ////   argminNLT (ArgminNLT)
  class ArgminNLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argminNLT"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Int))
    def doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> lowest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = 15390
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[Int] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else if (n < 0)
        throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
      else
        PFAArray.fromVector(argLowestN(a.toVector, n, lt))
  }
  provide(new ArgminNLT)

  //////////////////////////////////////////////////////////////////// numerical

  ////   sum (Sum)
  object Sum {
    def name = prefix + "sum"
    def errcodeBase = 15400
    class SumInt(pos: Option[String]) {
      def apply(a: PFAArray[Int]): Int =
        Math.floor(a.toVector.map(_.toDouble).sum) match {
          case x if (java.lang.Integer.MIN_VALUE <= x  &&  x <= java.lang.Integer.MAX_VALUE) =>
            x.toInt
          case _ =>
            throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
        }
    }
    class SumLong(pos: Option[String]) {
      def apply(a: PFAArray[Long]): Long =
        Math.floor(a.toVector.map(_.toDouble).sum) match {
          case x if (java.lang.Long.MIN_VALUE <= x  &&  x <= java.lang.Long.MAX_VALUE) =>
            x.toLong
          case _ =>
            throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
        }
    }
    class SumFloat {
      def apply(a: PFAArray[Float]): Float = a.toVector.sum
    }
    class SumDouble {
      def apply(a: PFAArray[Double]): Double = a.toVector.sum
    }
  }
  class Sum(val pos: Option[String] = None) extends LibFcn {
    def name = Sum.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A", oneOf = anyNumber))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the sum of numbers in <p>a</p>.</desc>
        <detail>Returns zero if the array is empty.</detail>
        <error code={s"${errcodeBase + 0}"}>If the array items have integer type and the final result is too large or small to be represented as an integer, an "int overflow" error is raised.</error>
        <error code={s"${errcodeBase + 1}"}>If the array items have long integer type and the final result is too large or small to be represented as a long integer, an "long overflow" error is raised.</error>
      </doc>
    def errcodeBase = Sum.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$SumInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$SumLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$SumFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$SumDouble())")
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Sum)

  ////   product (Product)
  object Product {
    def name = prefix + "product"
    def errcodeBase = 15410
    class ProductInt(pos: Option[String]) {
      def apply(a: PFAArray[Int]): Int =
        Math.floor(a.toVector.map(_.toDouble).product) match {
          case x if (java.lang.Integer.MIN_VALUE <= x  &&  x <= java.lang.Integer.MAX_VALUE) =>
            x.toInt
          case _ =>
            throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
        }
    }
    class ProductLong(pos: Option[String]) {
      def apply(a: PFAArray[Long]): Long =
        Math.floor(a.toVector.map(_.toDouble).product) match {
          case x if (java.lang.Long.MIN_VALUE <= x  &&  x <= java.lang.Long.MAX_VALUE) =>
            x.toLong
          case _ =>
            throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
        }
    }
    class ProductFloat {
      def apply(a: PFAArray[Float]): Float = a.toVector.product
    }
    class ProductDouble {
      def apply(a: PFAArray[Double]): Double = a.toVector.product
    }
  }
  class Product(val pos: Option[String] = None) extends LibFcn {
    def name = Product.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A", oneOf = anyNumber))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the product of numbers in <p>a</p>.</desc>
        <detail>Returns one if the array is empty.</detail>
        <error code={s"${errcodeBase + 0}"}>If the array items have integer type and the final result is too large or small to be represented as an integer, an "int overflow" error is raised.</error>
        <error code={s"${errcodeBase + 1}"}>If the array items have long integer type and the final result is too large or small to be represented as a long integer, an "long overflow" error is raised.</error>
      </doc>
    def errcodeBase = Product.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$ProductInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$ProductLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$ProductFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$ProductDouble())")
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Product)

  ////   lnsum (Lnsum)
  class Lnsum(val pos: Option[String] = None) extends LibFcn with Function1[PFAArray[java.lang.Number], Double] {
    def name = prefix + "lnsum"
    def sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    def doc =
      <doc>
        <desc>Return the sum of the natural logarithm of numbers in <p>a</p>.</desc>
        <detail>Returns zero if the array is empty and <c>NaN</c> if any value in the array is zero or negative.</detail>
      </doc>
    def errcodeBase = 15420
    def apply(a: PFAArray[java.lang.Number]): Double = a.toVector.map(x => java.lang.Math.log(x.doubleValue)).sum
  }
  provide(new Lnsum)

  ////   mean (Mean)
  class Mean(val pos: Option[String] = None) extends LibFcn with Function1[PFAArray[java.lang.Number], Double] {
    def name = prefix + "mean"
    def sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    def doc =
      <doc>
        <desc>Return the arithmetic mean of numbers in <p>a</p>.</desc>
        <detail>Returns <c>NaN</c> if the array is empty.</detail>
      </doc>
    def errcodeBase = 15430
    def apply(a: PFAArray[java.lang.Number]): Double = {
      val numer = a.toVector.map(_.doubleValue).sum
      val denom = a.toVector.size.toDouble
      numer / denom
    }
  }
  provide(new Mean)

  ////   geomean (GeoMean)
  class GeoMean(val pos: Option[String] = None) extends LibFcn with Function1[PFAArray[java.lang.Number], Double] {
    def name = prefix + "geomean"
    def sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    def doc =
      <doc>
        <desc>Return the geometric mean of numbers in <p>a</p>.</desc>
        <detail>Returns <c>NaN</c> if the array is empty.</detail>
      </doc>
    def errcodeBase = 15440
    def apply(a: PFAArray[java.lang.Number]): Double =
      Math.pow(a.toVector.map(_.doubleValue).product, 1.0/a.toVector.size)
  }
  provide(new GeoMean)

  ////   median (Median)
  object Median {
    def name = prefix + "median"
    def errcodeBase = 15450
    class SorterInt(pos: Option[String]) {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sorted)
          val half = a.toVector.size / 2
          if (sa.size % 2 == 1)
            sa(half)
          else
            sa(half - 1)
        }
    }
    class SorterLong(pos: Option[String]) {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sorted)
          val half = a.toVector.size / 2
          if (sa.size % 2 == 1)
            sa(half)
          else
            sa(half - 1)
        }
    }
    class SorterFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sortWith((x: Float, y: Float) => NumericalLT.apply(x, y)))
          if (a.toVector.size % 2 == 1)
            sa(a.toVector.size / 2)
          else
            (sa(a.toVector.size / 2 - 1) + sa(a.toVector.size / 2)) / 2.0f
        }
    }
    class SorterDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sortWith((x: Double, y: Double) => NumericalLT.apply(x, y)))
          if (a.toVector.size % 2 == 1)
            sa(a.toVector.size / 2)
          else
            (sa(a.toVector.size / 2 - 1) + sa(a.toVector.size / 2)) / 2.0
        }
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sortWith(comparisonOperatorLT))
          val half = a.toVector.size / 2
          if (sa.size % 2 == 1)
            sa(half)
          else
            sa(half - 1)
        }
    }
  }
  class Median(val pos: Option[String] = None) extends LibFcn {
    def name = Median.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the value that is in the center of a sorted version of <p>a</p>.</desc>
        <detail>If <p>a</p> has an odd number of elements, the median is the exact center of the sorted array.  If <p>a</p> has an even number of elements and is a <c>float</c> or <c>double</c>, the median is the average of the two elements closest to the center of the sorted array.  For any other type, the median is the left (first) of the two elements closest to the center of the sorted array.</detail>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Median.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Median)

  ////   ntile (NTile)
  object NTile {
    def name = prefix + "ntile"
    def errcodeBase = 15460
    class SorterInt(pos: Option[String]) {
      def apply(a: PFAArray[Int], p: Double): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (java.lang.Double.isNaN(p))
          throw new PFARuntimeException("p not a number", errcodeBase + 1, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sorted)
          if (p <= 0.0)
            return sa.toVector.head
          if (p >= 1.0)
            return sa.toVector.last
          val k = (sa.size - 1.0) * p
          val f = Math.floor(k)
          if (sa.size % 2 == 1)
            sa(f.toInt)
          else
            sa(k.toInt)
        }
    }
    class SorterLong(pos: Option[String]) {
      def apply(a: PFAArray[Long], p: Double): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (java.lang.Double.isNaN(p))
          throw new PFARuntimeException("p not a number", errcodeBase + 1, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sorted)
          if (p <= 0.0)
            return sa.toVector.head
          if (p >= 1.0)
            return sa.toVector.last
          val k = (sa.size - 1.0) * p
          val f = Math.floor(k)
          if (sa.size % 2 == 1)
            sa(f.toInt)
          else
            sa(k.toInt)
        }
    }
    class SorterFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float], p: Double): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (java.lang.Double.isNaN(p))
          throw new PFARuntimeException("p not a number", errcodeBase + 1, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sortWith((x: Float, y: Float) => NumericalLT.apply(x, y)))
          if (p <= 0.0)
            return sa.toVector.head
          if (p >= 1.0)
            return sa.toVector.last
          val k = (sa.size - 1.0)*p
          val f = Math.floor(k)
          val c = Math.ceil(k)
          if (f == c)
            return sa(k.toInt)
          val d0 = sa(f.toInt) * (c - k)
          val d1 = sa(c.toInt) * (k - f)
            (d0 + d1).toFloat
        }
    }
    class SorterDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double], p: Double): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (java.lang.Double.isNaN(p))
          throw new PFARuntimeException("p not a number", errcodeBase + 1, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sortWith((x: Double, y: Double) => NumericalLT.apply(x, y)))
          if (p <= 0.0)
            return sa.toVector.head
          if (p >= 1.0)
            return sa.toVector.last
          val k = (sa.size - 1.0)*p
          val f = Math.floor(k)
          val c = Math.ceil(k)
          if (f == c)
            return sa(k.toInt)
          val d0 = sa(f.toInt) * (c - k)
          val d1 = sa(c.toInt) * (k - f)
            d0 + d1
        }
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X], p: Double): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else if (java.lang.Double.isNaN(p))
          throw new PFARuntimeException("p not a number", errcodeBase + 1, name, pos)
        else {
          val sa = PFAArray.fromVector(a.toVector.sortWith(comparisonOperatorLT))
          if (p <= 0.0)
            return sa.toVector.head
          if (p >= 1.0)
            return sa.toVector.last
          val k = (sa.size - 1.0) * p
          val f = Math.floor(k)
          if (sa.size % 2 == 1)
            sa(f.toInt)
          else
            sa(k.toInt)
        }
    }
  }
  class NTile(val pos: Option[String] = None) extends LibFcn {
    def name = NTile.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "p" -> P.Double), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the value that is at the "n-tile" of <p>a</p> (like a percentile).</desc>
        <param name="a">Array of objects to be take the percentile of.</param>
        <param name="p">A double between 0 and 1.</param>
        <detail>If <p>a</p> has an even number of elements and is a <c>float</c> or <c>double</c>, this function will take the average of the two elements closest to the center of the sorted array.  For any other type, it returns the left (first) of the two elements closest to the center of the sorted array.  If <p>p</p> is exactly one (or greater), the max of the array is returned.  If <p>p</p> is zero (or less), the min of the array is returned.</detail>{errorOnEmpty(errcodeBase + 0)}
        <error code={s"${errcodeBase + 1}"}>If <p>p</p> is NaN, this function raises a "p not a number" error.</error>
      </doc>
    def errcodeBase = NTile.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "$SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new NTile)

  ////   mode (Mode)
  object Mode {
    def name = prefix + "mode"
    def errcodeBase = 15470
    class SorterInt(pos: Option[String]) {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          new Median.SorterInt(pos)(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterLong(pos: Option[String]) {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          new Median.SorterLong(pos)(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterFloat(pos: Option[String]) {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          new Median.SorterFloat(pos)(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterDouble(pos: Option[String]) {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          new Median.SorterDouble(pos)(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
        else
          new Median.SorterOther(comparisonOperatorLT, pos)(PFAArray.fromVector(mostCommon(a)))
    }
    def mostCommon[X](a: PFAArray[X]): Vector[X] = {
      val counter = mutable.Map[X, Int]()
      for (x <- a.toVector) {
        if (!counter.contains(x))
          counter(x) = 0
        counter(x) += 1
      }
      val max = counter.values.max
      counter filter {_._2 == max} map {_._1} toVector
    }
  }
  class Mode(val pos: Option[String] = None) extends LibFcn {
    def name = Mode.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the mode (most common) value of <p>a</p>.</desc>
        <detail>If several different values are equally common, the median of these is returned.</detail>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Mode.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterInt(" + posToJava + "))")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterLong(" + posToJava + "))")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterFloat(" + posToJava + "))")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "$SorterDouble(" + posToJava + "))")
      case AvroArray(x) =>
        JavaCode("(%s)(new %s$SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaType(fcnType.ret, false, true, false), this.getClass.getName, javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Mode)

  class Logsumexp(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "logsumexp"
    def sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    
    def doc =
      <doc>
        <desc>Compute <m>{"""z = \\log(\\sum_{n = 1}^{N} e^{x_n})"""}</m> in a numerically stable way.</desc>
        <detail>Returns <c>NaN</c> if the array is empty.</detail>
      </doc>
    def errcodeBase = 15480

    def apply(datum: PFAArray[Double]): Double =
      if (datum.toVector.isEmpty)
        java.lang.Double.NaN
      else {
        val datumVec = datum.toVector
        val dmax = datumVec.max
        val res = datumVec map {i => Math.exp(i - dmax)} sum

        Math.log(res) + dmax
      }
  }
  provide(new Logsumexp)

  //////////////////////////////////////////////////////////////////// set or set-like functions

  ////   distinct (Distinct)
  class Distinct(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "distinct"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array with the same contents as <p>a</p> but with duplicates removed.</desc>
        <detail>The order of the original array is preserved.</detail>
      </doc>
    def errcodeBase = 15490
    def apply[X](a: PFAArray[X]): PFAArray[X] =
      PFAArray.fromVector(a.toVector.map(Option(_)).distinct.map(_ match {case None => null; case Some(x) => x}).asInstanceOf[Vector[X]])
  }
  provide(new Distinct)

  ////   seteq (SetEq)
  class SetEq(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "seteq"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>a</p> and <p>b</p> are equivalent, ignoring order and duplicates, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 15500
    def apply[X](a: PFAArray[X], b: PFAArray[X]): Boolean = a.toVector.toSet == b.toVector.toSet
  }
  provide(new SetEq)

  ////   union (Union)
  class Union(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "union"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array that represents the union of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
        <detail>The order of the original arrays is preserved.</detail>
      </doc>
    def errcodeBase = 15510
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = {
      val seen = mutable.Set[Option[X]]()
      var out = List[X]()
      for (ai <- a.toVector) {
        val wrapped = Option(ai)  // because mutable.Set is a hash set that can't take null values
        if (!(seen contains wrapped)) {
          seen.add(wrapped)
          out = ai :: out
        }
      }
      for (bi <- b.toVector) {
        val wrapped = Option(bi)
        if (!(seen contains wrapped)) {
          seen.add(wrapped)
          out = bi :: out
        }
      }
      PFAArray.fromVector(out.reverse.toVector)
    }
  }
  provide(new Union)

  ////   intersection (Intersection)
  class Intersection(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "intersection"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array that represents the intersection of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
        <detail>The order of the original arrays is preserved.</detail>
      </doc>
    def errcodeBase = 15520
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = {
      val aset = a.toVector.toSet
      val bset = b.toVector.toSet
      var out = List[X]()
      for (ai <- a.toVector)
        if (bset contains ai)
          out = ai :: out
      for (bi <- b.toVector)
        if (aset contains bi)
          out = bi :: out
      PFAArray.fromVector(out.reverse.toVector)
    }
  }
  provide(new Intersection)

  ////   diff (Diff)
  class Diff(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "diff"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array that represents the difference of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
        <detail>The order of the original arrays is preserved.</detail>
      </doc>
    def errcodeBase = 15530
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = {
      val bset = b.toVector.toSet
      var out = List[X]()
      for (ai <- a.toVector)
        if (!(bset contains ai))
          out = ai :: out
      PFAArray.fromVector(out.reverse.toVector)
    }
  }
  provide(new Diff)

  ////   symdiff (SymDiff)
  class SymDiff(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "symdiff"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array that represents the symmetric difference of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
        <detail>The symmetric difference is (<p>a</p> diff <p>b</p>) union (<p>b</p> diff <p>a</p>).</detail>
        <detail>The order of the original arrays is preserved.</detail>
      </doc>
    def errcodeBase = 15540
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = {
      val aset = a.toVector.toSet
      val bset = b.toVector.toSet
      var out = List[X]()
      for (ai <- a.toVector)
        if (!(bset contains ai))
          out = ai :: out
      for (bi <- b.toVector)
        if (!(aset contains bi))
          out = bi :: out
      PFAArray.fromVector(out.reverse.toVector)
    }
  }
  provide(new SymDiff)

  ////   subset (Subset)
  class Subset(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "subset"
    def sig = Sig(List("little" -> P.Array(P.Wildcard("A")), "big" -> P.Array(P.Wildcard("A"))), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>little</p> is a subset of <p>big</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 15550
    def apply[X](little: PFAArray[X], big: PFAArray[X]): Boolean = little.toVector.toSet subsetOf big.toVector.toSet
  }
  provide(new Subset)

  ////   disjoint (Disjoint)
  class Disjoint(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "disjoint"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>a</p> and <p>b</p> are disjoint, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 15560
    def apply[X](a: PFAArray[X], b: PFAArray[X]): Boolean = (a.toVector.toSet intersect b.toVector.toSet).isEmpty
  }
  provide(new Disjoint)

  //////////////////////////////////////////////////////////////////// functional programming

  ////   map (MapApply)
  class MapApply(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "map"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Wildcard("B"))), P.Array(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and return an array of the results.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15570
    def apply[X, Y](a: PFAArray[X], fcn: X => Y): PFAArray[Y] = PFAArray.fromVector(a.toVector.map(fcn))
  }
  provide(new MapApply)

  ////   mapWithIndex (MapWithIndex)
  class MapWithIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "mapWithIndex"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A")), P.Wildcard("B"))), P.Array(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to index, element pairs from <p>a</p> and return an array of the results.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15580
    def apply[X, Y](a: PFAArray[X], fcn: (Int, X) => Y): PFAArray[Y] = PFAArray.fromVector(a.toVector.zipWithIndex map {case (x, i) => fcn(i, x)})
  }
  provide(new MapWithIndex)

  ////   filter (Filter)
  class Filter(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filter"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and return an array of the elements for which <p>fcn</p> returns <c>true</c>.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15590
    def apply[X](a: PFAArray[X], fcn: X => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.filter(fcn))
  }
  provide(new Filter)

  ////   filterWithIndex (FilterWithIndex)
  class FilterWithIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filterWithIndex"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each index, element pair of <p>a</p> and return an array of the elements for which <p>fcn</p> returns <c>true</c>.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15600
    def apply[X](a: PFAArray[X], fcn: (Int, X) => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.zipWithIndex collect {case (x, i) if fcn(i, x) => x})
  }
  provide(new FilterWithIndex)

  ////   filterMap (FilterMap)
  class FilterMap(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filterMap"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Array(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and return an array of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15610
    def apply[X, Y](a: PFAArray[X], fcn: X => Y): PFAArray[Y] = {
      val builder = Vector.newBuilder[Y]
      builder.sizeHint(a.toVector.size)
      for (item <- a.toVector)
        fcn(item) match {
          case null =>
          case x => builder += x
        }
      PFAArray.fromVector(builder.result)
    }
  }
  provide(new FilterMap)

  ////   filterMapWithIndex (FilterMapWithIndex)
  class FilterMapWithIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filterMapWithIndex"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Array(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each index, element pair of <p>a</p> and return an array of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15620
    def apply[X, Y](a: PFAArray[X], fcn: (Int, X) => Y): PFAArray[Y] = {
      val builder = Vector.newBuilder[Y]
      builder.sizeHint(a.toVector.size)
      for ((item, index) <- a.toVector.zipWithIndex)
        fcn(index, item) match {
          case null =>
          case x => builder += x
        }
      PFAArray.fromVector(builder.result)
    }
  }
  provide(new FilterMapWithIndex)

  ////   flatMap (FlatMap)
  class FlatMap(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "flatMap"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Array(P.Wildcard("B")))), P.Array(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and flatten the resulting arrays into a single array.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15630
    def apply[X, Y](a: PFAArray[X], fcn: X => PFAArray[Y]): PFAArray[Y] =
      PFAArray.fromVector(a.toVector.flatMap(x => fcn(x).toVector))
  }
  provide(new FlatMap)

  ////   flatMapWithIndex (FlatMapWithIndex)
  class FlatMapWithIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "flatMapWithIndex"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A")), P.Array(P.Wildcard("B")))), P.Array(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each index, element pair of <p>a</p> and flatten the resulting arrays into a single array.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 15640
    def apply[X, Y](a: PFAArray[X], fcn: (Int, X) => PFAArray[Y]): PFAArray[Y] =
      PFAArray.fromVector(a.toVector.zipWithIndex flatMap {case (x, i) => fcn(i, x).toVector})
  }
  provide(new FlatMapWithIndex)

  ////   zipmap (ZipMap)
  class ZipMap(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "zipmap"
    def sig = Sigs(List(Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Wildcard("Z"))), P.Array(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "c" -> P.Array(P.Wildcard("C")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")), P.Wildcard("Z"))), P.Array(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "c" -> P.Array(P.Wildcard("C")), "d" -> P.Array(P.Wildcard("D")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")), P.Wildcard("Z"))), P.Array(P.Wildcard("Z")))))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to the elements of <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> in lock-step and return a result for row.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned arrays" error if <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> do not all have the same length.</error>
      </doc>
    def errcodeBase = 15650
    def apply[A, B, Z](a: PFAArray[A], b: PFAArray[B], fcn: (A, B) => Z): PFAArray[Z] = {
      if (a.size != b.size)
        throw new PFARuntimeException("misaligned arrays", errcodeBase + 0, name, pos)
      PFAArray.fromVector(a.toVector zip b.toVector map {case (ai, bi) => fcn(ai, bi)})
    }
    def apply[A, B, C, Z](a: PFAArray[A], b: PFAArray[B], c: PFAArray[C], fcn: (A, B, C) => Z): PFAArray[Z] = {
      if (a.size != b.size  ||  b.size != c.size)
        throw new PFARuntimeException("misaligned arrays", errcodeBase + 0, name, pos)
      PFAArray.fromVector(a.toVector zip b.toVector zip c.toVector map {case ((ai, bi), ci) => fcn(ai, bi, ci)})
    }
    def apply[A, B, C, D, Z](a: PFAArray[A], b: PFAArray[B], c: PFAArray[C], d: PFAArray[D], fcn: (A, B, C, D) => Z): PFAArray[Z] = {
      if (a.size != b.size  ||  b.size != c.size  ||  c.size != d.size)
        throw new PFARuntimeException("misaligned arrays", errcodeBase + 0, name, pos)
      PFAArray.fromVector(a.toVector zip b.toVector zip c.toVector zip d.toVector map {case (((ai, bi), ci), di) => fcn(ai, bi, ci, di)})
    }
  }
  provide(new ZipMap)

  ////   zipmapWithIndex (ZipMapWithIndex)
  class ZipMapWithIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "zipmapWithIndex"
    def sig = Sigs(List(Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A"), P.Wildcard("B")), P.Wildcard("Z"))), P.Array(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "c" -> P.Array(P.Wildcard("C")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")), P.Wildcard("Z"))), P.Array(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "c" -> P.Array(P.Wildcard("C")), "d" -> P.Array(P.Wildcard("D")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")), P.Wildcard("Z"))), P.Array(P.Wildcard("Z")))))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to the indexes and elements of <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> in lock-step and return a result for row.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned arrays" error if <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> do not all have the same length.</error>
      </doc>
    def errcodeBase = 15660
    def apply[A, B, Z](a: PFAArray[A], b: PFAArray[B], fcn: (Int, A, B) => Z): PFAArray[Z] = {
      if (a.size != b.size)
        throw new PFARuntimeException("misaligned arrays", errcodeBase + 0, name, pos)
      PFAArray.fromVector((a.toVector zip b.toVector).zipWithIndex map {case ((ai, bi), i) => fcn(i, ai, bi)})
    }
    def apply[A, B, C, Z](a: PFAArray[A], b: PFAArray[B], c: PFAArray[C], fcn: (Int, A, B, C) => Z): PFAArray[Z] = {
      if (a.size != b.size  ||  b.size != c.size)
        throw new PFARuntimeException("misaligned arrays", errcodeBase + 0, name, pos)
      PFAArray.fromVector((a.toVector zip b.toVector zip c.toVector).zipWithIndex map {case (((ai, bi), ci), i) => fcn(i, ai, bi, ci)})
    }
    def apply[A, B, C, D, Z](a: PFAArray[A], b: PFAArray[B], c: PFAArray[C], d: PFAArray[D], fcn: (Int, A, B, C, D) => Z): PFAArray[Z] = {
      if (a.size != b.size  ||  b.size != c.size  ||  c.size != d.size)
        throw new PFARuntimeException("misaligned arrays", errcodeBase + 0, name, pos)
      PFAArray.fromVector((a.toVector zip b.toVector zip c.toVector zip d.toVector).zipWithIndex map {case ((((ai, bi), ci), di), i) => fcn(i, ai, bi, ci, di)})
    }
  }
  provide(new ZipMapWithIndex)

  ////   reduce (Reduce)
  class Reduce(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "reduce"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally.</desc>
        <detail>The first parameter of <p>fcn</p> is the running tally and the second parameter is an element from <p>a</p>.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from left (beginning) to right (end), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative.  It need not be commutative.</detail>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 15670
    def apply[X](a: PFAArray[X], fcn: (X, X) => X): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        a.toVector.reduceLeft(fcn)
  }
  provide(new Reduce)

  ////   reduceRight (ReduceRight)
  class ReduceRight(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "reduceRight"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally.</desc>
        <detail>The first parameter of <p>fcn</p> is an element from <p>a</p> and the second parameter is the running tally.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from right (end) to left (beginning), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative.  It need not be commutative.</detail>{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 15680
    def apply[X](a: PFAArray[X], fcn: (X, X) => X): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array", errcodeBase + 0, name, pos)
      else
        a.toVector.reduceRight(fcn)
  }
  provide(new ReduceRight)

  ////   fold (Fold)
  class Fold(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fold"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "zero" -> P.Wildcard("B"), "fcn" -> P.Fcn(List(P.Wildcard("B"), P.Wildcard("A")), P.Wildcard("B"))), P.Wildcard("B"))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally, starting with <p>zero</p>.</desc>
        <detail>The first parameter of <p>fcn</p> is the running tally and the second parameter is an element from <p>a</p>.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from left (beginning) to right (end), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative with <p>zero</p> as its identity; that is, <c>fcn(zero, zero) = zero</c>.  It need not be commutative.</detail>
      </doc>
    def errcodeBase = 15690
    def apply[X, Y](a: PFAArray[X], zero: Y, fcn: (Y, X) => Y): Y =
      a.toVector.foldLeft(zero)(fcn)
  }
  provide(new Fold)

  ////   foldright (FoldRight)
  class FoldRight(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "foldRight"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "zero" -> P.Wildcard("B"), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Wildcard("B"))), P.Wildcard("B"))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally, starting with <p>zero</p>.</desc>
        <detail>The first parameter of <p>fcn</p> is an element from <p>a</p> and the second parameter is the running tally.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from right (end) to left (beginning), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative with <p>zero</p> as its identity; that is, <c>fcn(zero, zero) = zero</c>.  It need not be commutative.</detail>
      </doc>
    def errcodeBase = 15700
    def apply[X, Y](a: PFAArray[X], zero: Y, fcn: (X, Y) => Y): Y =
      a.toVector.foldRight(zero)(fcn)
  }
  provide(new FoldRight)

  ////   takeWhile (TakeWhile)
  class TakeWhile(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "takeWhile"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to elements of <p>a</p> and create an array of the longest prefix that returns <c>true</c>, stopping with the first <c>false</c>.</desc>
        <detail>Beyond the prefix, the number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def errcodeBase = 15710
    def apply[X](a: PFAArray[X], fcn: X => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.takeWhile(fcn))
  }
  provide(new TakeWhile)

  ////   dropWhile (DropWhile)
  class DropWhile(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "dropWhile"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to elements of <p>a</p> and create an array of all elements after the longest prefix that returns <c>true</c>.</desc>
        <detail>Beyond the prefix, the number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def errcodeBase = 15720
    def apply[X](a: PFAArray[X], fcn: X => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.dropWhile(fcn))
  }
  provide(new DropWhile)

  //////////////////////////////////////////////////////////////////// functional tests

  ////   any (Any)
  class Any(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "any"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> for any element in <p>a</p> (logical or).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def errcodeBase = 15730
    def apply[X](a: PFAArray[X], fcn: X => Boolean): Boolean = a.toVector.exists(fcn)
  }
  provide(new Any)

  ////   all (All)
  class All(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "all"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> for all elements in <p>a</p> (logical and).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def errcodeBase = 15740
    def apply[X](a: PFAArray[X], fcn: X => Boolean): Boolean = a.toVector.forall(fcn)
  }
  provide(new All)

  ////   corresponds (Corresponds)
  class Corresponds(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "corresponds"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all pairs of elements, one from <p>a</p> and the other from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the lengths of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def errcodeBase = 15750
    def apply[X, Y](a: PFAArray[X], b: PFAArray[Y], fcn: (X, Y) => Boolean): Boolean = (a.toVector corresponds b.toVector)(fcn)
  }
  provide(new Corresponds)

  ////   correspondsWithIndex (CorrespondsWithIndex)
  class CorrespondsWithIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "correspondsWithIndex"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all triples of index, element from <p>a</p>, element from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the lengths of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def errcodeBase = 15760
    def apply[X, Y](a: PFAArray[X], b: PFAArray[Y], fcn: (Int, X, Y) => Boolean): Boolean = {
      val avec = a.toVector
      val bvec = b.toVector
      if (avec.size != bvec.size)
        false
      else
        (0 until avec.size) forall {i => fcn(i, avec(i), bvec(i))}
    }
  }
  provide(new CorrespondsWithIndex)

  //////////////////////////////////////////////////////////////////// restructuring

  ////   slidingWindow (SlidingWindow)
  object SlidingWindow {
    def name = prefix + "slidingWindow"
    def errcodeBase = 15770
    class WithClockCheck(engine: PFAEngineBase, pos: Option[String]) {
      def apply[X](a: PFAArray[X], size: Int, step: Int): PFAArray[PFAArray[X]] =
        if (size < 1)
          throw new PFARuntimeException("size < 1", errcodeBase + 0, name, pos)
        else if (step < 1)
          throw new PFARuntimeException("step < 1", errcodeBase + 1, name, pos)
        else {
          var i = 0
          val out =
            for (x <- a.toVector.sliding(size, step) if (x.size == size)) yield {
              if (i % 1000 == 0)
                engine.checkClock()
              i += 1
              PFAArray.fromVector(x)
            }
          PFAArray.fromVector(out.toVector)
        }
    }
  }
  class SlidingWindow(val pos: Option[String] = None) extends LibFcn {
    def name = SlidingWindow.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "size" -> P.Int, "step" -> P.Int), P.Array(P.Array(P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Return an array of subsequences of <p>a</p> with length <p>size</p> that slide through <p>a</p> in steps of length <p>step</p> from left to right.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>size</p> is non-positive, a {""""size < 1""""} runtime error is raised.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>step</p> is non-positive, a {""""step < 1""""} runtime error is raised.</error>
      </doc>
    def errcodeBase = SlidingWindow.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$WithClockCheck(thisEngineBase, " + posToJava + "))")
  }
  provide(new SlidingWindow)

  ////   combinations (Combinations)
  object Combinations {
    def name = prefix + "combinations"
    def errcodeBase = 15780
    class WithClockCheck(engine: PFAEngineBase, pos: Option[String]) {
      def apply[X](a: PFAArray[X], size: Int): PFAArray[PFAArray[X]] =
        if (size < 1)
          throw new PFARuntimeException("size < 1", errcodeBase + 0, name, pos)
        else {
          val vector = a.toVector
          var i = 0
          val out =
            for (indexes <- (0 until vector.size).combinations(size)) yield {
              if (i % 1000 == 0)
                engine.checkClock()
              i += 1
              PFAArray.fromVector(indexes.map(vector(_)).toVector)
            }
          PFAArray.fromVector(out.toVector)
        }
    }
  }
  class Combinations(val pos: Option[String] = None) extends LibFcn {
    def name = Combinations.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "size" -> P.Int), P.Array(P.Array(P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Return all combinations of elements of <p>a</p> with length <p>size</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>size</p> is non-positive, a {""""size < 1""""} runtime error is raised.</error>
      </doc>
    def errcodeBase = Combinations.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$WithClockCheck(thisEngineBase, " + posToJava + "))")
  }
  provide(new Combinations)

  ////   permutations (Permutations)
  object Permutations {
    def name = prefix + "permutations"
    def errcodeBase = 15790
    class WithClockCheck(engine: PFAEngineBase, pos: Option[String]) {
      def apply[X](a: PFAArray[X]): PFAArray[PFAArray[X]] = {
        val vector = a.toVector
        var i = 0
        val out =
          for (indexes <- (0 until vector.size).permutations) yield {
            if (i % 1000 == 0)
              engine.checkClock()
            i += 1
            PFAArray.fromVector(indexes.map(vector(_)).toVector)
          }
        PFAArray.fromVector(out.toVector)
      }
    }
  }
  class Permutations(val pos: Option[String] = None) extends LibFcn {
    def name = Permutations.name
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Array(P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Return all permutations of elements of <p>a</p>.</desc>{describeTimeout}
      </doc>
    def errcodeBase = Permutations.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$WithClockCheck(thisEngineBase, " + posToJava + "))")
  }
  provide(new Permutations)

  ////   flatten (Flatten)
  class Flatten(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "flatten"
    def sig = Sig(List("a" -> P.Array(P.Array(P.Wildcard("A")))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Concatenate the arrays in <p>a</p>.</desc>
      </doc>
    def errcodeBase = 15800
    def apply[X](a: PFAArray[PFAArray[X]]): PFAArray[X] = PFAArray.fromVector(a.toVector flatMap {_.toVector})
  }
  provide(new Flatten)

  ////   groupby (GroupBy)
  class GroupBy(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "groupby"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.String)), P.Map(P.Array(P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Groups elements of <p>a</p> by the string that <p>fcn</p> maps them to.</desc>
      </doc>
    def errcodeBase = 15810
    def apply[X](a: PFAArray[X], fcn: X => String): PFAMap[PFAArray[X]] =
      PFAMap.fromMap(a.toVector.groupBy(fcn) map {case (key, vector) => (key, PFAArray.fromVector(vector))})
  }
  provide(new GroupBy)

}
