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
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Random

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
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
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "a."

  val anyNumber = Set[Type](AvroInt(), AvroLong(), AvroFloat(), AvroDouble())

  def negativeIndex(length: Int, index: Int): Int =
    if (index >= 0) index else length + index

  def checkRange(length: Int, index: Int): Unit =
    if (index < 0  ||  index >= length)
      throw new PFARuntimeException("index out of range")

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

  val errorOnEmpty = <error>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
  val errorOnOutOfRange = <error>If <p>index</p> is beyond the range of <p>a</p>, an "array out of range" runtime error is raised.</error>
  val errorOnNegativeN = <error>If <p>n</p> is negative, an {""""n < 0""""} runtime error is raised.</error>

  //////////////////////////////////////////////////////////////////// basic access

  ////   len (Len)
  object Len extends LibFcn {
    val name = prefix + "len"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Int)
    val doc =
      <doc>
        <desc>Return the length of array <p>a</p>.</desc>
      </doc>
    def apply[X](a: PFAArray[X]): Int = a.toVector.size
  }
  provide(Len)

  ////   subseq (Subseq)
  object Subseq extends LibFcn {
    val name = prefix + "subseq"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "start" -> P.Int, "end" -> P.Int), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the subsequence of <p>a</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive).</desc>{describeIndex}
      </doc>
    def apply[X](a: PFAArray[X], start: Int, end: Int): PFAArray[X] = {
      val (normStart, normEnd) = startEnd(a.toVector.size, start, end)
      PFAArray.fromVector(a.toVector.slice(normStart, normEnd))
    }
  }
  provide(Subseq)

  ////   head (Head)
  object Head extends LibFcn {
    val name = prefix + "head"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the first item of the array.</desc>
        <error>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def apply[X](a: PFAArray[X]): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        a.toVector.head
  }
  provide(Head)

  ////   tail (Tail)
  object Tail extends LibFcn {
    val name = prefix + "tail"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return all but the first item of the array.</desc>
        <error>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def apply[X](a: PFAArray[X]): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        PFAArray.fromVector(a.toVector.tail)
  }
  provide(Tail)

  ////   last (Last)
  object Last extends LibFcn {
    val name = prefix + "last"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the first item of the array.</desc>
        <error>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def apply[X](a: PFAArray[X]): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        a.toVector.last
  }
  provide(Last)

  ////   init (Init)
  object Init extends LibFcn {
    val name = prefix + "init"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return all but the last item of the array.</desc>
        <error>If <p>a</p> is empty, an "empty array" runtime error is raised.</error>
      </doc>
    def apply[X](a: PFAArray[X]): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        PFAArray.fromVector(a.toVector.init)
  }
  provide(Init)

  ////   subseqto (SubseqTo)
  object SubseqTo extends LibFcn {
    val name = prefix + "subseqto"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "start" -> P.Int, "end" -> P.Int, "replacement" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new array by replacing <p>a</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) with <p>replacement</p>.</desc>{describeIndex}{sideEffectFree()}
      </doc>
    def apply[X](a: PFAArray[X], start: Int, end: Int, replacement: PFAArray[X]): PFAArray[X] = {
      val (normStart, normEnd) = startEnd(a.toVector.size, start, end)
      PFAArray.fromVector(a.toVector.patch(normStart, replacement.toVector, normEnd - normStart))
    }
  }
  provide(SubseqTo)

  //////////////////////////////////////////////////////////////////// searching

  ////   contains (Contains)
  object Contains extends LibFcn {
    val name = prefix + "contains"
    val sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Boolean)))
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>haystack</p> contains <p>needle</p>, <c>false</c> otherwise.</desc>
      </doc>
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Boolean =
      haystack.toVector.indexOfSlice(needle.toVector) != -1
    def apply[X](haystack: PFAArray[X], needle: X): Boolean =
      haystack.toVector.indexOf(needle) != -1
  }
  provide(Contains)

  ////   count (Count)
  object Count extends LibFcn {
    val name = prefix + "count"
    val sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Int)))
    val doc =
      <doc>
        <desc>Count the number of times <p>needle</p> appears in <p>haystack</p> or the number of times the <p>needle</p> function evaluates to <c>true</c>.</desc>
      </doc>
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Int = count(haystack.toVector, needle.toVector, 0)
    def apply[X](haystack: PFAArray[X], needle: X): Int = count(haystack.toVector, needle, 0)
    def apply[X](haystack: PFAArray[X], needle: X => Boolean): Int = haystack.toVector.count(needle)

    @tailrec
    def count[X](haystack: Vector[X], needle: Vector[X], sofar: Int): Int = haystack.indexOfSlice(needle) match {
      case -1 => sofar
      case where => count(haystack.slice(where + needle.size, haystack.size), needle, sofar + 1)
    }
    @tailrec
    def count[X](haystack: Vector[X], needle: X, sofar: Int): Int = haystack.indexOf(needle) match {
      case -1 => sofar
      case where => count(haystack.slice(where + 1, haystack.size), needle, sofar + 1)
    }
  }
  provide(Count)

  ////   index (Index)
  object Index extends LibFcn {
    val name = prefix + "index"
    val sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Int)))
    val doc =
      <doc>
        <desc>Return the lowest index where <p>haystack</p> contains <p>needle</p> or -1 if <p>haystack</p> does not contain <p>needle</p>.</desc>
      </doc>
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Int = haystack.toVector.indexOfSlice(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Int = haystack.toVector.indexOf(needle)
  }
  provide(Index)

  ////   rindex (RIndex)
  object RIndex extends LibFcn {
    val name = prefix + "rindex"
    val sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Int),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Int)))
    val doc =
      <doc>
        <desc>Return the highest index where <p>haystack</p> contains <p>needle</p> or -1 if <p>haystack</p> does not contain <p>needle</p>.</desc>
      </doc>
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Int = haystack.toVector.lastIndexOfSlice(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Int = haystack.toVector.lastIndexOf(needle)
  }
  provide(RIndex)

  ////   startswith (StartsWith)
  object StartsWith extends LibFcn {
    val name = prefix + "startswith"
    val sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Boolean)))
    val doc =
      <doc>
        <desc>Return <c>true</c> if the first (leftmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Boolean = haystack.toVector.startsWith(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Boolean = haystack.toVector.headOption match {
      case Some(x) => x == needle
      case None => false
    }
  }
  provide(StartsWith)

  ////   endswith (EndsWith)
  object EndsWith extends LibFcn {
    val name = prefix + "endswith"
    val sig = Sigs(List(Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Array(P.Wildcard("A"))), P.Boolean),
                        Sig(List("haystack" -> P.Array(P.Wildcard("A")), "needle" -> P.Wildcard("A")), P.Boolean)))
    val doc =
      <doc>
        <desc>Return <c>true</c> if the last (rightmost) subseqence of <p>haystack</p> is equal to <p>needle</p>, false otherwise.</desc>
      </doc>
    def apply[X](haystack: PFAArray[X], needle: PFAArray[X]): Boolean = haystack.toVector.endsWith(needle.toVector)
    def apply[X](haystack: PFAArray[X], needle: X): Boolean = haystack.toVector.lastOption match {
      case Some(x) => x == needle
      case None => false
    }
  }
  provide(EndsWith)

  //////////////////////////////////////////////////////////////////// manipulation

  ////   concat (Concat)
  object Concat extends LibFcn {
    val name = prefix + "concat"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Concatenate <p>a</p> and <p>b</p> to make a new array of the same type.</desc>
        <detail>The length of the returned array is the sum of the lengths of <p>a</p> and <p>b</p>.</detail>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector ++ b.toVector)
  }
  provide(Concat)

  ////   append (Append)
  object Append extends LibFcn {
    val name = prefix + "append"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "item" -> P.Wildcard("A")), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new array by adding <p>item</p> at the end of <p>a</p>.</desc>{sideEffectFree()}
        <detail>The length of the returned array is one more than <p>a</p>.</detail>
      </doc>
    def apply[X](a: PFAArray[X], item: X): PFAArray[X] = PFAArray.fromVector(a.toVector :+ item)
  }
  provide(Append)

  ////   cycle (Cycle)
  object Cycle extends LibFcn {
    val name = prefix + "cycle"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "item" -> P.Wildcard("A"), "maxLength" -> P.Int), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new array by adding <p>item</p> at the end of <p>a</p>, but keep the length less than or equal to <p>maxLength</p> by removing items from the beginning.</desc>{sideEffectFree()}
        <error>If <p>maxLength</p> is less than 0, this function raises a "maxLength out of range" error.</error>
      </doc>
    def apply[X](a: PFAArray[X], item: X, maxLength: Int): PFAArray[X] = {
      if (maxLength < 0)
        throw new PFARuntimeException("maxLength out of range")
      val out = a.toVector :+ item
      PFAArray.fromVector(out.takeRight(maxLength))
    }
  }
  provide(Cycle)

  ////   insert (Insert)
  object Insert extends LibFcn {
    val name = prefix + "insert"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "index" -> P.Int, "item" -> P.Wildcard("A")), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new array by inserting <p>item</p> at <p>index</p> of <p>a</p>.</desc>{describeOneIndex}{sideEffectFree()}{errorOnOutOfRange}
        <detail>The length of the returned array is one more than <p>a</p>.</detail>
      </doc>
    def apply[X](a: PFAArray[X], index: Int, item: X): PFAArray[X] = {
      val normIndex = negativeIndex(a.toVector.size, index)
      checkRange(a.toVector.size, normIndex)
      PFAArray.fromVector(a.toVector.patch(normIndex, Vector(item), 0))
    }
  }
  provide(Insert)

  ////   replace (Replace)
  object Replace extends LibFcn {
    val name = prefix + "replace"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "index" -> P.Int, "item" -> P.Wildcard("A")), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new array by replacing <p>index</p> of <p>a</p> with <p>item</p>.</desc>{describeOneIndex}{sideEffectFree()}{errorOnOutOfRange}
        <detail>The length of the returned array is equal to that of <p>a</p>.</detail>
      </doc>
    def apply[X](a: PFAArray[X], index: Int, item: X): PFAArray[X] = {
      val normIndex = negativeIndex(a.toVector.size, index)
      checkRange(a.toVector.size, normIndex)
      PFAArray.fromVector(a.toVector.updated(normIndex, item))
    }
  }
  provide(Replace)

  ////   remove (Remove)
  object Remove extends LibFcn {
    val name = prefix + "remove"
    val sig = Sigs(List(Sig(List("a" -> P.Array(P.Wildcard("A")), "start" -> P.Int, "end" -> P.Int), P.Array(P.Wildcard("A"))),
                        Sig(List("a" -> P.Array(P.Wildcard("A")), "index" -> P.Int), P.Array(P.Wildcard("A")))))
    val doc =
      <doc>
        <desc>Return a new array by removing elements from <p>a</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) or just a single <p>index</p>.</desc>{describeIndex}{sideEffectFree()}{errorOnOutOfRange}
        <detail>The length of the returned array is one less than <p>a</p>.</detail>
      </doc>
    def apply[X](a: PFAArray[X], start: Int, end: Int): PFAArray[X] = {
      val (normStart, normEnd) = startEnd(a.toVector.size, start, end)
      PFAArray.fromVector(a.toVector.patch(normStart, Vector[X](), normEnd - normStart))
    }
    def apply[X](a: PFAArray[X], index: Int): PFAArray[X] = {
      val normIndex = negativeIndex(a.toVector.size, index)
      checkRange(a.toVector.size, normIndex)
      PFAArray.fromVector(a.toVector.patch(normIndex, Vector[X](), 1))
    }
  }
  provide(Remove)

  ////   rotate (Rotate)
  object Rotate extends LibFcn {
    val name = prefix + "rotate"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "steps" -> P.Int), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array formed by rotating <p>a</p> left <p>steps</p> spaces.</desc>
        <error>If <p>steps</p> is less than zero, a "steps out of range" error is raised.</error>
      </doc>
    def apply[X](a: PFAArray[X], steps: Int): PFAArray[X] = {
      if (steps < 0)
        throw new PFARuntimeException("steps out of range")
      val vector = a.toVector
      val (left, right) = vector.splitAt(steps % vector.size)
      PFAArray.fromVector(right ++ left)
    }
  }
  provide(Rotate)

  //////////////////////////////////////////////////////////////////// reordering

  ////   sort (Sort)
  object Sort extends LibFcn {
    val name = prefix + "sort"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array with the same elements as <p>a</p> but in ascending order (as defined by Avro's sort order).</desc>{sideEffectFree()}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "SorterInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "SorterLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "SorterFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "SorterDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class SorterInt {
      def apply(a: PFAArray[Int]): PFAArray[Int] = PFAArray.fromVector(a.toVector.sorted)
    }
    class SorterLong {
      def apply(a: PFAArray[Long]): PFAArray[Long] = PFAArray.fromVector(a.toVector.sorted)
    }
    class SorterFloat {
      def apply(a: PFAArray[Float]): PFAArray[Float] = PFAArray.fromVector(a.toVector.sorted)
    }
    class SorterDouble {
      def apply(a: PFAArray[Double]): PFAArray[Double] = PFAArray.fromVector(a.toVector.sorted)
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.sortWith(comparisonOperatorLT))
    }
  }
  provide(Sort)

  ////   sortLT (SortLT)
  object SortLT extends LibFcn {
    val name = prefix + "sortLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array with the same elements as <p>a</p> but in ascending order as defined by the <p>lessThan</p> function.</desc>{sideEffectFree()}
      </doc>
    def apply[X](a: PFAArray[X], lt: (X, X) => java.lang.Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.sortWith((x: X, y: X) => lt(x, y).booleanValue))
  }
  provide(SortLT)

  ////   shuffle (Shuffle)
  object Shuffle extends LibFcn {
    val name = prefix + "shuffle"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array with the same elements as <p>a</p> but in a random order.</desc>
        {sideEffectFree(" (except for updating the random number generator)")}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Sorter(randomGenerator()))")

    class Sorter(randomGenerator: Random) {
      def apply[X](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.sortWith((x: X, y: X) => randomGenerator.nextBoolean()))
    }
  }
  provide(Shuffle)

  ////   reverse (Reverse)
  object Reverse extends LibFcn {
    val name = prefix + "reverse"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the elements of <p>a</p> in reversed order.</desc>
      </doc>
    def apply[X](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.reverse)
  }
  provide(Reverse)

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
  object Max extends LibFcn {
    val name = prefix + "max"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the maximum value in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "MaxInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "MaxLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "MaxFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "MaxDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MaxInt {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.max
    }
    class MaxLong {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.max
    }
    class MaxFloat {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.max
    }
    class MaxDouble {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.max
    }
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          highestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  provide(Max)

  ////   min (Min)
  object Min extends LibFcn {
    val name = prefix + "min"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the minimum value in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "MinInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "MinLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "MinFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "MinDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MinInt {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.min
    }
    class MinLong {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.min
    }
    class MinFloat {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.min
    }
    class MinDouble {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          a.toVector.min
    }
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          lowestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  provide(Min)

  ////   maxLT (MaxLT)
  object MaxLT extends LibFcn {
    val name = prefix + "maxLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the maximum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty}
      </doc>
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        highestN(a.toVector, 1, lt).head
  }
  provide(MaxLT)

  ////   minLT (MinLT)
  object MinLT extends LibFcn {
    val name = prefix + "minLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the minimum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty}
      </doc>
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): X =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        lowestN(a.toVector, 1, lt).head
  }
  provide(MinLT)

  ////   maxN (MaxN)
  object MaxN extends LibFcn {
    val name = prefix + "maxN"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the <p>n</p> highest values in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty}{errorOnNegativeN}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "MaxInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "MaxLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "MaxFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "MaxDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MaxInt {
      def apply(a: PFAArray[Int], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Int, y: Int) => x < y))
    }
    class MaxLong {
      def apply(a: PFAArray[Long], n: Int): PFAArray[Long] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Long, y: Long) => x < y))
    }
    class MaxFloat {
      def apply(a: PFAArray[Float], n: Int): PFAArray[Float] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Float, y: Float) => x < y))
    }
    class MaxDouble {
      def apply(a: PFAArray[Double], n: Int): PFAArray[Double] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(highestN(a.toVector, n, (x: Double, y: Double) => x < y))
    }
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[X] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(highestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  provide(MaxN)

  ////   minN (MinN)
  object MinN extends LibFcn {
    val name = prefix + "minN"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the <p>n</p> lowest values in <p>a</p> (as defined by Avro's sort order).</desc>{errorOnEmpty}{errorOnNegativeN}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "MinInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "MinLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "MinFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "MinDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MinInt {
      def apply(a: PFAArray[Int], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Int, y: Int) => x < y))
    }
    class MinLong {
      def apply(a: PFAArray[Long], n: Int): PFAArray[Long] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Long, y: Long) => x < y))
    }
    class MinFloat {
      def apply(a: PFAArray[Float], n: Int): PFAArray[Float] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Float, y: Float) => x < y))
    }
    class MinDouble {
      def apply(a: PFAArray[Double], n: Int): PFAArray[Double] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(lowestN(a.toVector, n, (x: Double, y: Double) => x < y))
    }
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[X] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(lowestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  provide(MinN)

  ////   maxNLT (MaxNLT)
  object MaxNLT extends LibFcn {
    val name = prefix + "maxNLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the <p>n</p> highest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty}{errorOnNegativeN}
      </doc>
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else if (n < 0)
        throw new PFARuntimeException("n < 0")
      else
        PFAArray.fromVector(highestN(a.toVector, n, lt))
  }
  provide(MaxNLT)

  ////   minNLT (MinNLT)
  object MinNLT extends LibFcn {
    val name = prefix + "minNLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the <p>n</p> lowest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{errorOnEmpty}{errorOnNegativeN}
      </doc>
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[X] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else if (n < 0)
        throw new PFARuntimeException("n < 0")
      else
        PFAArray.fromVector(lowestN(a.toVector, n, lt))
  }
  provide(MinNLT)

  ////   argmax (Argmax)
  object Argmax extends LibFcn {
    val name = prefix + "argmax"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Int)
    val doc =
      <doc>
        <desc>Return the index of the maximum value in <p>a</p> (as defined by Avro's sort order).</desc>{takesFirst("max")}{errorOnEmpty}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          argHighestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  provide(Argmax)

  ////   argmin (Argmin)
  object Argmin extends LibFcn {
    val name = prefix + "argmin"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Int)
    val doc =
      <doc>
        <desc>Return the index of the minimum value in <p>a</p> (as defined by Avro's sort order).</desc>{takesFirst("min")}{errorOnEmpty}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          argLowestN(a.toVector, 1, comparisonOperatorLT).head
    }
  }
  provide(Argmin)

  ////   argmaxLT (ArgmaxLT)
  object ArgmaxLT extends LibFcn {
    val name = prefix + "argmaxLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Int)
    val doc =
      <doc>
        <desc>Return the index of the maximum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{takesFirst("max")}{errorOnEmpty}
      </doc>
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): Int =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        argHighestN(a.toVector, 1, lt).head
  }
  provide(ArgmaxLT)

  ////   argminLT (ArgminLT)
  object ArgminLT extends LibFcn {
    val name = prefix + "argminLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Int)
    val doc =
      <doc>
        <desc>Return the index of the minimum value in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{takesFirst("min")}{errorOnEmpty}
      </doc>
    def apply[X](a: PFAArray[X], lt: (X, X) => Boolean): Int =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else
        argLowestN(a.toVector, 1, lt).head
  }
  provide(ArgminLT)

  ////   argmaxN (ArgmaxN)
  object ArgmaxN extends LibFcn {
    val name = prefix + "argmaxN"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Int))
    val doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> highest values in <p>a</p> (as defined by Avro's sort order).</desc>{sortsUnique}{errorOnEmpty}{errorOnNegativeN}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(argHighestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  provide(ArgmaxN)

  ////   argminN (ArgminN)
  object ArgminN extends LibFcn {
    val name = prefix + "argminN"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int), P.Array(P.Int))
    val doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> lowest values in <p>a</p> (as defined by Avro's sort order).</desc>{sortsUnique}{errorOnEmpty}{errorOnNegativeN}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X], n: Int): PFAArray[Int] =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else if (n < 0)
          throw new PFARuntimeException("n < 0")
        else
          PFAArray.fromVector(argLowestN(a.toVector, n, comparisonOperatorLT))
    }
  }
  provide(ArgminN)

  ////   argmaxNLT (ArgmaxNLT)
  object ArgmaxNLT extends LibFcn {
    val name = prefix + "argmaxNLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Int))
    val doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> highest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{sortsUnique}{errorOnEmpty}{errorOnNegativeN}
      </doc>
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[Int] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else if (n < 0)
        throw new PFARuntimeException("n < 0")
      else
        PFAArray.fromVector(argHighestN(a.toVector, n, lt))
  }
  provide(ArgmaxNLT)

  ////   argminNLT (ArgminNLT)
  object ArgminNLT extends LibFcn {
    val name = prefix + "argminNLT"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.Int))
    val doc =
      <doc>
        <desc>Return the indexes of the <p>n</p> lowest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>{sortsUnique}{errorOnEmpty}{errorOnNegativeN}
      </doc>
    def apply[X](a: PFAArray[X], n: Int, lt: (X, X) => Boolean): PFAArray[Int] =
      if (a.toVector.isEmpty)
        throw new PFARuntimeException("empty array")
      else if (n < 0)
        throw new PFARuntimeException("n < 0")
      else
        PFAArray.fromVector(argLowestN(a.toVector, n, lt))
  }
  provide(ArgminNLT)

  //////////////////////////////////////////////////////////////////// numerical

  ////   sum (Sum)
  object Sum extends LibFcn {
    val name = prefix + "sum"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A", oneOf = anyNumber))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the sum of numbers in <p>a</p>.</desc>
        <detail>Returns zero if the array is empty.</detail>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "SumInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "SumLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "SumFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "SumDouble())")
      case _ =>
        throw new Exception  // should never be called
    }
    class SumInt {
      def apply(a: PFAArray[Int]): Int = a.toVector.sum
    }
    class SumLong {
      def apply(a: PFAArray[Long]): Long = a.toVector.sum
    }
    class SumFloat {
      def apply(a: PFAArray[Float]): Float = a.toVector.sum
    }
    class SumDouble {
      def apply(a: PFAArray[Double]): Double = a.toVector.sum
    }
  }
  provide(Sum)

  ////   product (Product)
  object Product extends LibFcn {
    val name = prefix + "product"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A", oneOf = anyNumber))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the product of numbers in <p>a</p>.</desc>
        <detail>Returns one if the array is empty.</detail>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "ProductInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "ProductLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "ProductFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "ProductDouble())")
      case _ =>
        throw new Exception  // should never be called
    }
    class ProductInt {
      def apply(a: PFAArray[Int]): Int = a.toVector.product
    }
    class ProductLong {
      def apply(a: PFAArray[Long]): Long = a.toVector.product
    }
    class ProductFloat {
      def apply(a: PFAArray[Float]): Float = a.toVector.product
    }
    class ProductDouble {
      def apply(a: PFAArray[Double]): Double = a.toVector.product
    }
  }
  provide(Product)

  ////   lnsum (Lnsum)
  object Lnsum extends LibFcn {
    val name = prefix + "lnsum"
    val sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    val doc =
      <doc>
        <desc>Return the sum of the natural logarithm of numbers in <p>a</p>.</desc>
        <detail>Returns zero if the array is empty and <c>NaN</c> if any value in the array is zero or negative.</detail>
      </doc>
    def apply[X <: java.lang.Number](a: PFAArray[X]): Double = a.toVector map {x => java.lang.Math.log(x.doubleValue)} sum
  }
  provide(Lnsum)

  ////   mean (Mean)
  object Mean extends LibFcn {
    val name = prefix + "mean"
    val sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    val doc =
      <doc>
        <desc>Return the arithmetic mean of numbers in <p>a</p>.</desc>
        <detail>Returns <c>NaN</c> if the array is empty.</detail>
      </doc>
    def apply[X <: java.lang.Number](a: PFAArray[X]): Double = {
      val numer = a.toVector map {x => x.doubleValue} sum
      val denom = a.toVector.size.toDouble
      numer / denom
    }
  }
  provide(Mean)

  ////   geomean (GeoMean)
  object GeoMean extends LibFcn {
    val name = prefix + "geomean"
    val sig = Sig(List("a" -> P.Array(P.Double)), P.Double)
    val doc =
      <doc>
        <desc>Return the geometric mean of numbers in <p>a</p>.</desc>
        <detail>Returns <c>NaN</c> if the array is empty.</detail>
      </doc>
    def apply[X <: java.lang.Number](a: PFAArray[X]): Double =
      Math.pow(a.toVector map {x => x.doubleValue} product, 1.0/a.toVector.size)
  }
  provide(GeoMean)

  ////   median (Median)
  object Median extends LibFcn {
    val name = prefix + "median"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the value that is in the center of a sorted version of <p>a</p>.</desc>
        <detail>If <p>a</p> has an odd number of elements, the median is the exact center of the sorted array.  If <p>a</p> has an even number of elements and is a <c>float</c> or <c>double</c>, the median is the average of the two elements closest to the center of the sorted array.  For any other type, the median is the left (first) of the two elements closest to the center of the sorted array.</detail>{errorOnEmpty}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "SorterInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "SorterLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "SorterFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "SorterDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    class SorterInt {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else {
          val sorted = PFAArray.fromVector(a.toVector.sorted)
          val index = a.toVector.size / 2
          sorted(index)
        }
    }
    class SorterLong {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else {
          val sorted = PFAArray.fromVector(a.toVector.sorted)
          val index = a.toVector.size / 2
          sorted(index)
        }
    }
    class SorterFloat {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else {
          val sorted = PFAArray.fromVector(a.toVector.sorted)
          if (a.toVector.size % 2 == 1)
            sorted(a.toVector.size / 2)
          else
            (sorted(a.toVector.size / 2) + sorted(a.toVector.size / 2 + 1)) / 2.0f
        }
    }
    class SorterDouble {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else {
          val sorted = PFAArray.fromVector(a.toVector.sorted)
          if (a.toVector.size % 2 == 1)
            sorted(a.toVector.size / 2)
          else
            (sorted(a.toVector.size / 2) + sorted(a.toVector.size / 2 + 1)) / 2.0
        }
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else {
          val sorted = PFAArray.fromVector(a.toVector.sortWith(comparisonOperatorLT))
          val index = a.toVector.size / 2
          sorted(index)
        }
    }
  }
  provide(Median)

  ////   mode (Mode)
  object Mode extends LibFcn {
    val name = prefix + "mode"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the mode (most common) value of <p>a</p>.</desc>
        <detail>If several different values are equally common, the median of these is returned.</detail>{errorOnEmpty}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroArray(AvroInt()) =>
        JavaCode("(new " + this.getClass.getName + "SorterInt())")
      case AvroArray(AvroLong()) =>
        JavaCode("(new " + this.getClass.getName + "SorterLong())")
      case AvroArray(AvroFloat()) =>
        JavaCode("(new " + this.getClass.getName + "SorterFloat())")
      case AvroArray(AvroDouble()) =>
        JavaCode("(new " + this.getClass.getName + "SorterDouble())")
      case AvroArray(x) =>
        JavaCode("(new " + this.getClass.getName + "SorterOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s)))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
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
    class SorterInt {
      def apply(a: PFAArray[Int]): Int =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          new Median.SorterInt()(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterLong {
      def apply(a: PFAArray[Long]): Long =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          new Median.SorterLong()(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterFloat {
      def apply(a: PFAArray[Float]): Float =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          new Median.SorterFloat()(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterDouble {
      def apply(a: PFAArray[Double]): Double =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          new Median.SorterDouble()(PFAArray.fromVector(mostCommon(a)))
    }
    class SorterOther(comparisonOperatorLT: ComparisonOperatorLT) {
      def apply[X <: AnyRef](a: PFAArray[X]): X =
        if (a.toVector.isEmpty)
          throw new PFARuntimeException("empty array")
        else
          new Median.SorterOther(comparisonOperatorLT)(PFAArray.fromVector(mostCommon(a)))
    }
  }
  provide(Mode)

  //////////////////////////////////////////////////////////////////// set or set-like functions

  ////   distinct (Distinct)
  object Distinct extends LibFcn {
    val name = prefix + "distinct"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array with the same contents as <p>a</p> but with duplicates removed.</desc>
      </doc>
    def apply[X](a: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.distinct)
  }
  provide(Distinct)

  ////   seteq (SetEq)
  object SetEq extends LibFcn {
    val name = prefix + "seteq"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>a</p> and <p>b</p> are equivalent, ignoring order and duplicates, <c>false</c> otherwise.</desc>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): Boolean = a.toVector.toSet == b.toVector.toSet
  }
  provide(SetEq)

  ////   union (Union)
  object Union extends LibFcn {
    val name = prefix + "union"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array that represents the union of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.toSet union b.toVector.toSet toVector)
  }
  provide(Union)

  ////   intersect (Intersect)
  object Intersect extends LibFcn {
    val name = prefix + "intersect"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array that represents the intersection of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.toSet intersect b.toVector.toSet toVector)
  }
  provide(Intersect)

  ////   diff (Diff)
  object Diff extends LibFcn {
    val name = prefix + "diff"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array that represents the difference of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = PFAArray.fromVector(a.toVector.toSet diff b.toVector.toSet toVector)
  }
  provide(Diff)

  ////   symdiff (SymDiff)
  object SymDiff extends LibFcn {
    val name = prefix + "symdiff"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return an array that represents the symmetric difference of <p>a</p> and <p>b</p>, treated as sets (ignoring order and duplicates).</desc>
        <detail>The symmetric difference is (<p>a</p> diff <p>b</p>) union (<p>b</p> diff <p>a</p>).</detail>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): PFAArray[X] = {
      val x = a.toVector.toSet
      val y = b.toVector.toSet
      PFAArray.fromVector((x diff y) union (y diff x) toVector)
    }
  }
  provide(SymDiff)

  ////   subset (Subset)
  object Subset extends LibFcn {
    val name = prefix + "subset"
    val sig = Sig(List("little" -> P.Array(P.Wildcard("A")), "big" -> P.Array(P.Wildcard("A"))), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>little</p> is a subset of <p>big</p>, <c>false</c> otherwise.</desc>
      </doc>
    def apply[X](little: PFAArray[X], big: PFAArray[X]): Boolean = little.toVector.toSet subsetOf big.toVector.toSet
  }
  provide(Subset)

  ////   disjoint (Disjoint)
  object Disjoint extends LibFcn {
    val name = prefix + "disjoint"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("A"))), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>a</p> and <p>b</p> are disjoint, <c>false</c> otherwise.</desc>
      </doc>
    def apply[X](a: PFAArray[X], b: PFAArray[X]): Boolean = (a.toVector.toSet intersect b.toVector.toSet).isEmpty
  }
  provide(Disjoint)

  //////////////////////////////////////////////////////////////////// functional programming

  ////   map (MapApply)
  object MapApply extends LibFcn {
    val name = prefix + "map"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Wildcard("B"))), P.Array(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and return an array of the results.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X, Y](a: PFAArray[X], fcn: X => Y): PFAArray[Y] = PFAArray.fromVector(a.toVector.map(fcn))
  }
  provide(MapApply)

  ////   mapIndex (MapIndex)
  object MapIndex extends LibFcn {
    val name = prefix + "mapIndex"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Int, P.Wildcard("A")), P.Wildcard("B"))), P.Array(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to index, element pairs from <p>a</p> and return an array of the results.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X, Y](a: PFAArray[X], fcn: (Int, X) => Y): PFAArray[Y] = PFAArray.fromVector(a.toVector.zipWithIndex map {case (x, i) => fcn(i, x)})
  }
  provide(MapIndex)

  ////   filter (Filter)
  object Filter extends LibFcn {
    val name = prefix + "filter"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and return an array of the elements for which <p>fcn</p> returns <c>true</c>.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X](a: PFAArray[X], fcn: X => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.filter(fcn))
  }
  provide(Filter)

  ////   filtermap (FilterMap)
  object FilterMap extends LibFcn {
    val name = prefix + "filtermap"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Array(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and return an array of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
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
  provide(FilterMap)

  ////   flatmap (FlatMap)
  object FlatMap extends LibFcn {
    val name = prefix + "flatmap"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Array(P.Wildcard("B")))), P.Array(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and flatten the resulting arrays into a single array.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X, Y](a: PFAArray[X], fcn: X => PFAArray[Y]): PFAArray[Y] =
      PFAArray.fromVector(a.toVector.flatMap(x => fcn(x).toVector))
  }
  provide(FlatMap)

  ////   reduce (Reduce)
  object Reduce extends LibFcn {
    val name = prefix + "reduce"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally.</desc>
        <detail>The first parameter of <p>fcn</p> is the running tally and the second parameter is an element from <p>a</p>.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from left (beginning) to right (end), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative.  It need not be commutative.</detail>
      </doc>
    def apply[X](a: PFAArray[X], fcn: (X, X) => X): X =
      a.toVector.reduceLeft(fcn)
  }
  provide(Reduce)

  ////   reduceright (ReduceRight)
  object ReduceRight extends LibFcn {
    val name = prefix + "reduceright"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Wildcard("A"))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally.</desc>
        <detail>The first parameter of <p>fcn</p> is an element from <p>a</p> and the second parameter is the running tally.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from right (end) to left (beginning), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative.  It need not be commutative.</detail>
      </doc>
    def apply[X](a: PFAArray[X], fcn: (X, X) => X): X =
      a.toVector.reduceRight(fcn)
  }
  provide(ReduceRight)

  ////   fold (Fold)
  object Fold extends LibFcn {
    val name = prefix + "fold"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "zero" -> P.Wildcard("B"), "fcn" -> P.Fcn(List(P.Wildcard("B"), P.Wildcard("A")), P.Wildcard("B"))), P.Wildcard("B"))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally, starting with <p>zero</p>.</desc>
        <detail>The first parameter of <p>fcn</p> is the running tally and the second parameter is an element from <p>a</p>.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from left (beginning) to right (end), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative with <p>zero</p> as its identity; that is, <c>fcn(zero, zero) = zero</c>.  It need not be commutative.</detail>
      </doc>
    def apply[X, Y](a: PFAArray[X], zero: Y, fcn: (Y, X) => Y): Y =
      a.toVector.foldLeft(zero)(fcn)
  }
  provide(Fold)

  ////   foldright (FoldRight)
  object FoldRight extends LibFcn {
    val name = prefix + "foldright"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "zero" -> P.Wildcard("B"), "fcn" -> P.Fcn(List(P.Wildcard("B"), P.Wildcard("A")), P.Wildcard("B"))), P.Wildcard("B"))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element of <p>a</p> and accumulate a tally, starting with <p>zero</p>.</desc>
        <detail>The first parameter of <p>fcn</p> is an element from <p>a</p> and the second parameter is the running tally.</detail>
        <detail>The order in which <p>fcn</p> is called on elements of <p>a</p> is not guaranteed, though it accumulates from right (end) to left (beginning), called exactly once for each element.  For predictable results, <p>fcn</p> should be associative with <p>zero</p> as its identity; that is, <c>fcn(zero, zero) = zero</c>.  It need not be commutative.</detail>
      </doc>
    def apply[X, Y](a: PFAArray[X], zero: Y, fcn: (X, Y) => Y): Y =
      a.toVector.foldRight(zero)(fcn)
  }
  provide(FoldRight)

  ////   takeWhile (TakeWhile)
  object TakeWhile extends LibFcn {
    val name = prefix + "takeWhile"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to elements of <p>a</p> and create an array of the longest prefix that returns <c>true</c>, stopping with the first <c>false</c>.</desc>
        <detail>Beyond the prefix, the number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def apply[X](a: PFAArray[X], fcn: X => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.takeWhile(fcn))
  }
  provide(TakeWhile)

  ////   dropWhile (DropWhile)
  object DropWhile extends LibFcn {
    val name = prefix + "dropWhile"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to elements of <p>a</p> and create an array of all elements after the longest prefix that returns <c>true</c>.</desc>
        <detail>Beyond the prefix, the number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def apply[X](a: PFAArray[X], fcn: X => Boolean): PFAArray[X] = PFAArray.fromVector(a.toVector.dropWhile(fcn))
  }
  provide(DropWhile)

  //////////////////////////////////////////////////////////////////// functional tests

  ////   any (Any)
  object Any extends LibFcn {
    val name = prefix + "any"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> for any element in <p>a</p> (logical or).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def apply[X](a: PFAArray[X], fcn: X => Boolean): Boolean = a.toVector.exists(fcn)
  }
  provide(Any)

  ////   all (All)
  object All extends LibFcn {
    val name = prefix + "all"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> for all elements in <p>a</p> (logical and).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
      </doc>
    def apply[X](a: PFAArray[X], fcn: X => Boolean): Boolean = a.toVector.forall(fcn)
  }
  provide(All)

  ////   corresponds (Corresponds)
  object Corresponds extends LibFcn {
    val name = prefix + "corresponds"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "b" -> P.Array(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all pairs of elements, one from <p>a</p> and the other from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the lengths of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def apply[X, Y](a: PFAArray[X], b: PFAArray[Y], fcn: (X, Y) => Boolean): Boolean = (a.toVector corresponds b.toVector)(fcn)
  }
  provide(Corresponds)

  //////////////////////////////////////////////////////////////////// restructuring

  ////   slidingWindow (SlidingWindow)
  object SlidingWindow extends LibFcn {
    val name = prefix + "slidingWindow"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "size" -> P.Int, "step" -> P.Int, "allowIncomplete" -> P.Boolean), P.Array(P.Array(P.Wildcard("A"))))
    val doc =
      <doc>
        <desc>Return an array of subsequences of <p>a</p> with length <p>size</p> that slide through <p>a</p> in steps of length <p>step</p> from left to right.</desc>
        <detail>If <p>allowIncomplete</p> is <c>true</c>, the last window may be smaller than <p>size</p>.  If <c>false</c>, the last window may be skipped.</detail>
        <error>If <p>size</p> is non-positive, a {""""size < 1""""} runtime error is raised.</error>
        <error>If <p>step</p> is non-positive, a {""""step < 1""""} runtime error is raised.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "WithClockCheck(thisEngineBase))")
    class WithClockCheck(engine: PFAEngineBase) {
      def apply[X](a: PFAArray[X], size: Int, step: Int, allowIncomplete: Boolean): PFAArray[PFAArray[X]] =
        if (size < 1)
          throw new PFARuntimeException("size < 1")
        else if (step < 1)
          throw new PFARuntimeException("step < 1")
        else {
          var i = 0
          val out =
            for (x <- a.toVector.sliding(size, step) if (allowIncomplete  ||  x.size == size)) yield {
              if (i % 1000 == 0)
                engine.checkClock()
              i += 1
              PFAArray.fromVector(x)
            }
          PFAArray.fromVector(out.toVector)
        }
    }
  }
  provide(SlidingWindow)

  ////   combinations (Combinations)
  object Combinations extends LibFcn {
    val name = prefix + "combinations"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "size" -> P.Int), P.Array(P.Array(P.Wildcard("A"))))
    val doc =
      <doc>
        <desc>Return the unique combinations of <p>a</p> with length <p>size</p>.</desc>
        <error>If <p>size</p> is non-positive, a {""""size < 1""""} runtime error is raised.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "WithClockCheck(thisEngineBase))")
    class WithClockCheck(engine: PFAEngineBase) {
      def apply[X](a: PFAArray[X], size: Int): PFAArray[PFAArray[X]] =
        if (size < 1)
          throw new PFARuntimeException("size < 1")
        else {
          var i = 0
          val out =
            for (x <- a.toVector.combinations(size)) yield {
              if (i % 1000 == 0)
                engine.checkClock()
              i += 1
              PFAArray.fromVector(x)
            }
          PFAArray.fromVector(out.toVector)
        }
    }
  }
  provide(Combinations)

  ////   permutations (Permutations)
  object Permutations extends LibFcn {
    val name = prefix + "permutations"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Array(P.Array(P.Wildcard("A"))))
    val doc =
      <doc>
        <desc>Return the permutations of <p>a</p>.</desc>{describeTimeout}
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "WithClockCheck(thisEngineBase))")
    class WithClockCheck(engine: PFAEngineBase) {
      def apply[X](a: PFAArray[X]): PFAArray[PFAArray[X]] = {
        var i = 0
        val out =
          for (x <- a.toVector.permutations) yield {
            if (i % 1000 == 0)
              engine.checkClock()
            i += 1
            PFAArray.fromVector(x)
          }
        PFAArray.fromVector(out.toVector)
      }
    }
  }
  provide(Permutations)

  ////   flatten (Flatten)
  object Flatten extends LibFcn {
    val name = prefix + "flatten"
    val sig = Sig(List("a" -> P.Array(P.Array(P.Wildcard("A")))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Concatenate the arrays in <p>a</p>.</desc>
      </doc>
    def apply[X](a: PFAArray[PFAArray[X]]): PFAArray[X] = PFAArray.fromVector(a.toVector flatMap {_.toVector})
  }
  provide(Flatten)

  ////   groupby (GroupBy)
  object GroupBy extends LibFcn {
    val name = prefix + "groupby"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.String)), P.Map(P.Array(P.Wildcard("A"))))
    val doc =
      <doc>
        <desc>Groups elements of <p>a</p> by the string that <p>fcn</p> maps them to.</desc>
      </doc>
    def apply[X](a: PFAArray[X], fcn: X => String): PFAMap[PFAArray[X]] =
      PFAMap.fromMap(a.toVector.groupBy(fcn) map {case (key, vector) => (key, PFAArray.fromVector(vector))})
  }
  provide(GroupBy)

}
