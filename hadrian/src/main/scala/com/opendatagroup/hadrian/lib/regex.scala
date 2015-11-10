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

import org.apache.avro.Schema

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFASemanticException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.JVMNameMangle
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFARecord

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

import org.jcodings.specific.UTF8Encoding
import org.jcodings.specific.UTF16BEEncoding
import org.jcodings.specific.ASCIIEncoding
import org.joni.{Option => JoniOption}
import org.joni.Regex
import org.joni.Region
import org.joni.Syntax
import org.joni.exception.JOniException
import java.nio.charset.Charset

package object regex {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)
  val prefix = "re."

  object RawOrUtf16 extends Enumeration {
    type RawOrUtf16 = Value
    val Raw, Utf16 = Value
  }

  val UTF_16BE = Charset.forName("UTF-16BE")

  class Regexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value, errString: String, errCode: Int, fcnName: String, pos: Option[String]) {
    var matcher =
      try {
        (rawOrUtf16 match {
          case RawOrUtf16.Utf16 => new Regex(pattern, 0, pattern.length, JoniOption.NONE, UTF16BEEncoding.INSTANCE, Syntax.PosixExtended) 
          case RawOrUtf16.Raw => new Regex(pattern, 0, pattern.length, JoniOption.NONE, ASCIIEncoding.INSTANCE, Syntax.PosixExtended) 
        }).matcher(haystack, 0, haystack.length)
      }
      catch {
        case je: JOniException => throw new PFARuntimeException(errString, errCode, fcnName, pos, je)
      }

    def search(start: Int): Boolean =
      matcher.search(start, haystack.length, JoniOption.DEFAULT) != -1  &&  start < haystack.length

    def groupsFound(): Int = matcher.getEagerRegion.beg.length   
    def getRegion(): Region = matcher.getEagerRegion()
  } 
  object Regexer {
    def bytesPerChar(rawOrUtf16: RawOrUtf16.Value): Int = rawOrUtf16 match {
      case RawOrUtf16.Utf16 => 2
      case RawOrUtf16.Raw => 1
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////
  ////   index (Index)
  class Index(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "index"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.Int)),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Array(P.Int))))
    def doc =
      <doc>
        <desc>Return the indices in <p>haystack</p> of the begining and end of the first match defined by <p>pattern</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35000

    def apply[X](haystack: String, pattern: String): PFAArray[Int] =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): PFAArray[Int] =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): PFAArray[Int] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      val found: Boolean = re.search(0)
      if (found) {
        val region = re.getRegion()
        PFAArray.fromVector(Vector(region.beg(0)/constant, region.end(0)/constant))
      }
      else
        PFAArray.fromVector(Vector())
    }
  }
  provide(new Index)

  ////   contains (Contains)
  class Contains(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "contains"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Boolean),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Boolean)))
    def doc =
      <doc>
        <desc>Return true if <p>pattern</p> matches anywhere within <p>haystack</p>, otherwise return false.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35010

    def apply[X](haystack: String, pattern: String): Boolean =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): Boolean =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): Boolean = { 
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      re.search(0)
    }
  }
  provide(new Contains)

  ////   count (Count)
  class Count(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "count"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Int),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Int)))
    def doc =
      <doc>
        <desc>Count the number of times <p>pattern</p> matches in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35020

    def apply[X](haystack: String, pattern: String): Int =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): Int =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): Int = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var total: Int = 0
      var found: Boolean = re.search(0)
      var region: Region = re.getRegion()
      var start: Int = region.end(0)
      while (found){
        total += 1
        found = re.search(start)
        region = re.getRegion()
        start = region.end(0)
      }
      total
    }
  }
  provide(new Count)

  ////   rIndex (rIndex)
  class RIndex(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "rindex"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.Int)),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Array(P.Int))))
    def doc =
      <doc>
        <desc>Return the location indices of the last <p>pattern</p> match in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35030

    def apply[X](haystack: String, pattern: String): PFAArray[Int] =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): PFAArray[Int] =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): PFAArray[Int] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var found: Boolean = re.search(0)
      var region: Region = re.getRegion()
      var start: Int = 0
      if (found) {
        while (found) {
          region = re.getRegion()
          start = region.end(0)
          found = re.search(start)
        }
        PFAArray.fromVector(Vector(region.beg(0)/constant, region.end(0)/constant))
      }
      else
        PFAArray.fromVector(Vector())
    }
  }
  provide(new RIndex)

  ////   groups (Groups)
  class Groups(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "groups"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.Array(P.Int))),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Array(P.Array(P.Int)))))
    def doc =
      <doc>
        <desc>Return the location indices of each <p>pattern</p> sub-match (group-match) in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35040

    def apply[X](haystack: String, pattern: String): PFAArray[PFAArray[Int]] =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): PFAArray[PFAArray[Int]] =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): PFAArray[PFAArray[Int]] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var found: Boolean = re.search(0)
      var region: Region = re.getRegion()
      var start: Int = region.end(0)
      if (found)
        PFAArray.fromVector(
          (for (i <- 0 to re.groupsFound() - 1) yield
            PFAArray.fromVector(Vector(region.beg(i)/constant, region.end(i)/constant))).toVector)
      else
        PFAArray.fromVector(Vector())
    }
  }
  provide(new Groups)

  ////   indexAll (IndexAll)
  class IndexAll(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "indexall"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.Array(P.Int))),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Array(P.Array(P.Int)))))
    def doc =
      <doc>
        <desc>Return the location indices of every <p>pattern</p> match in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35050

    def apply[X](haystack: String, pattern: String): PFAArray[PFAArray[Int]] =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): PFAArray[PFAArray[Int]] =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): PFAArray[PFAArray[Int]] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var region: Region = re.getRegion()
      var start: Int = 0
      var found: Boolean = re.search(start)
      var out = List[PFAArray[Int]]()
      if (found) {
        while (found) {
          region = re.getRegion()
          start = region.end(0)
          out = PFAArray.fromVector(Vector(region.beg(0)/constant, region.end(0)/constant)) :: out
          found = re.search(start)
        }
        PFAArray.fromVector(out.toVector.reverse)
      }
      else
        PFAArray.fromVector(Vector())
    }
  }
  provide(new IndexAll)

  ////   findAll (FindAll)
  class FindAll(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "findall"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.String)),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes),   P.Array(P.Bytes))))
    def doc =
      <doc>
        <desc>Return an array containing each string that <p>pattern</p> matched in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35060

    def apply(haystack: String, pattern: String): PFAArray[String] =
      PFAArray.fromVector(indexer(haystack.getBytes(UTF_16BE),
                                  pattern.getBytes(UTF_16BE),
                                  RawOrUtf16.Utf16).toVector.reverse.map(new String(_, UTF_16BE)))

    def apply(haystack: Array[Byte], pattern: Array[Byte]): PFAArray[Array[Byte]] =
      PFAArray.fromVector(indexer(haystack, pattern, RawOrUtf16.Raw).toVector.reverse)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): List[Array[Byte]] = { 
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var region: Region = re.getRegion()
      var start: Int = 0
      var found: Boolean = re.search(start)
      var out = List[Array[Byte]]()
      while (found) {
        region = re.getRegion()
        start = region.end(0)
        out = haystack.slice(region.beg(0), region.end(0)) :: out
        found = re.search(start)
      }
      out
    }
  }
  provide(new FindAll)

  ////   findFirst (findFirst)
  class FindFirst(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "findfirst"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Union(List(P.String, P.Null))),
                        Sig(List("haystack" -> P.Bytes,  "pattern" -> P.Bytes ), P.Union(List(P.Bytes,  P.Null)))))
    def doc =
     <doc>
       <desc>Return the first occurance of what <p>pattern</p> matched in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
     </doc>
    def errcodeBase = 35070

    def apply(haystack: String, pattern: String): String = {
      val out = indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)
      if (out != null)
        new String(out, UTF_16BE)
      else
        null
    }

    def apply(haystack: Array[Byte], pattern: Array[Byte]): Array[Byte] =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): Array[Byte] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val found: Boolean = re.search(0)
      if (found) {
        val region: Region = re.getRegion()
        haystack.slice(region.beg(0), region.end(0))
      }
      else
        null
    }
  }
  provide(new FindFirst)

  ////   findGroupsFirst (FindGroupsFirst)
  class FindGroupsFirst(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "findgroupsfirst"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.String)),
                        Sig(List("haystack" -> P.Bytes,  "pattern" -> P.Bytes),  P.Array(P.Bytes))))
    def doc =
      <doc>
        <desc>Return an array of strings or bytes for each <p>pattern</p> sub-match (group-match) at the first occurance of <p>pattern</p> in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35080

    def apply(haystack: String, pattern: String): PFAArray[String] =
      PFAArray.fromVector(indexer(haystack.getBytes(UTF_16BE),
                                  pattern.getBytes(UTF_16BE),
                                  RawOrUtf16.Utf16).toVector.reverse.map(new String(_, UTF_16BE)))

    def apply(haystack: Array[Byte], pattern: Array[Byte]): PFAArray[Array[Byte]] =
      PFAArray.fromVector(indexer(haystack, pattern, RawOrUtf16.Raw).toVector.reverse)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): List[Array[Byte]] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      val start: Int = 0
      val found: Boolean = re.search(start)
      var out = List[Array[Byte]]()
      val region: Region = re.getRegion()
      if (found) {
        for (i <- 0 to re.groupsFound() - 1) yield {
          out = haystack.slice(region.beg(i), region.end(i)) :: out
        }
        out
      }
      else
        List()
    }
  }
  provide(new FindGroupsFirst)

  ////   findGroupsAll (FindGroupsAll)
  class FindGroupsAll(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "findgroupsall"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.Array(P.String))),
                        Sig(List("haystack" -> P.Bytes,  "pattern" -> P.Bytes),  P.Array(P.Array(P.Bytes)))))
    def doc =
      <doc>
       <desc>Return an array of strings or bytes for each <p>pattern</p> sub-match (group-match) at every occurance of <p>pattern</p> in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35090

    def apply(haystack: String, pattern: String): PFAArray[PFAArray[String]] =
      PFAArray.fromVector(indexer(haystack.getBytes(UTF_16BE),
                                  pattern.getBytes(UTF_16BE),
                                  RawOrUtf16.Utf16).reverse.map(x => PFAArray.fromVector(x.map(y => new String(y, UTF_16BE)).toVector)).toVector)

    def apply(haystack: Array[Byte], pattern: Array[Byte]): PFAArray[PFAArray[Array[Byte]]] =
      PFAArray.fromVector(indexer(haystack, pattern, RawOrUtf16.Raw).reverse.map(x => PFAArray.fromVector(x.toVector)).toVector)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): List[List[Array[Byte]]] = { 
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      var start: Int = 0
      var found: Boolean = re.search(start) 
      var region: Region = re.getRegion()
      var list_of_group_lists = List[List[Array[Byte]]]()
      while (found) { 
        region = re.getRegion()
        var group_list = List[Array[Byte]]()
        for (i <- 0 to re.groupsFound() - 1) yield {
          group_list = haystack.slice(region.beg(i), region.end(i)) :: group_list
        }
        list_of_group_lists = group_list.reverse :: list_of_group_lists
        start = region.end(0)
        found = re.search(start) 
      }
      list_of_group_lists
    }
  }
  provide(new FindGroupsAll)

  ////   groupsAll (GroupsAll)
  class GroupsAll(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "groupsall"

    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.Array(P.Array(P.Int)))),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes), P.Array(P.Array(P.Array(P.Int))))))
    def doc =
      <doc>
        <desc>Return the location indices of each <p>pattern</p> sub-match (group-match) for each occurance of <p>pattern</p> in <p>haystack</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35100

    def apply[X](haystack: String, pattern: String): PFAArray[PFAArray[PFAArray[Int]]] =
      indexer(haystack.getBytes(UTF_16BE), pattern.getBytes(UTF_16BE), RawOrUtf16.Utf16)

    def apply[X](haystack: Array[Byte], pattern: Array[Byte]): PFAArray[PFAArray[PFAArray[Int]]] =
      indexer(haystack, pattern, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): PFAArray[PFAArray[PFAArray[Int]]] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var start: Int = 0
      var found: Boolean = re.search(start)
      var region: Region = re.getRegion()
      var list_of_group_lists = List[List[Vector[Int]]]()
      if (found) {
        while (found) {
          region = re.getRegion()
          var group_list = List[Vector[Int]]()
          for (i <- 0 to re.groupsFound() - 1) yield {
            group_list = Vector(region.beg(i)/constant, region.end(i)/constant) :: group_list
          }
          list_of_group_lists = group_list.reverse :: list_of_group_lists
          start = region.end(0)
          found = re.search(start)
        }
        PFAArray.fromVector(list_of_group_lists.reverse.map(x => PFAArray.fromVector(x.map(z => PFAArray.fromVector(z.toVector)).toVector)).toVector)
      }
      else
        PFAArray.fromVector(Vector())
    }
  }
  provide(new GroupsAll)

  ////   replaceFirst (ReplaceFirst)
  class ReplaceFirst(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "replacefirst"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String, "replacement" -> P.String), P.String), 
                        Sig(List("haystack" -> P.Bytes,  "pattern" -> P.Bytes,  "replacement" -> P.Bytes), P.Bytes))) 
    def doc =
      <doc>
        <desc>Replace the first <p>pattern</p> match in <p>haystack</p> with <p>replacement</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35110

    def apply(haystack: String, pattern: String, replacement: String): String =
      new String(indexer(haystack.getBytes(UTF_16BE),
                         pattern.getBytes(UTF_16BE),
                         replacement.getBytes(UTF_16BE), RawOrUtf16.Utf16), UTF_16BE)

    def apply(haystack: Array[Byte], pattern: Array[Byte], replacement: Array[Byte]): Array[Byte] =
      indexer(haystack, pattern, replacement, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], replacement: Array[Byte], rawOrUtf16: RawOrUtf16.Value): Array[Byte] = { 
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      var if_the = re.search(0)
      val region: Region = re.getRegion()
      if (if_the)
        haystack.slice(0, region.beg(0)) ++ replacement ++ haystack.slice(region.end(0), haystack.length) 
      else
        haystack
    }
  }
  provide(new ReplaceFirst)

  ////   replaceLast (replaceLast)
  class ReplaceLast(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "replacelast"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String, "replacement" -> P.String), P.String), 
                        Sig(List("haystack" -> P.Bytes,  "pattern" -> P.Bytes,  "replacement" -> P.Bytes), P.Bytes))) 
    def doc =
      <doc>
        <desc>Replace the last <p>pattern</p> match in <p>haystack</p> with <p>replacement</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35120

    def apply(haystack: String, pattern: String, replacement: String): String =
      new String(indexer(haystack.getBytes(UTF_16BE),
                         pattern.getBytes(UTF_16BE),
                         replacement.getBytes(UTF_16BE),
                         RawOrUtf16.Utf16), UTF_16BE)

    def apply(haystack: Array[Byte], pattern: Array[Byte], replacement: Array[Byte]): Array[Byte] =
      indexer(haystack, pattern, replacement, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], replacement: Array[Byte], rawOrUtf16: RawOrUtf16.Value): Array[Byte] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      var found: Boolean = re.search(0)
      var region: Region = re.getRegion()
      var start: Int = 0
      if (found) {
        while (found) {
          region = re.getRegion()
          start = region.end(0)
          found = re.search(start)
        }
        haystack.slice(0, region.beg(0)) ++ replacement ++ haystack.slice(region.end(0), haystack.length)
      }
      else
        haystack
    }
  }
  provide(new ReplaceLast)

  ////   split (Split)
  class Split(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "split"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String), P.Array(P.String)),
                        Sig(List("haystack" -> P.Bytes, "pattern" -> P.Bytes),   P.Array(P.Bytes))))
    def doc =
      <doc>
        <desc>Break <p>haystack</p> into an array of strings or bytes on the separator defined by <p>pattern</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35130

    def apply(haystack: String, pattern: String): PFAArray[String] =
      PFAArray.fromVector(indexer(haystack.getBytes(UTF_16BE),
                                  pattern.getBytes(UTF_16BE),
                                  RawOrUtf16.Utf16).toVector.map(new String(_, UTF_16BE)))

    def apply(haystack: Array[Byte], pattern: Array[Byte]): PFAArray[Array[Byte]] =
      PFAArray.fromVector(indexer(haystack, pattern, RawOrUtf16.Raw).toVector)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], rawOrUtf16: RawOrUtf16.Value): List[Array[Byte]] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var out = List[Array[Byte]]()
      var found: Boolean = re.search(0)
      var region: Region = re.getRegion()
      var beg: Int = 0
      var end: Int = region.beg(0)
      var start: Int = 0
      // make a note of whether match was first thing in haystack
      var flag: Boolean = false
      if (region.beg(0) == 0)
        flag = true
      if (found) {
        while (found) {
          out = haystack.slice(beg, end) :: out
          beg = region.end(0)
          found = re.search(beg)
          region = re.getRegion()
          end  = region.beg(0)
        }
        // check if match is last thing in haystack
        if (beg != haystack.length)
          out = haystack.slice(beg, haystack.length) :: out
        out = out.reverse
        // check if match was first thing in haystack
        if (flag) {
          out = out.tail
        }
        out
      }
      else
        List(haystack)
    }
  }
  provide(new Split)

  ////   replaceAll (ReplaceAll)
  class ReplaceAll(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "replaceall"
    def sig = Sigs(List(Sig(List("haystack" -> P.String, "pattern" -> P.String, "replacement" -> P.String), P.String), 
                        Sig(List("haystack" -> P.Bytes,  "pattern" -> P.Bytes,  "replacement" -> P.Bytes), P.Bytes))) 
    def doc =
      <doc>
        <desc>Replace the all <p>pattern</p> matches in <p>haystack</p> with <p>replacement</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>pattern</p> is not a valid regular expression, a "bad pattern" error is raised.</error>
      </doc>
    def errcodeBase = 35140

    def apply(haystack: String, pattern: String, replacement: String): String =
      new String(indexer(haystack.getBytes(UTF_16BE),
                         pattern.getBytes(UTF_16BE),
                         replacement.getBytes(UTF_16BE),
                         RawOrUtf16.Utf16), UTF_16BE)

    def apply(haystack: Array[Byte], pattern: Array[Byte], replacement: Array[Byte]): Array[Byte] =
      indexer(haystack, pattern, replacement, RawOrUtf16.Raw)

    def indexer(haystack: Array[Byte], pattern: Array[Byte], replacement: Array[Byte], rawOrUtf16: RawOrUtf16.Value): Array[Byte] = {
      val re = new Regexer(haystack, pattern, rawOrUtf16, "bad pattern", errcodeBase + 0, name, pos)
      val constant: Int = Regexer.bytesPerChar(rawOrUtf16)
      var found: Boolean = re.search(0)
      var region: Region = re.getRegion()
      var s = new scala.collection.mutable.ArrayBuffer[Array[Byte]]()
      var beg: Int = 0
      var end: Int = region.beg(0)
      if (found) {
        while (found) {
          s += haystack.slice(beg,end)
          s += replacement
          beg = region.end(0)
          found = re.search(beg)
          region = re.getRegion()
          end  = region.beg(0)
        }
        // check if match is last thing in haystack
        if (beg != haystack.length)
          s += haystack.slice(beg, haystack.length)
        s.flatten.toArray
      }
      else
        haystack
    }
  }
  provide(new ReplaceAll)
}
