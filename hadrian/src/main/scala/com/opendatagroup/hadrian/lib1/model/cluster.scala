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

package com.opendatagroup.hadrian.lib1.model

import scala.language.postfixOps
import scala.collection.immutable.ListMap

import org.apache.avro.AvroRuntimeException
import org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.data.ComparisonOperator

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

import com.opendatagroup.hadrian.lib1.array.argLowestN

package object cluster {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.cluster."

  //////////////////////////////////////////////////////////////////// 

  ////   closest (Closest)
  object Closest extends LibFcn {
    val name = prefix + "closest"
    val sig = Sig(List(
      "datum" -> P.Array(P.Wildcard("A")),
      "clusters" -> P.Array(P.WildRecord("C", ListMap("center" -> P.Array(P.Wildcard("B"))))),
      "metric" -> P.Fcn(List(P.Array(P.Wildcard("A")), P.Array(P.Wildcard("B"))), P.Double)
    ), P.Wildcard("C"))
    val doc =
      <doc>
        <desc>Find the cluster <tp>C</tp> whose <pf>center</pf> is closest to the <p>datum</p>, according to the <p>metric</p>.</desc>
        <param name="datum">Sample datum.</param>
        <param name="clusters">Set of clusters; the record type <tp>C</tp> may contain additional identifying information for post-processing.</param>
        <param name="metric">Function used to compare each <p>datum</p> with the <pf>center</pf> of the <p>clusters</p>.  (See, for example, <f>metric.euclidean</f>.)</param>
        <ret>Returns the closest cluster record.</ret>
        <error>Raises a "no clusters" error if <p>clusters</p> is empty.</error>
      </doc>
    def apply[A, B](datum: PFAArray[A], clusters: PFAArray[PFARecord], metric: (PFAArray[A], PFAArray[B]) => Double): PFARecord = {
      val vector = clusters.toVector
      val length = vector.size
      if (length == 0)
        throw new PFARuntimeException("no clusters")
      var bestRecord: PFARecord = null
      var bestDistance = 0.0
      var i = 0
      while (i < length) {
        val thisRecord = vector(i)
        val thisDistance = metric(datum, thisRecord.get("center").asInstanceOf[PFAArray[B]])
        if (bestRecord == null  ||  thisDistance < bestDistance) {
          bestRecord = thisRecord
          bestDistance = thisDistance
        }
        i += 1
      }
      bestRecord
    }
  }
  provide(Closest)

  ////   closestN (ClosestN)
  object ClosestN extends LibFcn {
    val name = prefix + "closestN"
    val sig = Sig(List(
      "datum" -> P.Array(P.Wildcard("A")),
      "clusters" -> P.Array(P.WildRecord("C", ListMap("center" -> P.Array(P.Wildcard("B"))))),
      "metric" -> P.Fcn(List(P.Array(P.Wildcard("A")), P.Array(P.Wildcard("B"))), P.Double),
      "n" -> P.Int
    ), P.Array(P.Wildcard("C")))
    val doc =
      <doc>
        <desc>Find the <p>n</p> clusters <tp>C</tp> whose <pf>centers</pf> are closest to the <p>datum</p>, according to the <p>metric</p>.</desc>
        <param name="datum">Sample datum.</param>
        <param name="clusters">Set of clusters; the record type <tp>C</tp> may contain additional identifying information for post-processing.</param>
        <param name="metric">Function used to compare each <p>datum</p> with the <pf>center</pf> of the <p>clusters</p>.  (See, for example, <f>metric.euclidean</f>.)</param>
        <param name="n">Number of clusters to search for.</param>
        <ret>An array of the closest cluster records in order from the closest to the farthest.  The length of the array is minimum of <p>n</p> and the length of <p>clusters</p>.</ret>
        <detail>Note that this method can be used to implement k-nearest neighbors.</detail>
      </doc>
    def apply[A, B](datum: PFAArray[A], clusters: PFAArray[PFARecord], metric: (PFAArray[A], PFAArray[B]) => Double, n: Int): PFAArray[PFARecord] = {
      val vector = clusters.toVector
      val distances = vector map {record => record -> metric(datum, record.get("center").asInstanceOf[PFAArray[B]])} toMap

      val indexes = argLowestN(vector, n, (cluster1: PFARecord, cluster2: PFARecord) => distances(cluster1) < distances(cluster2))

      PFAArray.fromVector(indexes map {i => vector(i)})
    }
  }
  provide(ClosestN)

}
