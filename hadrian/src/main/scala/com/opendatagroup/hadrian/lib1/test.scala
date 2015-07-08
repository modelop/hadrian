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
import scala.collection.immutable.ListMap

import org.apache.avro.AvroRuntimeException

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.options.EngineOptions

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
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

package object test {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "test."

///////////////// KS TEST
  object KSTwoSample extends LibFcn {
    val name = prefix + "kolmogorov"
    val sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Double)
     
    val doc =
      <doc>
        <desc>Compare two datasets using the Kolmogorov-Smirnov test to determine if they might have been drawn from the same parent distribution.</desc>
        <param name="x">A bag of data.</param>
        <param name="y">Another bag of data.</param>
        <ret>Returns a value between 0.0 and 1.0 representing the cumulative probability that <p>x</p> and <p>y</p> were drawn from the same distribution: 1.0 indicates a perfect match.</ret>
        <detail>If both datasets are empty, this function returns 1.0</detail>
      </doc>

    ///// not callable from pfa, compute CDF of kolomogorov statistic
    def kolomogorov_cdf(z: Double): Double = {
      val L = Math.sqrt(2.0 * Math.PI) * (1.0/z)
      var sumpart = 0.0
      var v = 1
      while (v < 150) {
        sumpart += Math.exp(-Math.pow(2.0*v - 1.0, 2)*Math.pow(Math.PI,2)/(8.0*Math.pow(z,2)))
        v += 1
      }
      L*sumpart
    }

    def apply(x: PFAArray[Double], y: PFAArray[Double]): Double = {
      val xx = x.toVector.sorted
      val yy = y.toVector.sorted
      if (xx == yy)
        1.0
      else if (xx.isEmpty  ||  yy.isEmpty)
        0.0
      else {
        val n1 = xx.size
        val n2 = yy.size
        var j1 = 0
        var j2 = 0
        var fn1 = 0.0
        var fn2 = 0.0
        var d = 0.0
        var d1 = 0.0
        var d2 = 0.0
        var dt = 0.0
        while ((j1 < n1) && (j2 < n2)) {
          d1 = xx(j1)
          d2 = yy(j2)
          if (d1 <= d2) {
            j1 += 1
            fn1 = j1.toDouble/n1
          }
          if (d2 <= d1) {
            j2 += 1
            fn2 = j2.toDouble/n2
          }
          dt = Math.abs(fn1 - fn2)
          if (dt > d)
            d = dt
        }
        val en = Math.sqrt((n1 * n2)/(n1 + n2).toDouble)
        val stat = (en + 0.12 + 0.11/en)*d
        1.0 - kolomogorov_cdf(stat)
      }
    }
  }
  provide(KSTwoSample)
}






































