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

import org.apache.commons.math3.special
import org.apache.commons.math3.special.Beta
import org.apache.commons.math3.distribution.BetaDistribution
import org.apache.commons.math3.special.Gamma
import org.apache.commons.math3.util.CombinatoricsUtils.binomialCoefficient

package object spec {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m.special."

  ////   nChooseK (NChooseK)
  class NChooseK(val pos: Option[String] = None) extends LibFcn with Function2[Int, Int, Int] {
    def name = prefix + "nChooseK"
    def sig = Sig(List( "n" -> P.Int, "k" -> P.Int), P.Int)
    def doc =
      <doc>
        <desc> The number of ways to choose <p>k</p> elements from a set of <p>n</p> elements.</desc>
          <param name="n">Total number of elements.</param>
          <param name="k">Numer of elements chosen.</param>
        <ret>With <m>{"n"}</m> and <m>{"k"}</m>, this function evaluates the binomial coefficient.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "domain error" if <m>{"k \\leq 0"}</m> or <m>{"k \\geq n"}</m>.</error>
      </doc>
    def errcodeBase = 36000

    def apply(n: Int, k: Int): Int =
      if (n <= k  ||  k <= 0)
        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
      else
        binomialCoefficient(n, k).toInt
  }
  provide(new NChooseK)

  ////   lnBeta (LnBeta)
  class LnBeta(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "lnBeta"
    def sig = Sig(List("a" -> P.Double, "b" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the beta function parameterized by <p>a</p> and <p>b</p>.</desc>
        <ret>With <m>{"a"}</m> and <m>{"b"}</m>, this function evaluates natural logarithm of the beta function. The beta function is <m>{"\\int_{0}^{1} t^{a - 1}(1 - t)^{b - 1} dt "}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "domain error" if <m>{"a \\leq 0"}</m> or if <m>{"b \\leq 0"}</m>.</error>
      </doc>
    def errcodeBase = 36010

    def apply(a: Double, b: Double): Double =
      if (a <= 0.0  ||  b <= 0.0)
        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
      else
        Beta.logBeta(a,b)
  }
  provide(new LnBeta)

  ////   erf (Erf)
  class Erf(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "erf"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
    <doc>
    <desc>Return the error function of <p>x</p>.</desc>
    </doc>
    def errcodeBase = 36020
    def apply(x: Double): Double = special.Erf.erf(x)
  }
  provide(new Erf)

  // ////   erf (Erf)
  // class Erf(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
  //   val a1: Double =  0.254829592
  //   val a2: Double = -0.284496736
  //   val a3: Double =  1.421413741
  //   val a4: Double = -1.453152027
  //   val a5: Double =  1.061405429
  //   val  p: Double =  0.3275911

  //   def name = prefix + "special.erf"
  //   def sig = Sig(List("x" -> P.Double), P.Double)
  //   def doc =
  //     <doc>
  //       <desc>Return the error function of <p>x</p>.</desc>{wholeLine()}
  //     </doc>
  //   def errcodeBase = 36030
  //   def apply(x: Double): Double = {
  //     val sign = if (x < 0.0) -1.0 else 1.0
  //     val absx =  Math.abs(x)

  //     val t = 1.0/(1.0 + p*absx)
  //     val y = 1.0 - (((((a5*t + a4)*t + a3)*t + a2)*t + a1)*t * Math.exp(-x*x))
  //     sign*y
  //   }
  // }
  // provide(SpecialErf)

  ////   erfc (Erfc)
  class Erfc(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "erfc"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the complimentary error function of <p>x</p>.</desc>
      </doc>
    def errcodeBase = 36040
    def apply(x: Double): Double = special.Erf.erfc(x)
  }
  provide(new Erfc)

  ////   lnGamma (LnGamma)
  class LnGamma(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "lnGamma"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the natural log of the gamma function of <p>x</p>.</desc>
      </doc>
    def errcodeBase = 36050
    def apply(x: Double): Double = {
      if (x <= 0)
        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
      else
        Gamma.logGamma(x)
    }
  }
  provide(new LnGamma)
}

//////////////////////////////////////////// regularized beta function
//////   incompletebetafcn (IncompleteBetaFcn)
//  class IncompleteBetaFcn(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
//    def name = prefix + "incompletebetafcn"
//    def sig = Sig(List("x" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
//    def doc =
//    <doc>
//    <desc>Compute the incomplete beta function at <p>x</p> parameterized by <p>a</p> and <p>b</p>.</desc>
//    <param name="a">First parameter.</param>
//    <param name="b">Second parameter.</param>
//
//    <ret>With <m>{"a"}</m> and <m>{"b"}</m>, this function evaluates the incomplete beta function.  
//    The beta function is <m>{"\\int_{0}^{x} t^{a - 1}(1 - t)^{b - 1} dt "}</m>.</ret> 
//    <error code={s"${errcodeBase + 0}"}>Raises "domain error" if <m>{"a \\leq 0"}</m> or, <m>{"b \\leq 0"}</m> or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//    def errcodeBase = 36060
//
//    def apply(x: Double, a: Double, b: Double): Double =
//      if (a <= 0.0 || b <= 0.0 || x <= 0.0){
//        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
//      }else {
//        return new BetaDistribution(a,b).cumulativeProbability(x)
//      }
//  }
//  provide(new IncompleteBetaFcn)
//
//
//////////////////////////////////////////// inverse incomplete beta function
//////   incompletebetafcninv (IncompleteBetaFcninv)
//  class IncompleteBetaFcnInv(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
//    def name = prefix + "incompletebetafcninv"
//    def sig = Sig(List("x" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
//    def doc =
//    <doc>
//    <desc>Compute the inverse of the incomplete beta function at <p>x</p> parameterized by <p>a</p> and <p>b</p>.</desc>
//    <param name="a">First parameter.</param>
//    <param name="b">Second parameter.</param>
//
//    <ret>With <m>{"a"}</m> and <m>{"b"}</m>, this function evaluates the incomplete beta function.  
//    The beta function is <m>{"\\int_{0}^{x} t^{a - 1}(1 - t)^{b - 1} dt "}</m>.</ret> 
//    <error code={s"${errcodeBase + 0}"}>Raises "domain error" if <m>{"a \\leq 0"}</m> or, <m>{"b \\leq 0"}</m> or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//    def errcodeBase = 36070
//
//    def apply(x: Double, a: Double, b: Double): Double =
//      if (a <= 0.0 || b <= 0.0 || x <= 0.0){
//        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
//      }else {
//        return new BetaDistribution(a,b).inverseCumulativeProbability(x)
//      }
//  }
//  provide(IncompleteBetaFcnInv)
//
//
//////////////////////////////////////////// regularized gamma Q
//////   regularizedgammaqfcn (RegularizedGammaQFcn)
//  object RegularizedGammaQFcn extends LibFcn with Function2[Double, Double, Double] {
//    val name = prefix + "regularizedgammaqfcn"
//    val sig = Sig(List( "s" -> P.Double, "x" -> P.Double), P.Double)
//    val doc =
//    <doc>
//    <desc>Compute the regularized gamma Q parameterized by <p>s</p> and <p>x</p>.</desc>
//    <param name="s">First parameter.</param>
//    <param name="x">Second parameter.</param>
//
//    <ret>With <m>{"s"}</m> and <m>{"x"}</m>, this function evaluates the regularized gamma Q function, <m>{"\\frac{\\gamma(s,x)}{\\Gamma(s)}"}</m>, where <m>{"\\gamma(s,x)"}</m> is the lower incomplete gamma function and <m>{"\\Gamma(s)"}</m> is the gamma function.</ret> 
//    <error code={s"${errcodeBase + 0}"}>Raises "domain error" if <m>{"s"}</m> is zero or an integer less than zero, or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//    def errcodeBase = 36080
//
//    def apply(s: Double, x: Double): Double =
//      if (x <= 0.0 || s == 0.0){
//        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
//      }else if (s < 0.0 && Math.floor(s) == s){
//        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
//      }else {
//        return Gamma.regularizedGammaQ(s, x)
//      }
//  }
//  provide(RegularizedGammaQFcn)
//
//
//////////////////////////////////////////// regularized gamma P
//////   regularizedgammapfcn (RegularizedGammaPFcn)
//  object RegularizedGammaPFcn extends LibFcn with Function2[Double, Double, Double] {
//    val name = prefix + "regularizedgammapfcn"
//    val sig = Sig(List( "s" -> P.Double, "x" -> P.Double), P.Double)
//    val doc =
//    <doc>
//    <desc>Compute the regularized gamma P parameterized by <p>s</p> and <p>x</p>.</desc>
//    <param name="s">First parameter.</param>
//    <param name="x">Second parameter.</param>
//
//    <ret>With <m>{"s"}</m> and <m>{"x"}</m>, this function evaluates the regularized gamma P function, <m>{"\\frac{\\Gamma(s,x)}{\\Gamma(s)}"}</m>, where <m>{"\\Gamma(s,x)"}</m> is the upper incomplete gamma function and <m>{"\\Gamma(s)"}</m> is the gamma function.</ret> 
//    <error code={s"${errcodeBase + 0}"}>Raises "domain error" if <m>{"s"}</m> is zero or an integer less than zero, or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//    def errcodeBase = 36090
//
//    def apply(s: Double, x: Double): Double =
//      if (x <= 0.0 || s == 0.0){
//        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
//      }else if(s < 0.0 && Math.floor(s) == s){
//        throw new PFARuntimeException("domain error", errcodeBase + 0, name, pos)
//      }else {
//        return Gamma.regularizedGammaP(s, x)
//      }
//  }
//  provide(RegularizedGammaPFcn)
//
//
//
