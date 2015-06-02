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
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m.special."

  ////   nChooseK (NChooseK)
  object NChooseK extends LibFcn with Function2[Int, Int, Int] {
    val name = prefix + "nChooseK"
    val sig = Sig(List( "n" -> P.Int, "k" -> P.Int), P.Int)
    val doc =
      <doc>
        <desc> The number of ways to choose <p>k</p> elements from a set of <p>n</p> elements.</desc>
          <param name="n">Total number of elements.</param>
          <param name="k">Numer of elements chosen.</param>
        <ret>With <m>{"n"}</m> and <m>{"k"}</m>, this function evaluates the binomial coefficient.</ret> 
        <error>Raises "domain error" if <m>{"k \\leq 0"}</m> and <m>{"n \\leq k"}</m>.</error>
      </doc>

    def apply(n: Int, k: Int): Int =
      if (n <= k  ||  k <= 0)
        throw new PFARuntimeException("domain error")
      else
        binomialCoefficient(n, k).toInt
  }
  provide(NChooseK)

  ////   lnBeta (LnBeta)
  object LnBeta extends LibFcn with Function2[Double, Double, Double] {
    val name = prefix + "lnBeta"
    val sig = Sig(List("a" -> P.Double, "b" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the beta function parameterized by <p>a</p> and <p>b</p>.</desc>
        <ret>With <m>{"a"}</m> and <m>{"b"}</m>, this function evaluates natural logarithm of the beta function. The beta function is <m>{"\\int_{0}^{1} t^{a - 1}(1 - t)^{b - 1} dt "}</m>.</ret> 
        <error>Raises "domain error" if <m>{"a \\leq 0"}</m> or if <m>{"b \\leq 0"}</m>.</error>
      </doc>

    def apply(a: Double, b: Double): Double =
      if (a <= 0.0  ||  b <= 0.0)
        throw new PFARuntimeException("domain error")
      else
        Beta.logBeta(a,b)
  }
  provide(LnBeta)

  ////   erf (Erf)
  object Erf extends LibFcn with Function1[Double, Double] {
    val name = prefix + "erf"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
    <doc>
    <desc>Return the error function of <p>x</p>.</desc>
    </doc>
    def apply(x: Double): Double = special.Erf.erf(x)
  }
  provide(Erf)

  ////   erfc (Erfc)
  object Erfc extends LibFcn with Function1[Double, Double] {
    val name = prefix + "erfc"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the complimentary error function of <p>x</p>.</desc>
      </doc>
    def apply(x: Double): Double = special.Erf.erfc(x)
  }
  provide(Erfc)

  ////   lnGamma (LnGamma)
  object LnGamma extends LibFcn with Function1[Double, Double] {
    val name = prefix + "lnGamma"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the natural log of the gamma function of <p>x</p>.</desc>
      </doc>
    def apply(x: Double): Double = {
      if (x <= 0)
        throw new PFARuntimeException("domain error")
      else
        Gamma.logGamma(x)
    }
  }
  provide(LnGamma)
}

//////////////////////////////////////////// regularized beta function
//////   incompletebetafcn (IncompleteBetaFcn)
//  object IncompleteBetaFcn extends LibFcn with Function3[Double, Double, Double, Double] {
//    val name = prefix + "incompletebetafcn"
//    val sig = Sig(List("x" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
//    val doc =
//    <doc>
//    <desc>Compute the incomplete beta function at <p>x</p> parameterized by <p>a</p> and <p>b</p>.</desc>
//    <param name="a">First parameter.</param>
//    <param name="b">Second parameter.</param>
//
//    <ret>With <m>{"a"}</m> and <m>{"b"}</m>, this function evaluates the incomplete beta function.  
//    The beta function is <m>{"\\int_{0}^{x} t^{a - 1}(1 - t)^{b - 1} dt "}</m>.</ret> 
//    <error>Raises "domain error" if <m>{"a \\leq 0"}</m> or, <m>{"b \\leq 0"}</m> or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//
//    def apply(x: Double, a: Double, b: Double): Double =
//      if (a <= 0.0 || b <= 0.0 || x <= 0.0){
//        throw new PFARuntimeException("domain error")
//      }else {
//        return new BetaDistribution(a,b).cumulativeProbability(x)
//      }
//  }
//  provide(IncompleteBetaFcn)
//
//
//////////////////////////////////////////// inverse incomplete beta function
//////   incompletebetafcninv (IncompleteBetaFcninv)
//  object IncompleteBetaFcnInv extends LibFcn with Function3[Double, Double, Double, Double] {
//    val name = prefix + "incompletebetafcninv"
//    val sig = Sig(List("x" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
//    val doc =
//    <doc>
//    <desc>Compute the inverse of the incomplete beta function at <p>x</p> parameterized by <p>a</p> and <p>b</p>.</desc>
//    <param name="a">First parameter.</param>
//    <param name="b">Second parameter.</param>
//
//    <ret>With <m>{"a"}</m> and <m>{"b"}</m>, this function evaluates the incomplete beta function.  
//    The beta function is <m>{"\\int_{0}^{x} t^{a - 1}(1 - t)^{b - 1} dt "}</m>.</ret> 
//    <error>Raises "domain error" if <m>{"a \\leq 0"}</m> or, <m>{"b \\leq 0"}</m> or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//
//    def apply(x: Double, a: Double, b: Double): Double =
//      if (a <= 0.0 || b <= 0.0 || x <= 0.0){
//        throw new PFARuntimeException("domain error")
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
//    <error>Raises "domain error" if <m>{"s"}</m> is zero or an integer less than zero, or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//
//    def apply(s: Double, x: Double): Double =
//      if (x <= 0.0 || s == 0.0){
//        throw new PFARuntimeException("domain error")
//      }else if (s < 0.0 && Math.floor(s) == s){
//        throw new PFARuntimeException("domain error")
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
//    <error>Raises "domain error" if <m>{"s"}</m> is zero or an integer less than zero, or if <m>{"x \\leq 0"}</m>.</error>
//    </doc>
//
//    def apply(s: Double, x: Double): Double =
//      if (x <= 0.0 || s == 0.0){
//        throw new PFARuntimeException("domain error")
//      }else if(s < 0.0 && Math.floor(s) == s){
//        throw new PFARuntimeException("domain error")
//      }else {
//        return Gamma.regularizedGammaP(s, x)
//      }
//  }
//  provide(RegularizedGammaPFcn)
//
//
//
