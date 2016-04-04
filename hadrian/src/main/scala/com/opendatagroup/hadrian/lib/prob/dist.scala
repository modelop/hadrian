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

package com.opendatagroup.hadrian.lib.prob

import scala.collection.immutable.ListMap

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException

import com.opendatagroup.hadrian.data.PFARecord

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
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

import org.apache.commons.math3.distribution.BetaDistribution
import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.commons.math3.distribution.CauchyDistribution
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.distribution.FDistribution
import org.apache.commons.math3.distribution.GammaDistribution
import org.apache.commons.math3.distribution.GeometricDistribution
import org.apache.commons.math3.distribution.HypergeometricDistribution
import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.commons.math3.distribution.PascalDistribution
import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.distribution.UniformRealDistribution
import org.apache.commons.math3.distribution.WeibullDistribution
import org.apache.commons.math3.special.Erf
import org.apache.commons.math3.special.Gamma
import org.apache.commons.math3.util.CombinatoricsUtils.binomialCoefficient

package object dist {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "prob.dist."

  def binomialCoeff(n: Int, k: Int): Long =
    try {
      binomialCoefficient(n, k)
    }
    catch {
      case _: org.apache.commons.math3.exception.MathArithmeticException =>
        Math.round(Math.exp(Gamma.logGamma(n + 1) - Gamma.logGamma(k + 1) - Gamma.logGamma(n - k + 1))).toLong
    }

//////////////////////////////////////////// Gaussian distribution
////   gaussianLL (GaussianLL)
  class GaussianLL(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "gaussianLL"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("x" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    def doc =
      <doc>
        <desc>Compute the log-likelihood of a Gaussian (normal) distribution parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="x">Value at which to compute the log-likelihood.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"-(x - \\mu)^2/(2 \\sigma^2) - \\log(\\sigma \\sqrt{2\\pi})"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid parameterization" error if <p>sigma</p> or <pf>variance</pf> is negative or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13000

    def apply(x: Double, mu: Double, sigma: Double): Double =
      if (java.lang.Double.isInfinite(mu)  ||  java.lang.Double.isNaN(mu)  ||  java.lang.Double.isInfinite(sigma)  ||  java.lang.Double.isNaN(sigma)  ||  sigma < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (sigma == 0.0) {
        if (x != mu)
          java.lang.Double.NEGATIVE_INFINITY
        else
          java.lang.Double.POSITIVE_INFINITY
      }
      else
        -(x - mu)*(x - mu)/(2.0 * sigma*sigma) - Math.log(sigma * Math.sqrt(2.0 * Math.PI))

    def apply(x: Double, params: PFARecord): Double = {
      val mu = params.get("mean").asInstanceOf[java.lang.Number].doubleValue
      val variance = params.get("variance").asInstanceOf[java.lang.Number].doubleValue
      if (java.lang.Double.isInfinite(mu)  ||  java.lang.Double.isNaN(mu)  ||  java.lang.Double.isInfinite(variance)  ||  java.lang.Double.isNaN(variance)  ||  variance < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      val sigma = Math.sqrt(variance)
      if (sigma == 0.0) {
        if (x != mu)
          java.lang.Double.NEGATIVE_INFINITY
        else
          java.lang.Double.POSITIVE_INFINITY
      }
      else
        -(x - mu)*(x - mu)/(2.0 * sigma*sigma) - Math.log(sigma * Math.sqrt(2.0 * Math.PI))
    }
  }
  provide(new GaussianLL)

  ////   gaussianCDF (GaussianCDF)
  class GaussianCDF(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "gaussianCDF"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("x" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    def doc =
      <doc>
        <desc>Compute the cumultive distribution function (CDF) for the normal distribution, parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="x">Value at which to compute the CDF.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"0.5 * ( 1.0 + \\mathrm{Erf}(\\frac{x - \\mu}{\\sigma \\sqrt{2}}))"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid parameterization" error if <p>sigma</p> or <pf>variance</pf> is negative or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13010

    def apply(x: Double, mu: Double, sigma: Double): Double =
      if (java.lang.Double.isInfinite(mu)  ||  java.lang.Double.isNaN(mu)  ||  java.lang.Double.isInfinite(sigma)  ||  java.lang.Double.isNaN(sigma)  ||  sigma < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (sigma == 0.0) {
        if (x < mu)
          0.0
        else
          1.0
      }
      else
        0.5 * (1.0 + Erf.erf((x - mu)/(sigma * Math.sqrt(2.0))))

    def apply(x: Double, params: PFARecord): Double = {
      val mu = params.get("mean").asInstanceOf[java.lang.Number].doubleValue
      val variance = params.get("variance").asInstanceOf[java.lang.Number].doubleValue
      if (java.lang.Double.isInfinite(mu)  ||  java.lang.Double.isNaN(mu)  ||  java.lang.Double.isInfinite(variance)  ||  java.lang.Double.isNaN(variance)  ||  variance < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      val sigma = Math.sqrt(variance)
      if (sigma == 0.0) {
        if (x < mu)
          0.0
        else
          1.0
      }
      else
        0.5 * (1.0 + Erf.erf((x - mu)/(sigma * Math.sqrt(2.0))))
    }
  }
  provide(new GaussianCDF)

  ////   gaussianQF (GaussianQF)
  class GaussianQF(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "gaussianQF"
    def sig = Sigs(List(Sig(List("p" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("p" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    def doc =
      <doc>
        <desc>Compute the normal quantile (QF, the inverse of the CDF) parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="p">Probability at which to compute the QF.  Must be a value between 0 and 1.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"\\mu + \\sigma \\sqrt{2} \\mathrm{Erf}^{-1} (2p - 1)"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid parameterization" error if <p>sigma</p> or <pf>variance</pf> is negative or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises an "invalid input" error if <p>p</p> is less than zero or greater than one.</error>
      </doc>
    def errcodeBase = 13020


    def apply(p: Double, mu: Double, sigma: Double): Double = 
      if (java.lang.Double.isInfinite(mu)  ||  java.lang.Double.isNaN(mu)  ||  java.lang.Double.isInfinite(sigma)  ||  java.lang.Double.isNaN(sigma)  ||  sigma < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else if (sigma == 0.0)
        mu
      else
        mu + sigma * Math.sqrt(2.0) * Erf.erfInv(2.0 * p - 1.0)

    def apply(p: Double, params: PFARecord): Double = {
      val mu = params.get("mean").asInstanceOf[java.lang.Number].doubleValue
      val variance = params.get("variance").asInstanceOf[java.lang.Number].doubleValue
      if (java.lang.Double.isInfinite(mu)  ||  java.lang.Double.isNaN(mu)  ||  java.lang.Double.isInfinite(variance)  ||  java.lang.Double.isNaN(variance)  ||  variance < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      val sigma = Math.sqrt(variance)
      if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else if (sigma == 0.0)
        mu
      else
        mu + sigma * Math.sqrt(2.0) * Erf.erfInv(2.0 * p - 1.0)
    }
  }
  provide(new GaussianQF)

//////////////////////////////////////////// Exponential distribution
////   exponentialPDF (ExponentialPDF)
  class ExponentialPDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "exponentialPDF"
    def sig = Sig(List("x" -> P.Double, "lambda" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the exponential distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="lambda">Rate parameter.</param>

        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\lambda \\mathrm{e}^{- \\lambda x}"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"lambda < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13030

    def apply(x: Double, lambda: Double): Double =
      if (java.lang.Double.isInfinite(lambda)  ||  java.lang.Double.isNaN(lambda)  ||  lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (lambda == 0.0)
        0.0
      else if (x < 0.0)
        0.0
      else if (x == 0.0)
        lambda
      else
        lambda * Math.exp(-lambda*x)
  }
  provide(new ExponentialPDF)

  ////   exponentialCDF (ExponentialCDF)
  class ExponentialCDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "exponentialCDF"
    def sig = Sig(List("x" -> P.Double, "lambda" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the exponential distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="lambda">Rate parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"lambda < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13040

    def apply(x: Double, lambda: Double): Double =
      if (java.lang.Double.isInfinite(lambda)  ||  java.lang.Double.isNaN(lambda)  ||  lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (lambda == 0.0  ||  x == 0.0)
        0.0
      else if (x <= 0.0)
        0.0
      else
        1.0 - Math.exp(-lambda * x)
  }
  provide(new ExponentialCDF)

  ////   exponentialQF (ExponentialQF)
  class ExponentialQF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "exponentialQF"
    def sig = Sig(List("p" -> P.Double, "lambda" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the exponential distribution parameterized by <p>lambda</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="lambda">Rate parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"lambda < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises an "invalid input" error if <p>p</p> is less than zero or greater than one.</error>
      </doc>
    def errcodeBase = 13050

    def apply(p: Double, lambda: Double): Double =
      if (java.lang.Double.isInfinite(lambda)  ||  java.lang.Double.isNaN(lambda)  ||  lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (lambda == 0.0  &&  p == 0.0) 
        0.0
      else if (lambda == 0.0  &&  p > 0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else
        -Math.log(1.0 - p)/lambda
  }
  provide(new ExponentialQF)

//////////////////////////////////////////// Chi^2 distribution
////   chi2PDF (Chi2PDF)
  class Chi2PDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "chi2PDF"
    def sig = Sig(List("x" -> P.Double, "dof" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the Chi-squared distribution parameterized by its degrees of freedom <p>dof</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="dof">Degrees of freedom parameter.</param>

        <ret>With <m>{"dof"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{2^{\\frac{\\mathrm{df}}{2}} \\Gamma(\\frac{\\mathrm{df}}{2})} x^{\\frac{\\mathrm{df}}{2}-1}\\mathrm{e}^{-\\frac{x}{2}}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <p>dof</p> &lt; 0 or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13060

    def apply(x: Double, df: Int): Double =
      if (df < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (df == 0) {
        if (x != 0.0)
          0.0
        else
          java.lang.Double.POSITIVE_INFINITY
      }
      else if (x <= 0.0)
        0.0
      else
        new ChiSquaredDistribution(df).density(x)
  }
  provide(new Chi2PDF)

  ////   chi2CDF (Chi2CDF)
  class Chi2CDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "chi2CDF"
    def sig = Sig(List("x" -> P.Double, "dof" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the Chi-squared distribution parameterized by its degrees of freedom <p>dof</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="dof">Degrees of freedom parameter.</param>
        <ret>With <m>{"x1"}</m>, <m>{"x1"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <p>dof</p> &lt; 0 or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13070

    def apply(x: Double, df: Int): Double =
      if (df < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (df == 0) {
        if (x > 0.0)
          1.0
        else
          0.0
      }
      else
        new ChiSquaredDistribution(df).cumulativeProbability(x)
  }
  provide(new Chi2CDF)

  ////   chi2QF (Chi2QF)
  class Chi2QF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "chi2QF"
    def sig = Sig(List("p" -> P.Double, "dof" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the Chi-squared distribution parameterized by its degrees of freedom <p>dof</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="dof">Degrees of freedom parameter.</param>
        <ret>With <m>{"x1"}</m>, <m>{"x1"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <p>dof</p> &lt; 0 or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13080

    def apply(p: Double, df: Int): Double =
      if (df < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (df == 0)
        0.0
      else if (p == 0.0)
        0.0
      else
        new ChiSquaredDistribution(df).inverseCumulativeProbability(p)
  }
  provide(new Chi2QF)
  
//////////////////////////////////////////// Poisson distribution
////   poissonPDF (PoissonPDF)
  class PoissonPDF(val pos: Option[String] = None) extends LibFcn with Function2[Int, Double, Double] {
    def name = prefix + "poissonPDF"
    def sig = Sig(List("x" -> P.Int, "lambda" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the poisson distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="lambda">Mean and variance parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\lambda^{x}}{x!} \\mathrm{e}^{-\\lambda}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"lambda < 0"}</m> or any argument is not finite.</error>
      </doc>
    def errcodeBase = 13090

    def apply(x: Int, lambda: Double): Double =
      if (java.lang.Double.isInfinite(lambda)  ||  java.lang.Double.isNaN(lambda)  ||  lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (lambda == 0.0) {
        if (x != 0.0)
          0.0
        else
          1.0
      }
      else if (x < 0.0)
        0.0
      else
        new PoissonDistribution(lambda).probability(x)
  }
  provide(new PoissonPDF)

  ////   poissonCDF (PoissonCDF)
  class PoissonCDF(val pos: Option[String] = None) extends LibFcn with Function2[Int, Double, Double] {
    def name = prefix + "poissonCDF"
    def sig = Sig(List("x" -> P.Int, "lambda" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the poisson distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="lambda">Mean and variance parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"lambda < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13100

    def apply(x: Int, lambda: Double): Double =
      if (java.lang.Double.isInfinite(lambda)  ||  java.lang.Double.isNaN(lambda)  ||  lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (lambda == 0.0) {
        if (x >= 0)
          1.0
        else
          0.0
      }
      else
        new PoissonDistribution(lambda).cumulativeProbability(x)
  }
  provide(new PoissonCDF)

  ////   poissonQF (PoissonQF)
  class PoissonQF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "poissonQF"
    def sig = Sig(List("p" -> P.Double, "lambda" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the poisson distribution parameterized by <p>lambda</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="lambda">Mean and variance parameter.</param>
        <ret>With <m>{"lambda"}</m>, <m>{"lambda"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"lambda < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13110

    def apply(p: Double, lambda: Double): Double =
      if (java.lang.Double.isInfinite(lambda)  ||  java.lang.Double.isNaN(lambda)  ||  lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (lambda == 0.0)
        0.0
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else
        new PoissonDistribution(lambda).inverseCumulativeProbability(p)
  }
  provide(new PoissonQF)

//////////////////////////////////////////// Gamma distribution
////   gammaPDF (GammaPDF)
  class GammaPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "gammaPDF"
    def sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the gamma distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="shape">Shape parameter (a).</param>
          <param name="scale">Scale parameter (s).</param>

        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{s^{a} \\Gamma(a)} x^{a - 1} \\mathrm{e}^{-\\frac{x}{s}}  "}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"shape < 0"}</m> OR if <m>{"scale < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13120

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(shape)  ||  java.lang.Double.isNaN(shape)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  shape < 0.0  ||  scale < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (shape == 0.0  ||  scale == 0.0) {
        if (x != 0.0)
          0.0
        else
          java.lang.Double.POSITIVE_INFINITY
      }
      else if (x < 0.0)
        0.0
      else if (x == 0.0) {
        if (shape < 1.0)
          java.lang.Double.POSITIVE_INFINITY
        else if (shape == 1.0)
          1.0 / scale
        else
          0.0
      }
      else
        new GammaDistribution(shape, scale).density(x)
  }
  provide(new GammaPDF)

  ////   gammaCDF (GammaCDF)
  class GammaCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "gammaCDF"
    def sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the gamma distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x)~= P(X~\\leq~x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"shape < 0"}</m> OR if <m>{"scale < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13130

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(shape)  ||  java.lang.Double.isNaN(shape)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  shape < 0.0  ||  scale < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (shape == 0.0  ||  scale == 0.0) {
        if (x != 0.0)
          1.0
        else
          0.0
      }
      else if (x < 0.0)
        0.0
      else
        new GammaDistribution(shape, scale).cumulativeProbability(x)
  }
  provide(new GammaCDF)

  ////   gammaQF (GammaQF)
  class GammaQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "gammaQF"
    def sig = Sig(List("p" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the gamma distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x)~:= P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13140

    def apply(p: Double, shape: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(shape)  ||  java.lang.Double.isNaN(shape)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else
        new GammaDistribution(shape, scale).inverseCumulativeProbability(p)
  }
  provide(new GammaQF)

//////////////////////////////////////////// Beta distribution
////   betaPDF (BetaPDF)
  class BetaPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "betaPDF"
    def sig = Sig(List("x" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the beta distribution parameterized by <p>shape1</p> and <p>shape2</p>.</desc>
          <param name="x">Value at which to compute the PDF, defined between zero and one.</param>
          <param name="a">First shape parameter.</param>
          <param name="b">Second shape parameter.</param>
        <ret>With <m>{"a"}</m>, <m>{"b"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(a + n)}{\\Gamma(a)\\Gamma(b)} x^{a-1}(1-x)^{b-1}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"a \\leq 0"}</m> OR if <m>{"b \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13150

    def apply(x: Double, shape1: Double, shape2: Double): Double =
      if (java.lang.Double.isInfinite(shape1)  ||  java.lang.Double.isNaN(shape1)  ||  java.lang.Double.isInfinite(shape2)  ||  java.lang.Double.isNaN(shape2)  ||  shape1 <= 0.0  ||  shape2 <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x <= 0.0  ||  x >= 1.0)
        0.0
      else
        new BetaDistribution(shape1, shape2).density(x)
  }
  provide(new BetaPDF)

  ////   betaCDF (BetaCDF)
  class BetaCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "betaCDF"
    def sig = Sig(List("x" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the beta distribution parameterized by <p>shape1</p> and <p>shape2</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="a">First shape parameter.</param>
          <param name="b">Second shape parameter.</param>
        <ret>With <m>{"a"}</m>, <m>{"b"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"a \\leq 0"}</m> OR if <m>{"b \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13160

    def apply(x: Double, shape1: Double, shape2: Double): Double =
      if (java.lang.Double.isInfinite(shape1)  ||  java.lang.Double.isNaN(shape1)  ||  java.lang.Double.isInfinite(shape2)  ||  java.lang.Double.isNaN(shape2)  ||  shape1 <= 0.0  ||  shape2 <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x <= 0.0)
        0.0
      else if (x >= 1.0)
        1.0
      else
        new BetaDistribution(shape1, shape2).cumulativeProbability(x)
  }
  provide(new BetaCDF)

  ////   betaQF (BetaQF)
  class BetaQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "betaQF"
    def sig = Sig(List("p" -> P.Double, "a" -> P.Double, "b" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the beta distribution parameterized by <p>shape1</p> and <p>shape2</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="a">First shape parameter.</param>
          <param name="b">Second shape parameter.</param>
        <ret>With <m>{"a"}</m>, <m>{"b"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"a \\leq 0"}</m> OR if <m>{"b \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13170

    def apply(p: Double, shape1: Double, shape2: Double): Double =
      if (java.lang.Double.isInfinite(shape1)  ||  java.lang.Double.isNaN(shape1)  ||  java.lang.Double.isInfinite(shape2)  ||  java.lang.Double.isNaN(shape2)  ||  shape1 <= 0.0  ||  shape2 <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        1.0
      else if (p == 0.0)
        0.0
      else
        new BetaDistribution(shape1, shape2).inverseCumulativeProbability(p)
  }
  provide(new BetaQF)

//////////////////////////////////////////// Cauchy distribution
////   cauchyPDF (CauchyPDF)
  class CauchyPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "cauchyPDF"
    def sig = Sig(List("x" -> P.Double, "location" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the cauchy distribution parameterized by <p>location</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="location">Location parameter (l).</param>
          <param name="scale">Scale parameter (s).</param>

        <ret>With <m>{"location"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{(\\pi s (1 + (\\frac{x - l}{s})^{2})) }"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13180

    def apply(x: Double, location: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(location)  ||  java.lang.Double.isNaN(location)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new CauchyDistribution(location, scale).density(x)
  }
  provide(new CauchyPDF)

  ////   cauchyCDF (CauchyCDF)
  class CauchyCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "cauchyCDF"
    def sig = Sig(List("x" -> P.Double, "location" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the cauchy distribution parameterized by <p>location</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="location">Location parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"location"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13190

    def apply(x: Double, location: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(location)  ||  java.lang.Double.isNaN(location)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new CauchyDistribution(location, scale).cumulativeProbability(x)
  }
  provide(new CauchyCDF)

  ////   cauchyQF (CauchyQF)
  class CauchyQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "cauchyQF"
    def sig = Sig(List("p" -> P.Double, "location" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the cauchy distribution parameterized by <p>location</p> and <p>scale</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="location">Location parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"location"}</m>, <m>{"scale"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
      <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
      <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
    </doc>
    def errcodeBase = 13200

    def apply(p: Double, location: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(location)  ||  java.lang.Double.isNaN(location)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else
        new CauchyDistribution(location, scale).inverseCumulativeProbability(p)
  }
  provide(new CauchyQF)

//////////////////////////////////////////// F distribution
////   fPDF (FPDF)
  class FPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Int, Double] {
    def name = prefix + "fPDF"
    def sig = Sig(List("x" -> P.Double, "d1" -> P.Int, "d2" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the F distribution parameterized by <p>d1</p> and <p>d2</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="d1">Numerator degrees of freedom parameter.</param>
          <param name="d2">Denominator degrees of freedom parameter.</param>

        <ret>With <m>{"d1"}</m>, <m>{"d2"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(\\frac{d1 + d2}{2})}{\\Gamma(\\frac{d1}{2})\\Gamma(\\frac{d2}{2})} \\frac{d1}{d2}^{\\frac{d1}{2}-1}(1 + \\frac{d1}{d2} x)^{-\\frac{d1 + d2}{2}}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"d1 \\leq 0"}</m> OR if <m>{"d2 \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13210

    def apply(x: Double, d1: Int, d2: Int): Double =
      if (d2 <= 0  ||  d1 <= 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x <= 0)
        0.0
      else
        new FDistribution(d1, d2).density(x)
  }
  provide(new FPDF)

  ////   fCDF (FCDF)
  class FCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Int, Double] {
    def name = prefix + "fCDF"
    def sig = Sig(List("x" -> P.Double, "d1" -> P.Int, "d2" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the F distribution parameterized by <p>d1</p> and <p>d2</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="d1">Numerator degrees of freedom parameter.</param>
          <param name="d2">Denominator degrees of freedom parameter.</param>
        <ret>With <m>{"d1"}</m>, <m>{"d2"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"d1 \\leq 0"}</m> OR if <m>{"d2 \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13220

    def apply(x: Double, d1: Int, d2: Int): Double =
      if (d2 <= 0  ||  d1 <= 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new FDistribution(d1, d2).cumulativeProbability(x)
  }
  provide(new FCDF)

  ////   fQF (FQF)
  class FQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Int, Double] {
    def name = prefix + "fQF"
    def sig = Sig(List("p" -> P.Double, "d1" -> P.Int, "d2" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the F distribution parameterized by <p>d1</p> and <p>d2</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="d1">Numerator degrees of freedom parameter.</param>
          <param name="d2">Denominator degrees of freedom parameter.</param>
        <ret>With <m>{"d1"}</m>, <m>{"d2"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"d1 \\leq 0"}</m> OR if <m>{"d2 \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13230

    def apply(p: Double, d1: Int, d2: Int): Double =
      if (d1 <= 0  ||  d2 <= 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else
        new FDistribution(d1, d2).inverseCumulativeProbability(p)
  }
  provide(new FQF)

//////////////////////////////////////////// Lognormal distribution
////   lognormalPDF (LognormalPDF)
  class LognormalPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "lognormalPDF"
    def sig = Sig(List("x" -> P.Double, "meanlog" -> P.Double, "sdlog" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the lognormal distribution parameterized by <p>meanlog</p> and <p>sdlog</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="meanlog">Mean of the distribution on the log scale (<m>{"\\mu"}</m>).</param>
          <param name="sdlog">Standard deviation of the distribution on the log scale (<m>{"\\sigma"}</m>).</param>

        <ret>With <m>{"meanlog"}</m>, <m>{"sdlog"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{\\sqrt{2 \\pi} \\sigma x} \\mathrm{e}^{-\\frac{\\mathrm{log}(x) - \\mu}{2 \\sigma^{2}}}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"sdlog \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13240

    def apply(x: Double, meanlog: Double, sdlog: Double): Double =
      if (java.lang.Double.isInfinite(meanlog)  ||  java.lang.Double.isNaN(meanlog)  ||  java.lang.Double.isInfinite(sdlog)  ||  java.lang.Double.isNaN(sdlog)  ||  sdlog <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new LogNormalDistribution(meanlog, sdlog).density(x)
  }
  provide(new LognormalPDF)

  ////   lognormalCDF (LognormalCDF)
  class LognormalCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "lognormalCDF"
    def sig = Sig(List("x" -> P.Double, "meanlog" -> P.Double, "sdlog" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the lognormal distribution parameterized by <p>meanlog</p> and <p>sdlog</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="meanlog">Mean of the distribution on the log scale.</param>
          <param name="sdlog">Standard deviation of the distribution on the log scale.</param>
        <ret>With <m>{"meanlog"}</m>, <m>{"sdlog"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"sdlog \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13250

    def apply(x: Double, meanlog: Double, sdlog: Double): Double =
      if (java.lang.Double.isInfinite(meanlog)  ||  java.lang.Double.isNaN(meanlog)  ||  java.lang.Double.isInfinite(sdlog)  ||  java.lang.Double.isNaN(sdlog)  ||  sdlog <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new LogNormalDistribution(meanlog, sdlog).cumulativeProbability(x)
  }
  provide(new LognormalCDF)

  ////   lognormalQF (LognormalQF)
  class LognormalQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "lognormalQF"
    def sig = Sig(List("p" -> P.Double, "meanlog" -> P.Double, "sdlog" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the lognormal distribution parameterized by <p>meanlog</p> and <p>sdlog</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="meanlog">Mean of the distribution on the log scale.</param>
          <param name="sdlog">Standard deviation of the distribution on the log scale.</param>
        <ret>With <m>{"meanlog"}</m>, <m>{"sdlog"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"sdlog \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13260

    def apply(p: Double, meanlog: Double, sdlog: Double): Double =
      if (java.lang.Double.isInfinite(meanlog)  ||  java.lang.Double.isNaN(meanlog)  ||  java.lang.Double.isInfinite(sdlog)  ||  java.lang.Double.isNaN(sdlog)  ||  sdlog <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else
        new LogNormalDistribution(meanlog, sdlog).inverseCumulativeProbability(p)
  }
  provide(new LognormalQF)

//////////////////////////////////////////// T distribution
////   tPDF (TPDF)
  class TPDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "tPDF"
    def sig = Sig(List("x" -> P.Double, "dof" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the student's t distribution parameterized by <p>dof</p> and <p>x2</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="dof">Degrees of freedom parameter.</param>
        <ret>With <m>{"dof"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(\\frac{\\mathrm{df}+1}{2})}{\\sqrt{\\mathrm{df}\\pi} \\Gamma{\\frac{\\mathrm{df}}{2}}}(1 + x^{\\frac{2}{n}})^{-\\frac{\\mathrm{df} + 1}{2}}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"df \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13270

    def apply(x: Double, df: Int): Double =
      if (df <= 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new TDistribution(df).density(x)
  }
  provide(new TPDF)

  ////   tCDF (TCDF)
  class TCDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "tCDF"
    def sig = Sig(List("x" -> P.Double, "dof" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the student's t distribution parameterized by <p>dof</p> and <p>x2</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="dof">Degrees of freedom parameter.</param>
        <ret>With <m>{"dof"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"df \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13280

    def apply(x: Double, df: Int): Double =
      if (df <= 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new TDistribution(df).cumulativeProbability(x)
  }
  provide(new TCDF)

  ////   tQF (TQF)
  class TQF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "tQF"
    def sig = Sig(List("p" -> P.Double, "dof" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the student's t distribution parameterized by <p>dof</p> and <p>x2</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="dof">Degrees of freedom parameter.</param>
        <ret>With <m>{"dof"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"df \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13290

    def apply(p: Double, df: Int): Double =
      if (df <= 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else
        new TDistribution(df).inverseCumulativeProbability(p)
  }
  provide(new TQF)

//////////////////////////////////////////// Binomial distribution
//// binomialPDF (BinomialPDF)
  class BinomialPDF(val pos: Option[String] = None) extends LibFcn with Function3[Int, Int, Double, Double] {
    def name = prefix + "binomialPDF"
    def sig = Sig(List("x" -> P.Int, "size" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="size">The number of trials (n).</param>
          <param name="prob">The probability of success in each trial (p).</param>

        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\mathrm{choose}(n, x) p^{x} (1 - p)^{n - x}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"size < 0"}</m> OR if <m>{"prob < 0"}</m> OR if <m>{"prob > 1"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13300

    def apply(x: Int, size: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  size <= 0  ||  prob < 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x < 0)
        0.0
      else if (x >= size)
        0.0
      else
        new BinomialDistribution(size, prob).probability(x)
  }
  provide(new BinomialPDF)

  ////   binomialCDF (BinomialCDF)
  class BinomialCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Double, Double] {
    def name = prefix + "binomialCDF"
    def sig = Sig(List("x" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="size">The number of trials.</param>
         <param name="prob">The probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"size < 0"}</m> OR if <m>{"prob < 0"}</m> OR if <m>{"prob > 1"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
     </doc>
    def errcodeBase = 13310

    def apply(x: Double, size: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  size <= 0  ||  prob < 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x < 0)
        0.0
      else if (x >= size)
        1.0
      else if (prob == 1) {
        if (x < size)
          0.0
        else
          1.0
      }
      else if (prob == 0)
        1.0
      else
        new BinomialDistribution(size, prob).cumulativeProbability(Math.floor(x).toInt)
  }
  provide(new BinomialCDF)

  ////   binomialQF (BinomialQF)
  class BinomialQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Double, Double] {
    def name = prefix + "binomialQF"
    def sig = Sig(List("p" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="size">The number of trials.</param>
          <param name="prob">The probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x)~:= P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"size < 0"}</m> OR if <m>{"prob < 0"}</m> OR if <m>{"prob > 1"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13320

    def apply(p: Double, size: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  size <= 0  ||  prob < 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        size
      else if (p == 0.0)
        0.0
      else
        new BinomialDistribution(size, prob).inverseCumulativeProbability(p)
  }
  provide(new BinomialQF)

//////////////////////////////////////////// Uniform distribution
////   uniformPDF (UniformPDF)
  class UniformPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "uniformPDF"
    def sig = Sig(List("x" -> P.Double, "min" -> P.Double, "max" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the uniform distribution parameterized by <p>min</p> and <p>max</p>.</desc>
         <param name="x">Value at which to compute the PDF.</param>
         <param name="min">Lower bound.</param>
         <param name="max">Upper bound.</param>
        <ret>With <m>{"min"}</m>, <m>{"max"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{\\mathrm{max} - \\mathrm{min}}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"min \\geq max"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13330

    def apply(x: Double, min: Double, max: Double): Double =
      if (java.lang.Double.isInfinite(min)  ||  java.lang.Double.isNaN(min)  ||  java.lang.Double.isInfinite(max)  ||  java.lang.Double.isNaN(max)  ||  min >= max)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new UniformRealDistribution(min, max).density(x)
  }
  provide(new UniformPDF)

  ////   uniformCDF (UniformCDF)
  class UniformCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "uniformCDF"
    def sig = Sig(List("x" -> P.Double, "min" -> P.Double, "max" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the uniform distribution parameterized by <p>min</p> and <p>max</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="min">Lower bound.</param>
          <param name="max">Upper bound.</param>
        <ret>With <m>{"min"}</m>, <m>{"max"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"min \\geq max"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13340

    def apply(x: Double, min: Double, max: Double): Double =
      if (java.lang.Double.isInfinite(min)  ||  java.lang.Double.isNaN(min)  ||  java.lang.Double.isInfinite(max)  ||  java.lang.Double.isNaN(max)  ||  min >= max)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new UniformRealDistribution(min, max).cumulativeProbability(x)
  }
  provide(new UniformCDF)

  ////   uniformQF (UniformQF)
  class UniformQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "uniformQF"
    def sig = Sig(List("p" -> P.Double, "min" -> P.Double, "max" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the uniform distribution parameterized by <p>min</p> and <p>max</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="min">Lower bound.</param>
          <param name="max">Upper bound.</param>
        <ret>With <m>{"min"}</m>, <m>{"max"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"min \\geq max"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13350

    def apply(p: Double, min: Double, max: Double): Double =
      if (java.lang.Double.isInfinite(min)  ||  java.lang.Double.isNaN(min)  ||  java.lang.Double.isInfinite(max)  ||  java.lang.Double.isNaN(max)  ||  min >= max)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new UniformRealDistribution(min, max).inverseCumulativeProbability(p)
  }
  provide(new UniformQF)

//////////////////////////////////////////// Geometric distribution
////   geometricPDF (GeometricPDF)
  class GeometricPDF(val pos: Option[String] = None) extends LibFcn with Function2[Int, Double, Double] {
    def name = prefix + "geometricPDF"
    def sig = Sig(List("x" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the geometric distribution parameterized by <p>prob</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="prob">Probability of success of each trial (p).</param>
        <ret>With <m>{"prob"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"p (1 - p)^{x}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{prob} \\leq 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13360

    def apply(x: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new GeometricDistribution(prob).probability(x)
  }
  provide(new GeometricPDF)

  ////   geometricCDF (GeometricCDF)
  class GeometricCDF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "geometricCDF"
    def sig = Sig(List("x" -> P.Double, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the geometric distribution parameterized by <p>prob</p>.</desc>
           <param name="x">Value at which to compute the CDF.</param>
           <param name="prob">Probability of success of each trial.</param>
        <ret>With <m>{"prob"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{prob} \\leq 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13370

    def apply(x: Double, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new GeometricDistribution(prob).cumulativeProbability(Math.floor(x).toInt)
  }
  provide(new GeometricCDF)

  ////   geometricQF (GeometricQF)
  class GeometricQF(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "geometricQF"
    def sig = Sig(List("p" -> P.Double, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the geometric distribution parameterized by <p>prob</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="prob">Probability of success of each trial.</param>
        <ret>With <m>{"prob"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{prob} \\leq 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13380

    def apply(p: Double, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else if (prob == 1.0)
        0.0
      else
        Math.floor(Math.log(1.0 - p)/Math.log(1.0 - prob))
  }
  provide(new GeometricQF)

//////////////////////////////////////////HYPERGEOMETRIC DISTRIBUTION/////////////////////// 
////   hypergeometricPDF (HypergeometricPDF)
  class HypergeometricPDF(val pos: Option[String] = None) extends LibFcn with Function4[Int, Int, Int, Int, Double] {
    def name = prefix + "hypergeometricPDF"
    def sig = Sig(List("x" -> P.Int, "m" -> P.Int, "n" -> P.Int, "k" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the hypergeometric distribution parameterized by <p>m</p>, <p>n</p> and <p>k</p>.</desc>
          <param name="x">The number of white balls drawn without replacement from the urn.</param>
          <param name="m">The number of white balls in the urn.</param>
          <param name="n">The number of black balls in the urn.</param>
          <param name="k">The number of balls drawn from the urn.</param>
        <ret>With <m>{"m"}</m>, <m>{"n"}</m> and <m>{"k"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\mathrm{choose}(m, x) \\mathrm{choose}(n, k-x)}{\\mathrm{choose}(m+n, k)} "}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{m} + \\mathrm{n} < \\mathrm{k}"}</m>, <m>{"m < 0"}</m>, <m>{"n < 0"}</m>, <m>{"m + n = 0"}</m>, or <m>{"k < 0"}</m>.</error>
      </doc>
    def errcodeBase = 13390

    def apply(x: Int, m: Int, n: Int, k: Int): Double =
      if (m + n < k  ||  m < 0  ||  n <= 0  ||  m + n == 0  ||  k < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (x > m)
        0.0
      else
        new HypergeometricDistribution(m + n, m, k).probability(x)
  }
  provide(new HypergeometricPDF)

  ////   hypergeometricCDF (HypergeometricCDF)
  class HypergeometricCDF(val pos: Option[String] = None) extends LibFcn with Function4[Int, Int, Int, Int, Double] {
    def name = prefix + "hypergeometricCDF"
    def sig = Sig(List("x" -> P.Int, "m" -> P.Int, "n" -> P.Int, "k" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the hypergeometric distribution parameterized by <p>m</p>, <p>n</p> and <p>k</p>.</desc>
          <param name="x">The number of white balls drawn without replacement.</param>
          <param name="m">The number of white balls in the urn.</param>
          <param name="n">The number of black balls in the urn.</param>
          <param name="k">The number of balls drawn from the urn.</param>
        <ret>With <m>{"m"}</m>, <m>{"n"}</m> and <m>{"k"}</m> at <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{m} + \\mathrm{n} < \\mathrm{k}"}</m>, <m>{"m < 0"}</m>, <m>{"n < 0"}</m>, <m>{"m + n = 0"}</m>, or <m>{"k < 0"}</m>.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13400

    def apply(x: Int, m: Int, n: Int, k: Int): Double =
      if (m + n < k  ||  m < 0  ||  n <= 0  ||  m + n == 0  ||  k < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (x > m)
        0.0
      else
        new HypergeometricDistribution(m + n, m, k).cumulativeProbability(x)
  }
  provide(new HypergeometricCDF)

  ////   hypergeometricQF (HypergeometricQF)
  class HypergeometricQF(val pos: Option[String] = None) extends LibFcn with Function4[Double, Int, Int, Int, Double] {
    def name = prefix + "hypergeometricQF"
    def sig = Sig(List("p" -> P.Double, "m" -> P.Int, "n" -> P.Int, "k" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the hypergeometric distribution parameterized by <p>m</p>, <p>n</p> and <p>k</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="m">The number of white balls in the urn.</param>
          <param name="n">The number of black balls in the urn.</param>
          <param name="k">The number of balls drawn from the urn.</param>
        <ret>With <m>{"m"}</m>, <m>{"n"}</m> and <m>{"k"}</m> at <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x)~:= P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{m} + \\mathrm{n} < \\mathrm{k}"}</m>, <m>{"m < 0"}</m>, <m>{"n < 0"}</m>, <m>{"m + n = 0"}</m>, or <m>{"k < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13410

    def apply(p: Double, m: Int, n: Int, k: Int): Double =
      if (m + n < k  ||  m < 0  ||  n <= 0  ||  m + n == 0  ||  k < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new HypergeometricDistribution(m + n, m, k).inverseCumulativeProbability(p)
  }
  provide(new HypergeometricQF)

//////////////////////////////////////////// Weibull distribution
////   weibullPDF (WeibullPDF)
  class WeibullPDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "weibullPDF"
    def sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the weibull distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="shape">Shape parameter (a).</param>
          <param name="scale">Scale parameter (b).</param>
         <ret>With <m>{"shape"}</m>, <m>{"scale"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{a}{b}(\\frac{x}{b})^{a - 1}\\mathrm{e}^{-(\\frac{x}{b})^{a}}"}</m>.</ret> 
         <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13420

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(shape)  ||  java.lang.Double.isNaN(shape)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x < 0.0)
        0.0
      else
        new WeibullDistribution(shape, scale).density(x)
  }
  provide(new WeibullPDF)

  ////   weibullCDF (WeibullCDF)
  class WeibullCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "weibullCDF"
    def sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the weibull distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X~\\leq~x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13430

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(shape)  ||  java.lang.Double.isNaN(shape)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x < 0.0)
        0.0
      else
        new WeibullDistribution(shape, scale).cumulativeProbability(x)
  }
  provide(new WeibullCDF)

  ////   weibullQF (WeibullQF)
  class WeibullQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Double, Double, Double] {
    def name = prefix + "weibullQF"
    def sig = Sig(List("p" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the weibull distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13440

    def apply(p: Double, shape: Double, scale: Double): Double =
      if (java.lang.Double.isInfinite(shape)  ||  java.lang.Double.isNaN(shape)  ||  java.lang.Double.isInfinite(scale)  ||  java.lang.Double.isNaN(scale)  ||  shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else
        new WeibullDistribution(shape, scale).inverseCumulativeProbability(p)
  }
  provide(new WeibullQF)

//////////////////////////////////////////// Negative binomial distribution
////   negativeBinomialPDF (NegativeBinomialPDF)
  class NegativeBinomialPDF(val pos: Option[String] = None) extends LibFcn with Function3[Int, Int, Double, Double] {
    def name = prefix + "negativeBinomialPDF"
    def sig = Sig(List("x" -> P.Int, "size" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the density (PDF) of the negative binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the PDF (integer) .</param>
          <param name="size">Size parameter (integer).  Target number of successful trials (n).</param>
          <param name="prob">Probability of success in each trial (p).</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(x+n)}{\\Gamma(n) x!} p^{n} (1-p)^{x}"}</m>.</ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{prob} < 0"}</m>, if <m>{"\\mathrm{prob} > 1"}</m> or if <m>{"size < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13450

    def apply(x: Int, size: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  prob > 1.0  ||  prob <= 0.0  ||  size < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x == 0 && size == 0)
        1.0
      else if (size == 0.0)
        0.0
      else if (x < 0)
        0.0
      else
        binomialCoeff(size + x - 1, x).toDouble * Math.pow(1.0 - prob, x) * Math.pow(prob, size.toDouble)
  }
  provide(new NegativeBinomialPDF)

  ////   negativeBinomialCDF (NegativeBinomialCDF)
  class NegativeBinomialCDF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Double, Double] {
    def name = prefix + "negativeBinomialCDF"
    def sig = Sig(List("x" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the negative binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="size">Size parameter (integer).  Target number of successful trials.</param>
          <param name="prob">Probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{prob} < 0"}</m>, if <m>{"\\mathrm{prob} > 1"}</m>, or if <m>{"size < 0"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <p>x</p> is not finite.</error>
      </doc>
    def errcodeBase = 13460

    def apply(x: Double, size: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  prob > 1.0  ||  prob <= 0.0  ||  size < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (java.lang.Double.isInfinite(x)  ||  java.lang.Double.isNaN(x))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (x == 0 && size == 0)
        1.0
      else if (size == 0)
        0.0
      else if (x < 0)
        0.0
      else
        new PascalDistribution(size, prob).cumulativeProbability(Math.floor(x).toInt)
  }
  provide(new NegativeBinomialCDF)

  ////   negativeBinomialQF (NegativeBinomialQF)
  class NegativeBinomialQF(val pos: Option[String] = None) extends LibFcn with Function3[Double, Int, Double, Double] {
    def name = prefix + "negativeBinomialQF"
    def sig = Sig(List("p" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Compute the quantile function (QF) of the negative binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="size">Size parameter (integer).  Target number of successful trials.</param>
          <param name="prob">Probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <m>{"\\mathrm{prob} < 0"}</m>, if <m>{"\\mathrm{prob} > 1"}</m>, or if <m>{"size \\leq 0"}</m>, or if <m>{"size"}</m> or any argument is not finite.</error>
        <error code={s"${errcodeBase + 1}"}>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>
    def errcodeBase = 13470

    def apply(p: Double, size: Int, prob: Double): Double =
      if (java.lang.Double.isInfinite(prob)  ||  java.lang.Double.isNaN(prob)  ||  prob <= 0.0  ||  prob > 1.0  ||  size < 0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (!(0.0 <= p  &&  p <= 1.0))
        throw new PFARuntimeException("invalid input", errcodeBase + 1, name, pos)
      else if (p == 0 && size == 0)
        0.0
      else if (size == 0)
        0.0
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else
        new PascalDistribution(size, prob).inverseCumulativeProbability(p)
  }
  provide(new NegativeBinomialQF)
}
