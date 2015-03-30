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

package com.opendatagroup.hadrian.lib1.prob

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
import org.apache.commons.math3.util.CombinatoricsUtils.binomialCoefficient

package object dist {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "prob.dist."

//////////////////////////////////////////// Gaussian distribution
////   gaussianLL (GaussianLL)
  object GaussianLL extends LibFcn {
    val name = prefix + "gaussianLL"
    val sig = Sigs(List(Sig(List("x" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("x" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compute the log-likelihood of a Gaussian (normal) distribution parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="x">Value at which to compute the log-likelihood.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"-(x - \\mu)^2/(2 \\sigma^2) - \\log(\\sigma \\sqrt{2\\pi})"}</m>.</ret>
      </doc>

    def apply(x: Double, mu: Double, sigma: Double): Double =
      if (sigma < 0.0)
        throw new PFARuntimeException("invalid parameterization")
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
      if (variance < 0.0)
        throw new PFARuntimeException("invalid parameterization")
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
  provide(GaussianLL)

  ////   gaussianCDF (GaussianCDF)
  object GaussianCDF extends LibFcn {
    val name = prefix + "gaussianCDF"
    val sig = Sigs(List(Sig(List("x" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("x" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compute the cumultive distribution function (CDF) for the normal distribution, parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="x">Value at which to compute the CDF.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"0.5 * ( 1.0 + \\mathrm{Erf}(\\frac{x - \\mu}{\\sigma \\sqrt{2}}))"}</m>.</ret>
      </doc>

    def apply(x: Double, mu: Double, sigma: Double): Double =
      if (sigma < 0.0)
        throw new PFARuntimeException("invalid parameterization")
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
      if (variance < 0.0)
        throw new PFARuntimeException("invalid parameterization")
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
  provide(GaussianCDF)

  ////   gaussianQF (GaussianQF)
  object GaussianQF extends LibFcn {
    val name = prefix + "gaussianQF"
    val sig = Sigs(List(Sig(List("p" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("p" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compute the normal quantile (QF, the inverse of the CDF) parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="p">Probability at which to compute the QF.  Must be a value between 0 and 1.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"\\mu + \\sigma \\sqrt{2} \\mathrm{Erf}^{-1} (2p - 1)"}</m>.</ret>
      </doc>


    def apply(p: Double, mu: Double, sigma: Double): Double = 
      if (sigma < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
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
      if (variance < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      val sigma = Math.sqrt(variance)
      if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else if (sigma == 0.0)
        mu
      else
        mu + sigma * Math.sqrt(2.0) * Erf.erfInv(2.0 * p - 1.0)
    }
  }
  provide(GaussianQF)

//////////////////////////////////////////// Exponential distribution
////   exponentialPDF (ExponentialPDF)
  object ExponentialPDF extends LibFcn {
    val name = prefix + "exponentialPDF"
    val sig = Sig(List("x" -> P.Double, "lambda" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the exponential distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="lambda">Rate parameter.</param>

        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\lambda \\mathrm{e}^{- \\lambda x}"}</m>.</ret>
        <error>Raises "invalid parameterization" if <m>{"lambda < 0"}</m>.</error>
      </doc>

    def apply(x: Double, lambda: Double): Double =
      if (lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (lambda == 0.0)
        0.0
      else if (x < 0.0)
        0.0
      else if (x == 0.0)
        lambda
      else
        lambda * Math.exp(-lambda*x)
  }
  provide(ExponentialPDF)

  ////   exponentialCDF (ExponentialCDF)
  object ExponentialCDF extends LibFcn {
    val name = prefix + "exponentialCDF"
    val sig = Sig(List("x" -> P.Double, "lambda" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the exponential distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="lambda">Rate parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"lambda < 0"}</m>.</error>
      </doc>

    def apply(x: Double, lambda: Double): Double =
      if (lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (lambda == 0.0  ||  x == 0.0)
        0.0
      else if (x <= 0.0)
        0.0
      else
        1.0 - Math.exp(-lambda * x)
  }
  provide(ExponentialCDF)

  ////   exponentialQF (ExponentialQF)
  object ExponentialQF extends LibFcn {
    val name = prefix + "exponentialQF"
    val sig = Sig(List("p" -> P.Double, "lambda" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the exponential distribution parameterized by <p>lambda</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="lambda">Rate parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"lambda < 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, lambda: Double): Double =
      if (p > 1.0 || p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (lambda == 0.0 && p == 0.0) 
        0.0
      else if (lambda == 0.0 && p > 0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else
        -Math.log(1.0 - p)/lambda
  }
  provide(ExponentialQF)

//////////////////////////////////////////// Chi^2 distribution
////   chi2PDF (Chi2PDF)
  object Chi2PDF extends LibFcn {
    val name = prefix + "chi2PDF"
    val sig = Sig(List("x" -> P.Double, "df" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the Chi-squared distribution parameterized by its degrees of freedom <p>df</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="df">Degrees of freedom parameter.</param>

        <ret>With <m>{"df"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{2^{\\frac{\\mathrm{df}}{2}} \\Gamma(\\frac{\\mathrm{df}}{2})} x^{\\frac{\\mathrm{df}}{2}-1}\\mathrm{e}^{-\\frac{x}{2}}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <p>df</p> &lt; 0.</error>
      </doc>

    def apply(x: Double, df: Int): Double =
      if (df < 0)
        throw new PFARuntimeException("invalid parameterization")
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
  provide(Chi2PDF)

  ////   chi2CDF (Chi2CDF)
  object Chi2CDF extends LibFcn {
    val name = prefix + "chi2CDF"
    val sig = Sig(List("x" -> P.Double, "df" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the Chi-squared distribution parameterized by its degrees of freedom <p>df</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="df">Degrees of freedom parameter.</param>
        <ret>With <m>{"x1"}</m>, <m>{"x1"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <p>df</p> &lt; 0.</error>
      </doc>

    def apply(x: Double, df: Int): Double =
      if (df < 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (df == 0) {
        if (x > 0.0)
          1.0
        else
          0.0
      }
      else
        new ChiSquaredDistribution(df).cumulativeProbability(x)
  }
  provide(Chi2CDF)

  ////   chi2QF (Chi2QF)
  object Chi2QF extends LibFcn {
    val name = prefix + "chi2QF"
    val sig = Sig(List("p" -> P.Double, "df" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the Chi-squared distribution parameterized by its degrees of freedom <p>df</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="df">Degrees of freedom parameter.</param>
        <ret>With <m>{"x1"}</m>, <m>{"x1"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <p>df</p> &lt; 0.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, df: Int): Double =
      if (p.isNaN)
        Double.NaN
      else if (df < 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (df == 0)
        0.0
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 0.0)
        0.0
      else
        new ChiSquaredDistribution(df).inverseCumulativeProbability(p)
  }
  provide(Chi2QF)
  
//////////////////////////////////////////// Poisson distribution
////   poissonPDF (PoissonPDF)
  object PoissonPDF extends LibFcn {
    val name = prefix + "poissonPDF"
    val sig = Sig(List("k" -> P.Int, "lambda" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the poisson distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="lambda">Mean and variance parameter.</param>

        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\lambda^{x}}{x!} \\mathrm{e}^{-\\lambda}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"lambda < 0"}</m>.</error>
      </doc>

    def apply(k: Int, lambda: Double): Double =
      if (lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (lambda == 0.0) {
        if (k != 0.0)
          0.0
        else
          1.0
      }
      else if (k < 0.0)
        0.0
      else
        new PoissonDistribution(lambda).probability(k)
  }
  provide(PoissonPDF)

  ////   poissonCDF (PoissonCDF)
  object PoissonCDF extends LibFcn {
    val name = prefix + "poissonCDF"
    val sig = Sig(List("k" -> P.Double, "lambda" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the poisson distribution parameterized by <p>lambda</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="lambda">Mean and variance parameter.</param>
        <ret>With <m>{"lambda"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"lambda < 0"}</m>.</error>
      </doc>

    def apply(k: Double, lambda: Double): Double =
      if (lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (lambda == 0.0) {
        if (k >= 0.0)
          1.0
        else
          0.0
      }
      else
        new PoissonDistribution(lambda).cumulativeProbability(Math.floor(k).toInt)
  }
  provide(PoissonCDF)

  ////   poissonQF (PoissonQF)
  object PoissonQF extends LibFcn {
    val name = prefix + "poissonQF"
    val sig = Sig(List("p" -> P.Double, "lambda" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the poisson distribution parameterized by <p>lambda</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="lambda">Mean and variance parameter.</param>
        <ret>With <m>{"lambda"}</m>, <m>{"lambda"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"lambda < 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, lambda: Double): Double =
      if (lambda < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (lambda == 0.0)
        0.0
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else
        new PoissonDistribution(lambda).inverseCumulativeProbability(p)
  }
  provide(PoissonQF)

//////////////////////////////////////////// Gamma distribution
////   gammaPDF (GammaPDF)
  object GammaPDF extends LibFcn {
    val name = prefix + "gammaPDF"
    val sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the gamma distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="shape">Shape parameter (a).</param>
          <param name="scale">Scale parameter (s).</param>

        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{s^{a} \\Gamma(a)} x^{a - 1} \\mathrm{e}^{-\\frac{x}{s}}  "}</m>.</ret> 
        <error>Raises "invalid parameterization" if the <m>{"shape < 0"}</m> OR if <m>{"scale < 0"}</m>.</error>
      </doc>

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (shape < 0.0  ||  scale < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (shape == 0.0  ||  scale == 0.0) {
        if (x != 0.0)
          0.0
        else
          java.lang.Double.POSITIVE_INFINITY
      }
      else if (x < 0.0)
        0.0
      else
        new GammaDistribution(shape, scale).density(x)
  }
  provide(GammaPDF)

  ////   gammaCDF (GammaCDF)
  object GammaCDF extends LibFcn {
    val name = prefix + "gammaCDF"
    val sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the gamma distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x)~= P(X~\\leq~x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"shape < 0"}</m> OR if <m>{"scale < 0"}</m>.</error>
      </doc>

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (shape < 0.0  ||  scale < 0.0)
        throw new PFARuntimeException("invalid parameterization")
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
  provide(GammaCDF)

  ////   gammaQF (GammaQF)
  object GammaQF extends LibFcn {
    val name = prefix + "gammaQF"
    val sig = Sig(List("p" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the gamma distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x)~:= P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"shape < 0"}</m> OR if <m>{"scale < 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, shape: Double, scale: Double): Double =
      if (p.isNaN)
        Double.NaN
      else if (shape < 0.0  ||  scale < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        0.0
      else
        new GammaDistribution(shape, scale).inverseCumulativeProbability(p)
  }
  provide(GammaQF)

//////////////////////////////////////////// Beta distribution
////   betaPDF (BetaPDF)
  object BetaPDF extends LibFcn {
    val name = prefix + "betaPDF"
    val sig = Sig(List("x" -> P.Double, "shape1" -> P.Double, "shape2" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the beta distribution parameterized by <p>shape1</p> and <p>shape2</p>.</desc>
          <param name="x">Value at which to compute the PDF, defined between zero and one.</param>
          <param name="shape1">First shape parameter (a).</param>
          <param name="shape2">Second shape parameter (b).</param>
        <ret>With <m>{"shape1"}</m>, <m>{"shape2"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(a + n)}{\\Gamma(a)\\Gamma(b)} x^{a-1}(1-x)^{b-1}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"a \\leq 0"}</m> OR if <m>{"b \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, shape1: Double, shape2: Double): Double =
      if (shape1 <= 0.0  ||  shape2 <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x <= 0.0  ||  x >= 1.0)
        0.0
      else
        new BetaDistribution(shape1, shape2).density(x)
  }
  provide(BetaPDF)

  ////   betaCDF (BetaCDF)
  object BetaCDF extends LibFcn {
    val name = prefix + "betaCDF"
    val sig = Sig(List("x" -> P.Double, "shape1" -> P.Double, "shape2" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the beta distribution parameterized by <p>shape1</p> and <p>shape2</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="shape1">First shape parameter (a).</param>
          <param name="shape2">Second shape parameter (b).</param>
        <ret>With <m>{"shape1"}</m>, <m>{"shape2"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"a \\leq 0"}</m> OR if <m>{"b \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, shape1: Double, shape2: Double): Double =
      if (shape1 <= 0.0  ||  shape2 <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x <= 0.0)
        0.0
      else if (x >= 1.0)
        1.0
      else
        new BetaDistribution(shape1, shape2).cumulativeProbability(x)
  }
  provide(BetaCDF)

  ////   betaQF (BetaQF)
  object BetaQF extends LibFcn {
    val name = prefix + "betaQF"
    val sig = Sig(List("p" -> P.Double, "shape1" -> P.Double, "shape2" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the beta distribution parameterized by <p>shape1</p> and <p>shape2</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="shape1">First shape parameter.</param>
          <param name="shape2">Second shape parameter.</param>
        <ret>With <m>{"shape1"}</m>, <m>{"shape2"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"a \\leq 0"}</m> OR if <m>{"b \\leq 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, shape1: Double, shape2: Double): Double =
      if (p.isNaN)
        Double.NaN
      else if (shape1 <= 0.0  ||  shape2 <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        1.0
      else if (p == 0.0)
        0.0
      else
        new BetaDistribution(shape1, shape2).inverseCumulativeProbability(p)
  }
  provide(BetaQF)

//////////////////////////////////////////// Cauchy distribution
////   cauchyPDF (CauchyPDF)
  object CauchyPDF extends LibFcn {
    val name = prefix + "cauchyPDF"
    val sig = Sig(List("x" -> P.Double, "location" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the cauchy distribution parameterized by <p>location</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="location">Location parameter (l).</param>
          <param name="scale">Scale parameter (s).</param>

        <ret>With <m>{"location"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{(\\pi s (1 + (\\frac{x - l}{s})^{2})) }"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"scale \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, location: Double, scale: Double): Double =
      if (scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new CauchyDistribution(location, scale).density(x)
  }
  provide(CauchyPDF)

  ////   cauchyCDF (CauchyCDF)
  object CauchyCDF extends LibFcn {
    val name = prefix + "cauchyCDF"
    val sig = Sig(List("x" -> P.Double, "location" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the cauchy distribution parameterized by <p>location</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="location">Location parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"location"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"scale \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, location: Double, scale: Double): Double =
      if (scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new CauchyDistribution(location, scale).cumulativeProbability(x)
  }
  provide(CauchyCDF)

  ////   cauchyQF (CauchyQF)
  object CauchyQF extends LibFcn {
    val name = prefix + "cauchyQF"
    val sig = Sig(List("p" -> P.Double, "location" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the cauchy distribution parameterized by <p>location</p> and <p>scale</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="location">Location parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"location"}</m>, <m>{"scale"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
      <error>Raises "invalid parameterization" if <m>{"scale \\leq 0"}</m>.</error>
      <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
    </doc>

    def apply(p: Double, location: Double, scale: Double): Double =
      if (scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else
        new CauchyDistribution(location, scale).inverseCumulativeProbability(p)
  }
  provide(CauchyQF)

//////////////////////////////////////////// F distribution
////   fPDF (FPDF)
  object FPDF extends LibFcn {
    val name = prefix + "fPDF"
    val sig = Sig(List("x" -> P.Double, "df1" -> P.Int, "df2" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the F distribution parameterized by <p>df1</p> and <p>df2</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="df1">Numerator degrees of freedom parameter (d1).</param>
          <param name="df2">Denominator degrees of freedom parameter (d2).</param>

        <ret>With <m>{"df1"}</m>, <m>{"df2"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(\\frac{d1 + d2}{2})}{\\Gamma(\\frac{d1}{2})\\Gamma(\\frac{d2}{2})} \\frac{d1}{d2}^{\\frac{d1}{2}-1}(1 + \\frac{d1}{d2} x)^{-\\frac{d1 + d2}{2}}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if the <m>{"df1 \\leq 0"}</m> OR if <m>{"df2 \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, df1: Int, df2: Int): Double =
      if (df2 <= 0  ||  df1 <= 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x <= 0)
        0.0
      else
        new FDistribution(df1, df2).density(x)
  }
  provide(FPDF)

  ////   fCDF (FCDF)
  object FCDF extends LibFcn {
    val name = prefix + "fCDF"
    val sig = Sig(List("x" -> P.Double, "df1" -> P.Int, "df2" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the F distribution parameterized by <p>df1</p> and <p>df2</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="df1">Numerator degrees of freedom parameter.</param>
          <param name="df2">Denominator degrees of freedom parameter.</param>
        <ret>With <m>{"df1"}</m>, <m>{"df2"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"df1 \\leq 0"}</m> OR if <m>{"df2 \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, df1: Double, df2: Double): Double =
      if (df2 <= 0  ||  df1 <= 0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new FDistribution(df1, df2).cumulativeProbability(x)
  }
  provide(FCDF)

  ////   fQF (FQF)
  object FQF extends LibFcn {
    val name = prefix + "fQF"
    val sig = Sig(List("p" -> P.Double, "df1" -> P.Int, "df2" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the F distribution parameterized by <p>df1</p> and <p>df2</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="df1">Numerator degrees of freedom parameter.</param>
          <param name="df2">Denominator degrees of freedom parameter.</param>
        <ret>With <m>{"df1"}</m>, <m>{"df2"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"df1 \\leq 0"}</m> OR if <m>{"df2 \\leq 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, df1: Int, df2: Int): Double =
      if (p.isNaN)
        Double.NaN
      else if (df1 <= 0  ||  df2 <= 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else
        new FDistribution(df1, df2).inverseCumulativeProbability(p)
  }
  provide(FQF)

//////////////////////////////////////////// Lognormal distribution
////   lognormalPDF (LognormalPDF)
  object LognormalPDF extends LibFcn {
    val name = prefix + "lognormalPDF"
    val sig = Sig(List("x" -> P.Double, "meanlog" -> P.Double, "sdlog" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the lognormal distribution parameterized by <p>meanlog</p> and <p>sdlog</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="meanlog">Mean of the distribution on the log scale (<m>{"\\mu"}</m>).</param>
          <param name="sdlog">Standard deviation of the distribution on the log scale (<m>{"\\sigma"}</m>).</param>

        <ret>With <m>{"meanlog"}</m>, <m>{"sdlog"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{\\sqrt{2 \\pi} \\sigma x} \\mathrm{e}^{-\\frac{\\mathrm{log}(x) - \\mu}{2 \\sigma^{2}}}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"sdlog \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, meanlog: Double, sdlog: Double): Double =
      if (sdlog <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new LogNormalDistribution(meanlog, sdlog).density(x)
  }
  provide(LognormalPDF)

  ////   lognormalCDF (LognormalCDF)
  object LognormalCDF extends LibFcn {
    val name = prefix + "lognormalCDF"
    val sig = Sig(List("x" -> P.Double, "meanlog" -> P.Double, "sdlog" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the lognormal distribution parameterized by <p>meanlog</p> and <p>sdlog</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="meanlog">Mean of the distribution on the log scale.</param>
          <param name="sdlog">Standard deviation of the distribution on the log scale.</param>
        <ret>With <m>{"meanlog"}</m>, <m>{"sdlog"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"sdlog \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, meanlog: Double, sdlog: Double): Double =
      if (sdlog <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new LogNormalDistribution(meanlog, sdlog).cumulativeProbability(x)
  }
  provide(LognormalCDF)

  ////   lognormalQF (LognormalQF)
  object LognormalQF extends LibFcn {
    val name = prefix + "lognormalQF"
    val sig = Sig(List("p" -> P.Double, "meanlog" -> P.Double, "sdlog" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the lognormal distribution parameterized by <p>meanlog</p> and <p>sdlog</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="meanlog">Mean of the distribution on the log scale.</param>
          <param name="sdlog">Standard deviation of the distribution on the log scale.</param>
        <ret>With <m>{"meanlog"}</m>, <m>{"sdlog"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"sdlog \\leq 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, meanlog: Double, sdlog: Double): Double =
      if (p.isNaN)
        Double.NaN
      else if (sdlog <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else
        new LogNormalDistribution(meanlog, sdlog).inverseCumulativeProbability(p)
  }
  provide(LognormalQF)

//////////////////////////////////////////// T distribution
////   tPDF (TPDF)
  object TPDF extends LibFcn {
    val name = prefix + "tPDF"
    val sig = Sig(List("x" -> P.Double, "df" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the student's t distribution parameterized by <p>df</p> and <p>x2</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="df">Degrees of freedom parameter.</param>
        <ret>With <m>{"df"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(\\frac{\\mathrm{df}+1}{2})}{\\sqrt{\\mathrm{df}\\pi} \\Gamma{\\frac{\\mathrm{df}}{2}}}(1 + x^{\\frac{2}{n}})^{-\\frac{\\mathrm{df} + 1}{2}}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"df \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, df: Int): Double =
      if (df <= 0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new TDistribution(df).density(x)
  }
  provide(TPDF)

  ////   tCDF (TCDF)
  object TCDF extends LibFcn {
    val name = prefix + "tCDF"
    val sig = Sig(List("x" -> P.Double, "df" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the student's t distribution parameterized by <p>df</p> and <p>x2</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="df">Degrees of freedom parameter.</param>
        <ret>With <m>{"df"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"df \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, df: Int): Double =
      if (df <= 0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new TDistribution(df).cumulativeProbability(x)
  }
  provide(TCDF)

  ////   tQF (TQF)
  object TQF extends LibFcn {
    val name = prefix + "tQF"
    val sig = Sig(List("p" -> P.Double, "df" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the student's t distribution parameterized by <p>df</p> and <p>x2</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="df">Degrees of freedom parameter.</param>
        <ret>With <m>{"df"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"df \\leq 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, df: Int): Double =
      if (p.isNaN)
        Double.NaN
      else if (df <= 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p == 0.0)
        java.lang.Double.NEGATIVE_INFINITY
      else
        new TDistribution(df).inverseCumulativeProbability(p)
  }
  provide(TQF)

//////////////////////////////////////////// Binomial distribution
//// binomialPDF (BinomialPDF)
  object BinomialPDF extends LibFcn {
    val name = prefix + "binomialPDF"
    val sig = Sig(List("x" -> P.Int, "size" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="size">The number of trials (n).</param>
          <param name="prob">The probability of success in each trial (p).</param>

        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\mathrm{choose}(n, x) p^{x} (1 - p)^{n - x}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"size < 0"}</m> OR if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(x: Int, size: Int, prob: Double): Double =
      if (size < 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (prob < 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x < 0)
        0.0
      else
        new BinomialDistribution(size, prob).probability(x)
  }
  provide(BinomialPDF)

  ////   binomialCDF (BinomialCDF)
  object BinomialCDF extends LibFcn {
    val name = prefix + "binomialCDF"
    val sig = Sig(List("x" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="size">The number of trials.</param>
         <param name="prob">The probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"size < 0"}</m> OR if <m>{"prob < 0"}</m> OR if <m>{"prob > 1"}</m>.</error>
      </doc>

    def apply(x: Double, size: Int, prob: Double): Double =
      if (size < 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (prob < 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x < 0)
        0.0
      else
        new BinomialDistribution(size, prob).cumulativeProbability(Math.floor(x).toInt)
  }
  provide(BinomialCDF)

  ////   binomialQF (BinomialQF)
  object BinomialQF extends LibFcn {
    val name = prefix + "binomialQF"
    val sig = Sig(List("p" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="size">The number of trials.</param>
          <param name="prob">The probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x)~:= P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"size < 0"}</m> OR if <m>{"prob < 0"}</m> OR if <m>{"prob > 1"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, size: Int, prob: Double): Double =
      if (size < 0)
        throw new PFARuntimeException("invalid parameterization")
      else if (prob < 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p == 1)
        size
      else if (p == 0.0)
        0.0
      else if (p > 0.0  ||  p < 1.0)
        new BinomialDistribution(size, prob).inverseCumulativeProbability(p)
      else
        0.0
  }
  provide(BinomialQF)

//////////////////////////////////////////// Uniform distribution
////   uniformPDF (UniformPDF)
  object UniformPDF extends LibFcn {
    val name = prefix + "uniformPDF"
    val sig = Sig(List("x" -> P.Double, "min" -> P.Double, "max" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the uniform distribution parameterized by <p>min</p> and <p>max</p>.</desc>
         <param name="x">Value at which to compute the PDF.</param>
         <param name="min">Lower bound.</param>
         <param name="max">Upper bound.</param>
        <ret>With <m>{"min"}</m>, <m>{"max"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{1}{\\mathrm{max} - \\mathrm{min}}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"min \\geq max"}</m>.</error>
      </doc>

    def apply(x: Double, min: Double, max: Double): Double =
      if (min >= max)
        throw new PFARuntimeException("invalid parameterization")
      else
        new UniformRealDistribution(min, max).density(x)
  }
  provide(UniformPDF)

  ////   uniformCDF (UniformCDF)
  object UniformCDF extends LibFcn {
    val name = prefix + "uniformCDF"
    val sig = Sig(List("x" -> P.Double, "min" -> P.Double, "max" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the uniform distribution parameterized by <p>min</p> and <p>max</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="min">Lower bound.</param>
          <param name="max">Upper bound.</param>
        <ret>With <m>{"min"}</m>, <m>{"max"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"min \\geq max"}</m>.</error>
      </doc>

    def apply(x: Double, min: Double, max: Double): Double =
      if (min >= max)
        throw new PFARuntimeException("invalid parameterization")
      else
        new UniformRealDistribution(min, max).cumulativeProbability(x)
  }
  provide(UniformCDF)

  ////   uniformQF (UniformQF)
  object UniformQF extends LibFcn {
    val name = prefix + "uniformQF"
    val sig = Sig(List("p" -> P.Double, "min" -> P.Double, "max" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the uniform distribution parameterized by <p>min</p> and <p>max</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="min">Lower bound.</param>
          <param name="max">Upper bound.</param>
        <ret>With <m>{"min"}</m>, <m>{"max"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"min \\geq max"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, min: Double, max: Double): Double =
      if (min >= max)
        throw new PFARuntimeException("invalid parameterization")
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p > 0.0  ||  p < 1.0)
        new UniformRealDistribution(min, max).inverseCumulativeProbability(p)
      else
        0.0
  }
  provide(UniformQF)

//////////////////////////////////////////// Geometric distribution
////   geometricPDF (GeometricPDF)
  object GeometricPDF extends LibFcn {
    val name = prefix + "geometricPDF"
    val sig = Sig(List("x" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the geometric distribution parameterized by <p>prob</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="prob">Probability of success of each trial (p).</param>
        <ret>With <m>{"prob"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"p (1 - p)^{x}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"\\mathrm{prob} \\leq 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m>.</error>
      </doc>

    def apply(x: Int, prob: Double): Double =
      if (prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new GeometricDistribution(prob).probability(x)
  }
  provide(GeometricPDF)

  ////   geometricCDF (GeometricCDF)
  object GeometricCDF extends LibFcn {
    val name = prefix + "geometricCDF"
    val sig = Sig(List("x" -> P.Double, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the geometric distribution parameterized by <p>prob</p>.</desc>
           <param name="x">Value at which to compute the CDF.</param>
           <param name="prob">Probability of success of each trial.</param>
        <ret>With <m>{"prob"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"\\mathrm{prob} \\leq 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m>.</error>
      </doc>

    def apply(x: Double, prob: Double): Double =
      if (prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else
        new GeometricDistribution(prob).cumulativeProbability(Math.floor(x).toInt)
  }
  provide(GeometricCDF)

  ////   geometricQF (GeometricQF)
  object GeometricQF extends LibFcn {
    val name = prefix + "geometricQF"
    val sig = Sig(List("p" -> P.Double, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the geometric distribution parameterized by <p>prob</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="prob">Probability of success of each trial.</param>
        <ret>With <m>{"prob"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"\\mathrm{prob} \\leq 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, prob: Double): Double =
      if (prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p > 0.0  ||  p < 1.0)
        new GeometricDistribution(prob).inverseCumulativeProbability(p)
      else
        0.0
  }
  provide(GeometricQF)

//////////////////////////////////////////HYPERGEOMETRIC DISTRIBUTION/////////////////////// 
////   hypergeometricPDF (HypergeometricPDF)
  object HypergeometricPDF extends LibFcn {
    val name = prefix + "hypergeometricPDF"
    val sig = Sig(List("x" -> P.Int, "m" -> P.Int, "n" -> P.Int, "k" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the hypergeometric distribution parameterized by <p>m</p>, <p>n</p> and <p>k</p>.</desc>
          <param name="x">The number of white balls drawn without replacement from the urn.</param>
          <param name="m">The number of white balls in the urn.</param>
          <param name="n">The number of black balls in the urn.</param>
          <param name="k">The number of balls drawn from the urn.</param>
        <ret>With <m>{"m"}</m>, <m>{"n"}</m> and <m>{"k"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\mathrm{choose}(m, x) \\mathrm{choose}(n, k-x)}{\\mathrm{choose}(m+n, k)} "}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"\\mathrm{m} + \\mathrm{n} > \\mathrm{k}"}</m>.</error>
      </doc>

    def apply(x: Int, m: Int, n: Int, k: Int): Double =
      if (m + n < k)
        throw new PFARuntimeException("invalid parameterization")
      else if (x > m)
        0.0
      else
        new HypergeometricDistribution(m + n, m, k).probability(x)
  }
  provide(HypergeometricPDF)

  ////   hypergeometricCDF (HypergeometricCDF)
  object HypergeometricCDF extends LibFcn {
    val name = prefix + "hypergeometricCDF"
    val sig = Sig(List("x" -> P.Int, "m" -> P.Int, "n" -> P.Int, "k" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the hypergeometric distribution parameterized by <p>m</p>, <p>n</p> and <p>k</p>.</desc>
          <param name="x">The number of white balls drawn without replacement.</param>
          <param name="m">The number of white balls in the urn.</param>
          <param name="n">The number of black balls in the urn.</param>
          <param name="k">The number of balls drawn from the urn.</param>
        <ret>With <m>{"m"}</m>, <m>{"n"}</m> and <m>{"k"}</m> at <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"\\mathrm{m} + \\mathrm{n} > \\mathrm{k}"}</m>.</error>
      </doc>

    def apply(x: Int, m: Int, n: Int, k: Int): Double =
      if (m + n < k)
        throw new PFARuntimeException("invalid parameterization")
      else if (x > m)
        0.0
      else
        new HypergeometricDistribution(m + n, m, k).cumulativeProbability(x)
  }
  provide(HypergeometricCDF)

  ////   hypergeometricQF (HypergeometricQF)
  object HypergeometricQF extends LibFcn {
    val name = prefix + "hypergeometricQF"
    val sig = Sig(List("p" -> P.Double, "m" -> P.Int, "n" -> P.Int, "k" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the hypergeometric distribution parameterized by <p>m</p>, <p>n</p> and <p>k</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="m">The number of white balls in the urn.</param>
          <param name="n">The number of black balls in the urn.</param>
          <param name="k">The number of balls drawn from the urn.</param>
        <ret>With <m>{"m"}</m>, <m>{"n"}</m> and <m>{"k"}</m> at <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x)~:= P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"\\mathrm{m} + \\mathrm{n} > \\mathrm{k}"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, m: Int, n: Int, k: Int): Double =
      if (p < 0.0  ||  p > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (m + n < k)
        throw new PFARuntimeException("invalid parameterization")
      else
        new HypergeometricDistribution(m + n, m, k).inverseCumulativeProbability(p)
  }
  provide(HypergeometricQF)

//////////////////////////////////////////// Weibull distribution
////   weibullPDF (WeibullPDF)
  object WeibullPDF extends LibFcn {
    val name = prefix + "weibullPDF"
    val sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the weibull distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the PDF.</param>
          <param name="shape">Shape parameter (a).</param>
          <param name="scale">Scale parameter (b).</param>
         <ret>With <m>{"shape"}</m>, <m>{"scale"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{a}{b}(\\frac{x}{b})^{a - 1}\\mathrm{e}^{-(\\frac{x}{b})^{a}}"}</m>.</ret> 
         <error>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else {
        if (x >= 0.0)
          new WeibullDistribution(shape, scale).density(x)
        else
          0.0
      }
  }
  provide(WeibullPDF)

  ////   weibullCDF (WeibullCDF)
  object WeibullCDF extends LibFcn {
    val name = prefix + "weibullCDF"
    val sig = Sig(List("x" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the weibull distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X~\\leq~x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m>.</error>
      </doc>

    def apply(x: Double, shape: Double, scale: Double): Double =
      if (shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else {
        if (x >= 0.0)
          new WeibullDistribution(shape, scale).cumulativeProbability(x)
        else
          0.0
      }
  }
  provide(WeibullCDF)

  ////   weibullQF (WeibullQF)
  object WeibullQF extends LibFcn {
    val name = prefix + "weibullQF"
    val sig = Sig(List("p" -> P.Double, "shape" -> P.Double, "scale" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the weibull distribution parameterized by <p>shape</p> and <p>scale</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="shape">Shape parameter.</param>
          <param name="scale">Scale parameter.</param>
        <ret>With <m>{"shape"}</m>, <m>{"scale"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X~\\leq~x)~=~p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if the <m>{"shape \\leq 0"}</m> OR if <m>{"scale \\leq 0"}</m>.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, shape: Double, scale: Double): Double =
      if (p.isNaN)
        Double.NaN
      else if (shape <= 0.0  ||  scale <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p < 0.0  ||  p > 1.0)
        throw new PFARuntimeException("invalid input")
      else
        new WeibullDistribution(shape, scale).inverseCumulativeProbability(p)
  }
  provide(WeibullQF)

//////////////////////////////////////////// Negative binomial distribution
////   negativeBinomialPDF (NegativeBinomialPDF)
  object NegativeBinomialPDF extends LibFcn {
    val name = prefix + "negativeBinomialPDF"
    val sig = Sig(List("x" -> P.Int, "size" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the density (PDF) of the negative binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the PDF (integer) .</param>
          <param name="size">Size parameter (integer).  Target number of successful trials (n).</param>
          <param name="prob">Probability of success in each trial (p).</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function evaluates the probability density function at <m>{"x"}</m>.  The PDF implemented is <m>{"\\frac{\\Gamma(x+n)}{\\Gamma(n) x!} p^{n} (1-p)^{x}"}</m>.</ret> 
        <error>Raises "invalid parameterization" if <m>{"\\mathrm{prob} < 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m>.</error>
        <error>Raises "invalid parameterization" if <m>{"size < 0"}</m>.</error>
      </doc>

    def apply(x: Int, size: Int, prob: Double): Double =
      if (prob > 1.0  ||  prob <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (size < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x == 0 && size == 0.0)
        1.0
      else if (size == 0.0)
        0.0
      else {
        if (x >= 0.0)
          binomialCoefficient(size + x - 1, x).toDouble * Math.pow(1.0 - prob, x) * Math.pow(prob, size.toDouble)
        else
          0.0
      }
  }
  provide(NegativeBinomialPDF)

  ////   negativeBinomialCDF (NegativeBinomialCDF)
  object NegativeBinomialCDF extends LibFcn {
    val name = prefix + "negativeBinomialCDF"
    val sig = Sig(List("x" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the distribution function (CDF) of the negative binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="x">Value at which to compute the CDF.</param>
          <param name="size">Size parameter (integer).  Target number of successful trials.</param>
          <param name="prob">Probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"x"}</m>, this function returns the value <m>{"p"}</m> where <m>{"p = F_{X}(x) = P(X \\leq x)"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"\\mathrm{prob} < 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m>.</error>
        <error>Raises "invalid parameterization" if <m>{"size < 0"}</m>.</error>
      </doc>

    def apply(x: Double, size: Int, prob: Double): Double =
      if (prob > 1.0  ||  prob <= 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (size < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (x == 0 && size == 0.0)
        1.0
      else if (size == 0.0)
        0.0
      else {
        if (x >= 0.0)
          new PascalDistribution(size, prob).cumulativeProbability(Math.floor(x).toInt)
        else
          0.0
      }
  }
  provide(NegativeBinomialCDF)

  ////   negativeBinomialQF (NegativeBinomialQF)
  object NegativeBinomialQF extends LibFcn {
    val name = prefix + "negativeBinomialQF"
    val sig = Sig(List("p" -> P.Double, "size" -> P.Int, "prob" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Compute the quantile function (QF) of the negative binomial distribution parameterized by <p>size</p> and <p>prob</p>.</desc>
          <param name="p">Value at which to compute the QF.  Must be a value between 0 and 1.</param>
          <param name="size">Size parameter (integer).  Target number of successful trials.</param>
          <param name="prob">Probability of success in each trial.</param>
        <ret>With <m>{"size"}</m>, <m>{"prob"}</m> and <m>{"p"}</m>, this function returns the value <m>{"x"}</m> such that <m>{"F_{X}(x) := P(X \\leq x) = p"}</m>. </ret> 
        <error>Raises "invalid parameterization" if <m>{"\\mathrm{prob} < 0"}</m> OR if <m>{"\\mathrm{prob} > 1"}</m>.</error>
        <error>Raises "invalid parameterization" if <m>{"size \\leq 0"}</m> OR if <m>{"size"}</m> is not an integer.</error>
        <error>Raises "invalid input" if <m>{"p < 0"}</m> OR if <m>{"p > 1"}</m>.</error>
      </doc>

    def apply(p: Double, size: Double, prob: Double): Double =
      if (prob <= 0.0  ||  prob > 1.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (size < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (p == 0 && size == 0.0)
        0.0
      else if (size == 0.0)
        0.0
      else if (p == 1.0)
        java.lang.Double.POSITIVE_INFINITY
      else if (p > 1.0  ||  p < 0.0)
        throw new PFARuntimeException("invalid input")
      else if (p > 0.0  ||  p < 1.0)
        new PascalDistribution(size.toInt, prob).inverseCumulativeProbability(p)
      else
        0.0
  }
  provide(NegativeBinomialQF)
}
