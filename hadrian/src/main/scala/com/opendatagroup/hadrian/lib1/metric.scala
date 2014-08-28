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

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

import com.opendatagroup.hadrian.data.PFAArray

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

package object metric {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "metric."

  ////   simpleEuclidean (SimpleEuclidean)
  object SimpleEuclidean extends LibFcn with Function2[PFAArray[Double], PFAArray[Double], Double] {
    val name = prefix + "simpleEuclidean"
    val sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Double)
    val doc =
      <doc>
        <desc>Euclidean metric without a special similarity function and without any handling of missing values.</desc>
        <param name="x">First sample vector.</param>
        <param name="y">Second sample vector.  (Must have the same dimension as <p>x</p>.)</param>
        <ret>Returns <m>{"\\sqrt{\\sum_i (x_i - y_i)^2}"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if all vectors do not have the same dimension.</error>
      </doc>
    def apply(x: PFAArray[Double], y: PFAArray[Double]): Double = {
      var out = 0.0
      val xvector = x.toVector
      val yvector = y.toVector
      val size = xvector.size
      if (yvector.size != size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      var i = 0
      while (i < size) {
        val diff = xvector(i) - yvector(i)
        out += diff * diff
        i += 1
      }
      Math.sqrt(out)
    }
  }
  provide(SimpleEuclidean)

  ////   absDiff (AbsDiff)
  object AbsDiff extends LibFcn with Function2[java.lang.Number, java.lang.Number, Double] {
    val name = prefix + "absDiff"
    val sig = Sig(List("x" -> P.Double, "y" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Similarity function (1-dimensional metric) that returns the absolute Euclidean distance between <p>x</p> and <p>y</p>.</desc>
      </doc>
    def apply(x: java.lang.Number, y: java.lang.Number): Double = Math.abs(x.doubleValue - y.doubleValue)
  }
  provide(AbsDiff)

  ////   gaussianSimilarity (GaussianSimilarity)
  object GaussianSimilarity extends LibFcn with Function3[java.lang.Number, java.lang.Number, java.lang.Number, Double] {
    val name = prefix + "gaussianSimilarity"
    val sig = Sig(List("x" -> P.Double, "y" -> P.Double, "sigma" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Similarity function (1-dimensional metric) that returns <m>{"\\exp(-\\ln(2) (x - y)^2 / \\mbox{sigma}^2)"}</m>.</desc>
      </doc>
    def apply(x: java.lang.Number, y: java.lang.Number, sigma: java.lang.Number): Double = Math.exp(-Math.log(2) * Math.pow(x.doubleValue - y.doubleValue, 2) / Math.pow(sigma.doubleValue, 2))
  }
  provide(GaussianSimilarity)

  ////   common methods for metrics that may encounter missing values
  trait MetricWithMissingValues extends LibFcn {
    def increment(tally: Double, x: Double): Double
    def finalize(x: Double): Double

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = {
      (paramTypes(1), paramTypes(2)) match {
        case (AvroArray(AvroDouble()), AvroArray(AvroDouble())) =>
          JavaCode("%s.MODULE$.applyDouble(%s, %s, %s)",
            this.getClass.getName,
            wrapArg(0, args, paramTypes, true),
            wrapArg(1, args, paramTypes, true),
            wrapArg(2, args, paramTypes, true))
        case _ => paramTypes.size match {
          case 3 =>
            JavaCode("%s.MODULE$.apply(%s, %s, %s)",
              this.getClass.getName,
              wrapArg(0, args, paramTypes, true),
              wrapArg(1, args, paramTypes, true),
              wrapArg(2, args, paramTypes, true))
          case 4 =>
            JavaCode("%s.MODULE$.apply(%s, %s, %s, %s)",
              this.getClass.getName,
              wrapArg(0, args, paramTypes, true),
              wrapArg(1, args, paramTypes, true),
              wrapArg(2, args, paramTypes, true),
              wrapArg(3, args, paramTypes, true))
        }
      }
    }

    def applyDouble(similarity: (Double, Double) => Double, x: PFAArray[Double], y: PFAArray[Double]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      var tally = 0.0
      var i = 0
      while (i < length) {
        tally = increment(tally, similarity(xvector(i), yvector(i)))
        i += 1
      }
      finalize(tally)
    }

    def apply[A, B](similarity: (A, B) => Double, x: PFAArray[A], y: PFAArray[B]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      var tally = 0.0
      var denom = 0.0
      var i = 0
      while (i < length) {
        val x = xvector(i)
        val y = yvector(i)
        if (x != null  &&  y != null) {
          tally = increment(tally, similarity(x, y))
          denom += 1.0
        }
        i += 1
      }
      finalize(tally * length / denom)
    }

    def apply[A, B](similarity: (A, B) => Double, x: PFAArray[A], y: PFAArray[B], missing: PFAArray[Double]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val mvector = missing.toVector
      val length = xvector.size
      if (length != yvector.size  ||  length != mvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      var tally = 0.0
      var numer = 0.0
      var denom = 0.0
      var i = 0
      while (i < length) {
        val x = xvector(i)
        val y = yvector(i)
        if (x != null  &&  y != null) {
          tally = increment(tally, similarity(x, y))
          denom += mvector(i)
        }
        numer += mvector(i)
        i += 1
      }
      finalize(tally * numer / denom)
    }
  }

  ////   euclidean (Euclidean)
  object Euclidean extends LibFcn with MetricWithMissingValues {
    val name = prefix + "euclidean"
    val sig = Sigs(List(Sig(List("similarity" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Wildcard("A")))), "y" -> P.Array(P.Union(List(P.Null, P.Wildcard("B"))))), P.Double),
                        Sig(List("similarity" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Wildcard("A")))), "y" -> P.Array(P.Union(List(P.Null, P.Wildcard("B")))), "missingWeight" -> P.Array(P.Double)), P.Double)))
    val doc =
      <doc>
        <desc>Euclidean metric, which is the distance function for ordinary space, given by the Pythagorean formula (also known as the 2-norm).</desc>
        <param name="similarity">Similarity function (1-dimensional metric) that quantifies the distance between components of <p>x</p> and components of <p>y</p>.</param>
        <param name="x">First sample vector, which may have missing values.</param>
        <param name="y">Second sample vector, which may have missing values.  (Must have the same dimension as <p>x</p>.)</param>
        <param name="missingWeight">Optional missing-value weights: a vector with the same dimension as <p>x</p> and <p>y</p> that determines the normalized contribution of missing values in the sum.  If not provided, missing-value weights of 1.0 are assumed.</param>
        <ret>With <m>{"I(x_i,y_i)"}</m> = 0 if component <m>i</m> of <p>x</p> or <p>y</p> is missing, 1 otherwise, this function returns <m>{"\\sqrt{(\\sum_i I(x_i,y_i) \\mbox{similarity}(x_i,y_i)^2)(\\sum_i q_i)/(\\sum_i I(x_i,y_i) q_i)}"}</m> where <m>{"q_i"}</m> are components of the missing-value weights.  Without missing values, it is <m>{"\\sqrt{\\sum_i \\mbox{similarity}(x_i,y_i)^2}"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if all vectors do not have the same dimension.</error>
      </doc>
    def increment(tally: Double, x: Double): Double = tally + (x * x)
    def finalize(x: Double): Double = Math.sqrt(x)
  }
  provide(Euclidean)

  ////   squaredEuclidean (SquaredEuclidean)
  object SquaredEuclidean extends LibFcn with MetricWithMissingValues {
    val name = prefix + "squaredEuclidean"
    val sig = Sigs(List(Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double)))), P.Double),
                        Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double))), "missingWeight" -> P.Array(P.Double)), P.Double)))
    val doc =
      <doc>
        <desc>Euclidean metric squared, which has the same ordering as the Euclidean metric, but avoids a square root calculation.</desc>
        <param name="similarity">Similarity function (1-dimensional metric) that quantifies the distance between components of <p>x</p> and components of <p>y</p>.</param>
        <param name="x">First sample vector, which may have missing values.</param>
        <param name="y">Second sample vector, which may have missing values.  (Must have the same dimension as <p>x</p>.)</param>
        <param name="missingWeight">Optional missing-value weights: a vector with the same dimension as <p>x</p> and <p>y</p> that determines the normalized contribution of missing values in the sum.  If not provided, missing-value weights of 1.0 are assumed.</param>
        <ret>With <m>{"I(x_i,y_i)"}</m> = 0 if component <m>i</m> of <p>x</p> or <p>y</p> is missing, 1 otherwise, this function returns <m>{"(\\sum_i I(x_i,y_i) \\mbox{similarity}(x_i,y_i)^2)(\\sum_i q_i)/(\\sum_i I(x_i,y_i) q_i)"}</m> where <m>{"q_i"}</m> are components of the missing-value weights.  Without missing values, it is <m>{"\\sum_i \\mbox{similarity}(x_i,y_i)^2"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if all vectors do not have the same dimension.</error>
      </doc>
    def increment(tally: Double, x: Double): Double = tally + (x * x)
    def finalize(x: Double): Double = x
  }
  provide(SquaredEuclidean)

  ////   chebyshev (Chebyshev)
  object Chebyshev extends LibFcn with MetricWithMissingValues {
    val name = prefix + "chebyshev"
    val sig = Sigs(List(Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double)))), P.Double),
                        Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double))), "missingWeight" -> P.Array(P.Double)), P.Double)))
    val doc =
      <doc>
        <desc>Chebyshev metric, also known as the infinity norm or chessboard distance (since it is the number of moves required for a chess king to travel between two points).</desc>
        <param name="similarity">Similarity function (1-dimensional metric) that quantifies the distance between components of <p>x</p> and components of <p>y</p>.</param>
        <param name="x">First sample vector, which may have missing values.</param>
        <param name="y">Second sample vector, which may have missing values.  (Must have the same dimension as <p>x</p>.)</param>
        <param name="missingWeight">Optional missing-value weights: a vector with the same dimension as <p>x</p> and <p>y</p> that determines the normalized contribution of missing values in the sum.  If not provided, missing-value weights of 1.0 are assumed.</param>
        <ret>With <m>{"I(x_i,y_i)"}</m> = 0 if component <m>i</m> of <p>x</p> or <p>y</p> is missing, 1 otherwise, this function returns <m>{"(\\max_i I(x_i,y_i) \\mbox{similarity}(x_i,y_i))(\\sum_i q_i)/(\\sum_i I(x_i,y_i) q_i)"}</m> where <m>{"q_i"}</m> are components of the missing-value weights.  Without missing values, it is <m>{"\\max_i \\mbox{similarity}(x_i,y_i)"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if all vectors do not have the same dimension.</error>
      </doc>
    def increment(tally: Double, x: Double): Double = {
      if (x > tally)
        x
      else
        tally
    }
    def finalize(x: Double): Double = x
  }
  provide(Chebyshev)

  ////   taxicab (Taxicab)
  object Taxicab extends LibFcn with MetricWithMissingValues {
    val name = prefix + "taxicab"
    val sig = Sigs(List(Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double)))), P.Double),
                        Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double))), "missingWeight" -> P.Array(P.Double)), P.Double)))
    val doc =
      <doc>
        <desc>Taxicab metric, also known as the 1-norm, city-block or Manhattan distance (since it is the distance when confined to a rectilinear city grid).</desc>
        <param name="similarity">Similarity function (1-dimensional metric) that quantifies the distance between components of <p>x</p> and components of <p>y</p>.</param>
        <param name="x">First sample vector, which may have missing values.</param>
        <param name="y">Second sample vector, which may have missing values.  (Must have the same dimension as <p>x</p>.)</param>
        <param name="missingWeight">Optional missing-value weights: a vector with the same dimension as <p>x</p> and <p>y</p> that determines the normalized contribution of missing values in the sum.  If not provided, missing-value weights of 1.0 are assumed.</param>
        <ret>With <m>{"I(x_i,y_i)"}</m> = 0 if component <m>i</m> of <p>x</p> or <p>y</p> is missing, 1 otherwise, this function returns <m>{"(\\sum_i I(x_i,y_i) \\mbox{similarity}(x_i,y_i))(\\sum_i q_i)/(\\sum_i I(x_i,y_i) q_i)"}</m> where <m>{"q_i"}</m> are components of the missing-value weights.  Without missing values, it is <m>{"\\sum_i \\mbox{similarity}(x_i,y_i)"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if all vectors do not have the same dimension.</error>
      </doc>
    def increment(tally: Double, x: Double): Double = tally + x
    def finalize(x: Double): Double = x
  }
  provide(Taxicab)

  ////   minkowski (Minkowski)
  object Minkowski extends LibFcn {
    val name = prefix + "minkowski"
    val sig = Sigs(List(Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double))), "p" -> P.Double), P.Double),
                        Sig(List("similarity" -> P.Fcn(List(P.Double, P.Double), P.Double), "x" -> P.Array(P.Union(List(P.Null, P.Double))), "y" -> P.Array(P.Union(List(P.Null, P.Double))), "p" -> P.Double, "missingWeight" -> P.Array(P.Double)), P.Double)))
    val doc =
      <doc>
        <desc>Minkowski metric, also known as the p-norm, a generalized norm whose limits include Euclidean, Chebyshev, and Taxicab.</desc>
        <param name="similarity">Similarity function (1-dimensional metric) that quantifies the distance between components of <p>x</p> and components of <p>y</p>.</param>
        <param name="x">First sample vector, which may have missing values.</param>
        <param name="y">Second sample vector, which may have missing values.  (Must have the same dimension as <p>x</p>.)</param>
        <param name="missingWeight">Optional missing-value weights: a vector with the same dimension as <p>x</p> and <p>y</p> that determines the normalized contribution of missing values in the sum.  If not provided, missing-value weights of 1.0 are assumed.</param>
        <ret>With <m>{"I(x_i,y_i)"}</m> = 0 if component <m>i</m> of <p>x</p> or <p>y</p> is missing, 1 otherwise, this function returns <m>{"((\\sum_i I(x_i,y_i) \\mbox{similarity}(x_i,y_i)^p)(\\sum_i q_i)/(\\sum_i I(x_i,y_i) q_i))^{1/p}"}</m> where <m>{"q_i"}</m> are components of the missing-value weights.  Without missing values, it is <m>{"(\\sum_i \\mbox{similarity}(x_i,y_i)^p)^{1/p}"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if all vectors do not have the same dimension.</error>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = {
      (paramTypes(1), paramTypes(2)) match {
        case (AvroArray(AvroDouble()), AvroArray(AvroDouble())) =>
          JavaCode("%s.MODULE$.applyDouble(%s, %s, %s, %s)",
            this.getClass.getName,
            wrapArg(0, args, paramTypes, true),
            wrapArg(1, args, paramTypes, true),
            wrapArg(2, args, paramTypes, true),
            wrapArg(3, args, paramTypes, true))

        case _ => paramTypes.size match {
          case 4 =>
            JavaCode("%s.MODULE$.apply(%s, %s, %s, %s, null)",
              this.getClass.getName,
              wrapArg(0, args, paramTypes, true),
              wrapArg(1, args, paramTypes, true),
              wrapArg(2, args, paramTypes, true),
              wrapArg(3, args, paramTypes, true))
          case 5 =>
            JavaCode("%s.MODULE$.apply(%s, %s, %s, %s, %s)",
              this.getClass.getName,
              wrapArg(0, args, paramTypes, true),
              wrapArg(1, args, paramTypes, true),
              wrapArg(2, args, paramTypes, true),
              wrapArg(3, args, paramTypes, true),
              wrapArg(4, args, paramTypes, true))
        }
      }
    }

    def applyDouble(similarity: (Double, Double) => Double, x: PFAArray[Double], y: PFAArray[Double], p: Double): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      if (p <= 0.0)
        throw new PFARuntimeException("Minkowski parameter p must be positive")
      var tally = 0.0
      var i = 0
      while (i < length) {
        tally += Math.pow(similarity(xvector(i), yvector(i)), p)
        i += 1
      }
      Math.pow(tally, 1.0/p)
    }

    def apply[A, B](similarity: (A, B) => Double, x: PFAArray[A], y: PFAArray[B], p: Double, missing: PFAArray[Double]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val mvector =
        if (missing == null)
          Vector.fill(xvector.size)(1.0)
        else
          missing.toVector
      val length = xvector.size
      if (length != yvector.size  ||  length != mvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      if (p <= 0.0)
        throw new PFARuntimeException("Minkowski parameter p must be positive")
      var tally = 0.0
      var numer = 0.0
      var denom = 0.0
      var i = 0
      while (i < length) {
        val x = xvector(i)
        val y = yvector(i)
        if (x != null  &&  y != null) {
          tally += Math.pow(similarity(x, y), p)
          denom += mvector(i)
        }
        numer += mvector(i)
        i += 1
      }
      Math.pow(tally * numer / denom, 1.0/p)
    }

  }
  provide(Minkowski)

  ////   simpleMatching (SimpleMatching)
  object SimpleMatching extends LibFcn {
    val name = prefix + "simpleMatching"
    val sig = Sig(List("x" -> P.Array(P.Boolean), "y" -> P.Array(P.Boolean)), P.Double)
    val doc =
      <doc>
        <desc>Simple metric on binary vectors.</desc>
        <param name="x">First sample vector.</param>
        <param name="y">Second sample vector.  (Must have the same dimension as <p>x</p>.)</param>
        <ret>Where <m>{"a_{11}"}</m> is the number of <p>x</p>, <p>y</p> coordinate pairs that are equal to <c>true, true</c>, <m>{"a_{10}"}</m> is the number of <c>true, false</c>, <m>{"a_{01}"}</m> is the number of <c>false, true</c>, and <m>{"a_{00}"}</m> is the number of <c>false, false</c>, this function returns <m>{"(a_{11} + a_{00})/(a_{11} + a_{10} + a_{01} + a_{00})"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if <p>x</p> and <p>y</p> do not have the same dimension.</error>
      </doc>
    def apply(x: PFAArray[Boolean], y: PFAArray[Boolean]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      val pairs = xvector zip yvector
      val a11 = pairs count {case (true, true) => true;  case _ => false}
      val a10 = pairs count {case (true, false) => true;  case _ => false}
      val a01 = pairs count {case (false, true) => true;  case _ => false}
      val a00 = pairs count {case (false, false) => true;  case _ => false}
      (a11 + a00)/(a11 + a10 + a01 + a00).toDouble
    }
  }
  provide(SimpleMatching)

  ////   jaccard (Jaccard)
  object Jaccard extends LibFcn {
    val name = prefix + "jaccard"
    val sig = Sig(List("x" -> P.Array(P.Boolean), "y" -> P.Array(P.Boolean)), P.Double)
    val doc =
      <doc>
        <desc>Jaccard similarity of binary vectors.</desc>
        <param name="x">First sample vector.</param>
        <param name="y">Second sample vector.  (Must have the same dimension as <p>x</p>.)</param>
        <ret>Where <m>{"a_{11}"}</m> is the number of <p>x</p>, <p>y</p> coordinate pairs that are equal to <c>true, true</c>, <m>{"a_{10}"}</m> is the number of <c>true, false</c>, <m>{"a_{01}"}</m> is the number of <c>false, true</c>, and <m>{"a_{00}"}</m> is the number of <c>false, false</c>, this function returns <m>{"a_{11}/(a_{11} + a_{10} + a_{01})"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if <p>x</p> and <p>y</p> do not have the same dimension.</error>
      </doc>
    def apply(x: PFAArray[Boolean], y: PFAArray[Boolean]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      val pairs = xvector zip yvector
      val a11 = pairs count {case (true, true) => true;  case _ => false}
      val a10 = pairs count {case (true, false) => true;  case _ => false}
      val a01 = pairs count {case (false, true) => true;  case _ => false}
      val a00 = pairs count {case (false, false) => true;  case _ => false}
      a11/(a11 + a10 + a01).toDouble
    }
  }
  provide(Jaccard)

  ////   tanimoto (Tanimoto)
  object Tanimoto extends LibFcn {
    val name = prefix + "tanimoto"
    val sig = Sig(List("x" -> P.Array(P.Boolean), "y" -> P.Array(P.Boolean)), P.Double)
    val doc =
      <doc>
        <desc>Tanimoto similarity of binary vectors.</desc>
        <param name="x">First sample vector.</param>
        <param name="y">Second sample vector.  (Must have the same dimension as <p>x</p>.)</param>
        <ret>Where <m>{"a_{11}"}</m> is the number of <p>x</p>, <p>y</p> coordinate pairs that are equal to <c>true, true</c>, <m>{"a_{10}"}</m> is the number of <c>true, false</c>, <m>{"a_{01}"}</m> is the number of <c>false, true</c>, and <m>{"a_{00}"}</m> is the number of <c>false, false</c>, this function returns <m>{"(a_{11} + a_{00})/(a_{11} + 2*(a_{10} + a_{01}) + a_{00})"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if <p>x</p> and <p>y</p> do not have the same dimension.</error>
      </doc>
    def apply(x: PFAArray[Boolean], y: PFAArray[Boolean]): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      val pairs = xvector zip yvector
      val a11 = pairs count {case (true, true) => true;  case _ => false}
      val a10 = pairs count {case (true, false) => true;  case _ => false}
      val a01 = pairs count {case (false, true) => true;  case _ => false}
      val a00 = pairs count {case (false, false) => true;  case _ => false}
      (a11 + a00)/(a11 + 2*(a10 + a01) + a00).toDouble
    }
  }
  provide(Tanimoto)

  ////   binarySimilarity (BinarySimilarity)
  object BinarySimilarity extends LibFcn {
    val name = prefix + "binarySimilarity"
    val sig = Sig(List("x" -> P.Array(P.Boolean), "y" -> P.Array(P.Boolean), "c00" -> P.Double, "c01" -> P.Double, "c10" -> P.Double, "c11" -> P.Double, "d00" -> P.Double, "d01" -> P.Double, "d10" -> P.Double, "d11" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Genaralized similarity of binary vectors, using <p>c00</p>, <p>c01</p>, <p>c10</p>, <p>c11</p>, <p>d00</p>, <p>d01</p>, <p>d10</p>, and <p>d11</p> as parameters to reproduce all other binary similarity metrics.</desc>
        <param name="x">First sample vector.</param>
        <param name="y">Second sample vector.  (Must have the same dimension as <p>x</p>.)</param>
        <ret>Where <m>{"a_{11}"}</m> is the number of <p>x</p>, <p>y</p> coordinate pairs that are equal to <c>true, true</c>, <m>{"a_{10}"}</m> is the number of <c>true, false</c>, <m>{"a_{01}"}</m> is the number of <c>false, true</c>, and <m>{"a_{00}"}</m> is the number of <c>false, false</c>, this function returns <m>{"(c_{11}a_{11} + c_{10}a_{10} + c_{01}a_{01} + c_{00}a_{00})/(d_{11}a_{11} + d_{10}a_{10} + d_{01}a_{01} + d_{00}a_{00})"}</m>.</ret>
        <error>Raises "dimensions of vectors do not match" if <p>x</p> and <p>y</p> do not have the same dimension.</error>
      </doc>
    def apply(x: PFAArray[Boolean], y: PFAArray[Boolean], c00: Double, c01: Double, c10: Double, c11: Double, d00: Double, d01: Double, d10: Double, d11: Double): Double = {
      val xvector = x.toVector
      val yvector = y.toVector
      val length = xvector.size
      if (length != yvector.size)
        throw new PFARuntimeException("dimensions of vectors do not match")
      val pairs = xvector zip yvector
      val a11 = pairs count {case (true, true) => true;  case _ => false}
      val a10 = pairs count {case (true, false) => true;  case _ => false}
      val a01 = pairs count {case (false, true) => true;  case _ => false}
      val a00 = pairs count {case (false, false) => true;  case _ => false}
      (c11*a11 + c10*a10 + c01*a01 + c00*a00)/(d11*a11 + d10*a10 + d01*a01 + d00*a00).toDouble
    }
  }
  provide(BinarySimilarity)

}
