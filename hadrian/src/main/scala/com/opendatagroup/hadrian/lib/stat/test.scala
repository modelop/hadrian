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

package com.opendatagroup.hadrian.lib.stat

import scala.language.postfixOps
import scala.collection.immutable.ListMap

import org.ejml.simple.SimpleMatrix
import org.apache.commons.math3.distribution.ChiSquaredDistribution

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
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "stat.test."

  ///////////////// KS TEST
  class KSTwoSample(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "kolmogorov"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Double)
     
    def doc =
      <doc>
        <desc>Compare two datasets using the Kolmogorov-Smirnov test to determine if they might have been drawn from the same parent distribution.</desc>
        <param name="x">A bag of data.</param>
        <param name="y">Another bag of data.</param>
        <ret>Returns a value between 0.0 and 1.0 representing the cumulative probability that <p>x</p> and <p>y</p> were drawn from the same distribution: 1.0 indicates a perfect match.</ret>
        <detail>If both datasets (ignoring NaN values) are empty, this function returns 1.0</detail>
      </doc>
    def errcodeBase = 38000

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
      val xx = x.toVector.filter(!_.isNaN).sorted
      val yy = y.toVector.filter(!_.isNaN).sorted
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
  provide(new KSTwoSample)

  ////   residual (Residual)
  class Residual(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "residual"
    def sig = Sigs(List(Sig(List("observation" -> P.Double, "prediciton" -> P.Double), P.Double),
                        Sig(List("observation" -> P.Array(P.Double), "prediciton" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("observation" -> P.Map(P.Double), "prediciton" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Compare an observation with its prediction by element-wise subtraction.</desc>
        <param name="observation">Scalar or vector of observations.</param>
        <param name="prediction">Scalar or vector of predictions.</param>
        <ret>Scalar or vector of <p>observation</p> minus <p>prediction</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes or keys as <p>observation</p>.</error>
      </doc>
    def errcodeBase = 38010

    def apply(observation: Double, prediction: Double): Double =
      observation - prediction

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double]): PFAArray[Double] = {
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
      PFAArray.fromVector(observation.toVector.zip(prediction.toVector) map {case (o, p) => o - p})
    }

    def apply(observation: PFAMap[java.lang.Double], prediction: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val observationMap = observation.toMap
      val predictionMap = prediction.toMap
      if (observationMap.keySet != predictionMap.keySet)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
      PFAMap.fromMap(observationMap map {case (k, o) =>
        try {
          (k, java.lang.Double.valueOf(o.doubleValue - predictionMap(k).doubleValue))
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
        }
      })
    }
  }
  provide(new Residual)

  ////   pull (Pull)
  class Pull(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "pull"
    def sig = Sigs(List(Sig(List("observation" -> P.Double, "prediciton" -> P.Double, "uncertainty" -> P.Double), P.Double),
                        Sig(List("observation" -> P.Array(P.Double), "prediciton" -> P.Array(P.Double), "uncertainty" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("observation" -> P.Map(P.Double), "prediciton" -> P.Map(P.Double), "uncertainty" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Compare an observation with its prediction by element-wise subtraction, weighted by element-wise uncertainties.</desc>
        <param name="observation">Scalar or vector of observations.</param>
        <param name="prediction">Scalar or vector of predictions.</param>
        <param name="uncertainty">Scalar or vector of predictions.</param>
        <ret>Scalar or vector of <p>observation</p> minus <p>prediction</p> divided by <p>uncertainty</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes or keys as <p>observation</p>.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "misaligned uncertainty" error if <p>prediction</p> does not have the same indexes or keys as <p>uncertainty</p>.</error>
      </doc>
    def errcodeBase = 38020

    def apply(observation: Double, prediction: Double, uncertainty: Double): Double =
      (observation - prediction) / uncertainty

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double], uncertainty: PFAArray[Double]): PFAArray[Double] = {
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
      if (observation.size != uncertainty.size)
        throw new PFARuntimeException("misaligned uncertainty", errcodeBase + 1, name, pos)
      PFAArray.fromVector(observation.toVector.zip(prediction.toVector).zip(uncertainty.toVector) map {case ((o, p), u) => (o - p)/u})
    }

    def apply(observation: PFAMap[java.lang.Double], prediction: PFAMap[java.lang.Double], uncertainty: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val observationMap = observation.toMap
      val predictionMap = prediction.toMap
      val uncertaintyMap = uncertainty.toMap
      if (observationMap.keySet != predictionMap.keySet)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
      if (observationMap.keySet != uncertaintyMap.keySet)
        throw new PFARuntimeException("misaligned uncertainty", errcodeBase + 1, name, pos)
      PFAMap.fromMap(observationMap map {case (k, o) =>
        val p =
          try {
            predictionMap(k)
          }
          catch {
            case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
          }
        val u =
          try {
            uncertaintyMap(k)
          }
          catch {
            case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned uncertainty", errcodeBase + 1, name, pos)
          }
        (k, java.lang.Double.valueOf((o.doubleValue - p.doubleValue) / u.doubleValue))
      })
    }
  }
  provide(new Pull)

  ////   mahalanobis (Mahalanobis)
  class Mahalanobis(val pos: Option[String] = None) extends LibFcn with com.opendatagroup.hadrian.lib.la.EJMLInterface {
    def name = prefix + "mahalanobis"
    def sig = Sigs(List(Sig(List("observation" -> P.Array(P.Double), "prediction" -> P.Array(P.Double), "covariance" -> P.Array(P.Array(P.Double))), P.Double),
                        Sig(List("observation" -> P.Map(P.Double), "prediction" -> P.Map(P.Double), "covariance" -> P.Map(P.Map(P.Double))), P.Double)))
    def doc =
      <doc>
        <desc>Compare an observation with its prediction by computing the Mahalanobis distance for a given covariance matrix.</desc>
        <param name="observation">Vector of observations <m>{"""\vec{o}"""}</m>.</param>
        <param name="prediction">Vector of predictions <m>{"""\vec{p}"""}</m>.</param>
        <param name="covariance">Matrix of covariance <m>{"""C"""}</m>.</param>
        <ret>Scalar result of a similarity transformation: <m>{"""\sqrt{(\vec{o} - \vec{p})^T C^{-1} (\vec{o} - \vec{p})}"""}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "too few rows/cols" error if <p>observation</p> has fewer than one element.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes or keys as <p>observation</p>.</error>
        <error code={s"${errcodeBase + 2}"}>Raises a "misaligned covariance" error if <p>covariance</p> does not have the same indexes or keys as <p>observation</p>.</error>
      </doc>
    def errcodeBase = 38030

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double], covariance: PFAArray[PFAArray[Double]]): Double = {
      if (observation.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 1, name, pos)
      if ((covariance.toVector.size != observation.size)  ||  (covariance.toVector exists {_.size != observation.size}))
        throw new PFARuntimeException("misaligned covariance", errcodeBase + 2, name, pos)
      val x = toDenseVector(observation.toVector zip prediction.toVector map {case (o, p) => o - p})
      val cinv = toDense(covariance).invert
      Math.sqrt(x.transpose.mult(cinv.mult(x)).get(0, 0))
    }

    def apply(observation: PFAMap[java.lang.Double], prediction: PFAMap[java.lang.Double], covariance: PFAMap[PFAMap[java.lang.Double]]): Double = {
      val observationMap = observation.toMap
      val predictionMap = prediction.toMap
      if (observationMap.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)

      val keys = observationMap.keySet.toVector
      if (keys.size != predictionMap.size)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 1, name, pos)
      val x =
        try {
          toDenseVector(keys map {k => observationMap(k).doubleValue - predictionMap(k).doubleValue})
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned prediction", errcodeBase + 1, name, pos)
        }

      val covarianceMap = covariance.toMap
      if (keys.toSet != covarianceMap.keySet)
        throw new PFARuntimeException("misaligned covariance", errcodeBase + 2, name, pos)
      val c =
        try {
          new SimpleMatrix(keys map {k =>
            val row = covarianceMap(k).toMap
            if (keys.size != row.size)
              throw new PFARuntimeException("misaligned covariance", errcodeBase + 2, name, pos)
            keys map {kk => row(kk).doubleValue} toArray
          } toArray)
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned covariance", errcodeBase + 2, name, pos)
        }

      val cinv = c.invert
      Math.sqrt(x.transpose.mult(cinv.mult(x)).get(0, 0))
    }
  }
  provide(new Mahalanobis)

  ////   updateChi2 (UpdateChi2)
  class UpdateChi2(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "updateChi2"
    def sig = Sigs(List(Sig(List("pull" -> P.Double, "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "dof" -> P.Int))), P.Wildcard("A")),
                        Sig(List("pull" -> P.Array(P.Double), "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "dof" -> P.Int))), P.Wildcard("A")),
                        Sig(List("pull" -> P.Map(P.Double), "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "dof" -> P.Int))), P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Update the state of a chi-square calculation.</desc>
        <param name="pull">Observation minus prediction divided by uncertainty.  If this is a scalar, it will be squared and added to the chi-square.  If a vector, each component will be squared and added to the chi-square.</param>
        <param name="state">Record of the previous <pf>chi2</pf> and <pf>dof</pf>.</param>
      </doc>
    def errcodeBase = 38040

    def update(x: Double, state: PFARecord): PFARecord = {
      val chi2 = state.get("chi2").asInstanceOf[java.lang.Double].doubleValue + x
      val DOF = state.get("dof").asInstanceOf[java.lang.Integer].intValue + 1
      state.multiUpdate(Array("chi2", "dof"), Array(chi2, DOF))
    }

    def apply(pull: Double, state: PFARecord): PFARecord = update(pull * pull, state)
    def apply(pull: PFAArray[Double], state: PFARecord): PFARecord = update(pull.toVector.map(x => Math.pow(x.doubleValue, 2)).sum, state)
    def apply(pull: PFAMap[java.lang.Double], state: PFARecord): PFARecord = update(pull.toMap.values.map(x => Math.pow(x.doubleValue, 2)).sum, state)
  }
  provide(new UpdateChi2)

  ////   reducedChi2 (ReducedChi2)
  class ReducedChi2(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "reducedChi2"
    def sig = Sig(List("state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "dof" -> P.Int))), P.Double)
    def doc =
      <doc>
        <desc>Return the reduced chi-square, which is <pf>chi2</pf>/<pf>dof</pf>.</desc>
        <param name="state">Record of the <pf>chi2</pf> and <pf>dof</pf>.</param>
      </doc>
    def errcodeBase = 38050
    def apply(state: PFARecord): Double =
      state.get("chi2").asInstanceOf[java.lang.Double].doubleValue / state.get("dof").asInstanceOf[java.lang.Number].doubleValue
  }
  provide(new ReducedChi2)

  ////   chi2Prob (Chi2Prob)
  class Chi2Prob(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "chi2Prob"
    def sig = Sig(List("state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "dof" -> P.Int))), P.Double)
    def doc =
      <doc>
        <desc>Return the chi-square probability, which is the CDF of the chi-square function.</desc>
        <param name="state">Record of the <pf>chi2</pf> and <pf>dof</pf>.</param>
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <pf>dof</pf> is less than zero.</error>
      </doc>
    def errcodeBase = 38060
    def apply(state: PFARecord): Double = {
      val chi2 = state.get("chi2").asInstanceOf[java.lang.Double].doubleValue
      val DOF = state.get("dof").asInstanceOf[java.lang.Number].doubleValue
      if (DOF < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
      else if (DOF == 0.0) {
        if (chi2 > 0.0)
          1.0
        else
          0.0
      }
      else if (chi2.isNaN)
        java.lang.Double.NaN
      else if (java.lang.Double.isInfinite(chi2)) {
        if (chi2 > 0.0)
          1.0
        else
          0.0
      }
      else
        new ChiSquaredDistribution(DOF).cumulativeProbability(chi2)
    }
  }
  provide(new Chi2Prob)

}
