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
import scala.util.Random

import org.apache.avro.AvroRuntimeException

import org.ejml.simple.SimpleMatrix
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.special

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

package object reg {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.reg."

  //////////////////////////////////////////////////////////////////// predictors

  ////   linear (Linear)
  object Linear extends LibFcn {
    val name = prefix + "linear"
    val sig = Sigs(List(Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Array(P.Double), "const" -> P.Double))), P.Double),
                        Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Array(P.Array(P.Double)), "const" -> P.Array(P.Double)))), P.Array(P.Double)),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Map(P.Double), "const" -> P.Double))), P.Double),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Map(P.Map(P.Double)), "const" -> P.Map(P.Double)))), P.Map(P.Double))))

    val doc =
      <doc>
        <desc>Apply matrix <p>model</p> to independent variables <p>datum</p> to predict the dependent, predicted variables.</desc>
        <param name="datum">Vector of independent variables with <m>d</m> dimensions.</param>
        <param name="model">Parameters of the linear model.
          <paramField name="coeff">Vector or matrix of coefficients that multiply the input variables, which has <m>p</m> rows and <m>d</m> columns.</paramField>
          <paramField name="const">Scalar or vector of constant offsets, which has <m>p</m> dimensions.</paramField>
        </param>
        <ret>Returns a <m>p</m> dimensional vector of dependent, predicted variables.</ret>
        <detail>The vectors and matrix may be expressed as arrays (indexed by integers) or maps (indexed by strings).</detail>
        <detail>The simpler signature is may be used in the <m>p = 1</m>case.</detail>
        <error>Raises a "misaligned coeff" error if any row of <pf>coeff</pf> does not have the same indexes as <p>datum</p>.</error>
        <error>Raises a "misaligned const" error if <pf>const</pf> does not have the same indexes as <p>datum</p>.</error>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("%s.apply%s(%s, %s)",
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        (paramTypes(0), retType) match {
          case (AvroArray(_), AvroDouble()) => "1"
          case (AvroArray(_), AvroArray(AvroDouble())) => "2"
          case (AvroMap(_), AvroDouble()) => "3"
          case (AvroMap(_), AvroMap(AvroDouble())) => "4"
        },
        args(0).toString,
        args(1).toString)

    def apply1(datum: PFAArray[Double], model: PFARecord): Double = {
      val coeff = model.get("coeff").asInstanceOf[PFAArray[Double]]
      val const = model.get("const").asInstanceOf[java.lang.Double].doubleValue
      val datumVector = datum.toVector
      val coeffVector = coeff.toVector
      if (datumVector.size != coeffVector.size)
        throw new PFARuntimeException("misaligned coeff")
      val d = datumVector.size
      var out = 0.0
      var i = 0
      while (i < d) {
        out += datumVector(i) * coeffVector(i)
        i += 1
      }
      out + const
    }

    def apply2(datum: PFAArray[Double], model: PFARecord): PFAArray[Double] = {
      val coeff = model.get("coeff").asInstanceOf[PFAArray[PFAArray[Double]]]
      val const = model.get("const").asInstanceOf[PFAArray[Double]].toVector
      val datumVector = datum.toVector
      val d = datumVector.size
      val coeffVector = coeff.toVector
      val p = coeffVector.size
      if (const.size != p)
        throw new PFARuntimeException("misaligned const")
      val out = Array.fill[Double](p)(0.0)
      var j = 0
      while (j < p) {
        val row = coeffVector(j).toVector
        if (d != row.size)
          throw new PFARuntimeException("misaligned coeff")
        var i = 0
        while (i < d) {
          out(j) += datumVector(i) * row(i)
          i += 1
        }
        out(j) += const(j)
        j += 1
      }
      PFAArray.fromVector(out.toVector)
    }

    def apply3(datum: PFAMap[java.lang.Double], model: PFARecord): Double = {
      val coeff = model.get("coeff").asInstanceOf[PFAMap[java.lang.Double]]
      val const = model.get("const").asInstanceOf[java.lang.Double].doubleValue
      val datumMap = datum.toMap
      val coeffMap = coeff.toMap
      if (datumMap.size != coeffMap.size)
        throw new PFARuntimeException("misaligned coeff")
      val dset = datumMap.keySet
      var out = 0.0
      try {
        dset foreach {i => out += datumMap(i) * coeffMap(i)}
      }
      catch {
        case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned coeff")
      }
      out + const
    }

    def apply4(datum: PFAMap[java.lang.Double], model: PFARecord): PFAMap[java.lang.Double] = {
      val coeff = model.get("coeff").asInstanceOf[PFAMap[PFAMap[java.lang.Double]]]
      val const = model.get("const").asInstanceOf[PFAMap[java.lang.Double]].toMap
      val datumMap = datum.toMap
      val dset = datumMap.keySet
      val coeffMap = coeff.toMap
      val pset = coeffMap.keySet
      PFAMap.fromMap(pset map {j =>
        val row = coeffMap(j).toMap
        var out = 0.0
        try {
          dset foreach {i => out += datumMap(i) * row(i)}
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned coeff")
        }
        try {
          out += const(j)
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned const")
        }
        j -> java.lang.Double.valueOf(out)
      } toMap)
    }
  }
  provide(Linear)

  ////   linearVariance (LinearVariance)
  object LinearVariance extends LibFcn with com.opendatagroup.hadrian.lib1.la.EJMLInterface {
    val name = prefix + "linearVariance"
    val sig = Sigs(List(Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Array(P.Array(P.Double))))), P.Double),
                        Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Array(P.Array(P.Array(P.Double)))))), P.Array(P.Double)),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Map(P.Map(P.Double))))), P.Double),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Map(P.Map(P.Map(P.Double)))))), P.Map(P.Double))))
    val doc =
      <doc>
        <desc>Propagate variances from <p>model</p> <pf>covar</pf> (covariance matrix) to the dependent, predicted variable(s).</desc>
        <param name="datum">Vector of independent variables <m>{"""\vec{o}"""}</m> with <m>d</m> dimensions.</param>
        <param name="model">Parameters of the linear model.
          <paramField name="covar">Covariance matrix <m>{"""C"""}</m> or array/map of covariance matrices, one for each dependent, predicted variable.  Each matrix has <m>d + 1</m> rows and <m>d + 1</m> columns: the last or empty string-key row and column corresponds to the model's constant term.  If there are <m>p</m> dependent, predicted variables, the outermost array/map has <m>p</m> items.</paramField>
        </param>
        <ret>Propagated variance(s) <m>{"""\vec{o}^T C \vec{o}"""}</m> for each dependent, predicted variable.</ret>
        <detail>The "error" or "uncertainty" in the predicted variable(s) is the square root of this value/these values.</detail>
        <error>Raises a "misaligned covariance" error if any covariance matrix does not have the same indexes as <p>datum</p> plus the implicit index for a constant (last in array signature, empty string-key in map signature).</error>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("%s.apply%s(%s, %s)",
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        (paramTypes(0), retType) match {
          case (AvroArray(_), AvroDouble()) => "1"
          case (AvroArray(_), AvroArray(AvroDouble())) => "2"
          case (AvroMap(_), AvroDouble()) => "3"
          case (AvroMap(_), AvroMap(AvroDouble())) => "4"
        },
        args(0).toString,
        args(1).toString)

    def apply1(datum: PFAArray[Double], model: PFARecord): Double = {
      val covar = model.get("covar").asInstanceOf[PFAArray[PFAArray[Double]]]
      val datumVector = datum.toVector :+ 1.0
      if (covar.size != datumVector.size  ||  covar.toVector.exists(_.size != datumVector.size))
        throw new PFARuntimeException("misaligned covariance")
      val x = toDenseVector(datumVector)
      val C = toDense(covar)
      x.transpose.mult(C.mult(x)).get(0, 0)
    }

    def apply2(datum: PFAArray[Double], model: PFARecord): PFAArray[Double] = {
      val covars = model.get("covar").asInstanceOf[PFAArray[PFAArray[PFAArray[Double]]]]
      val datumVector = datum.toVector :+ 1.0
      val x = toDenseVector(datumVector)
      val results = covars.toVector map {covar =>
        if (covar.size != datumVector.size  ||  covar.toVector.exists(_.size != datumVector.size))
          throw new PFARuntimeException("misaligned covariance")
        val C = toDense(covar)
        x.transpose.mult(C.mult(x)).get(0, 0)
      }
      PFAArray.fromVector(results)
    }

    def apply3(datum: PFAMap[java.lang.Double], model: PFARecord): Double = {
      val covar = model.get("covar").asInstanceOf[PFAMap[PFAMap[java.lang.Double]]]
      val datumMap = datum.toMap.updated("", java.lang.Double.valueOf(1.0))
      val keys = datumMap.keySet.toVector
      val x = toDenseVector(PFAArray.fromVector(keys map {k => datumMap(k).doubleValue}))
      val C =
        try {
          new SimpleMatrix(keys map {k =>
            val row = covar.toMap.apply(k).toMap
            if (keys.size != row.size)
              throw new PFARuntimeException("misaligned covariance")
            keys map {kk => row(kk).doubleValue} toArray
          } toArray)
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned covariance")
        }
      x.transpose.mult(C.mult(x)).get(0, 0)
    }

    def apply4(datum: PFAMap[java.lang.Double], model: PFARecord): PFAMap[java.lang.Double] = {
      val covars = model.get("covar").asInstanceOf[PFAMap[PFAMap[PFAMap[java.lang.Double]]]]
      val datumMap = datum.toMap.updated("", java.lang.Double.valueOf(1.0))
      val keys = datumMap.keySet.toVector
      val x = toDenseVector(PFAArray.fromVector(keys map {k => datumMap(k).doubleValue}))
      PFAMap.fromMap(covars.toMap map {case (depkey, covar) =>
        val C =
          try {
            new SimpleMatrix(keys map {k =>
              val row = covar.toMap.apply(k).toMap
              if (keys.size != row.size)
                throw new PFARuntimeException("misaligned covariance")
              keys map {kk => row(kk).doubleValue} toArray
            } toArray)
          }
          catch {
            case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned covariance")
          }
        (depkey, java.lang.Double.valueOf(x.transpose.mult(C.mult(x)).get(0, 0)))
      })
    }
  }
  provide(LinearVariance)


  //////////////////////////////////////////////////////////////////// quality

  ////   residual (Residual)
  object Residual extends LibFcn {
    val name = prefix + "residual"
    val sig = Sigs(List(Sig(List("observation" -> P.Double, "prediciton" -> P.Double), P.Double),
                        Sig(List("observation" -> P.Array(P.Double), "prediciton" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("observation" -> P.Map(P.Double), "prediciton" -> P.Map(P.Double)), P.Map(P.Double))))
    val doc =
      <doc>
        <desc>Compare an observation with its prediction by element-wise subtraction.</desc>
        <param name="observation">Scalar or vector of observations.</param>
        <param name="prediction">Scalar or vector of predictions.</param>
        <ret>Scalar or vector of <p>observation</p> minus <p>prediction</p>.</ret>
        <error>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes as <p>observation</p>.</error>
      </doc>

    def apply(observation: Double, prediction: Double): Double =
      observation - prediction

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double]): PFAArray[Double] = {
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction")
      PFAArray.fromVector(observation.toVector.zip(prediction.toVector) map {case (o, p) => o - p})
    }

    def apply(observation: PFAMap[java.lang.Double], prediction: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val observationMap = observation.toMap
      val predictionMap = prediction.toMap
      if (observationMap.size != predictionMap.size)
        throw new PFARuntimeException("misaligned prediction")
      PFAMap.fromMap(observationMap map {case (k, o) =>
        try {
          (k, java.lang.Double.valueOf(o.doubleValue - predictionMap(k).doubleValue))
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned prediction")
        }
      })
    }
  }
  provide(Residual)

  ////   pull (Pull)
  object Pull extends LibFcn {
    val name = prefix + "pull"
    val sig = Sigs(List(Sig(List("observation" -> P.Double, "prediciton" -> P.Double, "uncertainty" -> P.Double), P.Double),
                        Sig(List("observation" -> P.Array(P.Double), "prediciton" -> P.Array(P.Double), "uncertainty" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("observation" -> P.Map(P.Double), "prediciton" -> P.Map(P.Double), "uncertainty" -> P.Map(P.Double)), P.Map(P.Double))))
    val doc =
      <doc>
        <desc>Compare an observation with its prediction by element-wise subtraction, weighted by element-wise uncertainties.</desc>
        <param name="observation">Scalar or vector of observations.</param>
        <param name="prediction">Scalar or vector of predictions.</param>
        <param name="uncertainty">Scalar or vector of predictions.</param>
        <ret>Scalar or vector of <p>observation</p> minus <p>prediction</p> divided by <p>uncertainty</p>.</ret>
        <error>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes as <p>observation</p>.</error>
        <error>Raises a "misaligned uncertainty" error if <p>prediction</p> does not have the same indexes as <p>uncertainty</p>.</error>
      </doc>

    def apply(observation: Double, prediction: Double, uncertainty: Double): Double =
      (observation - prediction) / uncertainty

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double], uncertainty: PFAArray[Double]): PFAArray[Double] = {
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction")
      if (observation.size != uncertainty.size)
        throw new PFARuntimeException("misaligned uncertainty")
      PFAArray.fromVector(observation.toVector.zip(prediction.toVector).zip(uncertainty.toVector) map {case ((o, p), u) => (o - p)/u})
    }

    def apply(observation: PFAMap[java.lang.Double], prediction: PFAMap[java.lang.Double], uncertainty: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val observationMap = observation.toMap
      val predictionMap = prediction.toMap
      val uncertaintyMap = uncertainty.toMap
      if (observationMap.size != predictionMap.size)
        throw new PFARuntimeException("misaligned prediction")
      if (observationMap.size != uncertaintyMap.size)
        throw new PFARuntimeException("misaligned uncertainty")
      PFAMap.fromMap(observationMap map {case (k, o) =>
        val p =
          try {
            predictionMap(k)
          }
          catch {
            case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned prediction")
          }
        val u =
          try {
            uncertaintyMap(k)
          }
          catch {
            case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned uncertainty")
          }
        (k, java.lang.Double.valueOf((o.doubleValue - p.doubleValue) / u.doubleValue))
      })
    }
  }
  provide(Pull)

  ////   mahalanobis (Mahalanobis)
  object Mahalanobis extends LibFcn with com.opendatagroup.hadrian.lib1.la.EJMLInterface {
    val name = prefix + "mahalanobis"
    val sig = Sigs(List(Sig(List("observation" -> P.Array(P.Double), "prediction" -> P.Array(P.Double), "covariance" -> P.Array(P.Array(P.Double))), P.Double),
                        Sig(List("observation" -> P.Map(P.Double), "prediction" -> P.Map(P.Double), "covariance" -> P.Map(P.Map(P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compare an observation with its prediction by computing the Mahalanobis distance for a given covariance matrix.</desc>
        <param name="observation">Vector of observations <m>{"""\vec{o}"""}</m>.</param>
        <param name="prediction">Vector of predictions <m>{"""\vec{p}"""}</m>.</param>
        <param name="covariance">Matrix of covariance <m>{"""C"""}</m>.</param>
        <ret>Scalar result of a similarity transformation: <m>{"""\sqrt{(\vec{o} - \vec{p})^T C^{-1} (\vec{o} - \vec{p})}"""}</m>.</ret>
        <error>Raises a "too few rows/cols" error if <p>observation</p> has fewer than one element.</error>
        <error>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes as <p>observation</p>.</error>
        <error>Raises a "misaligned covariance" error if <p>covariance</p> does not have the same indexes as <p>observation</p>.</error>
      </doc>

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double], covariance: PFAArray[PFAArray[Double]]): Double = {
      if (observation.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction")
      if (covariance.toVector exists {_.size != observation.size})
        throw new PFARuntimeException("misaligned covariance")
      val x = toDenseVector(observation.toVector zip prediction.toVector map {case (o, p) => o - p})
      val cinv = toDense(covariance).invert
      Math.sqrt(x.transpose.mult(cinv.mult(x)).get(0, 0))
    }

    def apply(observation: PFAMap[java.lang.Double], prediction: PFAMap[java.lang.Double], covariance: PFAMap[PFAMap[java.lang.Double]]): Double = {
      val observationMap = observation.toMap
      val predictionMap = prediction.toMap
      if (observationMap.size < 1)
        throw new PFARuntimeException("too few rows/cols")

      val keys = observationMap.keySet.toVector
      if (keys.size != predictionMap.size)
        throw new PFARuntimeException("misaligned prediction")
      val x =
        try {
          toDenseVector(keys map {k => observationMap(k).doubleValue - predictionMap(k).doubleValue})
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned prediction")
        }

      val covarianceMap = covariance.toMap
      if (keys.size != covarianceMap.size)
        throw new PFARuntimeException("misaligned covariance")
      val c =
        try {
          new SimpleMatrix(keys map {k =>
            val row = covarianceMap(k).toMap
            if (keys.size != row.size)
              throw new PFARuntimeException("misaligned covariance")
            keys map {kk => row(kk).doubleValue} toArray
          } toArray)
        }
        catch {
          case err: java.util.NoSuchElementException => throw new PFARuntimeException("misaligned covariance")
        }

      val cinv = c.invert
      Math.sqrt(x.transpose.mult(cinv.mult(x)).get(0, 0))
    }
  }
  provide(Mahalanobis)

  ////   updateChi2 (UpdateChi2)
  object UpdateChi2 extends LibFcn {
    val name = prefix + "updateChi2"
    val sig = Sigs(List(Sig(List("pull" -> P.Double, "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Wildcard("A")),
                        Sig(List("pull" -> P.Array(P.Double), "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Wildcard("A")),
                        Sig(List("pull" -> P.Map(P.Double), "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Wildcard("A"))))
    val doc =
      <doc>
        <desc>Update the state of a chi-square calculation.</desc>
        <param name="pull">Observation minus prediction divided by uncertainty.  If this is a scalar, it will be squared and added to the chi-square.  If a vector, each component will be squared and added to the chi-square.</param>
        <param name="state">Record of the previous <pf>chi2</pf> and <pf>DOF</pf>.</param>
      </doc>

    def update(x: Double, state: PFARecord): PFARecord = {
      val chi2 = state.get("chi2").asInstanceOf[java.lang.Double].doubleValue + x
      val DOF = state.get("DOF").asInstanceOf[java.lang.Integer].intValue + 1
      state.multiUpdate(Array("chi2", "DOF"), Array(chi2, DOF))
    }

    def apply(pull: Double, state: PFARecord): PFARecord = update(pull * pull, state)
    def apply(pull: PFAArray[Double], state: PFARecord): PFARecord = update(pull.toVector.map(x => Math.pow(x.doubleValue, 2)).sum, state)
    def apply(pull: PFAMap[java.lang.Double], state: PFARecord): PFARecord = update(pull.toMap.values.map(x => Math.pow(x.doubleValue, 2)).sum, state)
  }
  provide(UpdateChi2)

  ////   reducedChi2 (ReducedChi2)
  object ReducedChi2 extends LibFcn {
    val name = prefix + "reducedChi2"
    val sig = Sig(List("state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Double)
    val doc =
      <doc>
        <desc>Return the reduced chi-square, which is <pf>chi2</pf>/<pf>DOF</pf>.</desc>
        <param name="state">Record of the <pf>chi2</pf> and <pf>DOF</pf>.</param>
      </doc>
    def apply(state: PFARecord): Double =
      state.get("chi2").asInstanceOf[java.lang.Double].doubleValue / state.get("DOF").asInstanceOf[java.lang.Number].doubleValue
  }
  provide(ReducedChi2)

  ////   chi2Prob (Chi2Prob)
  object Chi2Prob extends LibFcn {
    val name = prefix + "chi2Prob"
    val sig = Sig(List("state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Double)
    val doc =
      <doc>
        <desc>Return the chi-square probability, which is the CDF of the chi-square function.</desc>
        <param name="state">Record of the <pf>chi2</pf> and <pf>DOF</pf>.</param>
        <error>Raises "invalid parameterization" if <pf>DOF</pf> is less than zero.</error>
      </doc>
    def apply(state: PFARecord): Double = {
      val chi2 = state.get("chi2").asInstanceOf[java.lang.Double].doubleValue
      val DOF = state.get("DOF").asInstanceOf[java.lang.Number].doubleValue
      if (DOF < 0.0)
        throw new PFARuntimeException("invalid parameterization")
      else if (DOF == 0.0) {
        if (chi2 > 0.0)
          1.0
        else
          0.0
      }
      else
        new ChiSquaredDistribution(DOF).cumulativeProbability(chi2)
    }
  }
  provide(Chi2Prob)

  //////////////////////////////////////////////////////////////////// fitting

}
