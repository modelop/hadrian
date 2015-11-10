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

package com.opendatagroup.hadrian.lib.model

import scala.language.postfixOps
import scala.collection.immutable.ListMap
import scala.util.Random

import org.apache.avro.AvroRuntimeException

import org.ejml.simple.SimpleMatrix
import org.ejml.factory.DecompositionFactory
import org.ejml.factory.LinearSolverFactory
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.special

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFASemanticException
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
import com.opendatagroup.hadrian.signature.Lifespan
import com.opendatagroup.hadrian.signature.PFAVersion

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
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.reg."

  //////////////////////////////////////////////////////////////////// predictors

  ////   linear (Linear)
  class Linear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "linear"
    def sig = Sigs(List(Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Array(P.Double), "const" -> P.Double))), P.Double),
                        Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Array(P.Array(P.Double)), "const" -> P.Array(P.Double)))), P.Array(P.Double)),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Map(P.Double), "const" -> P.Double))), P.Double),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("coeff" -> P.Map(P.Map(P.Double)), "const" -> P.Map(P.Double)))), P.Map(P.Double))))

    def doc =
      <doc>
        <desc>Apply matrix <p>model</p> to independent variables <p>datum</p> to predict the dependent, predicted variables.</desc>
        <param name="datum">Vector of independent variables with <m>d</m> dimensions.</param>
        <param name="model">Parameters of the linear model.
          <paramField name="coeff">Vector or matrix of coefficients that multiply the input variables, which has <m>p</m> rows and <m>d</m> columns.</paramField>
          <paramField name="const">Scalar or vector of constant offsets, which has <m>p</m> dimensions.</paramField>
        </param>
        <ret>Returns a <m>p</m> dimensional vector of dependent, predicted variables.</ret>
        <detail>The vectors and matrix may be expressed as arrays (indexed by integers) or maps (indexed by strings). In the array signature, the number of rows and/or columns in <p>x</p> must be equal to the number of rows and/or columns of <p>y</p>, respectively (dense matrix). In the map signature, missing row-column combinations are assumed to be zero (sparse matrix).</detail>
        <detail>The simpler signature is may be used in the <m>p = 1</m>case.</detail>
        <error code={s"${errcodeBase + 0}"}>The array signature raises a "misaligned coeff" error if any row of <pf>coeff</pf> does not have the same indexes as <p>datum</p>.</error>
        <error code={s"${errcodeBase + 1}"}>The array signature raises a "misaligned const" error if <pf>const</pf> does not have the same indexes as <p>coeff</p>.</error>
      </doc>
    def errcodeBase = 31000

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
        throw new PFARuntimeException("misaligned coeff", errcodeBase + 0, name, pos)
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
        throw new PFARuntimeException("misaligned const", errcodeBase + 1, name, pos)
      val out = Array.fill[Double](p)(0.0)
      var j = 0
      while (j < p) {
        val row = coeffVector(j).toVector
        if (row.size != d)
          throw new PFARuntimeException("misaligned coeff", errcodeBase + 0, name, pos)
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
      val datumMap = datum.toMap
      val const = model.get("const").asInstanceOf[java.lang.Double].doubleValue
      val coeffMap = model.get("coeff").asInstanceOf[PFAMap[java.lang.Double]].toMap
      var out = 0.0
      (datumMap.keySet union coeffMap.keySet) foreach {key =>
        val d = datumMap.get(key) match {
          case None => 0.0
          case Some(x) => x.doubleValue
        }
        val cc = coeffMap.get(key) match {
          case None => 0.0
          case Some(x) => x.doubleValue
        }
        out += d * cc
      }
      out + const
    }

    def apply4(datum: PFAMap[java.lang.Double], model: PFARecord): PFAMap[java.lang.Double] = {
      val datumMap = datum.toMap
      val constMap = model.get("const").asInstanceOf[PFAMap[java.lang.Double]].toMap
      val coeffMap = model.get("coeff").asInstanceOf[PFAMap[PFAMap[java.lang.Double]]].toMap

      val innerKeys =
        if (coeffMap.isEmpty)
          datumMap.keySet
        else
          datumMap.keySet union (coeffMap map {_._2.toMap.keySet} reduce {_ union _})
      val outerKeys = constMap.keySet union coeffMap.keySet

      PFAMap.fromMap(outerKeys map {outerKey =>
        val row = coeffMap.get(outerKey) match {
          case None => Map[String, java.lang.Double]()
          case Some(x) => x.toMap
        }
        var out = 0.0

        innerKeys foreach {innerKey =>
          val d = datumMap.get(innerKey) match {
            case None => 0.0
            case Some(x) => x.doubleValue
          }
          val cc = row.get(innerKey) match {
            case None => 0.0
            case Some(x) => x.doubleValue
          }
          out += d * cc
        }

        val c = constMap.get(outerKey) match {
          case None => 0.0
          case Some(x) => x.doubleValue
        }
        out += c

        outerKey -> java.lang.Double.valueOf(out)
      } toMap)
    }
  }
  provide(new Linear)

  ////   linearVariance (LinearVariance)
  class LinearVariance(val pos: Option[String] = None) extends LibFcn with com.opendatagroup.hadrian.lib.la.EJMLInterface {
    def name = prefix + "linearVariance"
    def sig = Sigs(List(Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Array(P.Array(P.Double))))), P.Double),
                        Sig(List("datum" -> P.Array(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Array(P.Array(P.Array(P.Double)))))), P.Array(P.Double)),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Map(P.Map(P.Double))))), P.Double),
                        Sig(List("datum" -> P.Map(P.Double), "model" -> P.WildRecord("M", ListMap("covar" -> P.Map(P.Map(P.Map(P.Double)))))), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Propagate variances from <p>model</p> <pf>covar</pf> (covariance matrix) to the dependent, predicted variable(s).</desc>
        <param name="datum">Vector of independent variables <m>{"""\vec{o}"""}</m> with <m>d</m> dimensions.</param>
        <param name="model">Parameters of the linear model.
          <paramField name="covar">Covariance matrix <m>{"""C"""}</m> or array/map of covariance matrices, one for each dependent, predicted variable.  Each matrix has <m>d + 1</m> rows and <m>d + 1</m> columns: the last (array) or empty string (map) row and column corresponds to the model's constant term.  If there are <m>p</m> dependent, predicted variables, the outermost array/map has <m>p</m> items.</paramField>
        </param>
        <ret>Propagated variance(s) <m>{"""\vec{o}^T C \vec{o}"""}</m> for each dependent, predicted variable.</ret>
        <detail>The "error" or "uncertainty" in the predicted variable(s) is the square root of this value/these values.</detail>
        <detail>The vectors and matrix may be expressed as arrays (indexed by integers) or maps (indexed by strings). In the array signature, the number of rows and/or columns in <p>x</p> must be equal to the number of rows and/or columns of <p>y</p>, respectively (dense matrix). In the map signature, missing row-column combinations are assumed to be zero (sparse matrix).</detail>
        <error code={s"${errcodeBase + 0}"}>The array signature raises a "misaligned covariance" error if any covariance matrix does not have the same indexes as <p>datum</p> plus the implicit index for a constant (last in array signature).</error>
      </doc>
    def errcodeBase = 31010

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
        throw new PFARuntimeException("misaligned covariance", errcodeBase + 0, name, pos)
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
          throw new PFARuntimeException("misaligned covariance", errcodeBase + 0, name, pos)
        val C = toDense(covar)
        x.transpose.mult(C.mult(x)).get(0, 0)
      }
      PFAArray.fromVector(results)
    }

    def apply3(datum: PFAMap[java.lang.Double], model: PFARecord): Double = {
      val covarMap = model.get("covar").asInstanceOf[PFAMap[PFAMap[java.lang.Double]]].toMap
      val datumMap = datum.toMap.updated("", java.lang.Double.valueOf(1.0))
      val keys =
        if (covarMap.isEmpty)
          datumMap.keys.toVector
        else
          (datumMap.keySet union (covarMap map {_._2.toMap.keySet} reduce {_ union _})).toVector
      val x = toDenseVector(PFAArray.fromVector(keys map {k => datumMap.get(k) match {case None => 0.0; case Some(x) => x.doubleValue}}))
      val C =
        new SimpleMatrix(keys map {k =>
          val row = covarMap.get(k) match {
            case None => Map[String, java.lang.Double]()
            case Some(x) => x.toMap
          }
          keys map {kk => row.get(kk) match {case None => 0.0; case Some(x) => x.doubleValue}} toArray
        } toArray)
      x.transpose.mult(C.mult(x)).get(0, 0)
    }

    def apply4(datum: PFAMap[java.lang.Double], model: PFARecord): PFAMap[java.lang.Double] = {
      val covarsMap = model.get("covar").asInstanceOf[PFAMap[PFAMap[PFAMap[java.lang.Double]]]].toMap
      val datumMap = datum.toMap.updated("", java.lang.Double.valueOf(1.0))
      val keys =
        if (covarsMap.map(_._2.toMap.size).sum == 0)
          datumMap.keys.toVector
        else
          (datumMap.keySet union (covarsMap flatMap {_._2.toMap.values} map {_.toMap.keySet} reduce {_ union _})).toVector
      val x = toDenseVector(PFAArray.fromVector(keys map {k => datumMap.get(k) match {case None => 0.0; case Some(x) => x.doubleValue}}))
      PFAMap.fromMap(covarsMap map {case (depkey, covar) =>
        val C =
          new SimpleMatrix(keys map {k =>
            val row = covar.toMap.get(k) match {
              case None => Map[String, java.lang.Double]()
              case Some(x) => x.toMap
            }
            keys map {kk => row.get(kk) match {case None => 0.0; case Some(x) => x.doubleValue}} toArray
          } toArray)
        (depkey, java.lang.Double.valueOf(x.transpose.mult(C.mult(x)).get(0, 0)))
      })
    }
  }
  provide(new LinearVariance)

  ////   gaussianProcess (GaussianProcess)
  object GaussianProcess {
    case class Fit(xmean: Array[Double], xstd: Array[Double], xnorm: Array[PFAArray[Double]], ymean: Double, ystd: Double, krigingWeight: AnyRef, beta: Double, gamma: Array[Double])
  }
  class GaussianProcess(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "gaussianProcess"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Double))),
                                 "krigingWeight" -> P.Union(List(P.Null, P.Double)),
                                 "kernel" -> P.Fcn(List(P.Array(P.Double), P.Array(P.Double)), P.Double)), P.Double),
                        Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Array(P.Double)))),
                                 "krigingWeight" -> P.Union(List(P.Null, P.Double)),
                                 "kernel" -> P.Fcn(List(P.Array(P.Double), P.Array(P.Double)), P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Array(P.Double), "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Array(P.Double), "to" -> P.Double))),
                                 "krigingWeight" -> P.Union(List(P.Null, P.Double)),
                                 "kernel" -> P.Fcn(List(P.Array(P.Double), P.Array(P.Double)), P.Double)), P.Double),
                        Sig(List("x" -> P.Array(P.Double), "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Array(P.Double), "to" -> P.Array(P.Double)))),
                                 "krigingWeight" -> P.Union(List(P.Null, P.Double)),
                                 "kernel" -> P.Fcn(List(P.Array(P.Double), P.Array(P.Double)), P.Double)), P.Array(P.Double))))
    def doc =
      <doc>
        <desc>Fit the training data in <p>table</p> with a Gaussian Process model and predict the value of model at <p>x</p>.</desc>
        <param name="x">Position (scalar or vector) at which to predict the value of the model.</param>
        <param name="table">Training data for the Gaussian Process.
          <paramField name="x">Independent variable (scalar or vector, but same as <p>x</p>) of a training datum.</paramField>
          <paramField name="to">Dependent variable (scalar or vector) of a training datum.</paramField>
          <paramField name="sigma">Optional uncertainty for the datum. If present, it must have the same type as <pf>to</pf> and is used in the Gaussian Process fit as a nugget.</paramField>
        </param>
        <param name="krigingWeight">If a number, the Gaussian Process is performed with the specified Kriging weight. If <c>null</c>, universal Kriging is performed.</param>
        <param name="kernel">A function to use as a kernel. For instance, <f>m.kernel.rbf</f> (radial basis function) with partially applied <c>gamma</c> is a squared exponential kernel.</param>
        <ret>Returns a scalar or vector prediction with the same type as <pf>to</pf>.</ret>
        <nondeterministic type="unstable" />
        <error code={s"${errcodeBase + 0}"}>If <p>table</p> is empty, a "table must have at least 1 entry" error is raised.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is an empty array, an "x must have at least 1 feature" error is raised.</error>
        <error code={s"${errcodeBase + 2}"}>If any <pf>x</pf> in the <p>table</p> has a different length than the input parameter <p>x</p>, a "table must have the same number of features as x" error is raised.</error>
        <error code={s"${errcodeBase + 3}"}>If any <pf>to</pf> in the <p>table</p> is an empty array, a "table outputs must have at least 1 dimension" error is raised.</error>
        <error code={s"${errcodeBase + 4}"}>If the <pf>to</pf> fields in <p>table</p> do not all have the same dimensions, a "table outputs must all have the same number of dimensions" error is raised.</error>
        <error code={s"${errcodeBase + 5}"}>If <p>x</p> or a component of <p>x</p> is not finite, an "x is not finite" error is raised.</error>
        <error code={s"${errcodeBase + 6}"}>If any value in the <p>table</p> is not finite, a "table value is not finite" error is raised.</error>
        <error code={s"${errcodeBase + 7}"}>If <p>krigingWeight</p> is a number but is not finite, a "krigingWeight is not finite" error is raised.</error>
        <error code={s"${errcodeBase + 8}"}>If evaluating <p>kernel</p> on all combinations of <p>table</p> <pf>x</pf> (with <m>{"""1 + (\mbox{sigma}/\mbox{to})^2"""}</m> on the diagonal) yields a non-positive definite matrix, a "matrix of kernel results is not positive definite" error is raised.</error>
      </doc>
    def errcodeBase = 31080

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode = {
      val record = paramTypes(1).asInstanceOf[AvroArray].items.asInstanceOf[AvroRecord]
      val toType = record.field("to").avroType
      record.fieldOption("sigma") match {
        case Some(x) =>
          if (!toType.accepts(x.avroType)  ||  !x.avroType.accepts(toType))
            throw new PFASemanticException(name + " is being given a table record in which the \"sigma\" field does not have the same type as the \"to\" field: " + x.avroType, None)
        case None =>
      }
      JavaCode("%s.apply%s(%s, %s, %s, %s)",
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        (paramTypes(0), retType) match {
          case (AvroDouble(), AvroDouble()) => "1"
          case (AvroDouble(), AvroArray(AvroDouble())) => "2"
          case (AvroArray(AvroDouble()), AvroDouble()) => "3"
          case (AvroArray(AvroDouble()), AvroArray(AvroDouble())) => "4"
        },
        args(0).toString,
        args(1).toString,
        args(2).toString,
        args(3).toString)
    }

    def wrapKernel(kernel: (PFAArray[Double], PFAArray[Double]) => Double): ((Array[Double], Array[Double]) => Double) =
      {(x: Array[Double], y: Array[Double]) => kernel(PFAArray.fromVector(x.toVector), PFAArray.fromVector(y.toVector))}

    def check0(table: PFAArray[PFARecord]) {
      val n_samples = table.toVector.size
      if (n_samples < 1)
        throw new PFARuntimeException("table must have at least 1 entry", errcodeBase + 0, name, pos)
    }

    def check12(x: PFAArray[Double], table: PFAArray[PFARecord], onlyCheckFirst: Boolean) {
      val n_features = x.toVector.size
      if (n_features < 1)
        throw new PFARuntimeException("x must have at least 1 feature", errcodeBase + 1, name, pos)
      if (onlyCheckFirst  &&  table.toVector.head.get("x").asInstanceOf[PFAArray[Double]].toVector.size != n_features)
        throw new PFARuntimeException("table must have the same number of features as x", errcodeBase + 2, name, pos)
      else if (table.toVector.exists(_.get("x").asInstanceOf[PFAArray[Double]].toVector.size != n_features))
        throw new PFARuntimeException("table must have the same number of features as x", errcodeBase + 2, name, pos)
    }

    def check34(table: PFAArray[PFARecord]): Int = {
      val n_outputs = table.toVector.head.get("to").asInstanceOf[PFAArray[Double]].toVector.size
      if (n_outputs < 1)
        throw new PFARuntimeException("table outputs must have at least 1 dimension", errcodeBase + 3, name, pos)
      if (table.toVector.exists(_.get("to").asInstanceOf[PFAArray[Double]].toVector.size != n_outputs))
        throw new PFARuntimeException("table outputs must all have the same number of dimensions", errcodeBase + 4, name, pos)
      n_outputs
    }

    def check5(x: Double) {
      if (java.lang.Double.isNaN(x)  ||  java.lang.Double.isInfinite(x))
        throw new PFARuntimeException("x is not finite", errcodeBase + 5, name, pos)
    }

    def check5(x: PFAArray[Double]) {
      if (x.toVector.exists(xi => java.lang.Double.isNaN(xi)  ||  java.lang.Double.isInfinite(xi)))
        throw new PFARuntimeException("x is not finite", errcodeBase + 5, name, pos)
    }

    def check6(table: PFAArray[PFARecord], xvector: Boolean, tovector: Boolean) {
      table.toVector foreach {rec =>
        if (xvector) {
          val x = rec.get("x").asInstanceOf[PFAArray[Double]].toVector
          if (x.exists(xi => java.lang.Double.isNaN(xi)  ||  java.lang.Double.isInfinite(xi)))
            throw new PFARuntimeException("table value is not finite", errcodeBase + 6, name, pos)
        }
        else {
          val x = rec.get("x").asInstanceOf[java.lang.Double]
          if (x.isNaN  ||  x.isInfinite)
            throw new PFARuntimeException("table value is not finite", errcodeBase + 6, name, pos)
        }
        if (tovector) {
          val to = rec.get("to").asInstanceOf[PFAArray[Double]].toVector
          if (to.exists(toi => java.lang.Double.isNaN(toi)  ||  java.lang.Double.isInfinite(toi)))
            throw new PFARuntimeException("table value is not finite", errcodeBase + 6, name, pos)
        }
        else {
          val to = rec.get("to").asInstanceOf[java.lang.Double]
          if (to.isNaN  ||  to.isInfinite)
            throw new PFARuntimeException("table value is not finite", errcodeBase + 6, name, pos)
        }
      }
    }

    def check7(krigingWeight: AnyRef) {
      krigingWeight match {
        case null =>
        case beta: java.lang.Double if (beta.isNaN  ||  beta.isInfinite) =>
          throw new PFARuntimeException("krigingWeight is not finite", errcodeBase + 7, name, pos)
        case _ =>
      }
    }

    def nugget(table: PFAArray[PFARecord]) = table.toVector map {t =>
      try {
        val sigma = t.get("sigma").asInstanceOf[java.lang.Double].doubleValue
        val to = t.get("to").asInstanceOf[java.lang.Double].doubleValue
        if (to == 0.0)
          java.lang.Double.POSITIVE_INFINITY
        else
          Math.pow(sigma / to, 2)
      }
      catch {
        case err: org.apache.avro.AvroRuntimeException =>
          2.2204460492503131e-15
      }
    } toArray

    def nugget(table: PFAArray[PFARecord], i: Int) = table.toVector map {t =>
      try {
        val sigma = t.get("sigma").asInstanceOf[PFAArray[java.lang.Double]].toVector.apply(i).doubleValue
        val to = t.get("to").asInstanceOf[PFAArray[java.lang.Double]].toVector.apply(i).doubleValue
        if (to == 0.0)
          java.lang.Double.POSITIVE_INFINITY
        else
          Math.pow(sigma / to, 2)
      }
      catch {
        case err: org.apache.avro.AvroRuntimeException =>
          2.2204460492503131e-15
      }
    } toArray

    def xtrainingScalar(table: PFAArray[PFARecord]): Array[Array[Double]] =
      table.toVector.map(t => Array(t.get("x").asInstanceOf[java.lang.Double].doubleValue)).toArray

    def xtrainingVector(table: PFAArray[PFARecord]): Array[Array[Double]] =
      table.toVector.map(_.get("x").asInstanceOf[PFAArray[Double]].toVector.toArray).toArray

    def ytraining(table: PFAArray[PFARecord]): Array[Double] =
      table.toVector.map(_.get("to").asInstanceOf[java.lang.Double].doubleValue).toArray

    def ytraining(table: PFAArray[PFARecord], i: Int): Array[Double] =
      table.toVector.map(_.get("to").asInstanceOf[PFAArray[Double]].toVector.apply(i)).toArray

    def fit(xtrain: Array[Array[Double]], ytrain: Array[Double], krigingWeight: AnyRef, nugget: Array[Double], kern: (Array[Double], Array[Double]) => Double): GaussianProcess.Fit = {
      val n_samples = xtrain.size
      val n_features = xtrain.head.size

      val xmean = 0 until n_features map {i => xtrain.map(row => row(i)).sum / n_samples} toArray
      val xstd = 0 until n_features map {i => Math.sqrt(xtrain.map(row => Math.pow(row(i), 2)).sum / n_samples - Math.pow(xmean(i), 2))} toArray
      val ymean = ytrain.sum / ytrain.size
      val ystd = Math.sqrt(ytrain.map(Math.pow(_, 2)).sum / ytrain.size - ymean*ymean)

      val xnorm = xtrain.map(row => row.zipWithIndex map {case (x, i) => (x - xmean(i)) / xstd(i)})
      val ynorm = ytrain map {y => (y - ymean) / ystd}

      val R = SimpleMatrix.diag(nugget.map(_ + 1.0): _*)
      for (i <- 0 until n_samples; j <- 0 until n_samples if i != j)
        R.set(i, j, kern(xnorm(i), xnorm(j)))

      val choleskyDecomposition = DecompositionFactory.chol(n_samples, true)
      if (!choleskyDecomposition.decompose(R.getMatrix)) {
        throw new PFARuntimeException("matrix of kernel results is not positive definite", errcodeBase + 8, name, pos)
      }
      val C = R

      val F = new SimpleMatrix(Array.fill(n_samples)(Array(1.0)))
      val linearSolver1 = LinearSolverFactory.linear(n_samples)

      linearSolver1.setA(C.getMatrix)
      val Ft = new SimpleMatrix(n_samples, 1)
      linearSolver1.solve(F.getMatrix, Ft.getMatrix)

      val qrDecomposition = DecompositionFactory.qr(n_samples, 1)

      if (!qrDecomposition.decompose(Ft.getMatrix)) {
        println("Ft")
        println(Ft)
        throw new Exception("FIXME")
      }
      val Q = qrDecomposition.getQ(null, true)
      val G = qrDecomposition.getR(null, true)

      val linearSolver2 = LinearSolverFactory.linear(n_samples)

      linearSolver2.setA(C.getMatrix)
      val Yt = new SimpleMatrix(n_samples, 1)
      linearSolver2.solve(new SimpleMatrix(n_samples, 1, true, ynorm: _*).getMatrix, Yt.getMatrix)

      val beta = krigingWeight match {
        case null =>
          val linearSolver3 = LinearSolverFactory.linear(1)
          linearSolver3.setA(G)
          val betaMatrix = new SimpleMatrix(1, 1)
          linearSolver3.solve(new SimpleMatrix(Q).transpose.mult(Yt).getMatrix, betaMatrix.getMatrix)

          betaMatrix.get(0, 0)

        case x: java.lang.Double => x.doubleValue
      }

      val rho = Yt.minus(Ft.scale(beta))

      val linearSolver4 = LinearSolverFactory.linear(n_samples)
      linearSolver4.setA(C.transpose.getMatrix)
      val gamma = new SimpleMatrix(n_samples, 1)
      linearSolver4.solve(rho.getMatrix, gamma.getMatrix)

      GaussianProcess.Fit(xmean, xstd, xnorm.map(row => PFAArray.fromVector(row.toVector)), ymean, ystd, krigingWeight, beta, (0 until n_samples).map(gamma.get(_, 0)).toArray)
    }

    def predict(x_pred: Array[Double], gpfit: GaussianProcess.Fit, kernel: (PFAArray[Double], PFAArray[Double]) => Double): Double = {
      val x_pred_norm = PFAArray.fromVector(x_pred.zipWithIndex map {case (x, i) => (x - gpfit.xmean(i))/gpfit.xstd(i)} toVector)
      var y_norm = gpfit.beta
      var i = 0
      while (i < gpfit.xnorm.size) {
        val r = kernel(x_pred_norm, gpfit.xnorm(i))
        y_norm += r * gpfit.gamma(i)
        i += 1
      }
      gpfit.ymean + gpfit.ystd * y_norm
    }

    def GPparams = "GPparams"

    def apply1(x: Double, table: PFAArray[PFARecord], krigingWeight: AnyRef, kernel: (PFAArray[Double], PFAArray[Double]) => Double): Double = {
      table.metadata.get(GPparams) match {
        case Some(gpfit: GaussianProcess.Fit) if (gpfit.krigingWeight == krigingWeight) =>
          check5(x)
          predict(Array(x), gpfit, kernel)
        case _ =>
          check0(table)
          check5(x)
          check6(table, false, false)
          check7(krigingWeight)
          val xtrain = xtrainingScalar(table)
          val ytrain = ytraining(table)
          val gpfit = fit(xtrain, ytrain, krigingWeight, nugget(table), wrapKernel(kernel))
          table.metadata = table.metadata.updated(GPparams, gpfit)
          predict(Array(x), gpfit, kernel)
      }
    }

    def apply2(x: Double, table: PFAArray[PFARecord], krigingWeight: AnyRef, kernel: (PFAArray[Double], PFAArray[Double]) => Double): PFAArray[Double] = {
      table.metadata.get(GPparams) match {
        case Some(gpfits: Array[GaussianProcess.Fit]) if (gpfits.head.krigingWeight == krigingWeight) =>
          check5(x)
          PFAArray.fromVector(gpfits map {gpfit =>
            predict(Array(x), gpfit, kernel)
          } toVector)
        case _ =>
          check0(table)
          val n_outputs = check34(table)
          check5(x)
          check6(table, false, true)
          check7(krigingWeight)
          val xtrain = xtrainingScalar(table)
          table.metadata = table.metadata.updated(GPparams, Array.fill[GaussianProcess.Fit](n_outputs)(null))
          PFAArray.fromVector(0 until n_outputs map {i =>
            val ytrain = ytraining(table, i)
            val gpfit = fit(xtrain, ytrain, krigingWeight, nugget(table, i), wrapKernel(kernel))
            table.metadata(GPparams).asInstanceOf[Array[GaussianProcess.Fit]](i) = gpfit
            predict(Array(x), gpfit, kernel)
          } toVector)
      }
    }

    def apply3(x: PFAArray[Double], table: PFAArray[PFARecord], krigingWeight: AnyRef, kernel: (PFAArray[Double], PFAArray[Double]) => Double): Double = {
      table.metadata.get(GPparams) match {
        case Some(gpfit: GaussianProcess.Fit) if (gpfit.krigingWeight == krigingWeight) =>
          check12(x, table, true)
          check5(x)
          predict(x.toVector.toArray, gpfit, kernel)
        case _ =>
          check0(table)
          check12(x, table, false)
          check5(x)
          check6(table, true, false)
          check7(krigingWeight)
          val xtrain = xtrainingVector(table)
          val ytrain = ytraining(table)
          val gpfit = fit(xtrain, ytrain, krigingWeight, nugget(table), wrapKernel(kernel))
          table.metadata = table.metadata.updated(GPparams, gpfit)
          predict(x.toVector.toArray, gpfit, kernel)
      }
    }

    def apply4(x: PFAArray[Double], table: PFAArray[PFARecord], krigingWeight: AnyRef, kernel: (PFAArray[Double], PFAArray[Double]) => Double): PFAArray[Double] = {
      table.metadata.get(GPparams) match {
        case Some(gpfits: Array[GaussianProcess.Fit]) if (gpfits.head.krigingWeight == krigingWeight) =>
          check12(x, table, true)
          check5(x)
          PFAArray.fromVector(gpfits map {gpfit =>
            predict(x.toVector.toArray, gpfit, kernel)
          } toVector)
        case _ =>
          check0(table)
          check12(x, table, false)
          val n_output = check34(table)
          check5(x)
          check6(table, true, true)
          check7(krigingWeight)
          val xtrain = xtrainingVector(table)
          table.metadata = table.metadata.updated(GPparams, Array.fill[GaussianProcess.Fit](n_output)(null))
          PFAArray.fromVector(0 until n_output map {i =>
            val ytrain = ytraining(table, i)
            val gpfit = fit(xtrain, ytrain, krigingWeight, nugget(table, i), wrapKernel(kernel))
            table.metadata(GPparams).asInstanceOf[Array[GaussianProcess.Fit]](i) = gpfit
            predict(x.toVector.toArray, gpfit, kernel)
          } toVector)
      }
    }
  }
  provide(new GaussianProcess)














  //////////////////////////////////////////////////////////////////// quality

  ////   residual (Residual)
  class Residual(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "residual"
    def sig = Sigs(List(Sig(List("observation" -> P.Double, "prediciton" -> P.Double), P.Double, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.residual instead"))),
                        Sig(List("observation" -> P.Array(P.Double), "prediciton" -> P.Array(P.Double)), P.Array(P.Double), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.residual instead"))),
                        Sig(List("observation" -> P.Map(P.Double), "prediciton" -> P.Map(P.Double)), P.Map(P.Double), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.residual instead")))))
    def doc =
      <doc>
        <desc>Compare an observation with its prediction by element-wise subtraction.</desc>
        <param name="observation">Scalar or vector of observations.</param>
        <param name="prediction">Scalar or vector of predictions.</param>
        <ret>Scalar or vector of <p>observation</p> minus <p>prediction</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes as <p>observation</p>.</error>
      </doc>
    def errcodeBase = 31020

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
      if (observationMap.size != predictionMap.size)
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
    def sig = Sigs(List(Sig(List("observation" -> P.Double, "prediciton" -> P.Double, "uncertainty" -> P.Double), P.Double, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.pull instead"))),
                        Sig(List("observation" -> P.Array(P.Double), "prediciton" -> P.Array(P.Double), "uncertainty" -> P.Array(P.Double)), P.Array(P.Double), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.pull instead"))),
                        Sig(List("observation" -> P.Map(P.Double), "prediciton" -> P.Map(P.Double), "uncertainty" -> P.Map(P.Double)), P.Map(P.Double), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.pull instead")))))
    def doc =
      <doc>
        <desc>Compare an observation with its prediction by element-wise subtraction, weighted by element-wise uncertainties.</desc>
        <param name="observation">Scalar or vector of observations.</param>
        <param name="prediction">Scalar or vector of predictions.</param>
        <param name="uncertainty">Scalar or vector of predictions.</param>
        <ret>Scalar or vector of <p>observation</p> minus <p>prediction</p> divided by <p>uncertainty</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes as <p>observation</p>.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "misaligned uncertainty" error if <p>prediction</p> does not have the same indexes as <p>uncertainty</p>.</error>
      </doc>
    def errcodeBase = 31030

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
      if (observationMap.size != predictionMap.size)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 0, name, pos)
      if (observationMap.size != uncertaintyMap.size)
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
    def sig = Sigs(List(Sig(List("observation" -> P.Array(P.Double), "prediction" -> P.Array(P.Double), "covariance" -> P.Array(P.Array(P.Double))), P.Double, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.mahalanobis instead"))),
                        Sig(List("observation" -> P.Map(P.Double), "prediction" -> P.Map(P.Double), "covariance" -> P.Map(P.Map(P.Double))), P.Double, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.mahalanobis instead")))))
    def doc =
      <doc>
        <desc>Compare an observation with its prediction by computing the Mahalanobis distance for a given covariance matrix.</desc>
        <param name="observation">Vector of observations <m>{"""\vec{o}"""}</m>.</param>
        <param name="prediction">Vector of predictions <m>{"""\vec{p}"""}</m>.</param>
        <param name="covariance">Matrix of covariance <m>{"""C"""}</m>.</param>
        <ret>Scalar result of a similarity transformation: <m>{"""\sqrt{(\vec{o} - \vec{p})^T C^{-1} (\vec{o} - \vec{p})}"""}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "too few rows/cols" error if <p>observation</p> has fewer than one element.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "misaligned prediction" error if <p>prediction</p> does not have the same indexes as <p>observation</p>.</error>
        <error code={s"${errcodeBase + 2}"}>Raises a "misaligned covariance" error if <p>covariance</p> does not have the same indexes as <p>observation</p>.</error>
      </doc>
    def errcodeBase = 31040

    def apply(observation: PFAArray[Double], prediction: PFAArray[Double], covariance: PFAArray[PFAArray[Double]]): Double = {
      if (observation.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (observation.size != prediction.size)
        throw new PFARuntimeException("misaligned prediction", errcodeBase + 1, name, pos)
      if (covariance.toVector exists {_.size != observation.size})
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
      if (keys.size != covarianceMap.size)
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
    def sig = Sigs(List(Sig(List("pull" -> P.Double, "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Wildcard("A"), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.updateChi2 instead"))),
                        Sig(List("pull" -> P.Array(P.Double), "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Wildcard("A"), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.updateChi2 instead"))),
                        Sig(List("pull" -> P.Map(P.Double), "state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Wildcard("A"), Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.updateChi2 instead")))))
    def doc =
      <doc>
        <desc>Update the state of a chi-square calculation.</desc>
        <param name="pull">Observation minus prediction divided by uncertainty.  If this is a scalar, it will be squared and added to the chi-square.  If a vector, each component will be squared and added to the chi-square.</param>
        <param name="state">Record of the previous <pf>chi2</pf> and <pf>DOF</pf>.</param>
      </doc>
    def errcodeBase = 31050

    def update(x: Double, state: PFARecord): PFARecord = {
      val chi2 = state.get("chi2").asInstanceOf[java.lang.Double].doubleValue + x
      val DOF = state.get("DOF").asInstanceOf[java.lang.Integer].intValue + 1
      state.multiUpdate(Array("chi2", "DOF"), Array(chi2, DOF))
    }

    def apply(pull: Double, state: PFARecord): PFARecord = update(pull * pull, state)
    def apply(pull: PFAArray[Double], state: PFARecord): PFARecord = update(pull.toVector.map(x => Math.pow(x.doubleValue, 2)).sum, state)
    def apply(pull: PFAMap[java.lang.Double], state: PFARecord): PFARecord = update(pull.toMap.values.map(x => Math.pow(x.doubleValue, 2)).sum, state)
  }
  provide(new UpdateChi2)

  ////   reducedChi2 (ReducedChi2)
  class ReducedChi2(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "reducedChi2"
    def sig = Sig(List("state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Double, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.reducedChi2 instead")))
    def doc =
      <doc>
        <desc>Return the reduced chi-square, which is <pf>chi2</pf>/<pf>DOF</pf>.</desc>
        <param name="state">Record of the <pf>chi2</pf> and <pf>DOF</pf>.</param>
      </doc>
    def errcodeBase = 31060
    def apply(state: PFARecord): Double =
      state.get("chi2").asInstanceOf[java.lang.Double].doubleValue / state.get("DOF").asInstanceOf[java.lang.Number].doubleValue
  }
  provide(new ReducedChi2)

  ////   chi2Prob (Chi2Prob)
  class Chi2Prob(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "chi2Prob"
    def sig = Sig(List("state" -> P.WildRecord("A", ListMap("chi2" -> P.Double, "DOF" -> P.Int))), P.Double, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use stat.test.chi2Prob instead")))
    def doc =
      <doc>
        <desc>Return the chi-square probability, which is the CDF of the chi-square function.</desc>
        <param name="state">Record of the <pf>chi2</pf> and <pf>DOF</pf>.</param>
        <error code={s"${errcodeBase + 0}"}>Raises "invalid parameterization" if <pf>DOF</pf> is less than zero.</error>
      </doc>
    def errcodeBase = 31070
    def apply(state: PFARecord): Double = {
      val chi2 = state.get("chi2").asInstanceOf[java.lang.Double].doubleValue
      val DOF = state.get("DOF").asInstanceOf[java.lang.Number].doubleValue
      if (DOF < 0.0)
        throw new PFARuntimeException("invalid parameterization", errcodeBase + 0, name, pos)
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
  provide(new Chi2Prob)

  //////////////////////////////////////////////////////////////////// fitting

}
