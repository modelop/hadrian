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

package com.opendatagroup.hadrian.lib.model

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

package object naive {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.naive."

  ////   Gaussian NB
  class Gaussian(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "gaussian"
    def sig = Sigs(List(Sig(List("datum" -> P.Array(P.Double), "classModel" -> P.Array(P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double)))), P.Double),
                        Sig(List("datum" -> P.Map(P.Double),   "classModel" ->   P.Map(P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double)))), P.Double)))
    def doc =
      <doc>
        <desc>Score <p>datum</p> using a Gaussian Naive Bayes model.</desc>
        <param name="datum"> Vector of independent variables with <m>d</m> dimensions. </param>
        <param name="classModel"> Array or map of <m>d</m> records, each containing the <p>mean</p> and <p>variance</p> of each of independent variable, for one class. </param>
        <ret>Returns the unscaled log-likelihood that <p>datum</p> is a member of the class specified by <p>classModel</p>.</ret>
        <detail><p>datum</p> or <p>classModel</p> may be expressed as arrays (indexed by integers), or maps (indexed by strings).</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "datum and classModel misaligned" error if <p>datum</p> and <p>classModel</p> have different lengths, of if their keys if using the map signature don't match one to one.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "variance less than or equal to zero" error if a variance inside of <p>classModel</p> is incorrectly specified.</error>
      </doc>
    def errcodeBase = 10000

    def apply(datum: PFAArray[Double], classModel: PFAArray[PFARecord]): Double = {
      val datumVector = datum.toVector
      val classVector = classModel.toVector
      if (datumVector.size != classVector.size)
        throw new PFARuntimeException("datum and classModel misaligned", errcodeBase + 0, name, pos)
      var ll = 0.0
      var variance = 0.0
      var mean = 0.0
      for ( (x, rec) <- datumVector zip classVector ) {
        variance = rec.get("variance").asInstanceOf[java.lang.Number].doubleValue
        mean = rec.get("mean").asInstanceOf[java.lang.Number].doubleValue
        if (variance <= 0.0)
          throw new PFARuntimeException("variance less than or equal to zero", errcodeBase + 1, name, pos)
        ll -= 0.5*Math.log(2.0*Math.PI*variance) + 0.5*Math.pow(x - mean, 2) / variance
      }
      ll
    }

    def apply(datum: PFAMap[java.lang.Double],   classModel: PFAMap[PFARecord]): Double = {
      val datumMap = datum.asInstanceOf[PFAMap[java.lang.Double]].toMap
      val classMap = classModel.asInstanceOf[PFAMap[PFARecord]].toMap
      if (datumMap.size != classMap.size)
        throw new PFARuntimeException("datum and classModel misaligned", errcodeBase + 0, name, pos)
      val dset = datumMap.keySet
      var ll = 0.0
      var variance = 0.0
      var mean = 0.0
      try {
        dset foreach {i =>
          variance = classMap(i).get("variance").asInstanceOf[java.lang.Number].doubleValue
          mean = classMap(i).get("mean").asInstanceOf[java.lang.Number].doubleValue
          if (variance <= 0.0)
            throw new PFARuntimeException("variance less than or equal to zero", errcodeBase + 1, name, pos)
          ll -= 0.5*Math.log(2.0*Math.PI*variance) + 0.5*Math.pow(datumMap(i) - mean, 2) / variance
        }
      }
      catch {
        case err: java.util.NoSuchElementException => throw new PFARuntimeException("datum and classModel misaligned", errcodeBase + 0, name, pos)
      }
      ll
    }

  }
  provide(new Gaussian)

  ////   Multinomial NB
  class Multinomial(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "multinomial"
    def sig = Sigs(List(Sig(List("datum" -> P.Array(P.Double),  "classModel" -> P.Array(P.Double)), P.Double),
                        Sig(List("datum" -> P.Map(P.Double),    "classModel" -> P.Map(P.Double)), P.Double),
                        Sig(List("datum" -> P.Array(P.Double),  "classModel" -> P.WildRecord("C", ListMap("values" -> P.Array(P.Double)))), P.Double),
                        Sig(List("datum" -> P.Map(P.Double),    "classModel" -> P.WildRecord("C", ListMap("values" -> P.Map(P.Double)))), P.Double)))
    def doc =
      <doc>
        <desc>Score <p>datum</p> using a Multinomial Naive Bayes model.</desc>
        <param name="datum">Vector of independent variables with <m>d</m> dimensions. </param>
        <param name="classModel">Array or map of multinomial (<m>d</m> different) likelihoods of each independent variable for this class. The record form is for histograms built by <f>stat.sample.fillHistogram</f> or <f>stat.sample.fillCounter</f>.</param>
        <ret>Returns the unscaled log-likelihood of <p>datum</p> for this class.</ret>
        <detail><p>datum</p> or <p>classModel</p> may be expressed as arrays (indexed by integers), or maps (indexed by strings).</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "datum and classModel misaligned" error if when using the map signature the keys of <p>datum</p> and <p>classModel</p> don't match one to one, of if when using the array signature they are different lengths.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "classModel must be non-empty and strictly positive" error if classModel is empty or any items are less than or equal to zero.</error>
      </doc>
    def errcodeBase = 10010

    def apply(datum: PFAArray[Double], classModel: PFARecord): Double =
      apply(datum, classModel.get("values").asInstanceOf[PFAArray[Double]])

    def apply(datum: PFAMap[java.lang.Double], classModel: PFARecord): Double =
      apply(datum, classModel.get("values").asInstanceOf[PFAMap[java.lang.Double]])

    def apply(datum: PFAArray[Double], classModel: PFAArray[Double]): Double = {
      val datumVector = datum.toVector
      val classVector = classModel.toVector
      val normalizing = classVector.sum
      if (classVector.isEmpty  ||  !classVector.forall(_ > 0.0))
        throw new PFARuntimeException("classModel must be non-empty and strictly positive", errcodeBase + 1, name, pos)
      var p = 0.0
      var lp = 0.0
      var ll = 0.0
      if (datumVector.size != classVector.size)
        throw new PFARuntimeException("datum and classModel misaligned", errcodeBase + 0, name, pos)

      (datumVector, classVector).zipped foreach {(d, c) =>
        p = c/normalizing + Double.MinPositiveValue
        ll += d*Math.log(p)
      }
      ll
    }

    def apply(datum: PFAMap[java.lang.Double],   classModel: PFAMap[java.lang.Double]): Double = {
      val datumMap = datum.asInstanceOf[PFAMap[java.lang.Double]].toMap
      val classMap = classModel.asInstanceOf[PFAMap[java.lang.Double]].toMap
      val normalizing = classMap.values.map(_.doubleValue).sum
      if (classMap.isEmpty  ||  !classMap.values.forall(_ > 0.0))
        throw new PFARuntimeException("classModel must be non-empty and strictly positive", errcodeBase + 1, name, pos)
      var p  = 0.0
      var ll = 0.0
      if (datumMap.size != classMap.size)
        throw new PFARuntimeException("datum and classModel misaligned", errcodeBase + 0, name, pos)
      val dset = datumMap.keySet
      try {
        dset foreach {i =>
          p = classMap(i)/normalizing + Double.MinPositiveValue
          ll += datumMap(i)*Math.log(p)
        }
      }
      catch {
        case err: java.util.NoSuchElementException => throw new PFARuntimeException("datum and classModel misaligned", errcodeBase + 0, name, pos)
      }
      ll
    }
  }
  provide(new Multinomial)

  class Bernoulli(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "bernoulli"
    def sig = Sigs(List(Sig(List("datum" -> P.Array(P.String), "classModel" -> P.Map(P.Double)), P.Double),
                        Sig(List("datum" -> P.Array(P.String), "classModel" -> P.WildRecord("C", ListMap("values" -> P.Map(P.Double)))), P.Double)))
    def doc =
      <doc>
        <desc>Score <p>datum</p> using a Bernoulli Naive Bayes model.</desc>
        <param name="datum">Vector of independent variables with <m>d</m> dimensions. The record form is for histograms built by <f>stat.sample.fillCounter</f>.</param>
        <param name="classModel">Array or map of <m>d</m> likelihoods of the presence of each independent variable for this class. </param>
        <ret>Returns the unscaled log-likelihood of <p>datum</p> for this class.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "probability in classModel must be strictly between 0 and 1" error if a value in <p>classModel</p> is not strictly between zero and one.</error>
      </doc>
    def errcodeBase = 10020

    def apply(datum: PFAArray[String], classModel: PFARecord): Double =
      apply(datum, classModel.get("values").asInstanceOf[PFAMap[java.lang.Double]])

    def apply(datum: PFAArray[String], classModel: PFAMap[java.lang.Double]): Double = {
      val dset = datum.asInstanceOf[PFAArray[String]].toVector.distinct.toSet
      val classMap = classModel.asInstanceOf[PFAMap[java.lang.Double]].toMap
      val cset = classMap.keySet
      var ll = 0.0
      cset foreach {i =>
        if ((classMap(i) <= 0.0) || (classMap(i) >= 1.0))
          throw new PFARuntimeException("probability in classModel must be strictly between 0 and 1", errcodeBase + 0, name, pos)
        ll += Math.log(1.0 - classMap(i))
      }
      dset foreach {i =>
        classMap.get(i) match {
          case Some(value) => ll += Math.log(value) - Math.log(1.0 - value)
          case None => // continue
        }
      }
      ll
    }
  }
  provide(new Bernoulli)
}
