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


package object kernel {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m.kernel."

  //////////////////////////////////////////////////////////////////
  def dot(a: Vector[Double], b: Vector[Double], code: Int, fcnName: String, pos: Option[String]): Double = {
    if (a.size != b.size)
      throw new PFARuntimeException("arrays must have same length", code, fcnName, pos)
    (for ((i,j) <- a zip b) yield i * j) sum 
  }
  //////////////////////////////////////////////////////////////////
  ////  linear (Linear)
  class Linear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "linear"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Double) 
    def doc =
      <doc>
        <desc>Linear kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <ret>Returns the dot product of <p>x</p> and <p>y</p>, <m>{"\\sum_{i=1}^{n} x_{i} y_{j}"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23000
    
    def apply(x: PFAArray[Double], y: PFAArray[Double]): Double = {
      dot(x.toVector, y.toVector, errcodeBase + 0, name, pos)
    }
  }
  provide(new Linear)

  ////  rbf (RBF)
  class RBF(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "rbf"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double), "gamma" -> P.Double), P.Double) 
    def doc =
      <doc>
        <desc>Radial Basis Function (RBF) kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <param name="gamma">Gamma coefficient.</param>
        <ret>Returns the result of <m>{"\\mathrm{exp}(-\\gamma || x - y ||^{2})"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23010
    
    def apply(x: PFAArray[Double], y: PFAArray[Double], gamma: Double): Double = {
      if (x.size != y.size)
        throw new PFARuntimeException("arrays must have same length", errcodeBase + 0, name, pos)
      var out = 0.0
      for ((i,j) <- (x.toVector zip y.toVector)) 
        out += Math.pow(Math.abs(i - j), 2)
      Math.exp(-gamma*out)
    }
  }
  provide(new RBF)

  ////  poly (Poly)
  class Poly(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "poly"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double), "gamma" -> P.Double, "intercept" -> P.Double, "degree" -> P.Double), P.Double) 
    def doc =
      <doc>
        <desc>Polynomial kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <param name="gamma">Gamma coefficient.</param>
        <param name="intecept">Intercept constant.</param>
        <param name="degree">Degree of the polynomial kernel.</param>
        <ret>Returns the result of <m>{"(\\gamma \\sum_{i=1}^{n} x_{i} y_{j} + \\mathrm{intercept})^{\\mathrm{degree}}"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23020
    
    def apply(x: PFAArray[Double], y: PFAArray[Double], gamma: Double, intercept: Double, degree: Double): Double = {
      Math.pow(gamma*dot(x.toVector, y.toVector, errcodeBase + 0, name, pos) + intercept, degree) 
    }
  }
  provide(new Poly)
  
  ////  sigmoid (Sigmoid)
  class Sigmoid(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "sigmoid"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double), "gamma" -> P.Double, "intercept" -> P.Double), P.Double) 
    def doc =
      <doc>
        <desc>Sigmoid kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <param name="gamma">Gamma coefficient.</param>
        <param name="intecept">Intercept constant.</param>
        <ret>Returns the result of <m>{"\\mathrm{tanh}( \\mathrm{gamma} \\sum_{i=1}^{n} x_{i} y_{j} + \\mathrm{intercept})"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23030
    
    def apply(x: PFAArray[Double], y: PFAArray[Double], gamma: Double, intercept: Double): Double = {
      Math.tanh(gamma * dot(x.toVector, y.toVector, errcodeBase + 0, name, pos) + intercept) 
    }
  }
  provide(new Sigmoid)
}
