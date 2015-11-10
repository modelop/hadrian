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

package object neural {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.neural."
  //////////////////////////////////////////////////////////////////
  
  ////  simpleLayers (SimpleLayers)
  class SimpleLayers(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "simpleLayers"
    def sig = Sig(List(
      "datum" -> P.Array(P.Double),
      "model" -> P.Array(P.WildRecord("M", ListMap(
        "weights" -> P.Array(P.Array(P.Double)),
        "bias" ->    P.Array(P.Double)
      ))),
      "activation" -> P.Fcn(List(P.Double), P.Double)), P.Array(P.Double))
    def doc =
      <doc>
        <desc>Apply a feedforward artificial neural network <p>model</p> to an input <p>datum</p>.</desc>
        <param name="datum">Length <p>d</p> vector of independent variables.</param>
        <param name="model">Array containing the parameters of each layer of the feedforward neural network model.</param>
          <paramField name="weights">Matrix of weights.  Each of <p>i</p> rows is a node (or neuron), and in each of <p>j</p> columns are the weights for that node.  In the zeroth layer, <p>i</p> must equal <p>d</p>.</paramField>
          <paramField name="bias">Length <p>j</p> vector of biases that are added to each node output.</paramField>
        <param name="activation">Function applied at the output of each node, except the last.  Usually an "S"-shaped sigmoid or hyperbolic tangent.</param>
        <ret>Returns an array of network outputs.  For a neural network with a single neuron in the last layer (single output), this is an array of length one.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "no layers" error if the length of model is zero.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "weights, bias, or datum misaligned" error if there is any misalignment between inputs and outputs through the layers of the network.</error>
      </doc>
    def errcodeBase = 11000

    def matrix_vector_mult(a: Vector[PFAArray[Double]], b: Vector[Double]): Vector[Double] =
      for (row <- a) yield row.toVector zip b map Function.tupled(_ * _) sum

    def apply(datum: PFAArray[Double], model: PFAArray[PFARecord], activation: (Double => Double)): PFAArray[Double] = {
      val vector = model.toVector
      val length = vector.size
      if (length == 0)
        throw new PFARuntimeException("no layers", errcodeBase + 0, name, pos)
      var x = datum.toVector
      var i = 0
      // pass datum through first N - 1 layers, apply activation function
      while (i < (length - 1)) {
        val thisLayer = vector(i)
        val bias    = thisLayer("bias").asInstanceOf[PFAArray[Double]].toVector
        val weights = thisLayer("weights").asInstanceOf[PFAArray[PFAArray[Double]]].toVector
        if (bias.size != weights.size || weights.exists(_.toVector.size != datum.size))
          throw new PFARuntimeException("weights, bias, or datum misaligned", errcodeBase + 1, name, pos)
        x = (matrix_vector_mult(weights, x), bias).zipped.map(_+_).map(activation(_))
        i += 1
      }
      // pass through final layer, don't apply activation function
      val thisLayer = vector(i)
      val bias    = thisLayer("bias").asInstanceOf[PFAArray[Double]].toVector
      val weights = thisLayer("weights").asInstanceOf[PFAArray[PFAArray[Double]]].toVector
      if (bias.size != weights.size || weights.exists(_.toVector.size != x.size))
        throw new PFARuntimeException("weights, bias, or datum misaligned", errcodeBase + 1, name, pos)
      PFAArray.fromVector((matrix_vector_mult(weights, x), bias).zipped.map(_+_))
    }
  }
  provide(new SimpleLayers)
}
