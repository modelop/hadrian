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

package test.scala.lib1.model.neural

import scala.collection.JavaConversions._
import scala.language.postfixOps

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1ModelNeuralSuite extends FlatSpec with Matchers {
  "model.neural.simpleLayered" must "do XOR problem" taggedAs(Lib1, Lib1ModelNeural) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: array
      items:
        type: record
        name: layer
        fields:
          - {name: weights, type: {type: array, items: {type: array, items: double}}}
          - {name: bias,    type: {type: array, items: double}}
    init:
      - {weights: [[ -6.0,  -8.0],
                   [-25.0, -30.0]],
         bias:     [  4.0,  50.0]}
      - {weights: [[-12.0,  30.0]],
         bias:     [-25.0]}
action:
  m.link.logit:
    model.neural.simpleLayers:
      - input
      - cell: model
      - params: [{x: double}]
        ret: double
        do: {m.link.logit: [x]}
""").head
    engine.action(engine.jsonInput("""[0.0, 0.0]""")).asInstanceOf[PFAArray[Double]].toVector(0) should be (0.0 +- 0.1)
    engine.action(engine.jsonInput("""[1.0, 0.0]""")).asInstanceOf[PFAArray[Double]].toVector(0) should be (1.0 +- 0.1)
    engine.action(engine.jsonInput("""[0.0, 1.0]""")).asInstanceOf[PFAArray[Double]].toVector(0) should be (1.0 +- 0.1)
    engine.action(engine.jsonInput("""[1.0, 1.0]""")).asInstanceOf[PFAArray[Double]].toVector(0) should be (0.0 +- 0.1)
  }
}














