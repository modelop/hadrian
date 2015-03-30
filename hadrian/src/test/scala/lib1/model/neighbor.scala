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

package test.scala.lib1.model.neighbor

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
class Lib1ModelNeighborSuite extends FlatSpec with Matchers {
  "nearest neighbor model" must "find k nearest neighbors" taggedAs(Lib1, Lib1ModelNeighbor) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: {type: array, items: double}}
cells:
  codebook:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.nearestK:
    - 2
    - input
    - cell: codebook
""").head

    engine.action(PFAArray.fromVector(Vector(1.2, 1.2, 1.2, 1.2, 1.2))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector.toSet map {x: PFAArray[Double] => x.toVector} should be (Set(Vector(1.0, 1.0, 1.0, 1.0, 1.0), Vector(2.0, 2.0, 2.0, 2.0, 2.0)))
    engine.action(PFAArray.fromVector(Vector(4.1, 4.1, 4.1, 4.1, 4.1))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector.toSet map {x: PFAArray[Double] => x.toVector} should be (Set(Vector(4.0, 4.0, 4.0, 4.0, 4.0), Vector(5.0, 5.0, 5.0, 5.0, 5.0)))
  }

  it must "find all neighbors within a ball" taggedAs(Lib1, Lib1ModelNeighbor) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: {type: array, items: double}}
cells:
  codebook:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.ballR:
    - m.sqrt: 5
    - input
    - cell: codebook
""").head

    engine.action(PFAArray.fromVector(Vector(1.2, 1.2, 1.2, 1.2, 1.2))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector.toSet map {x: PFAArray[Double] => x.toVector} should be (Set(Vector(1.0, 1.0, 1.0, 1.0, 1.0), Vector(2.0, 2.0, 2.0, 2.0, 2.0)))
    engine.action(PFAArray.fromVector(Vector(4.1, 4.1, 4.1, 4.1, 4.1))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector.toSet map {x: PFAArray[Double] => x.toVector} should be (Set(Vector(4.0, 4.0, 4.0, 4.0, 4.0), Vector(5.0, 5.0, 5.0, 5.0, 5.0)))
  }

  it must "take the average of some points" taggedAs(Lib1, Lib1ModelNeighbor) in {
    val engine1 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  points:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.mean:
    - cell: points
""").head
    engine1.action(PFAArray.fromVector(Vector(1.2, 1.2, 1.2, 1.2, 1.2))).asInstanceOf[PFAArray[Double]].toVector should be (Vector(3.0, 3.0, 3.0, 3.0, 3.0))

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  points:
    type:
      type: array
      items:
        type: array
        items: double
    init:
      - [1, 1, 1, 1, 1]
      - [2, 2, 2, 2, 2]
      - [3, 3, 3, 3, 3]
      - [4, 4, 4, 4, 4]
      - [5, 5, 5, 5, 5]
action:
  model.neighbor.mean:
    - cell: points
    - params: [{point: {type: array, items: double}}]
      ret: double
      do: {m.exp: {u-: {metric.simpleEuclidean: [input, point]}}}
""").head
    engine2.action(PFAArray.fromVector(Vector(1.2, 1.2, 1.2, 1.2, 1.2))).asInstanceOf[PFAArray[Double]].toVector foreach {_ should be (1.253377 +- 0.001)}
  }

}
