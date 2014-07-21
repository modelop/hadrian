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

package test.scala.lib1.model.cluster

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1ModelClusterSuite extends FlatSpec with Matchers {
  "cluster model" must "find closest cluster" taggedAs(Lib1, Lib1ModelCluster) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: string
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: {type: array, items: double}}
          - {name: id, type: string}
    init:
      - {id: one, center: [1, 1, 1, 1, 1]}
      - {id: two, center: [2, 2, 2, 2, 2]}
      - {id: three, center: [3, 3, 3, 3, 3]}
      - {id: four, center: [4, 4, 4, 4, 4]}
      - {id: five, center: [5, 5, 5, 5, 5]}
action:
  attr:
    model.cluster.closest:
      - input
      - cell: clusters
      - params:
          - x: {type: array, items: double}
          - y: {type: array, items: double}
        ret: double
        do:
          metric.euclidean:
            - fcnref: metric.absDiff
            - x
            - y
  path: [[id]]
""").head

    engine.action(PFAArray.fromVector(Vector(1.2, 1.2, 1.2, 1.2, 1.2))) should be ("one")
    engine.action(PFAArray.fromVector(Vector(1.8, 1.8, 1.8, 1.8, 1.8))) should be ("two")
    engine.action(PFAArray.fromVector(Vector(2.2, 2.2, 2.2, 2.2, 2.2))) should be ("two")
    engine.action(PFAArray.fromVector(Vector(5.0, 5.0, 5.0, 5.0, 5.0))) should be ("five")
    engine.action(PFAArray.fromVector(Vector(-1000.0, -1000.0, -1000.0, -1000.0, -1000.0))) should be ("one")
  }

  it must "find the closest N clusters" taggedAs(Lib1, Lib1ModelCluster) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: string}
cells:
  clusters:
    type:
      type: array
      items:
        type: record
        name: Cluster
        fields:
          - {name: center, type: {type: array, items: double}}
          - {name: id, type: string}
    init:
      - {id: one, center: [1, 1, 1, 1, 1]}
      - {id: two, center: [2, 2, 2, 2, 2]}
      - {id: three, center: [3, 3, 3, 3, 3]}
      - {id: four, center: [4, 4, 4, 4, 4]}
      - {id: five, center: [5, 5, 5, 5, 5]}
action:
  a.map:
    - model.cluster.closestN:
        - input
        - cell: clusters
        - params:
            - x: {type: array, items: double}
            - y: {type: array, items: double}
          ret: double
          do:
            metric.euclidean:
              - fcnref: metric.absDiff
              - x
              - y
        - 3
    - params: [{cluster: Cluster}]
      ret: string
      do: cluster.id
""").head

    engine.action(PFAArray.fromVector(Vector(1.2, 1.2, 1.2, 1.2, 1.2))).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three"))
    engine.action(PFAArray.fromVector(Vector(1.8, 1.8, 1.8, 1.8, 1.8))).asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "one", "three"))
    engine.action(PFAArray.fromVector(Vector(2.2, 2.2, 2.2, 2.2, 2.2))).asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "three", "one"))
    engine.action(PFAArray.fromVector(Vector(5.0, 5.0, 5.0, 5.0, 5.0))).asInstanceOf[PFAArray[String]].toVector should be (Vector("five", "four", "three"))
    engine.action(PFAArray.fromVector(Vector(-1000.0, -1000.0, -1000.0, -1000.0, -1000.0))).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three"))
  }
}
