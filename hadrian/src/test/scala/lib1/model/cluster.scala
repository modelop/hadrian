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
            - fcn: metric.absDiff
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
              - fcn: metric.absDiff
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

  it must "create some random seeds" taggedAs(Lib1, Lib1ModelCluster) in {
    val engine = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items:
    type: record
    name: Cluster
    fields:
      - {name: id, type: int}
      - {name: center, type: {type: array, items: double}}
      - {name: onemore, type: string}
cells:
  dataset:
    type: {type: array, items: {type: array, items: double}}
    init:
      - [1.1, 1.2, 1.3]
      - [2.1, 2.2, 2.3]
      - [3.1, 3.2, 3.3]
      - [4.1, 4.2, 4.3]
      - [5.1, 5.2, 5.3]
      - [5.1, 5.2, 5.3]
      - [5.1, 5.2, 5.3]
      - [1.1, 1.2, 1.3]
      - [1.1, 1.2, 1.3]
action:
  model.cluster.randomSeeds:
    - {cell: dataset}
    - input
    - params: [{i: int}, {vec: {type: array, items: double}}]
      ret: Cluster
      do:
        new: {id: i, center: vec, onemore: {string: hello}}
        type: Cluster
""").head

    val result = engine.action(java.lang.Integer.valueOf(5)).asInstanceOf[PFAArray[PFARecord]].toVector

    (result map {_.get("center")} toSet) should be (Set(
      PFAArray.fromVector(Vector(1.1, 1.2, 1.3)),
      PFAArray.fromVector(Vector(2.1, 2.2, 2.3)),
      PFAArray.fromVector(Vector(3.1, 3.2, 3.3)),
      PFAArray.fromVector(Vector(4.1, 4.2, 4.3)),
      PFAArray.fromVector(Vector(5.1, 5.2, 5.3))))

    (result map {_.get("id")} toSet) should be (Set(0, 1, 2, 3, 4))

    (result map {_.get("onemore")} toSet) should be (Set("hello"))

    evaluating { engine.action(java.lang.Integer.valueOf(6)) } should produce [PFARuntimeException]
  }

  it must "perform standard k-means" taggedAs(Lib1, Lib1ModelCluster) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output:
  type: array
  items:
    type: record
    name: Cluster
    fields:
      - {name: id, type: int}
      - {name: center, type: {type: array, items: double}}
cells:
  counter:
    type: int
    init: 0
  dataset:
    type: {type: array, items: {type: array, items: double}}
    init: []
method: emit
action:
  if:
    "<":
      - cell: counter
        to:
          params: [{x: int}]
          ret: int
          do: {"+": [x, 1]}
      - 81
  then:
    cell: dataset
    to:
      params: [{x: {type: array, items: {type: array, items: double}}}]
      ret: {type: array, items: {type: array, items: double}}
      do:
        a.append: [x, input]
  else:
    - let:
        clusters:
          value: [{id: 0, center: [1.1, 2.1, 3.1]}, {id: 0, center: [3.1, 1.1, 2.1]}, {id: 0, center: [2.1, 3.1, 1.1]}]
          type: {type: array, items: Cluster}
    - for: {i: 0}
      while: {"<": [i, 10]}
      step: {i: {+: [i, 1]}}
      do:
        set:
          clusters:
            model.cluster.kmeansIteration:
              - {cell: dataset}
              - clusters
              - {fcn: metric.simpleEuclidean}
              - params: [{data: {type: array, items: {type: array, items: double}}}, {cluster: Cluster}]
                ret: Cluster
                do: {model.cluster.updateMean: [data, cluster, 0.0]}
    - emit: clusters
""").head

    engine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef =>
      val results = x.asInstanceOf[PFAArray[PFARecord]].toVector map {_.get("center").asInstanceOf[PFAArray[Double]].toVector}

      results(0)(0) should be (1.0 +- 0.01)
      results(0)(1) should be (2.0 +- 0.01)
      results(0)(2) should be (3.0 +- 0.01)

      results(1)(0) should be (3.0 +- 0.01)
      results(1)(1) should be (1.0 +- 0.01)
      results(1)(2) should be (2.0 +- 0.01)

      results(2)(0) should be (2.0 +- 0.01)
      results(2)(1) should be (3.0 +- 0.01)
      results(2)(2) should be (1.0 +- 0.01)
    }

    engine.action(PFAArray.fromVector(Vector(1.1, 2.1, 3.1)))
    engine.action(PFAArray.fromVector(Vector(1.1, 2.1, 3.0)))
    engine.action(PFAArray.fromVector(Vector(1.1, 2.1, 2.9)))
    engine.action(PFAArray.fromVector(Vector(1.1, 2.0, 3.1)))
    engine.action(PFAArray.fromVector(Vector(1.1, 2.0, 3.0)))
    engine.action(PFAArray.fromVector(Vector(1.1, 2.0, 2.9)))
    engine.action(PFAArray.fromVector(Vector(1.1, 1.9, 3.1)))
    engine.action(PFAArray.fromVector(Vector(1.1, 1.9, 3.0)))
    engine.action(PFAArray.fromVector(Vector(1.1, 1.9, 2.9)))
    engine.action(PFAArray.fromVector(Vector(1.0, 2.1, 3.1)))
    engine.action(PFAArray.fromVector(Vector(1.0, 2.1, 3.0)))
    engine.action(PFAArray.fromVector(Vector(1.0, 2.1, 2.9)))
    engine.action(PFAArray.fromVector(Vector(1.0, 2.0, 3.1)))
    engine.action(PFAArray.fromVector(Vector(1.0, 2.0, 3.0)))
    engine.action(PFAArray.fromVector(Vector(1.0, 2.0, 2.9)))
    engine.action(PFAArray.fromVector(Vector(1.0, 1.9, 3.1)))
    engine.action(PFAArray.fromVector(Vector(1.0, 1.9, 3.0)))
    engine.action(PFAArray.fromVector(Vector(1.0, 1.9, 2.9)))
    engine.action(PFAArray.fromVector(Vector(0.9, 2.1, 3.1)))
    engine.action(PFAArray.fromVector(Vector(0.9, 2.1, 3.0)))
    engine.action(PFAArray.fromVector(Vector(0.9, 2.1, 2.9)))
    engine.action(PFAArray.fromVector(Vector(0.9, 2.0, 3.1)))
    engine.action(PFAArray.fromVector(Vector(0.9, 2.0, 3.0)))
    engine.action(PFAArray.fromVector(Vector(0.9, 2.0, 2.9)))
    engine.action(PFAArray.fromVector(Vector(0.9, 1.9, 3.1)))
    engine.action(PFAArray.fromVector(Vector(0.9, 1.9, 3.0)))
    engine.action(PFAArray.fromVector(Vector(0.9, 1.9, 2.9)))

    engine.action(PFAArray.fromVector(Vector(3.1, 1.1, 2.1)))
    engine.action(PFAArray.fromVector(Vector(3.0, 1.1, 2.1)))
    engine.action(PFAArray.fromVector(Vector(2.9, 1.1, 2.1)))
    engine.action(PFAArray.fromVector(Vector(3.1, 1.1, 2.0)))
    engine.action(PFAArray.fromVector(Vector(3.0, 1.1, 2.0)))
    engine.action(PFAArray.fromVector(Vector(2.9, 1.1, 2.0)))
    engine.action(PFAArray.fromVector(Vector(3.1, 1.1, 1.9)))
    engine.action(PFAArray.fromVector(Vector(3.0, 1.1, 1.9)))
    engine.action(PFAArray.fromVector(Vector(2.9, 1.1, 1.9)))
    engine.action(PFAArray.fromVector(Vector(3.1, 1.0, 2.1)))
    engine.action(PFAArray.fromVector(Vector(3.0, 1.0, 2.1)))
    engine.action(PFAArray.fromVector(Vector(2.9, 1.0, 2.1)))
    engine.action(PFAArray.fromVector(Vector(3.1, 1.0, 2.0)))
    engine.action(PFAArray.fromVector(Vector(3.0, 1.0, 2.0)))
    engine.action(PFAArray.fromVector(Vector(2.9, 1.0, 2.0)))
    engine.action(PFAArray.fromVector(Vector(3.1, 1.0, 1.9)))
    engine.action(PFAArray.fromVector(Vector(3.0, 1.0, 1.9)))
    engine.action(PFAArray.fromVector(Vector(2.9, 1.0, 1.9)))
    engine.action(PFAArray.fromVector(Vector(3.1, 0.9, 2.1)))
    engine.action(PFAArray.fromVector(Vector(3.0, 0.9, 2.1)))
    engine.action(PFAArray.fromVector(Vector(2.9, 0.9, 2.1)))
    engine.action(PFAArray.fromVector(Vector(3.1, 0.9, 2.0)))
    engine.action(PFAArray.fromVector(Vector(3.0, 0.9, 2.0)))
    engine.action(PFAArray.fromVector(Vector(2.9, 0.9, 2.0)))
    engine.action(PFAArray.fromVector(Vector(3.1, 0.9, 1.9)))
    engine.action(PFAArray.fromVector(Vector(3.0, 0.9, 1.9)))
    engine.action(PFAArray.fromVector(Vector(2.9, 0.9, 1.9)))

    engine.action(PFAArray.fromVector(Vector(2.1, 3.1, 1.1)))
    engine.action(PFAArray.fromVector(Vector(2.1, 3.0, 1.1)))
    engine.action(PFAArray.fromVector(Vector(2.1, 2.9, 1.1)))
    engine.action(PFAArray.fromVector(Vector(2.0, 3.1, 1.1)))
    engine.action(PFAArray.fromVector(Vector(2.0, 3.0, 1.1)))
    engine.action(PFAArray.fromVector(Vector(2.0, 2.9, 1.1)))
    engine.action(PFAArray.fromVector(Vector(1.9, 3.1, 1.1)))
    engine.action(PFAArray.fromVector(Vector(1.9, 3.0, 1.1)))
    engine.action(PFAArray.fromVector(Vector(1.9, 2.9, 1.1)))
    engine.action(PFAArray.fromVector(Vector(2.1, 3.1, 1.0)))
    engine.action(PFAArray.fromVector(Vector(2.1, 3.0, 1.0)))
    engine.action(PFAArray.fromVector(Vector(2.1, 2.9, 1.0)))
    engine.action(PFAArray.fromVector(Vector(2.0, 3.1, 1.0)))
    engine.action(PFAArray.fromVector(Vector(2.0, 3.0, 1.0)))
    engine.action(PFAArray.fromVector(Vector(2.0, 2.9, 1.0)))
    engine.action(PFAArray.fromVector(Vector(1.9, 3.1, 1.0)))
    engine.action(PFAArray.fromVector(Vector(1.9, 3.0, 1.0)))
    engine.action(PFAArray.fromVector(Vector(1.9, 2.9, 1.0)))
    engine.action(PFAArray.fromVector(Vector(2.1, 3.1, 0.9)))
    engine.action(PFAArray.fromVector(Vector(2.1, 3.0, 0.9)))
    engine.action(PFAArray.fromVector(Vector(2.1, 2.9, 0.9)))
    engine.action(PFAArray.fromVector(Vector(2.0, 3.1, 0.9)))
    engine.action(PFAArray.fromVector(Vector(2.0, 3.0, 0.9)))
    engine.action(PFAArray.fromVector(Vector(2.0, 2.9, 0.9)))
    engine.action(PFAArray.fromVector(Vector(1.9, 3.1, 0.9)))
    engine.action(PFAArray.fromVector(Vector(1.9, 3.0, 0.9)))
    engine.action(PFAArray.fromVector(Vector(1.9, 2.9, 0.9)))

  }

}
