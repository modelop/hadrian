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

package test.scala.lib1.metric

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1MetricSuite extends FlatSpec with Matchers {
  "simpleEuclidean" must "work" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.simpleEuclidean:
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (3.74 +- 0.01)
  }

  "euclidean" must "work" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (3.74 +- 0.01)
  }

  "euclidean" must "work with integers" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: int}
      - value: [0, 0, 0]
        type: {type: array, items: int}
""").head.action(null).asInstanceOf[Double] should be (3.74 +- 0.01)
  }

  "euclidean" must "work with integers 2" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - params: [{x: int}, {y: int}]
        ret: double
        do: {m.abs: {"-": [x, y]}}
      - value: [1, 2, 3]
        type: {type: array, items: int}
      - value: [0, 0, 0]
        type: {type: array, items: int}
""").head.action(null).asInstanceOf[Double] should be (3.74 +- 0.01)
  }

  it must "work with missing values" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcn: metric.absDiff
      - value: [null, {double: 2}, {double: 3}]
        type: {type: array, items: ["null", double]}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (4.42 +- 0.01)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [null, {double: 0}, {double: 0}]
        type: {type: array, items: ["null", double]}
""").head.action(null).asInstanceOf[Double] should be (4.42 +- 0.01)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcn: metric.absDiff
      - value: [null, {double: 2}, {double: 3}]
        type: {type: array, items: ["null", double]}
      - value: [null, {double: 0}, {double: 0}]
        type: {type: array, items: ["null", double]}
""").head.action(null).asInstanceOf[Double] should be (4.42 +- 0.01)
  }

  it must "work with missing value weights" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - fcn: metric.absDiff
      - value: [null, {double: 2}, {double: 3}]
        type: {type: array, items: ["null", double]}
      - value: [0, 0, 0]
        type: {type: array, items: double}
      - value: [5.0, 1.0, 1.0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (6.75 +- 0.01)
  }

  it must "work with Gaussian similarity" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - params: [{x: double}, {y: double}]
        ret: double
        do: {metric.gaussianSimilarity: [x, y, 1.5]}
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (0.79 +- 0.01)
  }

  it must "work with categorical inputs" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.euclidean: 
      - params: [{x: string}, {y: string}]
        ret: double
        do:
          if: {"==": [x, y]}
          then: 0.0
          else: 1.0
      - value: ["one", "two", "three"]
        type: {type: array, items: string}
      - value: ["one", "two", "THREE"]
        type: {type: array, items: string}
""").head.action(null).asInstanceOf[Double] should be (1.00 +- 0.01)
  }

  "squaredEuclidean" must "work" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.squaredEuclidean: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (14.00 +- 0.01)
  }

  "chebyshev" must "work" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.chebyshev: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (3.00 +- 0.01)
  }

  "taxicab" must "work" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.taxicab: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
""").head.action(null).asInstanceOf[Double] should be (6.00 +- 0.01)
  }

  "minkowski" must "reproduce chebyshev" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.minkowski: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
      - 10.0
""").head.action(null).asInstanceOf[Double] should be (3.00 +- 0.01)
  }

  it must "reproduce taxicab" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.minkowski: 
      - fcn: metric.absDiff
      - value: [1, 2, 3]
        type: {type: array, items: double}
      - value: [0, 0, 0]
        type: {type: array, items: double}
      - 1.0
""").head.action(null).asInstanceOf[Double] should be (6.00 +- 0.01)
  }

  "binaryMetrics" must "do simpleMatching" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.simpleMatching: 
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
""").head.action(null).asInstanceOf[Double] should be (0.333 +- 0.01)
  }

  it must "do jaccard" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.jaccard: 
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
""").head.action(null).asInstanceOf[Double] should be (0.333 +- 0.01)
  }

  it must "do tanimoto" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.tanimoto: 
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
""").head.action(null).asInstanceOf[Double] should be (0.2 +- 0.01)
  }

  it must "do binarySimilarity" taggedAs(Lib1, Lib1Metric) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - metric.binarySimilarity:
      - value: [true, true, false]
        type: {type: array, items: boolean}
      - value: [true, false, true]
        type: {type: array, items: boolean}
      - 1.0
      - 2.0
      - 3.0
      - 4.0
      - 4.0
      - 3.0
      - 2.0
      - 1.0
""").head.action(null).asInstanceOf[Double] should be (1.5 +- 0.01)
  }

}
