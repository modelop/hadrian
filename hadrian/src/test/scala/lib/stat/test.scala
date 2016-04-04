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

package test.scala.lib.test

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
class LibTestSuite extends FlatSpec with Matchers {

  "KSTwoSample" must "compute p-value" taggedAs(Lib, LibStatTest) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let:
      y:
       type: {type: array, items: double}
       new:
          - -0.6852076
          - -0.62961294
          -  1.47603708
          - -1.66223465
          - -0.34015844
          -  1.50852341
          - -0.0348001
          - -0.59529466
          -  0.71956491
          - -0.77441149

  - {stat.test.kolmogorov: [input, y]}
""").head
    val x = """[0.53535232,
                0.66523251,
                0.92733853,
                0.45348014,
               -0.37606127,
                1.22115272,
               -0.36264331,
                2.15954568,
                0.49463302,
               -0.81670101]"""
    engine.action(engine.jsonInput(x)).asInstanceOf[java.lang.Double].doubleValue should be (0.31285267601695582 +- .0000001)
    }

  "stat.test.residual" must "work" taggedAs(Lib, LibStatTest) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  stat.test.residual:
    - input
    - 3.0
""").head
    engine1.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Double].doubleValue should be (2.0 +- 0.1)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  stat.test.residual:
    - input
    - type: {type: array, items: double}
      value: [2.5, 2.5, 2.5]
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (-1.5 +- 0.1)
    out2(1) should be (-0.5 +- 0.1)
    out2(2) should be (0.5 +- 0.1)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action:
  stat.test.residual:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (-1.5 +- 0.1)
    out3("two").doubleValue should be (-0.5 +- 0.1)
    out3("three").doubleValue should be (0.5 +- 0.1)
  }

  "stat.test.pull" must "work" taggedAs(Lib, LibStatTest) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  stat.test.pull:
    - input
    - 3.0
    - 2.0
""").head
    engine1.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0 +- 0.1)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  stat.test.pull:
    - input
    - type: {type: array, items: double}
      value: [2.5, 2.5, 2.5]
    - type: {type: array, items: double}
      value: [2.0, 2.0, 2.0]
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (-0.75 +- 0.1)
    out2(1) should be (-0.25 +- 0.1)
    out2(2) should be (0.25 +- 0.1)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action:
  stat.test.pull:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
    - type: {type: map, values: double}
      value: {one: 2.0, two: 2.0, three: 2.0}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (-0.75 +- 0.1)
    out3("two").doubleValue should be (-0.25 +- 0.1)
    out3("three").doubleValue should be (0.25 +- 0.1)
  }

  "stat.test.mahalanobis" must "work" taggedAs(Lib, LibStatTest) in {
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  stat.test.mahalanobis:
    - input
    - type: {type: array, items: double}
      value: [2.5, 2.5, 2.5]
    - type: {type: array, items: {type: array, items: double}}
      value: [[2.0, 0.0, 0.0],
              [0.0, 4.0, 0.0],
              [0.0, 0.0, 1.0]]
""").head
    engine2.action(engine2.jsonInput("""[1, 2, 3]""")).asInstanceOf[java.lang.Double].doubleValue should be (1.19895788083 +- 0.1)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
action:
  stat.test.mahalanobis:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
    - type: {type: map, values: {type: map, values: double}}
      value: {one:   {one: 2.0, two: 0.0, three: 0.0},
              two:   {one: 0.0, two: 4.0, three: 0.0},
              three: {one: 0.0, two: 0.0, three: 1.0}}
""").head
    engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3}""")).asInstanceOf[java.lang.Double].doubleValue should be (1.19895788083 +- 0.1)
  }

  "stat.test.*chi2*" must "work" taggedAs(Lib, LibStatTest) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - let:
      state:
        type:
          type: record
          name: Chi2
          fields:
            - {name: chi2, type: double}
            - {name: dof, type: int}
        new:
          {chi2: 0.0, dof: -4}
  - set: {state: {stat.test.updateChi2: [0.0, state]}}
  - set: {state: {stat.test.updateChi2: [1.0, state]}}
  - set: {state: {stat.test.updateChi2: [1.5, state]}}
  - set: {state: {stat.test.updateChi2: [-0.75, state]}}
  - set: {state: {stat.test.updateChi2: [-1.0, state]}}
  - set: {state: {stat.test.updateChi2: [0.5, state]}}
  - set: {state: {stat.test.updateChi2: [-1.5, state]}}
  - type: {type: array, items: double}
    new: [state.chi2, {stat.test.reducedChi2: state}, {stat.test.chi2Prob: state}]
""").head
    val out = engine.action(null).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (7.3125 +- 0.1)
    out(1) should be (2.4375 +- 0.1)
    out(2) should be (0.9374 +- 0.1)
  }
}
