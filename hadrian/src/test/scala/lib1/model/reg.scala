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

package test.scala.lib1.model.reg

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
class Lib1ModelRegSuite extends FlatSpec with Matchers {
  "model.reg.linear" must "do one-level array signature" taggedAs(Lib1, Lib1ModelReg) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: double}}
        - {name: const, type: double}
    init:
      coeff: [1, 2, 3, 0, 5]
      const: 100.0
action:
  model.reg.linear:
    - input
    - cell: model
""").head
    engine.action(engine.jsonInput("""[0.1, 0.2, 0.3, 0.4, 0.5]""")).asInstanceOf[java.lang.Double].doubleValue should be (103.9 +- 0.1)
  }

  it must "do two-level array signature" taggedAs(Lib1, Lib1ModelReg) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: {type: array, items: double}}}
        - {name: const, type: {type: array, items: double}}
    init:
      coeff: [[1, 2, 3, 0, 5],
              [1, 1, 1, 1, 1],
              [0, 0, 0, 0, 1]]
      const: [0.0, 0.0, 100.0]
action:
  model.reg.linear:
    - input
    - cell: model
""").head
    val out = engine.action(engine.jsonInput("""[0.1, 0.2, 0.3, 0.4, 0.5]""")).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (3.9 +- 0.1)
    out(1) should be (1.5 +- 0.1)
    out(2) should be (100.5 +- 0.1)
  }

  it must "do one-level map signature" taggedAs(Lib1, Lib1ModelReg) in {
    val engine = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: map, values: double}}
        - {name: const, type: double}
    init:
      coeff: {one: 1, two: 2, three: 3, four: 0, five: 5}
      const: 100.0
action:
  model.reg.linear:
    - input
    - cell: model
""").head
    engine.action(engine.jsonInput("""{"one": 0.1, "two": 0.2, "three": 0.3, "four": 0.4, "five": 0.5}""")).asInstanceOf[java.lang.Double].doubleValue should be (103.9 +- 0.1)
  }

  it must "do two-level map signature" taggedAs(Lib1, Lib1ModelReg) in {
    val engine = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: map, values: {type: map, values: double}}}
        - {name: const, type: {type: map, values: double}}
    init:
      coeff:
        uno: {one: 1, two: 2, three: 3, four: 0, five: 5}
        dos: {one: 1, two: 1, three: 1, four: 1, five: 1}
        tres: {one: 0, two: 0, three: 0, four: 0, five: 1}
      const:
        {uno: 0.0, dos: 0.0, tres: 100.0}
action:
  model.reg.linear:
    - input
    - cell: model
""").head
    val out = engine.action(engine.jsonInput("""{"one": 0.1, "two": 0.2, "three": 0.3, "four": 0.4, "five": 0.5}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out("uno").doubleValue should be (3.9 +- 0.1)
    out("dos").doubleValue should be (1.5 +- 0.1)
    out("tres").doubleValue should be (100.5 +- 0.1)
  }

  "model.reg.norm.*" must "softmax" taggedAs(Lib1, Lib1ModelReg) in {
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.softmax: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.03205860328008499 +- 0.0000001)
    out2(1) should be (0.08714431874203257 +- 0.0000001)
    out2(2) should be (0.23688281808991013 +- 0.0000001)
    out2(3) should be (0.6439142598879722 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.softmax: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.03205860328008499 +- 0.0000001)
    out3("two").doubleValue should be (0.08714431874203257 +- 0.0000001)
    out3("three").doubleValue should be (0.23688281808991013 +- 0.0000001)
    out3("four").doubleValue should be (0.6439142598879722 +- 0.0000001)
  }

  it must "logit" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.logit: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9002495108803148 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.logit: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.7310585786300049 +- 0.0000001)
    out2(1) should be (0.8807970779778823 +- 0.0000001)
    out2(2) should be (0.9525741268224334 +- 0.0000001)
    out2(3) should be (0.9820137900379085 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.logit: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.7310585786300049 +- 0.0000001)
    out3("two").doubleValue should be (0.8807970779778823 +- 0.0000001)
    out3("three").doubleValue should be (0.9525741268224334 +- 0.0000001)
    out3("four").doubleValue should be (0.9820137900379085 +- 0.0000001)
  }

  it must "probit" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.probit: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9860965524865013 +- 0.0000001)

    // println("below is testing logit, not probit")
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.probit: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.841344746068543  +- 0.0000001)
    out2(1) should be (0.9772498680518207 +- 0.0000001)
    out2(2) should be (0.9986501019683699 +- 0.0000001)
    out2(3) should be (0.9999683287581669 +- 0.0000001)

    // println("below is testing logit, not probit")
    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.probit: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be   (0.841344746068543  +- 0.0000001)
    out3("two").doubleValue should be   (0.9772498680518207 +- 0.0000001)
    out3("three").doubleValue should be (0.9986501019683699 +- 0.0000001)
    out3("four").doubleValue should be  (0.9999683287581669 +- 0.0000001)
  }

  it must "cloglog" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.cloglog: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9998796388196516 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.cloglog: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.9340119641546875 +- 0.0000001)
    out2(1) should be (0.9993820210106689 +- 0.0000001)
    out2(2) should be (0.9999999981078213 +- 0.0000001)
    out2(3) should be (1.0 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.cloglog: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.9340119641546875 +- 0.0000001)
    out3("two").doubleValue should be (0.9993820210106689 +- 0.0000001)
    out3("three").doubleValue should be (0.9999999981078213 +- 0.0000001)
    out3("four").doubleValue should be (1.0 +- 0.0000001)
  }

  it must "loglog" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.loglog: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (1.203611803484212E-4 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.loglog: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.06598803584531254 +- 0.0000001)
    out2(1) should be (6.179789893310934E-4 +- 0.0000001)
    out2(2) should be (1.8921786948382924E-9 +- 0.0000001)
    out2(3) should be (1.9423376049564073E-24 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.loglog: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.06598803584531254 +- 0.0000001)
    out3("two").doubleValue should be (6.179789893310934E-4 +- 0.0000001)
    out3("three").doubleValue should be (1.8921786948382924E-9 +- 0.0000001)
    out3("four").doubleValue should be (1.9423376049564073E-24 +- 0.0000001)
  }

  it must "cauchit" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {model.reg.norm.cauchit: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.8642002512199081 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {model.reg.norm.cauchit: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.75 +- 0.0000001)
    out2(1) should be (0.8524163823495667 +- 0.0000001)
    out2(2) should be (0.8975836176504333 +- 0.0000001)
    out2(3) should be (0.9220208696226307 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {model.reg.norm.cauchit: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.75 +- 0.0000001)
    out3("two").doubleValue should be (0.8524163823495667 +- 0.0000001)
    out3("three").doubleValue should be (0.8975836176504333 +- 0.0000001)
    out3("four").doubleValue should be (0.9220208696226307 +- 0.0000001)
  }

  "model.reg.residual" must "work" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  model.reg.residual:
    - input
    - 3.0
""").head
    engine1.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Double].doubleValue should be (2.0 +- 0.1)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  model.reg.residual:
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
  model.reg.residual:
    - input
    - type: {type: map, values: double}
      value: {one: 2.5, two: 2.5, three: 2.5}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (-1.5 +- 0.1)
    out3("two").doubleValue should be (-0.5 +- 0.1)
    out3("three").doubleValue should be (0.5 +- 0.1)
  }

  "model.reg.pull" must "work" taggedAs(Lib1, Lib1ModelReg) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  model.reg.pull:
    - input
    - 3.0
    - 2.0
""").head
    engine1.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0 +- 0.1)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action:
  model.reg.pull:
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
  model.reg.pull:
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

  "model.reg.mahalanobis" must "work" taggedAs(Lib1, Lib1ModelReg) in {
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  model.reg.mahalanobis:
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
  model.reg.mahalanobis:
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

  "model.reg.*chi2*" must "work" taggedAs(Lib1, Lib1ModelReg) in {
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
            - {name: DOF, type: int}
        new:
          {chi2: 0.0, DOF: -4}
  - set: {state: {model.reg.updateChi2: [0.0, state]}}
  - set: {state: {model.reg.updateChi2: [1.0, state]}}
  - set: {state: {model.reg.updateChi2: [1.5, state]}}
  - set: {state: {model.reg.updateChi2: [-0.75, state]}}
  - set: {state: {model.reg.updateChi2: [-1.0, state]}}
  - set: {state: {model.reg.updateChi2: [0.5, state]}}
  - set: {state: {model.reg.updateChi2: [-1.5, state]}}
  - type: {type: array, items: double}
    new: [state.chi2, {model.reg.reducedChi2: state}, {model.reg.chi2Prob: state}]
""").head
    val out = engine.action(null).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (7.3125 +- 0.1)
    out(1) should be (2.4375 +- 0.1)
    out(2) should be (0.9374 +- 0.1)
  }
}
