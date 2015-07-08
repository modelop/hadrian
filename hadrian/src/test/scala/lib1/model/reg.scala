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

  "model.reg.linearVariance" must "do one-level array signature" taggedAs(Lib1, Lib1ModelReg) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: array, items: {type: array, items: double}}}
    init:
      covar: [[ 1.0, -0.1, 0.0],
              [-0.1,  2.0, 0.0],
              [ 0.0,  0.0, 0.0]]
action:
  model.reg.linearVariance:
    - input
    - cell: model
""").head
    engine.action(engine.jsonInput("""[0.1, 0.2]""")).asInstanceOf[java.lang.Double].doubleValue should be (0.086 +- 0.001)
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
        - {name: covar, type: {type: array, items: {type: array, items: {type: array, items: double}}}}
    init:
      covar:
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
action:
  model.reg.linearVariance:
    - input
    - cell: model
""").head
    val results = engine.action(engine.jsonInput("""[0.1, 0.2]""")).asInstanceOf[PFAArray[Double]].toVector
    results(0) should be (0.086 +- 0.001)
    results(1) should be (0.086 +- 0.001)
    results(2) should be (0.086 +- 0.001)
    results(3) should be (0.086 +- 0.001)
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
        - {name: covar, type: {type: map, values: {type: map, values: double}}}
    init:
      covar: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
action:
  model.reg.linearVariance:
    - input
    - cell: model
""").head
    engine.action(engine.jsonInput("""{"a": 0.1, "b": 0.2}""")).asInstanceOf[java.lang.Double].doubleValue should be (0.086 +- 0.001)
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
        - {name: covar, type: {type: map, values: {type: map, values: {type: map, values: double}}}}
    init:
      covar:
        one: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
        two: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
        three: {a: {a:  1.0, b: -0.1, "": 0.0},
                b: {a: -0.1, b:  2.0, "": 0.0},
               "": {a:  0.0, b:  0.0, "": 0.0}}
        four: {a: {a:  1.0, b: -0.1, "": 0.0},
               b: {a: -0.1, b:  2.0, "": 0.0},
              "": {a:  0.0, b:  0.0, "": 0.0}}
action:
  model.reg.linearVariance:
    - input
    - cell: model
""").head
    val results = engine.action(engine.jsonInput("""{"a": 0.1, "b": 0.2}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    results("one").doubleValue should be (0.086 +- 0.001)
    results("two").doubleValue should be (0.086 +- 0.001)
    results("three").doubleValue should be (0.086 +- 0.001)
    results("four").doubleValue should be (0.086 +- 0.001)
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
