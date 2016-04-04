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

package test.scala.lib.model.svm

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
class LibModelSvmSuite extends FlatSpec with Matchers {
  "model.svm.score" must "score data correctly with linear kernel" taggedAs(Lib, LibModelSvm) in {
    val engine1 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: 1.83613,
       posClass: [{supVec: [3.96989384, 3.60281757], coeff: -0.14933674}],
       negClass: [{supVec: [0.43689046, 2.45981766], coeff:  0.1070752},
                  {supVec: [1.47126216, 0.48686121], coeff:  0.04226154}]}

action:
  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.linear: [x, y]}
""").head
    engine1.action(engine1.jsonInput("""[0.0, 1.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (1.582057940 +- 0.0001)
    engine1.action(engine1.jsonInput("""[3.0, 3.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (-0.37776544 +- 0.0001)
  }

  it must "score data correctly with the polynomial kernel" taggedAs(Lib, LibModelSvm) in {
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: 1.27509531,
       posClass: [{supVec: [3.96989384,  3.60281757], coeff: -0.27658039}],
       negClass: [{supVec: [0.43689046,  2.45981766], coeff:  0.27658039}]}

action:
  - let: {gamma: 0.1}
  - let: {degree: 2}
  - let: {intercept: 0.3}

  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.poly: [x, y, gamma, intercept, degree]}
""").head
    engine2.action(engine2.jsonInput("""[0.0, 1.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (1.23696154 +- 0.0001)
    engine2.action(engine2.jsonInput("""[3.0, 3.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (-0.1762974 +- 0.0001)
  }

  it must "score data correctly with the RBF kernel" taggedAs(Lib, LibModelSvm) in {
    val engine3 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: 0.19901071,
       posClass: [{supVec: [2.96989384, 2.60281757], coeff: -1.0},
                  {supVec: [4.26890398, 3.91296131], coeff: -0.52019712},
                  {supVec: [4.70758215, 3.36311393], coeff: -0.27706044}],
       negClass: [{supVec: [0.43689046, 2.45981766], coeff:  0.94571794},
                  {supVec: [1.47126216, 0.48686121], coeff:  0.85153962}]}

action:
  - let: {gamma: 0.1}
  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.rbf: [x, y, gamma]}
""").head
    engine3.action(engine3.jsonInput("""[0.0, 1.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (1.243308940 +- 0.0001)
    engine3.action(engine3.jsonInput("""[3.0, 3.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (-0.56231871 +- 0.0001)
  }

  it must "score data correctly with a sigmoid kernel" taggedAs(Lib, LibModelSvm) in {
    val engine4 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: -0.05318959,
       posClass: [{supVec: [ 2.96989384, 2.60281757], coeff: -1.0},
                  {supVec: [ 4.31554013, 3.47119604], coeff: -1.0},
                  {supVec: [ 4.26890398, 3.91296131], coeff: -1.0},
                  {supVec: [ 4.70758215, 3.36311393], coeff: -1.0}],
       negClass: [{supVec: [ 0.43689046, 2.45981766], coeff:  1.0},
                  {supVec: [ 1.47126216, 0.48686121], coeff:  1.0},
                  {supVec: [ 0.60310957, 1.60019866], coeff:  1.0},
                  {supVec: [-1.2920146 , 1.55663529], coeff:  1.0}]}

action:
  - let: {gamma: 0.2}
  - let: {intercept: 0.4}

  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.sigmoid: [x, y, gamma, intercept]}
""").head
    engine4.action(engine4.jsonInput("""[0.0, 1.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (-0.79563574 +- 0.0001)
    engine4.action(engine4.jsonInput("""[3.0, 3.0]""")).asInstanceOf[java.lang.Double].doubleValue should be (-0.71633509 +- 0.0001)
  }

}








