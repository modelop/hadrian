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

package test.scala.lib1.model.naive

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
class Lib1ModelNaiveSuite extends FlatSpec with Matchers {
  "model.bayes.gaussian" must "do array signature" taggedAs(Lib1, Lib1ModelNaive) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  twoClass:
    type:
      type: map
      values:
        type: array
        items:
          type: record
          name: parameters
          fields:
            - {name: mean,  type: double}
            - {name: variance, type: double}
    init:
        {class1: [{mean: -2.0, variance: 1.0},
                  {mean:  1.0, variance: 1.0},
                  {mean:  2.0, variance: 1.0}],
         class2: [{mean: -1.0, variance: 1.0},
                  {mean:  1.0, variance: 4.0},
                  {mean:  1.0, variance: 4.0}]}
action:

    - let:
        class1params: {cell: twoClass, path: [{string: class1}]}
        class2params: {cell: twoClass, path: [{string: class2}]}

    - let:
        class1LL: {model.naive.gaussian: [input, class1params]}
        class2LL: {model.naive.gaussian: [input, class2params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
    - let:
        class1Lprior: -0.40546511
        class2Lprior: -1.09861229

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]
    - let:
        C: {a.logsumexp: [classLPost]}
    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}

""").head
    val out = engine.action(engine.jsonInput("""[-1.0, 0.0, 1.0]""")).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (-0.401714 +- 0.000001)
    out(1) should be (-1.106156 +- 0.000001)
    
  }

  it must "do the map signature too" taggedAs(Lib1, Lib1ModelNaive) in {
    val engine = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: array, items: double}
cells:
  twoClass:
    type:
      type: map
      values:
        type: map
        values:
          type: record
          name: parameters
          fields:
            - {name: mean,  type: double}
            - {name: variance, type: double}
    init:
        {class1: {f1: {mean: -2.0, variance: 1.0},
                  f2: {mean:  1.0, variance: 1.0},
                  f3: {mean:  2.0, variance: 1.0}},
         class2: {f1: {mean: -1.0, variance: 1.0},
                  f2: {mean:  1.0, variance: 4.0},
                  f3: {mean:  1.0, variance: 4.0}}}
action:

    - let:
        class1params: {cell: twoClass, path: [{string: class1}]}
        class2params: {cell: twoClass, path: [{string: class2}]}

    - let:
        class1LL: {model.naive.gaussian: [input, class1params]}
        class2LL: {model.naive.gaussian: [input, class2params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
    - let:
        class1Lprior: -0.40546511
        class2Lprior: -1.09861229

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]

    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}

""").head
    val out = engine.action(engine.jsonInput("""{"f1": -1.0, "f2": 0.0, "f3": 1.0}""")).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (-0.401714 +- 0.000001)
    out(1) should be (-1.106156 +- 0.000001)
    
  }


  "model.naive.multinomial" must "do array signature" taggedAs(Lib1, Lib1ModelNaive) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: int}
output: {type: array, items: double}
cells:
  threeClass:
    type:
      type: map
      values:
        type: array
        items: double
    init:
        {class1: [0.2,
                  0.2,
                  0.2,
                  0.4],
         class2: [0.22228381448432147,
                  0.27771618612438459,
                  0.44401330534751776,
                  0.055986696442301712],
         class3: [0.36322463751061512,
                  0.0009057970985842759,
                  0.36322463751061512,
                  0.27264492810555946]}

action:

    - let:
        class1params: {cell: threeClass, path: [{string: class1}]}
        class2params: {cell: threeClass, path: [{string: class2}]}
        class3params: {cell: threeClass, path: [{string: class3}]}

    - let:
        class1LL: {model.naive.multinomial: [input, class1params]}
        class2LL: {model.naive.multinomial: [input, class2params]}
        class3LL: {model.naive.multinomial: [input, class3params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
            - class3LL

    - let:
        class1Lprior: -0.84729786
        class2Lprior: -1.25276297
        class3Lprior: -1.25276297

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]
            - "+": [class3LL, class3Lprior]
    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}
""").head
    val out = engine.action(engine.jsonInput("""[0, 1, 2, 1]""")).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (-0.497 +- 0.001)
    out(1) should be (-0.946 +- 0.001)
    out(2) should be (-5.491 +- 0.001)
  }


  it must "do the map signature too" taggedAs(Lib1, Lib1ModelNaive) in {
    val engine = PFAEngine.fromYaml("""
input: {type: map, values: int}
output: {type: array, items: double}
cells:
  threeClass:
    type:
      type: map
      values:
        type: map
        values: double
    init:
        {class1: {f1: 0.2,
                  f2: 0.2,
                  f3: 0.2,
                  f4: 0.4},
         class2: {f1: 0.22228381448432147,
                  f2: 0.27771618612438459,
                  f3: 0.44401330534751776,
                  f4: 0.055986696442301712},
         class3: {f1: 0.36322463751061512,
                  f2: 0.0009057970985842759,
                  f3: 0.36322463751061512,  
                  f4: 0.27264492810555946}}
action:

    - let:
        class1params: {cell: threeClass, path: [{string: class1}]}
        class2params: {cell: threeClass, path: [{string: class2}]}
        class3params: {cell: threeClass, path: [{string: class3}]}

    - let:
        class1LL: {model.naive.multinomial: [input, class1params]}
        class2LL: {model.naive.multinomial: [input, class2params]}
        class3LL: {model.naive.multinomial: [input, class3params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
            - class3LL

    - let:
        class1Lprior: -0.84729786
        class2Lprior: -1.25276297
        class3Lprior: -1.25276297

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]
            - "+": [class3LL, class3Lprior]

    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}
""").head
    val out = engine.action(engine.jsonInput("""{"f1":0, "f2":1, "f3":2, "f4":1}""")).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (-0.497 +- 0.001)
    out(1) should be (-0.946 +- 0.001)
    out(2) should be (-5.491 +- 0.001)
  }


  "model.naive.bernoulli" must "do array(string) signature" taggedAs(Lib1, Lib1ModelNaive) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: string}
output: {type: array, items: double}
cells:
  threeClass:
    type:
      type: map
      values:
        type: map
        values: double
    init:
        {class1: {f1: 0.75,
                  f2: 0.75,
                  f3: 0.5},
         class2: {f1: 0.75,
                  f2: 0.25,
                  f3: 0.5},
         class3: {f1: 0.75,
                  f2: 0.5,
                  f3: 0.5}}
action:

    - let:
        class1params: {cell: threeClass, path: [{string: class1}]}
        class2params: {cell: threeClass, path: [{string: class2}]}
        class3params: {cell: threeClass, path: [{string: class3}]}

    - let:
        class1LL: {model.naive.bernoulli: [input, class1params]}
        class2LL: {model.naive.bernoulli: [input, class2params]}
        class3LL: {model.naive.bernoulli: [input, class3params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
            - class3LL

    - let:
        C: {a.logsumexp: [classLL]}

    - a.map:
        - classLL
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}

""").head
    val out = engine.action(engine.jsonInput("""["f1", "f2", "somethingelse"]""")).asInstanceOf[PFAArray[Double]].toVector
    out(0) should be (-0.6931471 +- 0.000001)
    out(1) should be (-1.7917594 +- 0.000001)
    out(2) should be (-1.0986122 +- 0.000001)
  }
}

