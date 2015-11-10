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

package test.scala.lib.link

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
class LibLinkSuite extends FlatSpec with Matchers {
  "m.link.*" must "softmax" taggedAs(Lib, LibLink) in {
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.softmax: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.03205860328008499 +- 0.0000001)
    out2(1) should be (0.08714431874203257 +- 0.0000001)
    out2(2) should be (0.23688281808991013 +- 0.0000001)
    out2(3) should be (0.6439142598879722 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.softmax: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.03205860328008499 +- 0.0000001)
    out3("two").doubleValue should be (0.08714431874203257 +- 0.0000001)
    out3("three").doubleValue should be (0.23688281808991013 +- 0.0000001)
    out3("four").doubleValue should be (0.6439142598879722 +- 0.0000001)
  }

  it must "logit" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.logit: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9002495108803148 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.logit: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.7310585786300049 +- 0.0000001)
    out2(1) should be (0.8807970779778823 +- 0.0000001)
    out2(2) should be (0.9525741268224334 +- 0.0000001)
    out2(3) should be (0.9820137900379085 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.logit: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.7310585786300049 +- 0.0000001)
    out3("two").doubleValue should be (0.8807970779778823 +- 0.0000001)
    out3("three").doubleValue should be (0.9525741268224334 +- 0.0000001)
    out3("four").doubleValue should be (0.9820137900379085 +- 0.0000001)
  }

  it must "probit" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.probit: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9860965524865013 +- 0.0000001)

    // println("below is testing logit, not probit")
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.probit: input}
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
action: {m.link.probit: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be   (0.841344746068543  +- 0.0000001)
    out3("two").doubleValue should be   (0.9772498680518207 +- 0.0000001)
    out3("three").doubleValue should be (0.9986501019683699 +- 0.0000001)
    out3("four").doubleValue should be  (0.9999683287581669 +- 0.0000001)
  }

  it must "cloglog" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.cloglog: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9998796388196516 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.cloglog: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.9340119641546875 +- 0.0000001)
    out2(1) should be (0.9993820210106689 +- 0.0000001)
    out2(2) should be (0.9999999981078213 +- 0.0000001)
    out2(3) should be (1.0 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.cloglog: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.9340119641546875 +- 0.0000001)
    out3("two").doubleValue should be (0.9993820210106689 +- 0.0000001)
    out3("three").doubleValue should be (0.9999999981078213 +- 0.0000001)
    out3("four").doubleValue should be (1.0 +- 0.0000001)
  }

  it must "loglog" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.loglog: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (1.203611803484212E-4 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.loglog: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.06598803584531254 +- 0.0000001)
    out2(1) should be (6.179789893310934E-4 +- 0.0000001)
    out2(2) should be (1.8921786948382924E-9 +- 0.0000001)
    out2(3) should be (1.9423376049564073E-24 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.loglog: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.06598803584531254 +- 0.0000001)
    out3("two").doubleValue should be (6.179789893310934E-4 +- 0.0000001)
    out3("three").doubleValue should be (1.8921786948382924E-9 +- 0.0000001)
    out3("four").doubleValue should be (1.9423376049564073E-24 +- 0.0000001)
  }

  it must "cauchit" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.cauchit: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.8642002512199081 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.cauchit: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.75 +- 0.0000001)
    out2(1) should be (0.8524163823495667 +- 0.0000001)
    out2(2) should be (0.8975836176504333 +- 0.0000001)
    out2(3) should be (0.9220208696226307 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.cauchit: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be (0.75 +- 0.0000001)
    out3("two").doubleValue should be (0.8524163823495667 +- 0.0000001)
    out3("three").doubleValue should be (0.8975836176504333 +- 0.0000001)
    out3("four").doubleValue should be (0.9220208696226307 +- 0.0000001)
  }

  it must "softplus" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.softplus: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (2.305083319768696 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.softplus: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[1, 2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (1.31326168752 +- 0.0000001)
    out2(1) should be (2.12692801104 +- 0.0000001)
    out2(2) should be (3.04858735157 +- 0.0000001)
    out2(3) should be (4.01814992792 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.softplus: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": 1, "two": 2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be   (1.31326168752 +- 0.0000001)
    out3("two").doubleValue should be   (2.12692801104 +- 0.0000001)
    out3("three").doubleValue should be (3.04858735157 +- 0.0000001)
    out3("four").doubleValue should be  (4.01814992792 +- 0.0000001)
  }

  it must "relu" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.relu: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (2.2 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.relu: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[-1, -2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (0.0 +- 0.0000001)
    out2(1) should be (0.0 +- 0.0000001)
    out2(2) should be (3.0 +- 0.0000001)
    out2(3) should be (4.0 +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.relu: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": -1, "two": -2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be   (0.0 +- 0.0000001)
    out3("two").doubleValue should be   (0.0 +- 0.0000001)
    out3("three").doubleValue should be (3.0 +- 0.0000001)
    out3("four").doubleValue should be  (4.0 +- 0.0000001)
  }

  it must "tanh" taggedAs(Lib, LibLink) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action: {m.link.tanh: input}
""").head
    engine1.action(engine1.jsonInput("""2.2""")).asInstanceOf[java.lang.Double].doubleValue should be (0.9757431300314515 +- 0.0000001)

    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
action: {m.link.tanh: input}
""").head
    val out2 = engine2.action(engine2.jsonInput("""[-1, -2, 3, 4]""")).asInstanceOf[PFAArray[Double]].toVector
    out2(0) should be (-0.761594155956 +- 0.0000001)
    out2(1) should be (-0.964027580076 +- 0.0000001)
    out2(2) should be (0.995054753687  +- 0.0000001)
    out2(3) should be (0.999329299739  +- 0.0000001)

    val engine3 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: map, values: double}
action: {m.link.tanh: input}
""").head
    val out3 = engine3.action(engine3.jsonInput("""{"one": -1, "two": -2, "three": 3, "four": 4}""")).asInstanceOf[PFAMap[java.lang.Double]].toMap
    out3("one").doubleValue should be   (-0.761594155956 +- 0.0000001)
    out3("two").doubleValue should be   (-0.964027580076 +- 0.0000001)
    out3("three").doubleValue should be (0.995054753687  +- 0.0000001)
    out3("four").doubleValue should be  (0.999329299739  +- 0.0000001)
  }



}













