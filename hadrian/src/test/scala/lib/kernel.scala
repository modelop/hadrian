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
class LibKernelSuite extends FlatSpec with Matchers {
  "m.kernel.*" must "compute linear kernel" taggedAs(Lib, LibKernel) in {
    val engine1 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.linear: [input, y]}
""").head
    engine1.action(engine1.jsonInput("""[1,2,3,4,5]""")).asInstanceOf[java.lang.Double].doubleValue should be (55.0 +- 0.00001)
    engine1.action(engine1.jsonInput("""[1,1,1,1,1]""")).asInstanceOf[java.lang.Double].doubleValue should be (15.0 +- 0.00001)
  }

  it must "compute polynomial kernel" taggedAs(Lib, LibKernel) in {
    val engine2 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: {gamma: 0.2}
  - let: {intercept: 0.4}
  - let: {degree: 3}
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.poly: [input, y, gamma, intercept, degree]}
""").head
    engine2.action(engine2.jsonInput("""[1,2,3,4,5]""")).asInstanceOf[java.lang.Double].doubleValue should be (1481.544 +- 0.00001)
    engine2.action(engine2.jsonInput("""[1,1,1,1,1]""")).asInstanceOf[java.lang.Double].doubleValue should be (39.304   +- 0.00001)
  }

  it must "compute RBF kernel" taggedAs(Lib, LibKernel) in {
    val engine3 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: {gamma: 0.2}
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.rbf: [input, y, gamma]}
""").head
    engine3.action(engine3.jsonInput("""[1,2,3,4,5]""")).asInstanceOf[java.lang.Double].doubleValue should be (1.0      +- 0.00001)
    engine3.action(engine3.jsonInput("""[1,1,1,1,1]""")).asInstanceOf[java.lang.Double].doubleValue should be (0.002478 +- 0.00001)
  }

  it must "compute sigmoid kernel" taggedAs(Lib, LibKernel) in {
    val engine4 = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let: {gamma: 0.2}
  - let: {intercept: 0.4}
  - let: 
      y: 
        value: [1,2,3,4,5]
        type: {type: array, items: double}
  - {m.kernel.sigmoid: [input, y, gamma, intercept]}
""").head
    engine4.action(engine4.jsonInput("""[1,2,3,4,5]""")).asInstanceOf[java.lang.Double].doubleValue should be (1.0     +- 0.00001)
    engine4.action(engine4.jsonInput("""[1,1,1,1,1]""")).asInstanceOf[java.lang.Double].doubleValue should be (0.99777 +- 0.00001)
  }

}

























































