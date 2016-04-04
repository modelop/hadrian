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

package test.scala.lib.spec

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
class LibSpecSuite extends FlatSpec with Matchers {
"log beta function" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - m.special.lnBeta: [input, 3] # [a, b]
""").head

    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (-1.0986 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[java.lang.Double].doubleValue should be    (-4.0943 +- 0.001)
    engine.action(java.lang.Double.valueOf(.01)).asInstanceOf[java.lang.Double].doubleValue should be    ( 4.5902 +- 0.001)
    }

  it must "raise the correct exceptions" taggedAs(Lib, LibSpec) in {
    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - m.special.lnBeta: [input, -3] # [a, b]
""").head

    evaluating { engine2.action(java.lang.Double.valueOf(0.5)) } should produce [PFARuntimeException]
    }


"n choose k" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - m.special.nChooseK: [input, 2]
""").head

    engine.action(java.lang.Integer.valueOf(20)).asInstanceOf[java.lang.Integer] should be    (190)
    engine.action(java.lang.Integer.valueOf(10)).asInstanceOf[java.lang.Integer] should be    (45)
    engine.action(java.lang.Integer.valueOf(3)).asInstanceOf[java.lang.Integer] should be     (3)

  }

  it must "raise the correct exceptions" taggedAs(Lib, LibSpec) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - m.special.nChooseK: [input, 4]
""").head
    evaluating { engine.action(java.lang.Integer.valueOf(1)) } should produce [PFARuntimeException]
    evaluating { engine.action(java.lang.Integer.valueOf(0)) } should produce [PFARuntimeException]

    val engine1 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - m.special.nChooseK: [input, 4] #[input, lambda]
""").head
    evaluating { engine1.action(java.lang.Integer.valueOf(-2)) } should produce [PFARuntimeException]
  }


"erf" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.special.erf: input}
""").head

    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (-1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be  (-0.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be     (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be   (0.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be  (1.00 +- 0.01)
  }


"erfc" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.special.erfc: input}
""").head

    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be  (1.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be     (1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be   (0.4795 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be  (0.00 +- 0.01)
  }


"lnGamma" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.special.lnGamma: input}
""").head
    engine.action(java.lang.Double.valueOf(0.1)).asInstanceOf[Double] should be   (2.2527 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be   (0.5724 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be  (46.919 +- 0.01)
  }

  it must "raise the correct exceptions" taggedAs(Lib, LibSpec) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.special.lnGamma: input}
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(-2.0)) } should produce [PFARuntimeException]
    evaluating { engine1.action(java.lang.Double.valueOf(-2.2)) } should produce [PFARuntimeException]
  }

//"regularized gamma P function" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
//    val engine = PFAEngine.fromYaml("""
//input: double
//output: double
//action:
//  - m.special.regularizedgammapfcn: [input, 3] # [a, b]
//""").head
//
//    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.08030 +- 0.001)
//    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.32332 +- 0.001)
//    engine.action(java.lang.Double.valueOf(3.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.57680 +- 0.001)
//    }
//
//  it must "raise the correct exceptions" taggedAs(Lib, LibSpec) in {
//    val engine2 = PFAEngine.fromYaml("""
//input: double
//output: double
//action:
//  - m.special.regularizedgammapfcn: [input, -3] # [a, b]
//""").head
//
//    evaluating { engine2.action(java.lang.Double.valueOf(1.40)) } should produce [PFARuntimeException]
//    evaluating { engine2.action(java.lang.Double.valueOf(-1.2)) } should produce [PFARuntimeException]
//    }
//
//"regularized gamma Q function" must "evaluate correctly" taggedAs(Lib, LibSpec) in {
//    val engine = PFAEngine.fromYaml("""
//input: double
//output: double
//action:
//  - m.special.regularizedgammaqfcn: [input, 3] # [a, b]
//""").head
//
//    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.91969 +- 0.001)
//    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.67667 +- 0.001)
//    engine.action(java.lang.Double.valueOf(3.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.42319 +- 0.001)
//    }
//
//  it must "raise the correct exceptions" taggedAs(Lib, LibSpec) in {
//    val engine2 = PFAEngine.fromYaml("""
//input: double
//output: double
//action:
//  - m.special.regularizedgammaqfcn: [input, -3] # [a, b]
//""").head
//
//    evaluating { engine2.action(java.lang.Double.valueOf(1.40)) } should produce [PFARuntimeException]
//    evaluating { engine2.action(java.lang.Double.valueOf(-1.2)) } should produce [PFARuntimeException]
//    }
}
