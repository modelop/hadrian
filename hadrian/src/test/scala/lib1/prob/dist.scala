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

package test.scala.lib1.prob.dist

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._


// TEST NORMAL log(PDF)
@RunWith(classOf[JUnitRunner])
class Lib1ProbDistSuite extends FlatSpec with Matchers {
  "normal distribution" must "have the right log likelihoods" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
""").head

    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (-1.612 +- 0.01)
    engine.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (-2.112 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-14.11 +- 0.01)
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (-4.737 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-2.112 +- 0.01)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, 2.0]
""").head

    engine2.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (-1.612 +- 0.01)
    engine2.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (-2.112 +- 0.01)
    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-14.11 +- 0.01)
    engine2.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (-4.737 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[java.lang.Double].doubleValue should be  (-2.112 +- 0.01)
    java.lang.Double.isNaN(engine2.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

// NORMAL CDF
  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianCDF:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]} 
""").head

    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.5    +- 0.001)
    engine.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.8413 +- 0.001)
    engine.action(java.lang.Double.valueOf(5.0 )).asInstanceOf[java.lang.Double].doubleValue should be (0.0062 +- 0.001)
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.9938 +- 0.001)
    engine.action(java.lang.Double.valueOf(8.0 )).asInstanceOf[java.lang.Double].doubleValue should be (0.1586 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)


    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianCDF: [input, 10.0, 2.0]
""").head

    engine1.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.5    +- 0.001)
    engine1.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.8413 +- 0.001)
    engine1.action(java.lang.Double.valueOf(5.0 )).asInstanceOf[java.lang.Double].doubleValue should be (0.0062 +- 0.001)
    engine1.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.9938 +- 0.001)
    engine1.action(java.lang.Double.valueOf(8.0 )).asInstanceOf[java.lang.Double].doubleValue should be (0.1586 +- 0.001)
    java.lang.Double.isNaN(engine1.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianQF:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]} 
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine.action(java.lang.Double.valueOf(0.01)).asInstanceOf[java.lang.Double].doubleValue should be (5.3473  +- 0.001)
    engine.action(java.lang.Double.valueOf(0.4 )).asInstanceOf[java.lang.Double].doubleValue should be (9.4933  +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5 )).asInstanceOf[java.lang.Double].doubleValue should be (10.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.99)).asInstanceOf[java.lang.Double].doubleValue should be (14.6527 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)


    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianQF: [input, 10.0, 2.0]
""").head

    engine1.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine1.action(java.lang.Double.valueOf(0.01)).asInstanceOf[java.lang.Double].doubleValue should be (5.3473  +- 0.001)
    engine1.action(java.lang.Double.valueOf(0.4 )).asInstanceOf[java.lang.Double].doubleValue should be (9.4933  +- 0.001)
    engine1.action(java.lang.Double.valueOf(0.5 )).asInstanceOf[java.lang.Double].doubleValue should be (10.0000 +- 0.001)
    engine1.action(java.lang.Double.valueOf(0.99)).asInstanceOf[java.lang.Double].doubleValue should be (14.6527 +- 0.001)
    engine1.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine1.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }


  it must "be a delta function in right conditions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: 0.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
""").head

    engine1.action(java.lang.Double.valueOf(9.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine1.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    engine1.action(java.lang.Double.valueOf(11.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, 0.0]
""").head

    engine2.action(java.lang.Double.valueOf(9.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine2.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    engine2.action(java.lang.Double.valueOf(11.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)


    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianCDF:
      - input
      - value: {count: 21, mean: 10, variance: 0.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]} 
""").head
    engine3.action(java.lang.Double.valueOf(9.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine3.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine3.action(java.lang.Double.valueOf(11.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianCDF: [input, 10.0, 0.0]
""").head
    engine4.action(java.lang.Double.valueOf(9.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine4.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine4.action(java.lang.Double.valueOf(11.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)


    val engine5 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianQF:
      - input
      - value: {count: 21, mean: 10, variance: 0.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]} 
""").head
    engine5.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    engine5.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine5.action(java.lang.Double.valueOf(0.4)).asInstanceOf[java.lang.Double].doubleValue should be (10.0)

    val engine6 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianQF: [input, 10.0, 0.0]
""").head
    engine6.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    engine6.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine6.action(java.lang.Double.valueOf(0.4)).asInstanceOf[java.lang.Double].doubleValue should be (10.0)
    }


  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL:
      - input
      - value: {count: 21, mean: 10, variance: -3.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianLL: [input, 10.0, -3.0]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianCDF:
      - input
      - value: {count: 21, mean: 10, variance: -3.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]} 
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianCDF: [input, 10.0, -3.0]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine5 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianQF:
      - input
      - value: {count: 21, mean: 10, variance: -3.0}
        type: {type: record, name: Rec, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]} 
""").head
    evaluating { engine5.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine6 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gaussianQF: [input, 10.0, 3.0]
""").head
    evaluating { engine6.action(java.lang.Double.valueOf(1.3)) } should produce [PFARuntimeException]
    evaluating { engine6.action(java.lang.Double.valueOf(-0.3)) } should produce [PFARuntimeException]
    }


/////////////////EXPONENTIAL DISTRIBUTION TESTS
  "exponential distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialPDF: [input, 1] #[input, rate]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (1.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.368 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.135 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.082 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }
  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialCDF: [input, 1]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.632 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.865 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.918 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }
  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, 1]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (0.3567 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (0.6931 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (1.6094 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have correct behavior handling edge cases" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialPDF: [input, 0] #[input, rate]
""").head

    engine1.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialCDF: [input, 0]
""").head

    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine2.action(java.lang.Double.valueOf(-1.3)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine2.action(java.lang.Double.valueOf(1.3)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, 0.9]
""").head

    engine3.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    //// check bad inputs
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialPDF: [input, -1.0]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialCDF: [input, -1]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(3.0)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, -1]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.exponentialQF: [input, 1.5]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-1.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]
    }

///////////// POISSON DISTRIBUTION TESTS /////////////////
  "poisson distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.poissonPDF: [input, 4]  #[input, lambda]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0183 +- 0.001)
    engine.action(java.lang.Integer.valueOf(1)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0733 +- 0.001)
    engine.action(java.lang.Integer.valueOf(2)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1465 +- 0.001)
    engine.action(java.lang.Integer.valueOf(10)).asInstanceOf[java.lang.Double].doubleValue should be   (0.0053 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonCDF: [input, 4] #[input, lambda]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0183 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2381 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2381 +- 0.001)
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be   (0.9972 +- 0.001)
    engine.action(java.lang.Double.valueOf(-10.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonQF: [input, 4] #[input, lambda]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (3.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (4.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (6.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "handle edge cases properly" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.poissonPDF: [input, 0]  #[input, lambda]
""").head

    engine1.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine1.action(java.lang.Integer.valueOf(4)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonCDF: [input, 0] #[input, lambda]
""").head

    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (1.0)
    engine2.action(java.lang.Double.valueOf(-1.3)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine2.action(java.lang.Double.valueOf(1.3)).asInstanceOf[java.lang.Double].doubleValue should be  (1.0)


    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonQF: [input, 0] #[input, lambda]
""").head

    engine3.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine3.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine3.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    }


  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.poissonPDF: [input, -4] #[input, lambda]
""").head
    evaluating { engine1.action(java.lang.Integer.valueOf(4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonCDF: [input, -3] #[input, lambda]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonQF: [input, -2] #[input, lambda]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.poissonQF: [input, 2] #[input, lambda]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


///////////////////////////////// CHI2 DISTRIBUTION /////////////////////////////////////
  "chi2 distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2PDF: [input, 4] # [input, degrees of freedom]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1516 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1839 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1791 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2CDF: [input, 4] #[input, degrees of freedom]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0902 +- 0.001)
    engine.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.7127 +- 0.001)
    engine.action(java.lang.Double.valueOf(8.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.9251 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }
  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2QF: [input, 4] #[input, degrees of freedom]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (2.1947 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (3.3567 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (5.9886 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "handle edge cases properly" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2PDF: [input, 0] #[input, degrees of freedom]
""").head
    engine1.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    engine1.action(java.lang.Double.valueOf(1.6)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2CDF: [input, 0] #[input, degrees of freedom]
""").head
    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine2.action(java.lang.Double.valueOf(1.6)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2QF: [input, 0] #[input, degrees of freedom]
""").head
    engine3.action(java.lang.Double.valueOf(0.4)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine3.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
   }


  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine5 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2PDF: [input, -1] #[input, degrees of freedom]
""").head
    evaluating { engine5.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2CDF: [input, -3] #[input, degrees of freedom]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine6 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2QF: [input, -3] #[input, degrees of freedom]
""").head
    evaluating { engine6.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.chi2QF: [input, 3] #[input, degrees of freedom]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine3.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }



//////////////////////// F-DISTRIBUTION ///////
// NOTE: R's df with df1 = 1, and df2 = * and x = 0 incorrectly produces Inf, this version does not.
  "F distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fPDF: [input, 4, 10] #[input, upper degrees of freedom, lower DOF]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2682 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1568 +- 0.001)
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be   (0.000614 +- 0.00001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fCDF: [input, 4, 10] #[input, upper degrees of freedom, lower DOF]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.1)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0200 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.9)).asInstanceOf[java.lang.Double].doubleValue should be    (0.5006 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.9657 +- 0.001)
    engine.action(java.lang.Double.valueOf(100.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.9999 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fQF: [input, 4, 10] #[input, upper degrees of freedom, lower DOF]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0000)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (0.0208 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (0.7158 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (11.282 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fPDF: [input, 0, 10] #[input, upper degrees of freedom, lower DOF]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fCDF: [input, 4, 0] #[input, upper degrees of freedom, lower DOF]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fQF: [input, 0, 10] #[input, upper degrees of freedom, lower DOF]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.fQF: [input, 4, 10] #[input, upper degrees of freedom, lower DOF]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


//////////////////////// GAMMA-DISTRIBUTION
  "Gamma distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaPDF: [input, 3, 3] #[input, shape, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0133 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0380 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0781 +- 0.00001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaCDF: [input, 3, 3] #[input, shape, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(3.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0803 +- 0.001)
    engine.action(java.lang.Double.valueOf(6.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.3233 +- 0.001)
    engine.action(java.lang.Double.valueOf(10.00)).asInstanceOf[java.lang.Double].doubleValue should be   (0.6472 +- 0.001)
    engine.action(java.lang.Double.valueOf(100.0)).asInstanceOf[java.lang.Double].doubleValue should be    (1.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaQF: [input, 3, 3] #[input, shape, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0000)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (0.5716 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (6.8552 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (33.687 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "handle edge cases properly" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaPDF: [input, 0, 3] #[input, shape, scale]
""").head

    engine1.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    engine1.action(java.lang.Double.valueOf(1.3)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaCDF: [input, 3, 0] #[input, shape, scale]
""").head

    engine2.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine2.action(java.lang.Double.valueOf(1.3)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaQF: [input, 0, 3] #[input, shape, scale]
""").head

    engine3.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine3.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaPDF: [input, -1.3, -3] #[input, shape, scale]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaCDF: [input, -3, 1] #[input, shape, scale]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaQF: [input, -1, 3.0] #[input, shape, scale]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.gammaQF: [input, 2, 3] #[input, shape, scale]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }




//////////////////////// BETA-DISTRIBUTION
  "beta distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaPDF: [input, 4, 3] #[input, shape1, shape2]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.100)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0486 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.800)).asInstanceOf[java.lang.Double].doubleValue should be  (1.2288 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.00001)
    engine.action(java.lang.Double.valueOf(9.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaCDF: [input, 4, 3] #[input, shape1, shape2]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.100)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0013 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.900)).asInstanceOf[java.lang.Double].doubleValue should be    (0.9842 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be    (1.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaQF: [input, 4, 3] #[input, shape1, shape2]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0000)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (0.0939 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (0.5292 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (0.9621 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (1.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaPDF: [input, 0, 3] #[input, shape1, shape2]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaCDF: [input, 4, -3] #[input, shape1, shape2]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaQF: [input, -4, 0] #[input, shape1, shape2]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.betaQF: [input, 4, 3] #[input, shape1, shape2]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


//////////////////////// CAUCHY-DISTRIBUTION
  "cauchy distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyPDF: [input, 4.0, 3.0] #[input, location, scale]
""").head

    engine.action(java.lang.Double.valueOf(-3.00)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0165 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0382 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.500)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0449 +- 0.001)
    engine.action(java.lang.Double.valueOf(10.00)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0212 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyCDF: [input, 4, 3] #[input, location, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2048 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.100)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2087 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.900)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2448 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.5000 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0396 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyQF: [input, 4, 3] #[input, location, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (-950.926 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (3.0252   +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (958.926  +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyPDF: [input, 4, -3] #[input, location, scale]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyCDF: [input, 4, 0] #[input, location, scale]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyQF: [input, 4, -1] #[input, location, scale]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.cauchyQF: [input, 4, 3] #[input, location, scale]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


//////////////////////// LOGNORMAL-DISTRIBUTION
  "lognormal distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalPDF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0539 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0849 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0826 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalCDF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.900)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0176 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2697 +- 0.001)
    engine.action(java.lang.Double.valueOf(100.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.9954 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalQF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (0.3361 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (5.7354 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (162.43 +- 0.1)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalPDF: [input, 2.0, -3.0] #[input, meanlog, sdlog]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalCDF: [input, 2.0, 0.0] #[input, meanlog, sdlog]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalQF: [input, 2.0, -1.0] #[input, meanlog, sdlog]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.lognormalQF: [input, 2.0, 1.0] #[input, meanlog, sdlog]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


//////////////////////// STUDENTT-DISTRIBUTION
  "studentt distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tPDF: [input, 2] #[input, degrees of freedom]
""").head

    engine.action(java.lang.Double.valueOf(-1.00)).asInstanceOf[java.lang.Double].doubleValue should be  (0.1924 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.1924 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0680 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0131 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tCDF: [input, 2] #[input, degrees of freedom]
""").head

    engine.action(java.lang.Double.valueOf(-0.90)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2315 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.5000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.900)).asInstanceOf[java.lang.Double].doubleValue should be    (0.7684 +- 0.001)
    engine.action(java.lang.Double.valueOf(100.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.9999 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tQF: [input, 2] #[input, degrees of freedom]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.NEGATIVE_INFINITY)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (-22.33 +- 0.1)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (-.2887 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (22.327 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tPDF: [input, -2] #[input, degrees of freedom]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tCDF: [input, -1] #[input, degrees of freedom]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tQF: [input, 0] #[input, degrees of freedom]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.tQF: [input, 2] #[input, degrees of freedom]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


///////////// BINOMIAL DISTRIBUTION TESTS /////////////////
  "binomial distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.binomialPDF: [input, 4, .4]  #[input, size, prob]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1296 +- 0.001)
    engine.action(java.lang.Integer.valueOf(1)).asInstanceOf[java.lang.Double].doubleValue should be    (0.3456 +- 0.001)
    engine.action(java.lang.Integer.valueOf(2)).asInstanceOf[java.lang.Double].doubleValue should be    (0.3456 +- 0.001)
    engine.action(java.lang.Integer.valueOf(10)).asInstanceOf[java.lang.Double].doubleValue should be   (0.0000 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.binomialCDF: [input, 4, .4]  #[input, size, prob]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1296 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.8208 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Double].doubleValue should be    (0.8208 +- 0.001)
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be   (1.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(-10.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.binomialQF: [input, 4, .4]  #[input, size, prob]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (2.0)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (2.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (4.0)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have correct behavior handling edge cases" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.binomialPDF: [input, 4, 0.0]  #[input, size, prob]
""").head

    engine1.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine1.action(java.lang.Integer.valueOf(1)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.binomialCDF: [input, 4, 0.0]  #[input, size, prob]
""").head

    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (1.0)
    engine2.action(java.lang.Double.valueOf(-1.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be  (1.0)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.binomialQF: [input, 4, 0.0]  #[input, size, prob]
""").head
    engine3.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine3.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine3.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be  (4.0)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.binomialPDF: [input, -4, 0.4]  #[input, size, prob]
""").head
    evaluating { engine1.action(java.lang.Integer.valueOf(5)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.binomialCDF: [input, 4, 1.1]  #[input, size, prob]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(4.0)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.binomialQF: [input, 4, 0.4]  #[input, size, prob]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]
    }


//////////////////////// UNIFORM-DISTRIBUTION
  "uniform distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformPDF: [input, 1.0, 3.0] #[input, min, max]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.5000 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.5000 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformCDF: [input, 1.0, 3.0] #[input, min, max]
""").head

    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.500)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2500 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.5000 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.300)).asInstanceOf[java.lang.Double].doubleValue should be    (0.6500 +- 0.001)
    engine.action(java.lang.Double.valueOf(5.000)).asInstanceOf[java.lang.Double].doubleValue should be    (1.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformQF: [input, 1.0, 3.0] #[input, min, max]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (1.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (1.0020 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (1.8000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (2.9980 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (3.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformPDF: [input, 5.0, 3.0] #[input, min, max]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(2.0)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformCDF: [input, 4.0, 3.0] #[input, min, max]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(2.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformQF: [input, 3.0, 3.0] #[input, min, max]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.uniformQF: [input, 1.0, 3.0] #[input, min, max]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


///////////// GEOMETRIC DISTRIBUTION TESTS /////////////////
  "geometric distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.geometricPDF: [input, 0.4] #[input, probability of success]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.4000 +- 0.001)
    engine.action(java.lang.Integer.valueOf(1)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2400 +- 0.001)
    engine.action(java.lang.Integer.valueOf(4)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0518 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0 +- 0.001)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.geometricCDF: [input, 0.4] #[input, probability of success]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.4   +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.640 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.784 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.500)).asInstanceOf[java.lang.Double].doubleValue should be    (0.784 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0   +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.geometricQF: [input, 0.4] #[input, probability of success]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (1.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (3.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.geometricPDF: [input, 1.4] #[input, probability of success]
""").head
    evaluating { engine1.action(java.lang.Integer.valueOf(2)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.geometricCDF: [input, -0.4] #[input, probability of success]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(2.4)) } should produce [PFARuntimeException]

val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.geometricQF: [input, -0.4] #[input, probability of success]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.geometricQF: [input, 0.4] #[input, probability of success]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }


///////////// HYPERGEOMETRIC DISTRIBUTION TESTS /////////////////
  "hypergeometric distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.hypergeometricPDF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0219 +- 0.001)
    engine.action(java.lang.Integer.valueOf(1)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2198 +- 0.001)
    engine.action(java.lang.Integer.valueOf(4)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.hypergeometricCDF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0219 +- 0.001)
    engine.action(java.lang.Integer.valueOf(1)).asInstanceOf[java.lang.Double].doubleValue should be    (0.2418 +- 0.001)
    engine.action(java.lang.Integer.valueOf(2)).asInstanceOf[java.lang.Double].doubleValue should be    (0.7363 +- 0.001)
    engine.action(java.lang.Integer.valueOf(2)).asInstanceOf[java.lang.Double].doubleValue should be    (0.7363 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.hypergeometricQF: [input, 10, 5, 3] #[input (number of white balls drawn), n white balls, n black balls, n balls drawn]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be  (2.0)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be  (2.0)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be  (3.0)
    engine.action(java.lang.Double.valueOf(0.99)).asInstanceOf[java.lang.Double].doubleValue should be (3.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be  (3.0)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
// 1. you cant draw more balls than are in the urn
// 2. you cant draw more white balls than are in the urn (this happens with probability zero)
// 3. in QF: you cant input probabilities greater than 1, less than 0

// check 1
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.hypergeometricPDF: [input, 4, 4, 20]
""").head
    evaluating { engine1.action(java.lang.Integer.valueOf(3)) } should produce [PFARuntimeException]

// check 2
    val engine2 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.hypergeometricCDF: [input, 10, 5, 3]
""").head
    engine2.action(java.lang.Integer.valueOf(2000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)

// check 3
    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.hypergeometricQF: [input, 10, 5, 3]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine3.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]
    }


///////////// NEGATIVE-BINOMIAL DISTRIBUTION TESTS /////////////////
  "negativeBinomial distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.negativeBinomialPDF: [input, 5, .7] #[input, size, probability ]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1681 +- 0.001)
    engine.action(java.lang.Integer.valueOf(3)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1588 +- 0.001)
    engine.action(java.lang.Integer.valueOf(6)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0257 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.negativeBinomialCDF: [input, 5, .7] #[input, size, probability ]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.1681 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.4202 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.6471 +- 0.001)
    engine.action(java.lang.Double.valueOf(2.500)).asInstanceOf[java.lang.Double].doubleValue should be    (0.6471 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.negativeBinomialQF: [input, 5, .7] #[input, size, probability ]
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (2.0)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (3.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "handle edge cases properly" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.negativeBinomialPDF: [input, 0, .7] #[input, size, probability ]
""").head

    engine.action(java.lang.Integer.valueOf(0)).asInstanceOf[java.lang.Double].doubleValue should be    (1.0000 +- 0.001)
    engine.action(java.lang.Integer.valueOf(3)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Integer.valueOf(6)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Integer.valueOf(-20)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - prob.dist.negativeBinomialPDF: [input, 4, 0.0] #[input, size, probability ]
""").head
    evaluating { engine1.action(java.lang.Integer.valueOf(5)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.negativeBinomialCDF: [input, 4, 1.1] #[input, size, probability ]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(4.0)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.negativeBinomialQF: [input, 0, -0.4] #[input, size, probability ]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(0.4)) } should produce [PFARuntimeException]

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.negativeBinomialQF: [input, 4, 0.4] #[input, size, probability ]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]
    }


//////////////////////// WEIBULL-DISTRIBUTION
  "weibull distribution" must "have the right PDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullPDF: [input, 2, 4] #[input, shape, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.300)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0373 +- 0.001)
    engine.action(java.lang.Double.valueOf(5.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.1310 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(9.000)).asInstanceOf[java.lang.Double].doubleValue should be  (0.0071 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right CDF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullCDF: [input, 2, 4] #[input, shape, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.100)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0006 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.900)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0494 +- 0.001)
    engine.action(java.lang.Double.valueOf(4.000)).asInstanceOf[java.lang.Double].doubleValue should be    (0.6321 +- 0.001)
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[java.lang.Double].doubleValue should be    (0.0000 +- 0.001)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "have the right QF" taggedAs(Lib1, Lib1ProbDist) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullQF: [input, 2, 4] #[input, shape, scale]
""").head

    engine.action(java.lang.Double.valueOf(0.000)).asInstanceOf[java.lang.Double].doubleValue should be (0.0000)
    engine.action(java.lang.Double.valueOf(0.001)).asInstanceOf[java.lang.Double].doubleValue should be (0.1265 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.400)).asInstanceOf[java.lang.Double].doubleValue should be (2.8589 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.999)).asInstanceOf[java.lang.Double].doubleValue should be (10.513 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.000)).asInstanceOf[java.lang.Double].doubleValue should be (java.lang.Double.POSITIVE_INFINITY)
    java.lang.Double.isNaN(engine.action(java.lang.Double.valueOf(Double.NaN)).asInstanceOf[java.lang.Double].doubleValue)
    }

  it must "raise the correct exceptions" taggedAs(Lib1, Lib1ProbDist) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullPDF: [input, -2, 4] #[input, shape, scale]
""").head
    evaluating { engine1.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullCDF: [input, 2, 0] #[input, shape, scale]
""").head
    evaluating { engine2.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullQF: [input, 0, 4] #[input, shape, scale]
""").head
    evaluating { engine3.action(java.lang.Double.valueOf(1.4)) } should produce [PFARuntimeException]

val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - prob.dist.weibullQF: [input, 2, 4] #[input, shape, scale]
""").head
    evaluating { engine4.action(java.lang.Double.valueOf(-0.4)) } should produce [PFARuntimeException]
    evaluating { engine4.action(java.lang.Double.valueOf(1.4)) } should produce  [PFARuntimeException]
    }
}
