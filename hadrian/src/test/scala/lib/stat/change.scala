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

package test.scala.lib.stat.change

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
class LibStatChangeSuite extends FlatSpec with Matchers {
  "trigger" must "trigger on hand-made values" taggedAs(Lib, LibStatChange) in {
    val engine = PFAEngine.fromYaml("""
input: boolean
output: History
cells:
  history:
    type:
      type: record
      name: History
      fields:
        - {name: numEvents, type: int}
        - {name: numRuns, type: int}
        - {name: currentRun, type: int}
        - {name: longestRun, type: int}
    init:
      numEvents: 0
      numRuns: 0
      currentRun: 0
      longestRun: 0
action:
  - cell: history
    to:
      params: [{history: History}]
      ret: History
      do: {stat.change.updateTrigger: [input, history]}
  - cell: history
""").head

    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 0, "numRuns": 0, "currentRun": 0, "longestRun": 0}""") 
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 0, "numRuns": 0, "currentRun": 0, "longestRun": 0}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 1, "numRuns": 1, "currentRun": 1, "longestRun": 1}""") 
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 1, "numRuns": 1, "currentRun": 0, "longestRun": 1}""") 
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 1, "numRuns": 1, "currentRun": 0, "longestRun": 1}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 2, "numRuns": 2, "currentRun": 1, "longestRun": 1}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 3, "numRuns": 2, "currentRun": 2, "longestRun": 2}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 4, "numRuns": 2, "currentRun": 3, "longestRun": 3}""") 
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 4, "numRuns": 2, "currentRun": 0, "longestRun": 3}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 5, "numRuns": 3, "currentRun": 1, "longestRun": 3}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 6, "numRuns": 3, "currentRun": 2, "longestRun": 3}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 7, "numRuns": 3, "currentRun": 3, "longestRun": 3}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 8, "numRuns": 3, "currentRun": 4, "longestRun": 4}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 9, "numRuns": 3, "currentRun": 5, "longestRun": 5}""") 
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 10, "numRuns": 3, "currentRun": 6, "longestRun": 6}""")
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 10, "numRuns": 3, "currentRun": 0, "longestRun": 6}""")
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 10, "numRuns": 3, "currentRun": 0, "longestRun": 6}""")
    engine.action(java.lang.Boolean.valueOf(true)).toString should be ("""{"numEvents": 11, "numRuns": 4, "currentRun": 1, "longestRun": 6}""")
    engine.action(java.lang.Boolean.valueOf(false)).toString should be ("""{"numEvents": 11, "numRuns": 4, "currentRun": 0, "longestRun": 6}""")

  }

  it must "do a Shewhart example" taggedAs(Lib, LibStatChange) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
cells:
  threshold:
    type: double
    init: 5.0
  minRunLength:
    type: int
    init: 5
  example:
    type:
      type: record
      name: MeanTrigger
      fields:
        - {name: count, type: double}
        - {name: mean, type: double}
        - {name: numEvents, type: int}
        - {name: numRuns, type: int}
        - {name: currentRun, type: int}
        - {name: longestRun, type: int}
    init:
      count: 0.0
      mean: 0.0
      numEvents: 0
      numRuns: 0
      currentRun: 0
      longestRun: 0
action:
  - cell: example
    to:
      params: [{x: MeanTrigger}]
      ret: MeanTrigger
      do:
        - let: {y: {stat.sample.update: [input, 1.0, x]}}
        - stat.change.updateTrigger: [{">": [y.mean, {cell: threshold}]}, y]
  - ">":
    - cell: example
      path: [[longestRun]]
    - cell: minRunLength
""").head

    engine.action(java.lang.Double.valueOf(1.3)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(1.6)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(3.5)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(8.9)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(13.4)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(15.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(12.8)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(14.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(16.3)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(14.4)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(12.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(java.lang.Double.valueOf(16.8)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(java.lang.Double.valueOf(15.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(java.lang.Double.valueOf(14.3)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  "zValue" must "compute z-values" taggedAs(Lib, LibStatChange) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - stat.change.zValue:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: R, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
      - false
""").head

    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (-5.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (2.50 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[java.lang.Double].doubleValue should be (-1.00 +- 0.01)
 
    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - stat.change.zValue:
      - input
      - value: {count: 21, mean: 10, variance: 4.0}
        type: {type: record, name: R, namespace: what.ever, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
      - true
""").head

    engine2.action(java.lang.Double.valueOf(10.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(12.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.02 +- 0.01)
    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (-5.12 +- 0.01)
    engine2.action(java.lang.Double.valueOf(15.0)).asInstanceOf[java.lang.Double].doubleValue should be (2.56 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[java.lang.Double].doubleValue should be (-1.02 +- 0.01)
 }

  "updateCUSUM" must "update CUSUM" taggedAs(Lib, LibStatChange) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  last:
    type: double
    init: 0.0
action:
  - cell: last
    to:
      params: [{x: double}]
      ret: double
      do: {stat.change.updateCUSUM: [input, x, 0.0]}
  - cell: last
""").head

    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be (3.0)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be (5.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (6.0)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (6.0)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (6.0)
    engine.action(java.lang.Double.valueOf(-1.0)).asInstanceOf[java.lang.Double].doubleValue should be (5.0)
    engine.action(java.lang.Double.valueOf(-2.0)).asInstanceOf[java.lang.Double].doubleValue should be (3.0)
    engine.action(java.lang.Double.valueOf(-2.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine.action(java.lang.Double.valueOf(-2.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(-2.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (0.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be (3.0)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[java.lang.Double].doubleValue should be (5.0)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (6.0)
  }

  it must "do a real CUSUM example" taggedAs(Lib, LibStatChange) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
cells:
  threshold:
    type: double
    init: 5.0
  minRunLength:
    type: int
    init: 5
  example:
    type:
      type: record
      name: CusumTrigger
      fields:
        - {name: last, type: double}
        - {name: numEvents, type: int}
        - {name: numRuns, type: int}
        - {name: currentRun, type: int}
        - {name: longestRun, type: int}
    init:
      last: 0.0
      numEvents: 0
      numRuns: 0
      currentRun: 0
      longestRun: 0
action:
  - cell: example
    to:
      params: [{x: CusumTrigger}]
      ret: CusumTrigger
      do:
        - let:
            llr:
              "-":
                - prob.dist.gaussianLL: [input, 15.0, 3.0]
                - prob.dist.gaussianLL: [input, 2.0, 5.0]
        - let:
            y:
              attr: x
              path: [[last]]
              to: {stat.change.updateCUSUM: [llr, x.last, 0.0]}
        - stat.change.updateTrigger: [{">": [y.last, {cell: threshold}]}, y]
  - ">":
    - cell: example
      path: [[longestRun]]
    - cell: minRunLength
""").head

    engine.action(java.lang.Double.valueOf(1.3)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(2.5)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(1.6)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(3.5)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(8.9)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(13.4)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(15.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(12.8)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(14.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(16.3)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(14.4)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(java.lang.Double.valueOf(12.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(java.lang.Double.valueOf(16.8)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(java.lang.Double.valueOf(15.2)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(java.lang.Double.valueOf(14.3)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

}
