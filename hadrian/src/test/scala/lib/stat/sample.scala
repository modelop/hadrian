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

package test.scala.lib.stat.sample

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
class LibStatSampleSuite extends FlatSpec with Matchers {
  "update" must "accumulate a counter" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}]}
    init: {count: 0}
action:
  attr:
    cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  path: [[count]]
""").head

    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (1.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (2.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (4.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (5.0 +- 0.01)
  }

  it must "accumulate a mean" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: double}]}
    init: {"count": 0.0, "mean": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[mean]]}
""").head

    engine.action(java.lang.Double.valueOf(3.2)).asInstanceOf[Double] should be (3.2 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (3.3 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.5)).asInstanceOf[Double] should be (3.7 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.2)).asInstanceOf[Double] should be (3.325 +- 0.01)
    engine.action(java.lang.Double.valueOf(9.7)).asInstanceOf[Double] should be (4.6 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (4.4 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.5)).asInstanceOf[Double] should be (4.557 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[Double] should be (4.25 +- 0.01)
  }

  it must "accumulate a variance" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
""").head

    engine.action(java.lang.Double.valueOf(3.2)).asInstanceOf[Double] should be (0.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (0.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.5)).asInstanceOf[Double] should be (0.326 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.2)).asInstanceOf[Double] should be (0.6668 +- 0.01)
    engine.action(java.lang.Double.valueOf(9.7)).asInstanceOf[Double] should be (7.036 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (6.0633 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.5)).asInstanceOf[Double] should be (5.345 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[Double] should be (5.3375 +- 0.01)
  }

  it must "handle error cases" taggedAs(Lib, LibStatSample) in {
    evaluating { PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
""") } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
""") } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: int}, {name: variance, type: double}]}
    init: {"count": 0.0, "mean": 0.0, "variance": 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [input, 1.0, state]}
  - {cell: state, path: [[variance]]}
""") } should produce [PFASemanticException]

    val engine = PFAEngine.fromYaml("""
input: "null"
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}, {name: hello, type: double}]}
    init: {count: 0, mean: 0, variance: 0, hello: 12}
action:
  attr:
    cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.update: [1.0, 1.0, state]}
  path: [[hello]]
""").head

    engine.action(null) should be (12.0)
  }

  "updateCovariance" must "accumulate a covariance using arrays" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  state:
    type:
      type: record
      name: State
      fields:
        - {name: count, type: double}
        - {name: mean, type: {type: array, items: double}}
        - {name: covariance, type: {type: array, items: {type: array, items: double}}}
    init:
      count: 0
      mean: [0, 0]
      covariance: [[0, 0], [0, 0]]
action:
  attr:
    cell: state
    to:
      params: [{state: State}]
      ret: State
      do:
        stat.sample.updateCovariance:
          - input
          - 1.0
          - state
  path: [{string: covariance}, 0, 1]
""").head

    engine.action(engine.jsonInput("""[12, 85]""")).asInstanceOf[java.lang.Double].doubleValue should be (0.00 +- 0.01)
    engine.action(engine.jsonInput("""[32, 40]""")).asInstanceOf[java.lang.Double].doubleValue should be (-225.00 +- 0.01)
    engine.action(engine.jsonInput("""[4, 90]""")).asInstanceOf[java.lang.Double].doubleValue should be (-260.00 +- 0.01)
    engine.action(engine.jsonInput("""[3, 77]""")).asInstanceOf[java.lang.Double].doubleValue should be (-208.00 +- 0.01)
    engine.action(engine.jsonInput("""[7, 87]""")).asInstanceOf[java.lang.Double].doubleValue should be (-179.28 +- 0.01)
    engine.action(engine.jsonInput("""[88, 2]""")).asInstanceOf[java.lang.Double].doubleValue should be (-932.50 +- 0.01)
    engine.action(engine.jsonInput("""[56, 5]""")).asInstanceOf[java.lang.Double].doubleValue should be (-1026.12 +- 0.01)
  }

  it must "accumulate a covariance using maps" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
cells:
  state:
    type:
      type: record
      name: State
      fields:
        - {name: count, type: {type: map, values: {type: map, values: double}}}
        - {name: mean, type: {type: map, values: double}}
        - {name: covariance, type: {type: map, values: {type: map, values: double}}}
    init:
      count: {x: {x: 0, y: 0}, y: {x: 0, y: 0}}
      mean: {x: 0, y: 0}
      covariance: {x: {x: 0, y: 0}, y: {x: 0, y: 0}}
action:
  attr:
    cell: state
    to:
      params: [{state: State}]
      ret: State
      do:
        stat.sample.updateCovariance:
          - input
          - 1.0
          - state
  path: [{string: covariance}, {string: x}, {string: y}]
""").head

    engine.action(engine.jsonInput("""{"x": 12, "y": 85}""")).asInstanceOf[java.lang.Double].doubleValue should be (0.00 +- 0.01)   
    engine.action(engine.jsonInput("""{"x": 32, "y": 40}""")).asInstanceOf[java.lang.Double].doubleValue should be (-225.00 +- 0.01)
    engine.action(engine.jsonInput("""{"x": 4, "y": 90}""")).asInstanceOf[java.lang.Double].doubleValue should be (-260.00 +- 0.01) 
    engine.action(engine.jsonInput("""{"x": 3, "y": 77}""")).asInstanceOf[java.lang.Double].doubleValue should be (-208.00 +- 0.01) 
    engine.action(engine.jsonInput("""{"x": 7, "y": 87}""")).asInstanceOf[java.lang.Double].doubleValue should be (-179.28 +- 0.01) 
    engine.action(engine.jsonInput("""{"x": 88, "y": 2}""")).asInstanceOf[java.lang.Double].doubleValue should be (-932.50 +- 0.01) 
    engine.action(engine.jsonInput("""{"x": 56, "y": 5}""")).asInstanceOf[java.lang.Double].doubleValue should be (-1026.12 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: double
cells:
  state:
    type:
      type: record
      name: State
      fields:
        - {name: count, type: {type: map, values: {type: map, values: double}}}
        - {name: mean, type: {type: map, values: double}}
        - {name: covariance, type: {type: map, values: {type: map, values: double}}}
    init:
      count: {}
      mean: {}
      covariance: {}
action:
  attr:
    cell: state
    to:
      params: [{state: State}]
      ret: State
      do:
        stat.sample.updateCovariance:
          - input
          - 1.0
          - state
  path: [{string: covariance}, {string: x}, {string: y}]
""").head

    engine2.action(engine.jsonInput("""{"x": 12, "y": 85}""")).asInstanceOf[java.lang.Double].doubleValue should be (0.00 +- 0.01)   
    engine2.action(engine.jsonInput("""{"x": 32, "y": 40}""")).asInstanceOf[java.lang.Double].doubleValue should be (-225.00 +- 0.01)
    engine2.action(engine.jsonInput("""{"x": 4, "y": 90}""")).asInstanceOf[java.lang.Double].doubleValue should be (-260.00 +- 0.01) 
    engine2.action(engine.jsonInput("""{"x": 3, "y": 77}""")).asInstanceOf[java.lang.Double].doubleValue should be (-208.00 +- 0.01) 
    engine2.action(engine.jsonInput("""{"w": 999, "z": 999}""")).asInstanceOf[java.lang.Double].doubleValue should be (-208.00 +- 0.01) 
    engine2.action(engine.jsonInput("""{"w": 999, "z": 999}""")).asInstanceOf[java.lang.Double].doubleValue should be (-208.00 +- 0.01) 
    engine2.action(engine.jsonInput("""{"w": 999, "z": 999}""")).asInstanceOf[java.lang.Double].doubleValue should be (-208.00 +- 0.01) 
    engine2.action(engine.jsonInput("""{"x": 7, "y": 87}""")).asInstanceOf[java.lang.Double].doubleValue should be (-179.28 +- 0.01) 
    engine2.action(engine.jsonInput("""{"x": 88, "y": 2}""")).asInstanceOf[java.lang.Double].doubleValue should be (-932.50 +- 0.01) 
    engine2.action(engine.jsonInput("""{"x": 56, "y": 5}""")).asInstanceOf[java.lang.Double].doubleValue should be (-1026.12 +- 0.01)
    engine2.action(engine.jsonInput("""{"w": 999, "z": 999}""")).asInstanceOf[java.lang.Double].doubleValue should be (-1026.12 +- 0.01)
    engine2.action(engine.jsonInput("""{"w": 999, "z": 999}""")).asInstanceOf[java.lang.Double].doubleValue should be (-1026.12 +- 0.01)
  }

  "updateWindow" must "accumulate a counter" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}]}}
    init: []
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[count]]}
""").head

    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (1.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (2.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}]}}
    init: [{x: 0.0, w: 0.0, count: 0.0}]
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[count]]}
""").head

    engine2.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (1.0 +- 0.01)
    engine2.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (2.0 +- 0.01)
    engine2.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)
    engine2.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)
    engine2.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (3.0 +- 0.01)
  }

  it must "accumulate a mean" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}]}}
    init: []
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[mean]]}
""").head

    engine.action(java.lang.Double.valueOf(3.2)).asInstanceOf[Double] should be (3.2 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (3.3 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.5)).asInstanceOf[Double] should be (3.7 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.2)).asInstanceOf[Double] should be (3.3666 +- 0.01)
    engine.action(java.lang.Double.valueOf(9.7)).asInstanceOf[Double] should be (5.4666 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (5.1 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.5)).asInstanceOf[Double] should be (6.2 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[Double] should be (3.666 +- 0.01)
  }

  it must "accumulate a variance" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}}
    init: []
action:
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, 3]}
  - {attr: {a.last: {cell: state}}, path: [[variance]]}
""").head

    engine.action(java.lang.Double.valueOf(3.2)).asInstanceOf[Double] should be (0.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (0.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.5)).asInstanceOf[Double] should be (0.326 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.2)).asInstanceOf[Double] should be (0.8822 +- 0.01)
    engine.action(java.lang.Double.valueOf(9.7)).asInstanceOf[Double] should be (9.8422 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (10.82 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.5)).asInstanceOf[Double] should be (6.86 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[Double] should be (1.96 +- 0.01)
  }

  it must "accumulate a mean with a sudden window shrink" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  counter:
    type: int
    init: 0
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}]}}
    init: []
action:
  - cell: counter
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, 1]}
  - let:
      windowSize:
        if: {"<": [{cell: counter}, 6]}
        then: 1000
        else: 3
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, windowSize]}
  - {attr: {a.last: {cell: state}}, path: [[mean]]}
""").head

    engine.action(java.lang.Double.valueOf(3.2)).asInstanceOf[Double] should be (3.2 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (3.3 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.5)).asInstanceOf[Double] should be (3.7 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.2)).asInstanceOf[Double] should be (3.325 +- 0.01)
    engine.action(java.lang.Double.valueOf(9.7)).asInstanceOf[Double] should be (4.6 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (5.1 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.5)).asInstanceOf[Double] should be (6.2 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[Double] should be (3.666 +- 0.01)
  }

  it must "accumulate a variance with a sudden window shrink" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  counter:
    type: int
    init: 0
  state:
    type: {type: array, items: {type: record, name: State, fields: [{name: x, type: double}, {name: w, type: double}, {name: count, type: double}, {name: mean, type: double}, {name: variance, type: double}]}}
    init: []
action:
  - cell: counter
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, 1]}
  - let:
      windowSize:
        if: {"<": [{cell: counter}, 6]}
        then: 1000
        else: 3
  - cell: state
    to:
      params: [{state: {type: array, items: State}}]
      ret: {type: array, items: State}
      do: {stat.sample.updateWindow: [input, 1.0, state, windowSize]}
  - {attr: {a.last: {cell: state}}, path: [[variance]]}
""").head

    engine.action(java.lang.Double.valueOf(3.2)).asInstanceOf[Double] should be (0.0 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (0.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.5)).asInstanceOf[Double] should be (0.326 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.2)).asInstanceOf[Double] should be (0.6668 +- 0.01)
    engine.action(java.lang.Double.valueOf(9.7)).asInstanceOf[Double] should be (7.036 +- 0.01)
    engine.action(java.lang.Double.valueOf(3.4)).asInstanceOf[Double] should be (10.82 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.5)).asInstanceOf[Double] should be (6.86 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.1)).asInstanceOf[Double] should be (1.96 +- 0.01)
  }

  "updateEWMA" must "accumulate an EWMA" taggedAs(Lib, LibStatSample) in {
    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {mean: 50.0, variance: 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
  - {cell: state, path: [[mean]]}
""").head

    // example data from http://www.itl.nist.gov/div898/handbook/pmc/section3/pmc324.htm
    engine2.action(java.lang.Double.valueOf(52.0)).asInstanceOf[Double] should be (50.60 +- 0.01)
    engine2.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (49.52 +- 0.01)
    engine2.action(java.lang.Double.valueOf(53.0)).asInstanceOf[Double] should be (50.56 +- 0.01)
    engine2.action(java.lang.Double.valueOf(49.3)).asInstanceOf[Double] should be (50.18 +- 0.01)
    engine2.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (50.16 +- 0.01)
    engine2.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (49.21 +- 0.01)
    engine2.action(java.lang.Double.valueOf(51.0)).asInstanceOf[Double] should be (49.75 +- 0.01)
    engine2.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (49.85 +- 0.01)
    engine2.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (50.26 +- 0.01)
    engine2.action(java.lang.Double.valueOf(50.5)).asInstanceOf[Double] should be (50.33 +- 0.01)
    engine2.action(java.lang.Double.valueOf(49.6)).asInstanceOf[Double] should be (50.11 +- 0.01)
    engine2.action(java.lang.Double.valueOf(47.6)).asInstanceOf[Double] should be (49.36 +- 0.01)
    engine2.action(java.lang.Double.valueOf(49.9)).asInstanceOf[Double] should be (49.52 +- 0.01)
    engine2.action(java.lang.Double.valueOf(51.3)).asInstanceOf[Double] should be (50.05 +- 0.01)
    engine2.action(java.lang.Double.valueOf(47.8)).asInstanceOf[Double] should be (49.38 +- 0.01)
    engine2.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (49.92 +- 0.01)
    engine2.action(java.lang.Double.valueOf(52.6)).asInstanceOf[Double] should be (50.73 +- 0.01)
    engine2.action(java.lang.Double.valueOf(52.4)).asInstanceOf[Double] should be (51.23 +- 0.01)
    engine2.action(java.lang.Double.valueOf(53.6)).asInstanceOf[Double] should be (51.94 +- 0.01)
    engine2.action(java.lang.Double.valueOf(52.1)).asInstanceOf[Double] should be (51.99 +- 0.01)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {mean: 50.0, variance: 0.0}
action:
  - cell: state
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
  - {cell: state, path: [[variance]]}
""").head

    engine3.action(java.lang.Double.valueOf(52.0)).asInstanceOf[Double] should be (0.840 +- 0.01)
    engine3.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (3.309 +- 0.01)
    engine3.action(java.lang.Double.valueOf(53.0)).asInstanceOf[Double] should be (4.859 +- 0.01)
    engine3.action(java.lang.Double.valueOf(49.3)).asInstanceOf[Double] should be (3.737 +- 0.01)
    engine3.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (2.617 +- 0.01)
    engine3.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (3.928 +- 0.01)
    engine3.action(java.lang.Double.valueOf(51.0)).asInstanceOf[Double] should be (3.421 +- 0.01)
    engine3.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (2.421 +- 0.01)
    engine3.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (2.075 +- 0.01)
    engine3.action(java.lang.Double.valueOf(50.5)).asInstanceOf[Double] should be (1.465 +- 0.01)
    engine3.action(java.lang.Double.valueOf(49.6)).asInstanceOf[Double] should be (1.137 +- 0.01)
    engine3.action(java.lang.Double.valueOf(47.6)).asInstanceOf[Double] should be (2.120 +- 0.01)
    engine3.action(java.lang.Double.valueOf(49.9)).asInstanceOf[Double] should be (1.546 +- 0.01)
    engine3.action(java.lang.Double.valueOf(51.3)).asInstanceOf[Double] should be (1.747 +- 0.01)
    engine3.action(java.lang.Double.valueOf(47.8)).asInstanceOf[Double] should be (2.290 +- 0.01)
    engine3.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (2.300 +- 0.01)
    engine3.action(java.lang.Double.valueOf(52.6)).asInstanceOf[Double] should be (3.113 +- 0.01)
    engine3.action(java.lang.Double.valueOf(52.4)).asInstanceOf[Double] should be (2.766 +- 0.01)
    engine3.action(java.lang.Double.valueOf(53.6)).asInstanceOf[Double] should be (3.117 +- 0.01)
    engine3.action(java.lang.Double.valueOf(52.1)).asInstanceOf[Double] should be (2.187 +- 0.01)

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
pools:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {}
action:
  - pool: state
    path: [{string: dummy}]
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
    init:
      new: {mean: input, variance: 0.0}
      type: State
  - pool: state
    path: [{string: dummy}, {string: mean}]
""").head

    engine4.action(java.lang.Double.valueOf(50.0)).asInstanceOf[Double] should be (50.00 +- 0.01)
    engine4.action(java.lang.Double.valueOf(52.0)).asInstanceOf[Double] should be (50.60 +- 0.01)
    engine4.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (49.52 +- 0.01)
    engine4.action(java.lang.Double.valueOf(53.0)).asInstanceOf[Double] should be (50.56 +- 0.01)
    engine4.action(java.lang.Double.valueOf(49.3)).asInstanceOf[Double] should be (50.18 +- 0.01)
    engine4.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (50.16 +- 0.01)
    engine4.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (49.21 +- 0.01)
    engine4.action(java.lang.Double.valueOf(51.0)).asInstanceOf[Double] should be (49.75 +- 0.01)
    engine4.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (49.85 +- 0.01)
    engine4.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (50.26 +- 0.01)
    engine4.action(java.lang.Double.valueOf(50.5)).asInstanceOf[Double] should be (50.33 +- 0.01)
    engine4.action(java.lang.Double.valueOf(49.6)).asInstanceOf[Double] should be (50.11 +- 0.01)
    engine4.action(java.lang.Double.valueOf(47.6)).asInstanceOf[Double] should be (49.36 +- 0.01)
    engine4.action(java.lang.Double.valueOf(49.9)).asInstanceOf[Double] should be (49.52 +- 0.01)
    engine4.action(java.lang.Double.valueOf(51.3)).asInstanceOf[Double] should be (50.05 +- 0.01)
    engine4.action(java.lang.Double.valueOf(47.8)).asInstanceOf[Double] should be (49.38 +- 0.01)
    engine4.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (49.92 +- 0.01)
    engine4.action(java.lang.Double.valueOf(52.6)).asInstanceOf[Double] should be (50.73 +- 0.01)
    engine4.action(java.lang.Double.valueOf(52.4)).asInstanceOf[Double] should be (51.23 +- 0.01)
    engine4.action(java.lang.Double.valueOf(53.6)).asInstanceOf[Double] should be (51.94 +- 0.01)
    engine4.action(java.lang.Double.valueOf(52.1)).asInstanceOf[Double] should be (51.99 +- 0.01)

    val engine4Var = PFAEngine.fromYaml("""
input: double
output: double
pools:
  state:
    type: {type: record, name: State, fields: [{name: mean, type: double}, {name: variance, type: double}]}
    init: {}
action:
  - pool: state
    path: [{string: dummy}]
    to:
      params: [{state: State}]
      ret: State
      do: {stat.sample.updateEWMA: [input, 0.3, state]}
    init:
      new: {mean: input, variance: 0.0}
      type: State
  - pool: state
    path: [{string: dummy}, {string: variance}]
""").head

    engine4Var.action(java.lang.Double.valueOf(50.0)).asInstanceOf[Double] should be (0.000 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(52.0)).asInstanceOf[Double] should be (0.840 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (3.310 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(53.0)).asInstanceOf[Double] should be (4.860 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(49.3)).asInstanceOf[Double] should be (3.737 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (2.618 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(47.0)).asInstanceOf[Double] should be (3.929 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(51.0)).asInstanceOf[Double] should be (3.422 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(50.1)).asInstanceOf[Double] should be (2.421 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (2.075 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(50.5)).asInstanceOf[Double] should be (1.465 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(49.6)).asInstanceOf[Double] should be (1.138 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(47.6)).asInstanceOf[Double] should be (2.121 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(49.9)).asInstanceOf[Double] should be (1.546 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(51.3)).asInstanceOf[Double] should be (1.747 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(47.8)).asInstanceOf[Double] should be (2.290 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(51.2)).asInstanceOf[Double] should be (2.300 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(52.6)).asInstanceOf[Double] should be (3.113 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(52.4)).asInstanceOf[Double] should be (2.767 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(53.6)).asInstanceOf[Double] should be (3.117 +- 0.01)
    engine4Var.action(java.lang.Double.valueOf(52.1)).asInstanceOf[Double] should be (2.187 +- 0.01)
  }

  "Holt-Winters" must "accumulate a trend" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - cell: state
    path: [{string: level}]
""").head

    engine.action(java.lang.Double.valueOf(50.0)).asInstanceOf[Double] should be (40.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(45.0)).asInstanceOf[Double] should be (50.40 +- 0.01)
    engine.action(java.lang.Double.valueOf(40.0)).asInstanceOf[Double] should be (45.02 +- 0.01)
    engine.action(java.lang.Double.valueOf(35.0)).asInstanceOf[Double] should be (36.73 +- 0.01)
    engine.action(java.lang.Double.valueOf(30.0)).asInstanceOf[Double] should be (29.97 +- 0.01)
    engine.action(java.lang.Double.valueOf(25.0)).asInstanceOf[Double] should be (24.63 +- 0.01)
    engine.action(java.lang.Double.valueOf(20.0)).asInstanceOf[Double] should be (19.80 +- 0.01)
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[Double] should be (14.96 +- 0.01)
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[Double] should be (10.02 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.0)).asInstanceOf[Double] should be (5.02 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[Double] should be (0.01 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - cell: state
    path: [{string: trend}]
""").head

    engine2.action(java.lang.Double.valueOf(50.0)).asInstanceOf[Double] should be (32.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(45.0)).asInstanceOf[Double] should be (14.72 +- 0.01)
    engine2.action(java.lang.Double.valueOf(40.0)).asInstanceOf[Double] should be (-1.36 +- 0.01)
    engine2.action(java.lang.Double.valueOf(35.0)).asInstanceOf[Double] should be (-6.90 +- 0.01)
    engine2.action(java.lang.Double.valueOf(30.0)).asInstanceOf[Double] should be (-6.79 +- 0.01)
    engine2.action(java.lang.Double.valueOf(25.0)).asInstanceOf[Double] should be (-5.62 +- 0.01)
    engine2.action(java.lang.Double.valueOf(20.0)).asInstanceOf[Double] should be (-4.99 +- 0.01)
    engine2.action(java.lang.Double.valueOf(15.0)).asInstanceOf[Double] should be (-4.87 +- 0.01)
    engine2.action(java.lang.Double.valueOf(10.0)).asInstanceOf[Double] should be (-4.93 +- 0.01)
    engine2.action(java.lang.Double.valueOf(5.0)).asInstanceOf[Double] should be (-4.99 +- 0.01)
    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[Double] should be (-5.01 +- 0.01)
  }

  it must "forecast a trend" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - stat.sample.forecast1HoltWinters: {cell: state}
""").head

    engine.action(java.lang.Double.valueOf(50.0)).asInstanceOf[Double] should be (72.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(45.0)).asInstanceOf[Double] should be (65.12 +- 0.01)
    engine.action(java.lang.Double.valueOf(40.0)).asInstanceOf[Double] should be (43.67 +- 0.01)
    engine.action(java.lang.Double.valueOf(35.0)).asInstanceOf[Double] should be (29.83 +- 0.01)
    engine.action(java.lang.Double.valueOf(30.0)).asInstanceOf[Double] should be (23.17 +- 0.01)
    engine.action(java.lang.Double.valueOf(25.0)).asInstanceOf[Double] should be (19.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(20.0)).asInstanceOf[Double] should be (14.81 +- 0.01)
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[Double] should be (10.09 +- 0.01)
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[Double] should be (5.09 +- 0.01)
    engine.action(java.lang.Double.valueOf(5.0)).asInstanceOf[Double] should be (0.03 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[Double] should be (-5.00 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
    init:
      {level: 0.0, trend: 0.0}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWinters: [input, 0.8, 0.8, oldValue]}
  - a.last: {stat.sample.forecastHoltWinters: [3, {cell: state}]}
""").head

    engine2.action(java.lang.Double.valueOf(50.0)).asInstanceOf[Double] should be (136.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(45.0)).asInstanceOf[Double] should be (94.56 +- 0.01)
    engine2.action(java.lang.Double.valueOf(40.0)).asInstanceOf[Double] should be (40.95 +- 0.01)
    engine2.action(java.lang.Double.valueOf(35.0)).asInstanceOf[Double] should be (16.02 +- 0.01)
    engine2.action(java.lang.Double.valueOf(30.0)).asInstanceOf[Double] should be (9.58 +- 0.01)
    engine2.action(java.lang.Double.valueOf(25.0)).asInstanceOf[Double] should be (7.76 +- 0.01)
    engine2.action(java.lang.Double.valueOf(20.0)).asInstanceOf[Double] should be (4.83 +- 0.01)
    engine2.action(java.lang.Double.valueOf(15.0)).asInstanceOf[Double] should be (0.35 +- 0.01)
    engine2.action(java.lang.Double.valueOf(10.0)).asInstanceOf[Double] should be (-4.77 +- 0.01)
    engine2.action(java.lang.Double.valueOf(5.0)).asInstanceOf[Double] should be (-9.94 +- 0.01)
    engine2.action(java.lang.Double.valueOf(0.0)).asInstanceOf[Double] should be (-15.01 +- 0.01)
  }

  "Holt-Winters periodic" must "accumulate an additive trend" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: HoltWinters
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: false}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - cell: state
""").head

    val inputSequence = Vector(8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0)
    val expectedLevels = Vector(4.41, 5.0301, 4.861461, 3.9758822099999995, 4.188847658099999, 4.488564182540999, 4.641300543221009, 4.452171014507124, 4.417702083098764, 4.468964465539193, 4.542144404518389, 4.521624943415324, 4.501496029430319, 4.50124424117252, 4.521651020236533, 4.5250653738300315, 4.52113444671092, 4.517931202495852, 4.521755238457936, 4.5242575202865805, 4.524388376599926, 4.5232770868680205, 4.523657174863217, 4.524410223942632)
    val expectedTrends = Vector(0.633, 0.62913, 0.3897993, 0.007185872999999787, 0.0689197455299998, 0.13815877920329986, 0.14253205364631272, 0.04303357893825353, 0.01978282583426929, 0.029226692816117207, 0.04241266666504093, 0.023533028334609074, 0.010434445638724935, 0.007228575469767743, 0.011182036548041254, 0.008851731661678497, 0.005016934027441439, 0.0025508805546886693, 0.00293282717690721, 0.002803663572428441, 0.0020018213947037003, 0.0010678880567208248, 8.615480382635968E-4, 8.289983506089451E-4)
    val expectedCycle0s = Vector(3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924, 1.4774493057281883, -0.523496688182195, -2.524384907518901)
    val expectedCycle1s = Vector(1.0, 1.0, 1.0, 3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924)
    val expectedCycle2s = Vector(1.0, 1.0, 3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924, 1.4774493057281883)
    val expectedCycle3s = Vector(1.0, 3.331, 0.9729099999999999, -0.6753149000000003, -1.6782939889999997, 3.7631371077100004, 1.4575832357131007, -0.644701978898908, -2.3747833119564117, 3.600381835982113, 1.5236903045860366, -0.5524001619564409, -2.5069407802694323, 3.508691757110924, 1.5012492134033357, -0.5247259344085236, -2.523252914473972, 3.4818481736712643, 1.4839868390940667, -0.5220523080529946, -2.5241570597053196, 3.4762352784271924, 1.4774493057281883, -0.523496688182195)

    for ((x, i) <- inputSequence.zipWithIndex) {
      val result = engine.action(java.lang.Double.valueOf(x))
      result.asInstanceOf[PFARecord].get("level").asInstanceOf[Double] should be (expectedLevels(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("trend").asInstanceOf[Double] should be (expectedTrends(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](0) should be (expectedCycle0s(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](1) should be (expectedCycle1s(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](2) should be (expectedCycle2s(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](3) should be (expectedCycle3s(i) +- 0.01)
    }
  }

  it must "forecast an additive trend" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: false}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - stat.sample.forecast1HoltWinters: {cell: state}
""").head

    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.04 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (6.66 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (6.25 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (7.31 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.23 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.95 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (3.11 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.26 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.90 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.85 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.21 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.15 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.04 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.96 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.03 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.04 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.03 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.00 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: false}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - a.head: {stat.sample.forecastHoltWinters: [1, {cell: state}]}
""").head

    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.04 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (6.66 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (6.25 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (7.31 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.23 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.95 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (3.11 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.26 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.90 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.85 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.21 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.15 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.04 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.96 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.03 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.04 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.03 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.01 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.01 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.00 +- 0.01)

  }

  it must "accumulate a multiplicative trend" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: HoltWinters
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: true}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - cell: state
""").head

    val inputSequence = Vector(8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0, 8.0, 6.0, 4.0, 2.0)
    val expectedLevels = Vector(4.709999999999999, 5.603099999999999, 5.663990999999998, 4.956855509999998, 5.069345548299841, 5.352449557989157, 5.515749692921031, 5.2872313440581244, 5.260499462297407, 5.321153447406931, 5.407280797054145, 5.375630707249961, 5.355414240440458, 5.3571763764799645, 5.382751618896728, 5.384939928849279, 5.380451760749505, 5.377167967546183, 5.382406885399155, 5.385124017416345, 5.385192034968023, 5.383914333497006, 5.384582834532246, 5.38549356133603)
    val expectedTrends = Vector(0.7229999999999996, 0.7740299999999996, 0.5600882999999997, 0.17992116299999955, 0.15969182558995274, 0.19671548081976165, 0.18669087705339538, 0.062128109278504734, 0.03547011196673803, 0.04302527390957382, 0.055955896630866045, 0.029674100700350883, 0.01470693044739483, 0.010823492125028223, 0.015249017212548925, 0.011330805034549559, 0.00658511309425238, 0.003624441204979981, 0.004108784199377763, 0.0036912885447213458, 0.002604307246808362, 0.0014397046314606797, 0.0012083435525944988, 0.001119058527951486)
    val expectedCycle0s = Vector(1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833, 1.1146186181950057, 0.7428864674776158, 0.3713727943024939)
    val expectedCycle1s = Vector(1.0, 1.0, 1.0, 1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833)
    val expectedCycle2s = Vector(1.0, 1.0, 1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833, 1.1146186181950057)
    val expectedCycle3s = Vector(1.0, 1.6286624203821658, 1.0637522085988116, 0.7355942302874423, 0.463133441426458, 1.5831679222846418, 1.115259086367753, 0.7262358859618385, 0.3867561738332491, 1.5270081211798712, 1.1263434765517717, 0.7383926017314475, 0.373520047372236, 1.4971346274577009, 1.1206280406961695, 0.7426421801075911, 0.37161757200134093, 1.4878910582637028, 1.1163089114241984, 0.7431099735714755, 0.37141589771361433, 1.4857887807214833, 1.1146186181950057, 0.7428864674776158)

    for ((x, i) <- inputSequence.zipWithIndex) {
      val result = engine.action(java.lang.Double.valueOf(x))
      result.asInstanceOf[PFARecord].get("level").asInstanceOf[Double] should be (expectedLevels(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("trend").asInstanceOf[Double] should be (expectedTrends(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](0) should be (expectedCycle0s(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](1) should be (expectedCycle1s(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](2) should be (expectedCycle2s(i) +- 0.01)
      result.asInstanceOf[PFARecord].get("cycle").asInstanceOf[PFAArray[Double]](3) should be (expectedCycle3s(i) +- 0.01)
    }
  }

  it must "forecast a multiplicative trend" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: true}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - stat.sample.forecast1HoltWinters: {cell: state}
""").head

    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.43 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (6.38 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (6.22 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.37 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.56 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.08 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.64 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.47 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.91 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.90 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.11 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.25 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.05 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.96 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.02 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.08 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.04 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.02 +- 0.01)
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.01 +- 0.01)
    engine.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.00 +- 0.01)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: double
cells:
  state:
    type:
      type: record
      name: HoltWinters
      fields:
        - {name: level, type: double}
        - {name: trend, type: double}
        - {name: cycle, type: {type: array, items: double}}
        - {name: multiplicative, type: boolean}
    init:
      {level: 3.0, trend: 0.3, cycle: [1.0, 1.0, 1.0, 1.0], multiplicative: true}
action:
  - cell: state
    to:
      params: [{oldValue: HoltWinters}]
      ret: HoltWinters
      do: {stat.sample.updateHoltWintersPeriodic: [input, 0.3, 0.3, 0.9, oldValue]}
  - a.head: {stat.sample.forecastHoltWinters: [1, {cell: state}]}
""").head

    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.43 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (6.38 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (6.22 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.37 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.56 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.08 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.64 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.47 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (5.91 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.90 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.11 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.25 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.05 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (3.96 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.02 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.08 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.04 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.02 +- 0.01)
    engine2.action(java.lang.Double.valueOf(8.0)).asInstanceOf[Double] should be (6.01 +- 0.01)
    engine2.action(java.lang.Double.valueOf(6.0)).asInstanceOf[Double] should be (4.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(4.0)).asInstanceOf[Double] should be (2.00 +- 0.01)
    engine2.action(java.lang.Double.valueOf(2.0)).asInstanceOf[Double] should be (8.00 +- 0.01)

  }

  it must "fill a simple histogram" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: array, items: double}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: numbins, type: int}
        - {name: low, type: double}
        - {name: high, type: double}
        - {name: values, type: {type: array, items: double}}
    init:
      numbins: 10
      low: -100.0
      high: 0.0
      values: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
action:
  attr:
    cell: histogram
    to:
      params: [{old: Histogram}]
      ret: Histogram
      do: {stat.sample.fillHistogram: [input, 1.0, old]}
  path: [{string: values}]
""").head

    engine.action(java.lang.Double.valueOf(-50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(-80.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(-100.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(-50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 0.0, 1.0, 0.0))
  }

  it must "fill a histogram with overflow counters" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: array, items: double}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: numbins, type: int}
        - {name: low, type: double}
        - {name: high, type: double}
        - {name: values, type: {type: array, items: double}}
        - {name: underflow, type: double}
        - {name: overflow, type: double}
        - {name: nanflow, type: double}
        - {name: infflow, type: double}
    init:
      numbins: 10
      low: -100.0
      high: 0.0
      values: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
      underflow: 0.0
      overflow: 0.0
      nanflow: 0.0
      infflow: 0.0
action:
  - let:
      out:
        cell: histogram
        to:
          params: [{old: Histogram}]
          ret: Histogram
          do: {stat.sample.fillHistogram: [input, 1.0, old]}
  - type: {type: array, items: double}
    new: [out.underflow, out.overflow, out.nanflow, out.infflow]
""").head

    engine.action(java.lang.Double.valueOf(-50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-80.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 1.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-100.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0, 1.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0, 2.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 1.0, 2.0))
  }

  it must "fill a histogram without infflow" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: array, items: double}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: numbins, type: int}
        - {name: low, type: double}
        - {name: high, type: double}
        - {name: values, type: {type: array, items: double}}
        - {name: underflow, type: double}
        - {name: overflow, type: double}
        - {name: nanflow, type: double}
    init:
      numbins: 10
      low: -100.0
      high: 0.0
      values: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
      underflow: 0.0
      overflow: 0.0
      nanflow: 0.0
action:
  - let:
      out:
        cell: histogram
        to:
          params: [{old: Histogram}]
          ret: Histogram
          do: {stat.sample.fillHistogram: [input, 1.0, old]}
  - type: {type: array, items: double}
    new: [out.underflow, out.overflow, out.nanflow]
""").head

    engine.action(java.lang.Double.valueOf(-50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-20.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(-80.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0))
    engine.action(java.lang.Double.valueOf(50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 1.0, 0.0))
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0))
    engine.action(java.lang.Double.valueOf(-100.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0))
    engine.action(java.lang.Double.valueOf(-50.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 2.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 3.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 3.0, 0.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 3.0, 1.0))
  }

  it must "fill a growable histogram" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: array, items: double}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: low, type: double}
        - {name: binsize, type: double}
        - {name: values, type: {type: array, items: double}}
    init:
      low: 10.0
      binsize: 2.0
      values: []
action:
  attr:
    cell: histogram
    to:
      params: [{old: Histogram}]
      ret: Histogram
      do: {stat.sample.fillHistogram: [input, 1.0, old]}
  path: [{string: values}]
""").head

    engine.action(java.lang.Double.valueOf(5.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector())
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0))
    engine.action(java.lang.Double.valueOf(20.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 0.0, 0.0, 0.0, 1.0))
    engine.action(java.lang.Double.valueOf(15.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 1.0))
    engine.action(java.lang.Double.valueOf(5.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 1.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 1.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 1.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 0.0, 1.0, 0.0, 0.0, 1.0))
  }

  it must "fill a variable bin width histogram" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: array, items: double}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: ranges, type: {type: array, items: {type: array, items: double}}}
        - {name: values, type: {type: array, items: double}}
    init:
      ranges: [[0.0, 5.0], [5.0, 10.0], [0.0, 10.0]]
      values: [0.0, 0.0, 0.0]
action:
  attr:
    cell: histogram
    to:
      params: [{old: Histogram}]
      ret: Histogram
      do: {stat.sample.fillHistogram: [input, 1.0, old]}
  path: [{string: values}]
""").head

    engine.action(java.lang.Double.valueOf(5.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 1.0, 1.0))
    engine.action(java.lang.Double.valueOf(3.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(1.0, 1.0, 2.0))
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 1.0, 3.0))
    engine.action(java.lang.Double.valueOf(10.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 1.0, 3.0))
    engine.action(java.lang.Double.valueOf(8.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 2.0, 4.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 2.0, 4.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 2.0, 4.0))
    engine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 2.0, 4.0))
  }

  it must "fill a two-dimensional histogram" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: {type: array, items: double}}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: xnumbins, type: int}
        - {name: xlow, type: double}
        - {name: xhigh, type: double}
        - {name: ynumbins, type: int}
        - {name: ylow, type: double}
        - {name: yhigh, type: double}
        - {name: values, type: {type: array, items: {type: array, items: double}}}
    init:
      xnumbins: 5
      ynumbins: 4
      xlow: 0.0
      xhigh: 5.0
      ylow: 0.0
      yhigh: 4.0
      values:
        - [0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0]
action:
  attr:
    cell: histogram
    to:
      params: [{old: Histogram}]
      ret: Histogram
      do:
        stat.sample.fillHistogram2d:
          - {attr: input, path: [0]}
          - {attr: input, path: [1]}
          - 1.0
          - old
  path: [{string: values}]
""").head

    engine.action(PFAArray.fromVector(Vector(-10.0, -10.0))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector map {_.toVector} should be (
      Vector(Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0)))

    engine.action(PFAArray.fromVector(Vector(2.0, 2.0))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector map {_.toVector} should be (
      Vector(Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 1.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0)))

    engine.action(PFAArray.fromVector(Vector(2.0, 2.0))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector map {_.toVector} should be (
      Vector(Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 2.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0)))

    engine.action(PFAArray.fromVector(Vector(0.0, 2.0))).asInstanceOf[PFAArray[PFAArray[Double]]].toVector map {_.toVector} should be (
      Vector(Vector(0.0, 0.0, 1.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 2.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0),
             Vector(0.0, 0.0, 0.0, 0.0)))
  }

  it must "fill a two-dimensional histogram with overflow bins" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  histogram:
    type:
      type: record
      name: Histogram
      fields:
        - {name: xnumbins, type: int}
        - {name: xlow, type: double}
        - {name: xhigh, type: double}
        - {name: ynumbins, type: int}
        - {name: ylow, type: double}
        - {name: yhigh, type: double}
        - {name: values, type: {type: array, items: {type: array, items: double}}}
        - {name: underunderflow, type: double}
        - {name: undermidflow, type: double}
        - {name: underoverflow, type: double}
        - {name: midunderflow, type: double}
        - {name: midoverflow, type: double}
        - {name: overunderflow, type: double}
        - {name: overmidflow, type: double}
        - {name: overoverflow, type: double}
        - {name: nanflow, type: double}
        - {name: infflow, type: double}
    init:
      xnumbins: 5
      ynumbins: 5
      xlow: 0.0
      xhigh: 5.0
      ylow: 0.0
      yhigh: 5.0
      values:
        - [0.0, 0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0, 0.0]
        - [0.0, 0.0, 0.0, 0.0, 0.0]
      underunderflow: 0.0
      undermidflow: 0.0
      underoverflow: 0.0
      midunderflow: 0.0
      midoverflow: 0.0
      overunderflow: 0.0
      overmidflow: 0.0
      overoverflow: 0.0
      nanflow: 0.0
      infflow: 0.0
action:
  - let:
      hist:
        cell: histogram
        to:
          params: [{old: Histogram}]
          ret: Histogram
          do:
            stat.sample.fillHistogram2d:
              - {attr: input, path: [0]}
              - {attr: input, path: [1]}
              - 1.0
              - old
  - type: {type: array, items: double}
    new: [hist.underunderflow, hist.undermidflow, hist.underoverflow, hist.midunderflow, hist.midoverflow, hist.overunderflow, hist.overmidflow, hist.overoverflow, hist.nanflow, hist.infflow]
""").head

    engine.action(PFAArray.fromVector(Vector(2.0, 2.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(-10.0, -10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(-10.0, 2.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(-10.0, 10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(2.0, -10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(2.0, 2.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(2.0, 10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(10.0, -10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(10.0, 2.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(10.0, 10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(java.lang.Double.NaN, 10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(10.0, java.lang.Double.NaN))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 0.0))
    engine.action(PFAArray.fromVector(Vector(java.lang.Double.POSITIVE_INFINITY, 10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0))
    engine.action(PFAArray.fromVector(Vector(java.lang.Double.NEGATIVE_INFINITY, 10.0))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0))
    engine.action(PFAArray.fromVector(Vector(10.0, java.lang.Double.POSITIVE_INFINITY))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 3.0))
    engine.action(PFAArray.fromVector(Vector(10.0, java.lang.Double.NEGATIVE_INFINITY))).asInstanceOf[PFAArray[Double]].toVector should be (
      Vector(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 4.0))
  }

  it must "fill a counter (categorical histogram)" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: map, values: double}
cells:
  state:
    type:
      type: record
      name: Counter
      fields:
        - {name: values, type: {type: map, values: double}}
    init: {values: {}}
action:
  - cell: state
    to:
      params: [{old: Counter}]
      ret: Counter
      do: {stat.sample.fillCounter: [input, 1.0, old]}
  - cell: state
    path: [{string: values}]
""").head

    engine.action("hello").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be   (Map("hello" -> 1.0))
    engine.action("my").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be      (Map("hello" -> 1.0, "my" -> 1.0))
    engine.action("baby").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be    (Map("hello" -> 1.0, "my" -> 1.0, "baby" -> 1.0))
    engine.action("hello").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be   (Map("hello" -> 2.0, "my" -> 1.0, "baby" -> 1.0))
    engine.action("my").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be      (Map("hello" -> 2.0, "my" -> 2.0, "baby" -> 1.0))
    engine.action("darling").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be (Map("hello" -> 2.0, "my" -> 2.0, "baby" -> 1.0, "darling" -> 1.0))
    engine.action("hello").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be   (Map("hello" -> 3.0, "my" -> 2.0, "baby" -> 1.0, "darling" -> 1.0))
    engine.action("my").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be      (Map("hello" -> 3.0, "my" -> 3.0, "baby" -> 1.0, "darling" -> 1.0))
    engine.action("ragtime").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be (Map("hello" -> 3.0, "my" -> 3.0, "baby" -> 1.0, "darling" -> 1.0, "ragtime" -> 1.0))
    engine.action("gal").asInstanceOf[PFAMap[java.lang.Double]].toMap map {case (k, v) => (k, v.doubleValue)} should be     (Map("hello" -> 3.0, "my" -> 3.0, "baby" -> 1.0, "darling" -> 1.0, "ragtime" -> 1.0, "gal" -> 1.0))
  }

  it must "maintain a top 5 list" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: array, items: double}
cells:
  state:
    type: {type: array, items: double}
    init: []
action:
  - cell: state
    to:
      params: [{old: {type: array, items: double}}]
      ret: {type: array, items: double}
      do: {stat.sample.topN: [input, old, 5, {fcn: u.lt}]}
fcns:
  lt:
    params: [{x: double}, {y: double}]
    ret: boolean
    do: {"<": [x, y]}
""").head

    engine.action(java.lang.Double.valueOf(2.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0))
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 1.0))
    engine.action(java.lang.Double.valueOf(1.5)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.0, 1.5, 1.0))
    engine.action(java.lang.Double.valueOf(3.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(3.0, 2.0, 1.5, 1.0))
    engine.action(java.lang.Double.valueOf(0.9)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(3.0, 2.0, 1.5, 1.0, 0.9))
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(3.0, 2.0, 1.5, 1.0, 1.0))
    engine.action(java.lang.Double.valueOf(4.0)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(4.0, 3.0, 2.0, 1.5, 1.0))
  }

  it must "maintain a top 5 list of strings" taggedAs(Lib, LibStatSample) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
cells:
  state:
    type: {type: array, items: string}
    init: []
action:
  - cell: state
    to:
      params: [{old: {type: array, items: string}}]
      ret: {type: array, items: string}
      do: {stat.sample.topN: [input, old, 5, {fcn: u.lt}]}
fcns:
  lt:
    params: [{x: string}, {y: string}]
    ret: boolean
    do: {"<": [{s.len: x}, {s.len: y}]}
""").head

    engine.action("two").asInstanceOf[PFAArray[String]].toVector should be (Vector("two"))
    engine.action("u").asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "u"))
    engine.action("to").asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "to", "u"))
    engine.action("three").asInstanceOf[PFAArray[String]].toVector should be (Vector("three", "two", "to", "u"))
    engine.action("").asInstanceOf[PFAArray[String]].toVector should be (Vector("three", "two", "to", "u", ""))
    engine.action("Z").asInstanceOf[PFAArray[String]].toVector should be (Vector("three", "two", "to", "u", "Z"))
    engine.action("wowie-wow").asInstanceOf[PFAArray[String]].toVector should be (Vector("wowie-wow", "three", "two", "to", "u"))
  }

}
