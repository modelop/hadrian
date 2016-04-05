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

package test.scala.lib.interp

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
class LibInterpSuite extends FlatSpec with Matchers {
  "interp bin" must "work" taggedAs(Lib, LibInterp) in {
    val engine1 = PFAEngine.fromYaml("""
input: double
output: int
action:
  interp.bin: [input, 5, 100, 110]
""").head
    engine1.action(java.lang.Double.valueOf(100.0)) should be (0)
    engine1.action(java.lang.Double.valueOf(101.0)) should be (0)
    engine1.action(java.lang.Double.valueOf(101.999)) should be (0)
    engine1.action(java.lang.Double.valueOf(102.0)) should be (1)
    engine1.action(java.lang.Double.valueOf(109.999)) should be (4)

    val engine2 = PFAEngine.fromYaml("""
input: double
output: int
action:
  interp.bin: [input, 100, 2]
""").head
    engine2.action(java.lang.Double.valueOf(99.0)) should be (-1)
    engine2.action(java.lang.Double.valueOf(99.999)) should be (-1)
    engine2.action(java.lang.Double.valueOf(100.0)) should be (0)
    engine2.action(java.lang.Double.valueOf(101.0)) should be (0)
    engine2.action(java.lang.Double.valueOf(101.999)) should be (0)
    engine2.action(java.lang.Double.valueOf(102.0)) should be (1)
    engine2.action(java.lang.Double.valueOf(109.999)) should be (4)
    engine2.action(java.lang.Double.valueOf(110.0)) should be (5)
    engine2.action(java.lang.Double.valueOf(0.0)) should be (-50)
  }

  "interp nearest" must "pick the closest item in 1-d" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: int}
    init:
      - {x: 1.0, to: 7}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  +:
    - interp.nearest:
        - input
        - cell: table
    - 100
""").head
    engine.action(java.lang.Double.valueOf(-0.2)) should be (105)
    engine.action(java.lang.Double.valueOf(-0.1)) should be (105)
    engine.action(java.lang.Double.valueOf(0.0)) should be (105)
    engine.action(java.lang.Double.valueOf(0.1)) should be (105)
    engine.action(java.lang.Double.valueOf(0.2)) should be (105)
    engine.action(java.lang.Double.valueOf(0.3)) should be (106)
    engine.action(java.lang.Double.valueOf(0.4)) should be (106)
    engine.action(java.lang.Double.valueOf(0.5)) should be (106)
    engine.action(java.lang.Double.valueOf(0.6)) should be (106)
    engine.action(java.lang.Double.valueOf(0.7)) should be (106)
    engine.action(java.lang.Double.valueOf(0.8)) should be (107)
    engine.action(java.lang.Double.valueOf(0.9)) should be (107)
    engine.action(java.lang.Double.valueOf(1.0)) should be (107)
    engine.action(java.lang.Double.valueOf(1.1)) should be (107)
    engine.action(java.lang.Double.valueOf(1.2)) should be (107)
  }

  it must "pick the closest item in N-d" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: {type: array, items: double}}
          - {name: to, type: int}
    init:
      - {x: [0.0, 0.0, 1.0], to: 7}
      - {x: [0.0, 0.0, 0.0], to: 5}
      - {x: [0.0, 0.0, 0.5], to: 6}
action:
  +:
    - interp.nearest:
        - type:
            type: array
            items: double
          new:
            - 0.0
            - 0.0
            - input
        - cell: table
    - 100
""").head
    engine.action(java.lang.Double.valueOf(-0.2)) should be (105)
    engine.action(java.lang.Double.valueOf(-0.1)) should be (105)
    engine.action(java.lang.Double.valueOf(0.0)) should be (105)
    engine.action(java.lang.Double.valueOf(0.1)) should be (105)
    engine.action(java.lang.Double.valueOf(0.2)) should be (105)
    engine.action(java.lang.Double.valueOf(0.3)) should be (106)
    engine.action(java.lang.Double.valueOf(0.4)) should be (106)
    engine.action(java.lang.Double.valueOf(0.5)) should be (106)
    engine.action(java.lang.Double.valueOf(0.6)) should be (106)
    engine.action(java.lang.Double.valueOf(0.7)) should be (106)
    engine.action(java.lang.Double.valueOf(0.8)) should be (107)
    engine.action(java.lang.Double.valueOf(0.9)) should be (107)
    engine.action(java.lang.Double.valueOf(1.0)) should be (107)
    engine.action(java.lang.Double.valueOf(1.1)) should be (107)
    engine.action(java.lang.Double.valueOf(1.2)) should be (107)
  }

  it must "pick the closest item in an abstract metric" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: string}
          - {name: to, type: int}
    init:
      - {x: "aaa", to: 3}
      - {x: "aaaaaa", to: 6}
      - {x: "aaaaaaaaa", to: 9}
action:
  +:
    - interp.nearest:
        - input
        - cell: table
        - params: [{a: string}, {b: string}]
          ret: double
          do: {m.abs: {"-": [{s.len: a}, {s.len: b}]}}
    - 100
""").head
    engine.action("b") should be (103)
    engine.action("bb") should be (103)
    engine.action("bbb") should be (103)
    engine.action("bbbb") should be (103)
    engine.action("bbbbb") should be (106)
    engine.action("bbbbbb") should be (106)
    engine.action("bbbbbbb") should be (106)
    engine.action("bbbbbbbb") should be (109)
    engine.action("bbbbbbbbb") should be (109)
    engine.action("bbbbbbbbbb") should be (109)
    engine.action("bbbbbbbbbbb") should be (109)
  }

  "interp linear" must "interpolate scalars in 1-d" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
    init:
      - {x: 1.0, to: 4}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  +:
    - interp.linear:
        - input
        - cell: table
    - 100
""").head
    engine.action(java.lang.Double.valueOf(-0.2)) should be (104.6)
    engine.action(java.lang.Double.valueOf(-0.1)) should be (104.8)
    engine.action(java.lang.Double.valueOf(0.0)) should be (105.0)
    engine.action(java.lang.Double.valueOf(0.1)) should be (105.2)
    engine.action(java.lang.Double.valueOf(0.2)) should be (105.4)
    engine.action(java.lang.Double.valueOf(0.3)) should be (105.6)
    engine.action(java.lang.Double.valueOf(0.4)) should be (105.8)
    engine.action(java.lang.Double.valueOf(0.5)) should be (106.0)
    engine.action(java.lang.Double.valueOf(0.6)) should be (105.6)
    engine.action(java.lang.Double.valueOf(0.7)) should be (105.2)
    engine.action(java.lang.Double.valueOf(0.8)) should be (104.8)
    engine.action(java.lang.Double.valueOf(0.9)) should be (104.4)
    engine.action(java.lang.Double.valueOf(1.0)) should be (104.0)
    engine.action(java.lang.Double.valueOf(1.1)) should be (103.6)
    engine.action(java.lang.Double.valueOf(1.2)) should be (103.2)
  }

  it must "interpolate vectors in 1-d" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: double
output:
  type: array
  items: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: {type: array, items: double}}
    init:
      - {x: 1.0, to: [1, 4]}
      - {x: 0.0, to: [1, 5]}
      - {x: 0.5, to: [1, 6]}
action:
  interp.linear:
    - input
    - cell: table
""").head
    engine.action(java.lang.Double.valueOf(-0.2)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 4.6)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(-0.1)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 4.8)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.0)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.1)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.2)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.2)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.4)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.6)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.4)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.8)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 6.0)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.6)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.6)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.7)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 5.2)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 4.8)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.9)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 4.4)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 4.0)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.1)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 3.6)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.2)).asInstanceOf[PFAArray[Double]].toVector.zip(Vector(1.0, 3.2)).map({case (x, y) => Math.pow(x - y, 2)}).sum should be (0.0 +- 0.001)
  }

  it must "interpolate with flat extremes" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
    init:
      - {x: 1.0, to: 4}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  +:
    - interp.linearFlat:
        - input
        - cell: table
    - 100
""").head
    engine.action(java.lang.Double.valueOf(-0.2)) should be (105.0)
    engine.action(java.lang.Double.valueOf(-0.1)) should be (105.0)
    engine.action(java.lang.Double.valueOf(0.0)) should be (105.0)
    engine.action(java.lang.Double.valueOf(0.1)) should be (105.2)
    engine.action(java.lang.Double.valueOf(0.2)) should be (105.4)
    engine.action(java.lang.Double.valueOf(0.3)) should be (105.6)
    engine.action(java.lang.Double.valueOf(0.4)) should be (105.8)
    engine.action(java.lang.Double.valueOf(0.5)) should be (106.0)
    engine.action(java.lang.Double.valueOf(0.6)) should be (105.6)
    engine.action(java.lang.Double.valueOf(0.7)) should be (105.2)
    engine.action(java.lang.Double.valueOf(0.8)) should be (104.8)
    engine.action(java.lang.Double.valueOf(0.9)) should be (104.4)
    engine.action(java.lang.Double.valueOf(1.0)) should be (104.0)
    engine.action(java.lang.Double.valueOf(1.1)) should be (104.0)
    engine.action(java.lang.Double.valueOf(1.2)) should be (104.0)
  }

  it must "interpolate with missing extremes" taggedAs(Lib, LibInterp) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: ["null", double]
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: Table
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
    init:
      - {x: 1.0, to: 4}
      - {x: 0.0, to: 5}
      - {x: 0.5, to: 6}
action:
  interp.linearMissing:
    - input
    - cell: table
""").head
    engine.action(java.lang.Double.valueOf(-0.2)).asInstanceOf[java.lang.Double] should be (null)
    engine.action(java.lang.Double.valueOf(-0.1)).asInstanceOf[java.lang.Double] should be (null)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[java.lang.Double].doubleValue should be (5.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.1)).asInstanceOf[java.lang.Double].doubleValue should be (5.2 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.2)).asInstanceOf[java.lang.Double].doubleValue should be (5.4 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.3)).asInstanceOf[java.lang.Double].doubleValue should be (5.6 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.4)).asInstanceOf[java.lang.Double].doubleValue should be (5.8 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[java.lang.Double].doubleValue should be (6.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.6)).asInstanceOf[java.lang.Double].doubleValue should be (5.6 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.7)).asInstanceOf[java.lang.Double].doubleValue should be (5.2 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[java.lang.Double].doubleValue should be (4.8 +- 0.001)
    engine.action(java.lang.Double.valueOf(0.9)).asInstanceOf[java.lang.Double].doubleValue should be (4.4 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.0)).asInstanceOf[java.lang.Double].doubleValue should be (4.0 +- 0.001)
    engine.action(java.lang.Double.valueOf(1.1)).asInstanceOf[java.lang.Double] should be (null)
    engine.action(java.lang.Double.valueOf(1.2)).asInstanceOf[java.lang.Double] should be (null)
  }

}
