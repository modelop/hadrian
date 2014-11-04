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

package test.scala.lib1.la

import scala.collection.JavaConversions._
import scala.language.postfixOps

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1LASuite extends FlatSpec with Matchers {
  def chi2Vector(x: PFAArray[Double], y: Vector[Double]): Double = {
    x.toVector.size should be (y.size)
    (x.toVector zip y) map {case (xi, yi) => Math.pow(xi - yi, 2)} sum
  }

  def chi2Vector(x: PFAMap[java.lang.Double], y: Map[String, Double]): Double = {
    x.toMap.keySet should be (y.keySet)
    (for (k <- y.keySet) yield
      Math.pow(x.toMap.apply(k).doubleValue - y(k), 2)).sum
  }

  def chi2(x: PFAArray[PFAArray[Double]], y: Vector[Vector[Double]]): Double = {
    val xv = x.toVector
    val yv = y

    xv.size should be (yv.size)

    for ((xi, yi) <- (xv zip yv))
      xi.toVector.size should be (yi.size)

    (for ((xi, yi) <- (xv zip yv)) yield
      (xi.toVector zip yi map {case (xxi, yyi) => Math.pow(xxi - yyi, 2)}).sum).sum
  }

  def chi2(x: PFAMap[PFAMap[java.lang.Double]], y: Map[String, Map[String, Double]]): Double = {
    val xm = x.toMap
    val ym = y

    xm.keySet should be (ym.keySet)

    for ((xi, yi) <- (xm.values zip ym.values))
      xi.toMap.keySet should be (yi.keySet)

    (for (r <- xm.keys; c <- xm(r).toMap.keys) yield
      Math.pow(xm(r).toMap.apply(c).doubleValue - ym(r).apply(c), 2)).sum
  }

  "linear algebra library" must "map arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.map:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - params: [{x: double}]
      ret: double
      do: {u-: x}
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(-1.0, -2.0, -3.0), Vector(-4.0, -5.0, -6.0), Vector(-7.0, -8.0, -9.0))) should be (0.00 +- 0.01)
  }

  it must "map maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.map:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {four: 4, five: 5, six: 6},
              tres: {seven: 7, eight: 8, nine: 9}}
    - params: [{x: double}]
      ret: double
      do: {u-: x}
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("uno" -> Map("one" -> -1.0, "two" -> -2.0, "three" -> -3.0),
             "dos" -> Map("four" -> -4.0, "five" -> -5.0, "six" -> -6.0),
             "tres" -> Map("seven" -> -7.0, "eight" -> -8.0, "nine" -> -9.0))) should be (0.00 +- 0.01)
  }

  it must "zipmap arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.zipmap:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - type: {type: array, items: {type: array, items: double}}
      value: [[101, 102, 103], [104, 105, 106], [107, 108, 109]]
    - params: [{x: double}, {y: double}]
      ret: double
      do: {+: [x, y]}
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(102.0, 104.0, 106.0), Vector(108.0, 110.0, 112.0), Vector(114.0, 116.0, 118.0))) should be (0.00 +- 0.01)
  }

  it must "zipmap maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.zipmap:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {one: 4, two: 5, three: 6},
              tres: {one: 7, two: 8, three: 9}}
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 101, two: 102, three: 103},
              dos: {one: 104, two: 105, three: 106},
              tres: {one: 107, two: 108, three: 109, four: 999.0}}
    - params: [{x: double}, {y: double}]
      ret: double
      do: {+: [x, y]}
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("uno" -> Map("one" -> 102.0, "two" -> 104.0, "three" -> 106.0, "four" -> 0.0),
             "dos" -> Map("one" -> 108.0, "two" -> 110.0, "three" -> 112.0, "four" -> 0.0),
             "tres" -> Map("one" -> 114.0, "two" -> 116.0, "three" -> 118.0, "four" -> 999.0))) should be (0.00 +- 0.01)
  }

  it must "multiply a matrix and a vector as arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  la.dot:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2], [3, 4], [5, 6]]
    - type: {type: array, items: double}
      value: [1, -2]
""").head
    chi2Vector(engine.action(null).asInstanceOf[PFAArray[Double]],
               Vector(-3.0, -5.0, -7.0)) should be (0.00 +- 0.01)
  }

  it must "multiply matrices as arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.dot:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2], [3, 4], [5, 6]]
    - type: {type: array, items: {type: array, items: double}}
      value: [[7, 8, 9], [10, 11, 12]]
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(27.0, 30.0, 33.0),
                Vector(61.0, 68.0, 75.0),
                Vector(95.0, 106.0, 117.0))) should be (0.00 +- 0.01)
  }

  it must "multiply a matrix and a vector as maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: double}
action:
  la.dot:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2}, b: {x: 3, y: 4}, c: {x: 5, y: 6}}
    - type: {type: map, values: double}
      value: {x: 1, y: -2}
""").head
    chi2Vector(engine.action(null).asInstanceOf[PFAMap[java.lang.Double]],
               Map("a" -> -3.0, "b" -> -5.0, "c" -> -7.0)) should be (0.00 +- 0.01)
  }

  it must "multiply matrices as maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.dot:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2}, b: {x: 3, y: 4}, c: {x: 5, y: 6}}
    - type: {type: map, values: {type: map, values: double}}
      value: {x: {alpha: 7, beta: 8, gamma: 9}, y: {alpha: 10, beta: 11, gamma: 12}}
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("a" -> Map("alpha" -> 27.0, "beta" -> 30.0, "gamma" -> 33.0),
             "b" -> Map("alpha" -> 61.0, "beta" -> 68.0, "gamma" -> 75.0),
             "c" -> Map("alpha" -> 95.0, "beta" -> 106.0, "gamma" -> 117.0))) should be (0.00 +- 0.01)
  }

  it must "transpose arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.transpose:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6]]
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(1.0, 4.0), Vector(2.0, 5.0), Vector(3.0, 6.0))) should be (0.00 +- 0.01)
  }

  it must "transpose maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.transpose:
    - type: {type: map, values: {type: map, values: double}}
      value: {"a": {"x": 1, "y": 2, "z": 3}, "b": {"x": 4, "y": 5, "z": 6}}
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("x" -> Map("a" -> 1.0, "b" -> 4.0),
             "y" -> Map("a" -> 2.0, "b" -> 5.0),
             "z" -> Map("a" -> 3.0, "b" -> 6.0))) should be (0.00 +- 0.01)
  }

  it must "compute a (pseudo)inverse from arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.inverse:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6]]
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(-0.944, 0.444),
                Vector(-0.111, 0.111),
                Vector(0.722, -0.222))) should be (0.00 +- 0.01)
  }

  it must "compute a (pseudo)inverse from maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.inverse:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2, z: 3}, b: {x: 4, y: 5, z: 6}}
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("x" -> Map("a" -> -0.944, "b" -> 0.444),
             "y" -> Map("a" -> -0.111, "b" -> 0.111),
             "z" -> Map("a" -> 0.722, "b" -> -0.222))) should be (0.00 +- 0.01)
  }

  it must "compute a trace from arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  la.trace:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
""").head
    engine.action(null).asInstanceOf[java.lang.Double].doubleValue should be (15.00 +- 0.01)
  }

  it must "compute a trace from maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  la.trace:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 4, b: 5, c: 6}, c: {a: 7, b: 8, c: 9}}
""").head
    engine.action(null).asInstanceOf[java.lang.Double].doubleValue should be (15.00 +- 0.01)
  }

  it must "compute a determinant from arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  la.det:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, -5, 6], [7, 8, 9]]
""").head
    engine.action(null).asInstanceOf[java.lang.Double].doubleValue should be (120.00 +- 0.01)
  }

  it must "compute a determinant from maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: double
action:
  la.det:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 4, b: -5, c: 6}, c: {a: 7, b: 8, c: 9}}
""").head
    engine.action(null).asInstanceOf[java.lang.Double].doubleValue should be (120.00 +- 0.01)
  }

  it must "identify symmetric matrices as arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [2, 4, 5], [3, 5, 6]]
    - 0.01
""").head
    engine.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [2, 4, 5], [3, 6, 5]]
    - 0.01
""").head
    engine2.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "identify symmetric matrices as maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 2, b: 4, c: 5}, c: {a: 3, b: 5, c: 6}}
    - 0.01
""").head
    engine.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  la.symmetric:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 1, b: 2, c: 3}, b: {a: 2, b: 4, c: 5}, c: {a: 3, b: 6, c: 5}}
    - 0.01
""").head
    engine2.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "compute an eigenbasis from arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.eigenBasis:
    - type: {type: array, items: {type: array, items: double}}
      value: [[898.98, -1026.12], [-1026.12, 1309.55]]
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(0.014, -0.017), Vector(0.102, 0.083))) should be (0.00 +- 0.01)
  }

  it must "compute an eigenbasis from maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.eigenBasis:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {a: 898.98, b: -1026.12}, b: {a: -1026.12, b: 1309.55}}
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("0" -> Map("a" -> 0.014, "b" -> -0.017), "1" -> Map("a" -> 0.102, "b" -> 0.083))) should be (0.00 +- 0.01)
  }

  it must "accumulate a covariance and compute eigenbasis using arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input:
  type: record
  name: Input
  fields:
    - {name: datum, type: {type: array, items: double}}
    - {name: update, type: boolean}
output: {type: array, items: double}
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
  - if: input.update
    then:
      cell: state
      to:
        params: [{state: State}]
        ret: State
        do:
          stat.sample.updateCovariance:
            - input.datum
            - 1.0
            - state
  - la.dot:
      - la.eigenBasis:
          - cell: state
            path: [[covariance]]
      - type: {type: array, items: double}
        new:
          - {"-": [input.datum.0, {cell: state, path: [[mean], 0]}]}
          - {"-": [input.datum.1, {cell: state, path: [[mean], 1]}]}
""").head
    engine.action(engine.fromJson("""{"datum": [12, 85], "update": true}""", engine.inputType))
    engine.action(engine.fromJson("""{"datum": [32, 40], "update": true}""", engine.inputType))
    engine.action(engine.fromJson("""{"datum": [4, 90], "update": true}""", engine.inputType))
    engine.action(engine.fromJson("""{"datum": [3, 77], "update": true}""", engine.inputType))
    engine.action(engine.fromJson("""{"datum": [7, 87], "update": true}""", engine.inputType))
    engine.action(engine.fromJson("""{"datum": [88, 2], "update": true}""", engine.inputType))
    engine.action(engine.fromJson("""{"datum": [56, 5], "update": true}""", engine.inputType))

    chi2Vector(engine.action(engine.fromJson("""{"datum": [12, 85], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(-0.728, 0.775)) should be (0.00 +- 0.01)
    chi2Vector(engine.action(engine.fromJson("""{"datum": [32, 40], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(0.295, -0.943)) should be (0.00 +- 0.01)
    chi2Vector(engine.action(engine.fromJson("""{"datum": [4, 90], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(-0.921, 0.378)) should be (0.00 +- 0.01)
    chi2Vector(engine.action(engine.fromJson("""{"datum": [3, 77], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(-0.718, -0.808)) should be (0.00 +- 0.01)
    chi2Vector(engine.action(engine.fromJson("""{"datum": [7, 87], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(-0.830, 0.433)) should be (0.00 +- 0.01)
    chi2Vector(engine.action(engine.fromJson("""{"datum": [88, 2], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(1.695, 1.585)) should be (0.00 +- 0.01)
    chi2Vector(engine.action(engine.fromJson("""{"datum": [56, 5], "update": false}""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(1.207, -1.420)) should be (0.00 +- 0.01)
  }

  it must "truncate a matrix using arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: double}}
action:
  la.truncate:
    - type: {type: array, items: {type: array, items: double}}
      value: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    - 2
""").head
    chi2(engine.action(null).asInstanceOf[PFAArray[PFAArray[Double]]],
         Vector(Vector(1.0, 2.0, 3.0), Vector(4.0, 5.0, 6.0))) should be (0.00 +- 0.01)
  }

  it must "truncate a matrix using maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.truncate:
    - type: {type: map, values: {type: map, values: double}}
      value: {a: {x: 1, y: 2, z: 3}, b: {x: 4, y: 5, z: 6}, c: {x: 7, y: 8, z: 9}}
    - type: {type: array, items: string}
      value: [a, b, q]
""").head
    chi2(engine.action(null).asInstanceOf[PFAMap[PFAMap[java.lang.Double]]],
         Map("a" -> Map("x" -> 1.0, "y" -> 2.0, "z" -> 3.0), "b" -> Map("x" -> 4.0, "y" -> 5.0, "z" -> 6.0))) should be (0.00 +- 0.01)
  }

  it must "accumulate a covariance and use that to update a PCA matrix using arrays" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
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
      mean: [0, 0, 0, 0, 0]
      covariance: [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]]
action:
  la.dot:
    - la.truncate:
        - la.eigenBasis:
            - attr:
                cell: state
                to:
                  params: [{state: State}]
                  ret: State
                  do:
                    stat.sample.updateCovariance:
                      - input
                      - 1.0
                      - state
              path: [[covariance]]
        - 2
    - a.mapWithIndex:
        - input
        - params: [{i: int}, {x: double}]
          ret: double
          do: {"-": [x, {cell: state, path: [[mean], i]}]}
""").head
    engine.action(engine.fromJson("""[23, 56, 12, 34, 72]""", engine.inputType))
    engine.action(engine.fromJson("""[52, 61, 12, 71, 91]""", engine.inputType))
    engine.action(engine.fromJson("""[15, 12, 89, 23, 48]""", engine.inputType))

    chi2Vector(engine.action(engine.fromJson("""[16, 27, 36, 84, 52]""", engine.inputType)).asInstanceOf[PFAArray[Double]],
         Vector(-0.038, -1.601)) should be (0.00 +- 0.01)
  }

}
