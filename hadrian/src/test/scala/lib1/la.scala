package test.scala.lib1.la

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1LASuite extends FlatSpec with Matchers {
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
    engine.action(null).toString should be ("[[-1.0, -2.0, -3.0], [-4.0, -5.0, -6.0], [-7.0, -8.0, -9.0]]")
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
    engine.action(null).toString should be ("{uno: {one: -1.0, two: -2.0, three: -3.0}, dos: {four: -4.0, five: -5.0, six: -6.0}, tres: {seven: -7.0, eight: -8.0, nine: -9.0}}")
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
    engine.action(null).toString should be ("[[102.0, 104.0, 106.0], [108.0, 110.0, 112.0], [114.0, 116.0, 118.0]]")
  }

  it must "zipmap maps" taggedAs(Lib1, Lib1LA) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: map, values: double}}
action:
  la.zipmap:
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 1, two: 2, three: 3},
              dos: {four: 4, five: 5, six: 6},
              tres: {seven: 7, eight: 8, nine: 9}}
    - type: {type: map, values: {type: map, values: double}}
      value: {uno: {one: 101, two: 102, three: 103},
              dos: {four: 104, five: 105, six: 106},
              tres: {seven: 107, eight: 108, nine: 109}}
    - params: [{x: double}, {y: double}]
      ret: double
      do: {+: [x, y]}
""").head
    engine.action(null).toString should be ("{uno: {one: 102.0, two: 104.0, three: 106.0}, dos: {four: 108.0, five: 110.0, six: 112.0}, tres: {seven: 114.0, eight: 116.0, nine: 118.0}}")
  }

}
