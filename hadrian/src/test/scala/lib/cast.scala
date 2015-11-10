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

package test.scala.lib.parse

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.data._
import test.scala._

@RunWith(classOf[JUnitRunner])
class LibCastSuite extends FlatSpec with Matchers {
  "cast" must "do toSigned" taggedAs(Lib, LibCast) in {
    val engine2 = PFAEngine.fromYaml("""
input: long
output: long
action: {cast.signed: [input, 2]}
""").head
    engine2.action(java.lang.Long.valueOf(-2)).asInstanceOf[java.lang.Long].longValue should be (-2L)
    engine2.action(java.lang.Long.valueOf(-1)).asInstanceOf[java.lang.Long].longValue should be (-1L)
    engine2.action(java.lang.Long.valueOf(0)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine2.action(java.lang.Long.valueOf(1)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine2.action(java.lang.Long.valueOf(2)).asInstanceOf[java.lang.Long].longValue should be (-2L)

    val engine8 = PFAEngine.fromYaml("""
input: long
output: long
action: {cast.signed: [input, 8]}
""").head

    engine8.action(java.lang.Long.valueOf(-2 - 256)).asInstanceOf[java.lang.Long].longValue should be (-2L)
    engine8.action(java.lang.Long.valueOf(-1 - 256)).asInstanceOf[java.lang.Long].longValue should be (-1L)
    engine8.action(java.lang.Long.valueOf(0 - 256)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1 - 256)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2 - 256)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine8.action(java.lang.Long.valueOf(-2 - 128)).asInstanceOf[java.lang.Long].longValue should be (126L)
    engine8.action(java.lang.Long.valueOf(-1 - 128)).asInstanceOf[java.lang.Long].longValue should be (127L)
    engine8.action(java.lang.Long.valueOf(0 - 128)).asInstanceOf[java.lang.Long].longValue should be (-128L)
    engine8.action(java.lang.Long.valueOf(1 - 128)).asInstanceOf[java.lang.Long].longValue should be (-127L)
    engine8.action(java.lang.Long.valueOf(2 - 128)).asInstanceOf[java.lang.Long].longValue should be (-126L)

    engine8.action(java.lang.Long.valueOf(-2)).asInstanceOf[java.lang.Long].longValue should be (-2L)
    engine8.action(java.lang.Long.valueOf(-1)).asInstanceOf[java.lang.Long].longValue should be (-1L)
    engine8.action(java.lang.Long.valueOf(0)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine8.action(java.lang.Long.valueOf(-2 + 128)).asInstanceOf[java.lang.Long].longValue should be (126L)
    engine8.action(java.lang.Long.valueOf(-1 + 128)).asInstanceOf[java.lang.Long].longValue should be (127L)
    engine8.action(java.lang.Long.valueOf(0 + 128)).asInstanceOf[java.lang.Long].longValue should be (-128L)
    engine8.action(java.lang.Long.valueOf(1 + 128)).asInstanceOf[java.lang.Long].longValue should be (-127L)
    engine8.action(java.lang.Long.valueOf(2 + 128)).asInstanceOf[java.lang.Long].longValue should be (-126L)

    engine8.action(java.lang.Long.valueOf(-2 + 256)).asInstanceOf[java.lang.Long].longValue should be (-2L)
    engine8.action(java.lang.Long.valueOf(-1 + 256)).asInstanceOf[java.lang.Long].longValue should be (-1L)
    engine8.action(java.lang.Long.valueOf(0 + 256)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1 + 256)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2 + 256)).asInstanceOf[java.lang.Long].longValue should be (2L)

    val engine64 = PFAEngine.fromYaml("""
input: long
output: long
action: {cast.signed: [input, 64]}
""").head

    engine64.action(java.lang.Long.valueOf(java.lang.Long.MIN_VALUE)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MIN_VALUE)
    engine64.action(java.lang.Long.valueOf(java.lang.Long.MIN_VALUE + 1L)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MIN_VALUE + 1L)
    engine64.action(java.lang.Long.valueOf(java.lang.Long.MIN_VALUE + 2L)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MIN_VALUE + 2L)

    engine64.action(java.lang.Long.valueOf(-1)).asInstanceOf[java.lang.Long].longValue should be (-1L)
    engine64.action(java.lang.Long.valueOf(0)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine64.action(java.lang.Long.valueOf(1)).asInstanceOf[java.lang.Long].longValue should be (1L)

    engine64.action(java.lang.Long.valueOf(java.lang.Long.MAX_VALUE - 2L)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE - 2L)
    engine64.action(java.lang.Long.valueOf(java.lang.Long.MAX_VALUE - 1L)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE - 1L)
    engine64.action(java.lang.Long.valueOf(java.lang.Long.MAX_VALUE)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE)
  }

  it must "do toUnsigned" taggedAs(Lib, LibCast) in {
    val engine1 = PFAEngine.fromYaml("""
input: long
output: long
action: {cast.unsigned: [input, 1]}
""").head
    engine1.action(java.lang.Long.valueOf(-2)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine1.action(java.lang.Long.valueOf(-1)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine1.action(java.lang.Long.valueOf(0)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine1.action(java.lang.Long.valueOf(1)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine1.action(java.lang.Long.valueOf(2)).asInstanceOf[java.lang.Long].longValue should be (0L)

    val engine8 = PFAEngine.fromYaml("""
input: long
output: long
action: {cast.unsigned: [input, 8]}
""").head

    engine8.action(java.lang.Long.valueOf(-2 - 2*256)).asInstanceOf[java.lang.Long].longValue should be (254L)
    engine8.action(java.lang.Long.valueOf(-1 - 2*256)).asInstanceOf[java.lang.Long].longValue should be (255L)
    engine8.action(java.lang.Long.valueOf(0 - 2*256)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1 - 2*256)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2 - 2*256)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine8.action(java.lang.Long.valueOf(-2 - 256)).asInstanceOf[java.lang.Long].longValue should be (254L)
    engine8.action(java.lang.Long.valueOf(-1 - 256)).asInstanceOf[java.lang.Long].longValue should be (255L)
    engine8.action(java.lang.Long.valueOf(0 - 256)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1 - 256)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2 - 256)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine8.action(java.lang.Long.valueOf(-2)).asInstanceOf[java.lang.Long].longValue should be (254L)
    engine8.action(java.lang.Long.valueOf(-1)).asInstanceOf[java.lang.Long].longValue should be (255L)
    engine8.action(java.lang.Long.valueOf(0)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine8.action(java.lang.Long.valueOf(-2 + 256)).asInstanceOf[java.lang.Long].longValue should be (254L)
    engine8.action(java.lang.Long.valueOf(-1 + 256)).asInstanceOf[java.lang.Long].longValue should be (255L)
    engine8.action(java.lang.Long.valueOf(0 + 256)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1 + 256)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2 + 256)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine8.action(java.lang.Long.valueOf(-2 + 2*256)).asInstanceOf[java.lang.Long].longValue should be (254L)
    engine8.action(java.lang.Long.valueOf(-1 + 2*256)).asInstanceOf[java.lang.Long].longValue should be (255L)
    engine8.action(java.lang.Long.valueOf(0 + 2*256)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine8.action(java.lang.Long.valueOf(1 + 2*256)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine8.action(java.lang.Long.valueOf(2 + 2*256)).asInstanceOf[java.lang.Long].longValue should be (2L)

    val engine63 = PFAEngine.fromYaml("""
input: long
output: long
action: {cast.unsigned: [input, 63]}
""").head

    engine63.action(java.lang.Long.valueOf(java.lang.Long.MIN_VALUE)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine63.action(java.lang.Long.valueOf(java.lang.Long.MIN_VALUE + 1L)).asInstanceOf[java.lang.Long].longValue should be (1L)
    engine63.action(java.lang.Long.valueOf(java.lang.Long.MIN_VALUE + 2L)).asInstanceOf[java.lang.Long].longValue should be (2L)

    engine63.action(java.lang.Long.valueOf(-1)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE)
    engine63.action(java.lang.Long.valueOf(0)).asInstanceOf[java.lang.Long].longValue should be (0L)
    engine63.action(java.lang.Long.valueOf(1)).asInstanceOf[java.lang.Long].longValue should be (1L)

    engine63.action(java.lang.Long.valueOf(java.lang.Long.MAX_VALUE - 2L)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE - 2L)
    engine63.action(java.lang.Long.valueOf(java.lang.Long.MAX_VALUE - 1L)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE - 1L)
    engine63.action(java.lang.Long.valueOf(java.lang.Long.MAX_VALUE)).asInstanceOf[java.lang.Long].longValue should be (java.lang.Long.MAX_VALUE)
  }

  it must "do toInt" taggedAs(Lib, LibCast) in {
    PFAEngine.fromYaml("""
input: int
output: int
action: {cast.int: input}
""").head.action(java.lang.Integer.valueOf(5)).asInstanceOf[java.lang.Integer] should be (5)

    PFAEngine.fromYaml("""
input: long
output: int
action: {cast.int: input}
""").head.action(java.lang.Long.valueOf(5)).asInstanceOf[java.lang.Integer] should be (5)

    PFAEngine.fromYaml("""
input: float
output: int
action: {cast.int: input}
""").head.action(java.lang.Float.valueOf(5.0F)).asInstanceOf[java.lang.Integer] should be (5)

    PFAEngine.fromYaml("""
input: double
output: int
action: {cast.int: input}
""").head.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Integer] should be (5)
  }

  it must "do toLong" taggedAs(Lib, LibCast) in {
    PFAEngine.fromYaml("""
input: int
output: long
action: {cast.long: input}
""").head.action(java.lang.Integer.valueOf(5)).asInstanceOf[java.lang.Long] should be (5L)

    PFAEngine.fromYaml("""
input: long
output: long
action: {cast.long: input}
""").head.action(java.lang.Long.valueOf(5)).asInstanceOf[java.lang.Long] should be (5L)

    PFAEngine.fromYaml("""
input: float
output: long
action: {cast.long: input}
""").head.action(java.lang.Float.valueOf(5.0F)).asInstanceOf[java.lang.Long] should be (5L)

    PFAEngine.fromYaml("""
input: double
output: long
action: {cast.long: input}
""").head.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Long] should be (5L)
  }

  it must "do toFloat" taggedAs(Lib, LibCast) in {
    PFAEngine.fromYaml("""
input: int
output: float
action: {cast.float: input}
""").head.action(java.lang.Integer.valueOf(5)).asInstanceOf[java.lang.Float] should be (5.0F)

    PFAEngine.fromYaml("""
input: long
output: float
action: {cast.float: input}
""").head.action(java.lang.Long.valueOf(5)).asInstanceOf[java.lang.Float] should be (5.0F)

    PFAEngine.fromYaml("""
input: float
output: float
action: {cast.float: input}
""").head.action(java.lang.Float.valueOf(5.0F)).asInstanceOf[java.lang.Float] should be (5.0F)

    PFAEngine.fromYaml("""
input: double
output: float
action: {cast.long: input}
""").head.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Float] should be (5.0F)
  }

  it must "do toDouble" taggedAs(Lib, LibCast) in {
    PFAEngine.fromYaml("""
input: int
output: double
action: {cast.double: input}
""").head.action(java.lang.Integer.valueOf(5)).asInstanceOf[java.lang.Double] should be (5.0)

    PFAEngine.fromYaml("""
input: long
output: double
action: {cast.double: input}
""").head.action(java.lang.Long.valueOf(5)).asInstanceOf[java.lang.Double] should be (5.0)

    PFAEngine.fromYaml("""
input: float
output: double
action: {cast.double: input}
""").head.action(java.lang.Float.valueOf(5.0F)).asInstanceOf[java.lang.Double] should be (5.0)

    PFAEngine.fromYaml("""
input: double
output: double
action: {cast.double: input}
""").head.action(java.lang.Double.valueOf(5.0)).asInstanceOf[java.lang.Double] should be (5.0)
  }

  "array fanouts" must "do fanoutBoolean" taggedAs(Lib, LibCast) in {
    val engine1 = PFAEngine.fromYaml("""
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: boolean
action:
  cast.fanoutBoolean: input
""").head
    engine1.action(engine1.jsonInput(""""three"""")).asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, true, false, false, false, false, false, false))

    val engine2 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: boolean
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutBoolean: [input, {cell: dictionary}, false]
""").head
    engine2.action("three").asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, true, false, false, false, false, false, false))
    engine2.action("sdfasdf").asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, false, false, false, false, false, false, false))

    val engine3 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: boolean
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutBoolean: [input, {cell: dictionary}, true]
""").head
    engine3.action("three").asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, true, false, false, false, false, false, false, false))
    engine3.action("adfadfadf").asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, false, false, false, false, false, false, false, true))

    val engine4 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: boolean
action:
  cast.fanoutBoolean: [input, 10, 20, false]
""").head
    engine4.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, true, false, false, false, false, false, false))
    engine4.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, false, false, false, false, false, false, false))
 
    val engine5 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: boolean
action:
  cast.fanoutBoolean: [input, 10, 20, true]
""").head
    engine5.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, true, false, false, false, false, false, false, false))
    engine5.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Boolean]].toVector should be (Vector(false, false, false, false, false, false, false, false, false, false, true))
 }

  it must "do fanoutInt" taggedAs(Lib, LibCast) in {
    val engine1 = PFAEngine.fromYaml("""
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: int
action:
  cast.fanoutInt: input
""").head
    engine1.action(engine1.jsonInput(""""three"""")).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 1, 0, 0, 0, 0, 0, 0))

    val engine2 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: int
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutInt: [input, {cell: dictionary}, false]
""").head
    engine2.action("three").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 1, 0, 0, 0, 0, 0, 0))
    engine2.action("sdfasdf").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    val engine3 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: int
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutInt: [input, {cell: dictionary}, true]
""").head
    engine3.action("three").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0))
    engine3.action("adfadfadf").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1))

    val engine4 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: int
action:
  cast.fanoutInt: [input, 10, 20, false]
""").head
    engine4.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 1, 0, 0, 0, 0, 0, 0))
    engine4.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
 
    val engine5 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: int
action:
  cast.fanoutInt: [input, 10, 20, true]
""").head
    engine5.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0))
    engine5.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1))
 }

  it must "do fanoutLong" taggedAs(Lib, LibCast) in {
    val engine1 = PFAEngine.fromYaml("""
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: long
action:
  cast.fanoutLong: input
""").head
    engine1.action(engine1.jsonInput(""""three"""")).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L))

    val engine2 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: long
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutLong: [input, {cell: dictionary}, false]
""").head
    engine2.action("three").asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L))
    engine2.action("sdfasdf").asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L))

    val engine3 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: long
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutLong: [input, {cell: dictionary}, true]
""").head
    engine3.action("three").asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L))
    engine3.action("adfadfadf").asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L))

    val engine4 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: long
action:
  cast.fanoutLong: [input, 10, 20, false]
""").head
    engine4.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L))
    engine4.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L))
 
    val engine5 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: long
action:
  cast.fanoutLong: [input, 10, 20, true]
""").head
    engine5.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L))
    engine5.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L))
 }

  it must "do fanoutFloat" taggedAs(Lib, LibCast) in {
    val engine1 = PFAEngine.fromYaml("""
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: float
action:
  cast.fanoutFloat: input
""").head
    engine1.action(engine1.jsonInput(""""three"""")).asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))

    val engine2 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: float
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutFloat: [input, {cell: dictionary}, false]
""").head
    engine2.action("three").asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))
    engine2.action("sdfasdf").asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))

    val engine3 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: float
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutFloat: [input, {cell: dictionary}, true]
""").head
    engine3.action("three").asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))
    engine3.action("adfadfadf").asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 1.0F))

    val engine4 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: float
action:
  cast.fanoutFloat: [input, 10, 20, false]
""").head
    engine4.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))
    engine4.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))
 
    val engine5 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: float
action:
  cast.fanoutFloat: [input, 10, 20, true]
""").head
    engine5.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F))
    engine5.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Float]].toVector should be (Vector(0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 1.0F))
 }

  it must "do fanoutDouble" taggedAs(Lib, LibCast) in {
    val engine1 = PFAEngine.fromYaml("""
input:
  type: enum
  name: Something
  symbols: [zero, one, two, three, four, five, six, seven, eight, nine]
output:
  type: array
  items: double
action:
  cast.fanoutDouble: input
""").head
    engine1.action(engine1.jsonInput(""""three"""")).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))

    val engine2 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: double
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutDouble: [input, {cell: dictionary}, false]
""").head
    engine2.action("three").asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine2.action("sdfasdf").asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))

    val engine3 = PFAEngine.fromYaml("""
input: string
output:
  type: array
  items: double
cells:
  dictionary:
    type: {type: array, items: string}
    init: [zero, one, two, three, four, five, six, seven, eight, nine]
action:
  cast.fanoutDouble: [input, {cell: dictionary}, true]
""").head
    engine3.action("three").asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine3.action("adfadfadf").asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0))

    val engine4 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: double
action:
  cast.fanoutDouble: [input, 10, 20, false]
""").head
    engine4.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine4.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
 
    val engine5 = PFAEngine.fromYaml("""
input: int
output:
  type: array
  items: double
action:
  cast.fanoutDouble: [input, 10, 20, true]
""").head
    engine5.action(java.lang.Integer.valueOf(13)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
    engine5.action(java.lang.Integer.valueOf(999)).asInstanceOf[PFAArray[Double]].toVector should be (Vector(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0))
 }

  it must "do cast.avro" taggedAs(Lib, LibCast) in {
    PFAEngine.fromYaml("""
input: string
output: string
action: {bytes.toBase64: {cast.avro: input}}
""").head.action("hello") should be ("CmhlbGxv")

    PFAEngine.fromYaml("""
input: int
output: string
action: {bytes.toBase64: {cast.avro: input}}
""").head.action(java.lang.Integer.valueOf(12)) should be ("GA==")

    PFAEngine.fromYaml("""
input: double
output: string
action: {bytes.toBase64: {cast.avro: input}}
""").head.action(java.lang.Double.valueOf(3.14)) should be ("H4XrUbgeCUA=")

    PFAEngine.fromYaml("""
input: [string, int]
output: string
action: {bytes.toBase64: {cast.avro: input}}
""").head.action("hello") should be ("AApoZWxsbw==")

    PFAEngine.fromYaml("""
input: [string, int]
output: string
action: {bytes.toBase64: {cast.avro: input}}
""").head.action(java.lang.Integer.valueOf(12)) should be ("Ahg=")

    val engine = PFAEngine.fromYaml("""
input:
  type: record
  name: Input
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
output: string
action: {bytes.toBase64: {cast.avro: input}}
""").head
    engine.action(engine.jsonInput("""{"one": 1, "two": 2.2, "three": "THREE"}""")) should be ("ApqZmZmZmQFAClRIUkVF")
  }
  
  it must "do cast.json" taggedAs(Lib, LibCast) in {
    PFAEngine.fromYaml("""
input: string
output: string
action: {cast.json: input}
""").head.action("hello") should be (""""hello"""")

    PFAEngine.fromYaml("""
input: int
output: string
action: {cast.json: input}
""").head.action(java.lang.Integer.valueOf(12)) should be ("12")

    PFAEngine.fromYaml("""
input: double
output: string
action: {cast.json: input}
""").head.action(java.lang.Double.valueOf(3.14)) should be ("3.14")

    PFAEngine.fromYaml("""
input: [string, int]
output: string
action: {cast.json: input}
""").head.action("hello") should be ("""{"string":"hello"}""")

    PFAEngine.fromYaml("""
input: [string, int]
output: string
action: {cast.json: input}
""").head.action(java.lang.Integer.valueOf(12)) should be ("""{"int":12}""")

    val engine = PFAEngine.fromYaml("""
input:
  type: record
  name: Input
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
output: string
action: {cast.json: input}
""").head
    engine.action(engine.jsonInput("""{"one": 1, "two": 2.2, "three": "THREE"}""")) should be ("""{"one":1,"two":2.2,"three":"THREE"}""")
  }
  
}
