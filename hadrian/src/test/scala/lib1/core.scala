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

package test.scala.lib1.core

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1CoreSuite extends FlatSpec with Matchers {
  "four-function calculator" must "do addition" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {+: [input, input]}
""").head.action(java.lang.Double.valueOf(3.14)) should be (6.28)
  }

  it must "handle addition int overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {+: [input, 10]}
""").head
    engine.action(java.lang.Integer.valueOf(2147483637)) should be (2147483647)
    evaluating { engine.action(java.lang.Integer.valueOf(2147483638)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {+: [input, -10]}
""").head
    engine2.action(java.lang.Integer.valueOf(-2147483638)) should be (-2147483648)
    evaluating { engine2.action(java.lang.Integer.valueOf(-2147483639)) } should produce [PFARuntimeException]
  }

  it must "handle addition long overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {+: [input, 10]}
""").head
    engine.action(java.lang.Long.valueOf(9223372036854775797L)) should be (9223372036854775807L)
    evaluating { engine.action(java.lang.Long.valueOf(9223372036854775798L)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {+: [input, -10]}
""").head
    engine2.action(java.lang.Long.valueOf(-9223372036854775798L)) should be (-9223372036854775808L)
    evaluating { engine2.action(java.lang.Long.valueOf(-9223372036854775799L)) } should produce [PFARuntimeException]
  }

  it must "do subtraction" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {-: [input, 1.1]}
""").head.action(java.lang.Double.valueOf(3.14)) should be (2.04)

    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {-: [1.1, input]}
""").head.action(java.lang.Double.valueOf(3.14)) should be (-2.04)
  }

  it must "handle subtraction int overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {-: [-10, input]}
""").head
    engine.action(java.lang.Integer.valueOf(2147483638)) should be (-2147483648)
    evaluating { engine.action(java.lang.Integer.valueOf(2147483639)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {-: [10, input]}
""").head
    engine2.action(java.lang.Integer.valueOf(-2147483637)) should be (2147483647)
    evaluating { engine2.action(java.lang.Integer.valueOf(-2147483638)) } should produce [PFARuntimeException]
  }

  it must "handle subtraction long overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {-: [-10, input]}
""").head
    engine.action(java.lang.Long.valueOf(9223372036854775798L)) should be (-9223372036854775808L)
    evaluating { engine.action(java.lang.Long.valueOf(9223372036854775799L)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {-: [10, input]}
""").head
    engine2.action(java.lang.Long.valueOf(-9223372036854775797L)) should be (9223372036854775807L)
    evaluating { engine2.action(java.lang.Long.valueOf(-9223372036854775798L)) } should produce [PFARuntimeException]
  }

  it must "handle negative int overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {u-: [input]}
""").head
    engine.action(java.lang.Integer.valueOf(2147483647)) should be (-2147483647)
    evaluating { engine.action(java.lang.Integer.valueOf(-2147483648)) } should produce [PFARuntimeException]
  }

  it must "handle negative long overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {u-: [input]}
""").head
    engine.action(java.lang.Long.valueOf(9223372036854775807L)) should be (-9223372036854775807L)
    evaluating { engine.action(java.lang.Long.valueOf(-9223372036854775808L)) } should produce [PFARuntimeException]
  }

  it must "do multiplication" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"*": [input, input]}
""").head.action(java.lang.Double.valueOf(3.14)).asInstanceOf[Double] should be (9.86 +- 0.01)

    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"*": [{"*": [input, input]}, input]}
""").head.action(java.lang.Double.valueOf(3.14)).asInstanceOf[Double] should be (30.96 +- 0.01)
  }

  it must "handle multiplication int overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"*": [input, 2]}
""").head
    engine.action(java.lang.Integer.valueOf(1073741823)) should be (2147483646)
    evaluating { engine.action(java.lang.Integer.valueOf(1073741824)) } should produce [PFARuntimeException]
  }

  it must "handle multiplication long overflows" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {"*": [input, 2]}
""").head
    engine.action(java.lang.Long.valueOf(4611686018427387903L)) should be (9223372036854775806L)
    evaluating { engine.action(java.lang.Long.valueOf(4611686018427387904L)) } should produce [PFARuntimeException]
    engine.action(java.lang.Long.valueOf(-4611686018427387904L)) should be (-9223372036854775808L)
    evaluating { engine.action(java.lang.Long.valueOf(-4611686018427387905L)) } should produce [PFARuntimeException]
  }

  it must "do floating-point division" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {/: [5, 3]}
""").head.action(null).asInstanceOf[Double] should be (1.67 +- 0.01)

    evaluating { PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {/: [5, 3]}
""").head } should produce [PFASemanticException]
  }

  it must "do integer division" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {//: [5, 3]}
""").head.action(null) should be (1)
  }

  it must "do negations with the right overflow handling" taggedAs(Lib1, Lib1Core) in {
    val intEngine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {u-: [input]}
""").head

    intEngine.action(java.lang.Integer.valueOf(12)) should be (-12)
    intEngine.action(java.lang.Integer.valueOf(2147483647)) should be (-2147483647)
    evaluating { intEngine.action(java.lang.Integer.valueOf(-2147483648)) } should produce [PFARuntimeException]

    val longEngine = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {u-: [input]}
""").head

    longEngine.action(java.lang.Long.valueOf(12)) should be (-12)
    longEngine.action(java.lang.Long.valueOf(9223372036854775807L)) should be (-9223372036854775807L)
    evaluating { longEngine.action(java.lang.Long.valueOf(-9223372036854775808L)) } should produce [PFARuntimeException]
  }

  it must "interpret % as a modulo operator (not a division remainder)" taggedAs(Lib1, Lib1Core) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"%": [input, 6]}
""").head
    engine1.action(java.lang.Integer.valueOf(15)) should be (3)
    engine1.action(java.lang.Integer.valueOf(-15)) should be (3)

    val engine2 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"%": [input, -6]}
""").head
    engine2.action(java.lang.Integer.valueOf(15)) should be (-3)
    engine2.action(java.lang.Integer.valueOf(-15)) should be (-3)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%": [input, 6]}
""").head
    engine3.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (3.2 +- 0.01)
    engine3.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (2.8 +- 0.01)

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%": [input, -6]}
""").head
    engine4.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (-2.8 +- 0.01)
    engine4.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (-3.2 +- 0.01)

    val engine5 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%": [input, 6.4]}
""").head
    engine5.action(java.lang.Double.valueOf(15)).asInstanceOf[Double] should be (2.2 +- 0.01)
    engine5.action(java.lang.Double.valueOf(-15)).asInstanceOf[Double] should be (4.2 +- 0.01)
    engine5.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (2.4 +- 0.01)
    engine5.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (4.0 +- 0.01)

    val engine6 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%": [input, -6.4]}
""").head
    engine6.action(java.lang.Double.valueOf(15)).asInstanceOf[Double] should be (-4.2 +- 0.01)
    engine6.action(java.lang.Double.valueOf(-15)).asInstanceOf[Double] should be (-2.2 +- 0.01)
    engine6.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (-4.0 +- 0.01)
    engine6.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (-2.4 +- 0.01)
  }

  it must "interpret %% as a division remainder (not a modulo operator)" taggedAs(Lib1, Lib1Core) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"%%": [input, 6]}
""").head
    engine1.action(java.lang.Integer.valueOf(15)) should be (3)
    engine1.action(java.lang.Integer.valueOf(-15)) should be (-3)

    val engine2 = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"%%": [input, -6]}
""").head
    engine2.action(java.lang.Integer.valueOf(15)) should be (3)
    engine2.action(java.lang.Integer.valueOf(-15)) should be (-3)

    val engine3 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%%": [input, 6]}
""").head
    engine3.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (3.2 +- 0.01)
    engine3.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (-3.2 +- 0.01)

    val engine4 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%%": [input, -6]}
""").head
    engine4.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (3.2 +- 0.01)
    engine4.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (-3.2 +- 0.01)

    val engine5 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%%": [input, 6.4]}
""").head
    engine5.action(java.lang.Double.valueOf(15)).asInstanceOf[Double] should be (2.2 +- 0.01)
    engine5.action(java.lang.Double.valueOf(-15)).asInstanceOf[Double] should be (-2.2 +- 0.01)
    engine5.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (2.4 +- 0.01)
    engine5.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (-2.4 +- 0.01)

    val engine6 = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"%%": [input, -6.4]}
""").head
    engine6.action(java.lang.Double.valueOf(15)).asInstanceOf[Double] should be (2.2 +- 0.01)
    engine6.action(java.lang.Double.valueOf(-15)).asInstanceOf[Double] should be (-2.2 +- 0.01)
    engine6.action(java.lang.Double.valueOf(15.2)).asInstanceOf[Double] should be (2.4 +- 0.01)
    engine6.action(java.lang.Double.valueOf(-15.2)).asInstanceOf[Double] should be (-2.4 +- 0.01)
  }

  it must "do exponentiation (pow or **)" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {"**": [input, 30]}
""").head.action(java.lang.Double.valueOf(2.5)).asInstanceOf[Double] should be (867361737988.4036 +- 0.01)

    PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"**": [input, 30]}
""").head.action(java.lang.Integer.valueOf(2)) should be (1073741824)

    evaluating { PFAEngine.fromYaml("""
input: int
output: int
action:
  - {"**": [input, 30]}
""").head.action(java.lang.Integer.valueOf(3)) } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: long
output: long
action:
  - {"**": [input, 30]}
""").head.action(java.lang.Long.valueOf(3)) should be (205891132094649L)
  }

  "comparison operators" must "do cmp" taggedAs(Lib1, Lib1Core) in {
    val intEngine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {cmp: [input, 5]}
""").head
    intEngine.action(java.lang.Integer.valueOf(3)) should be (-1)
    intEngine.action(java.lang.Integer.valueOf(5)) should be (0)
    intEngine.action(java.lang.Integer.valueOf(7)) should be (1)

    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {cmp: [input, 5.3]}
""").head
    doubleEngine.action(java.lang.Double.valueOf(5.2)) should be (-1)
    doubleEngine.action(java.lang.Double.valueOf(5.3)) should be (0)
    doubleEngine.action(java.lang.Double.valueOf(5.4)) should be (1)

    val stringEngine = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cmp: [input, [HAL]]}
""").head
    stringEngine.action("GZK") should be (-1)
    stringEngine.action("HAL") should be (0)
    stringEngine.action("IBM") should be (1)
  }

  it must "do specific numerical operators" taggedAs(Lib1, Lib1Core) in {
    val LE = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"<=": [input, 5.3]}
""").head

    val LT = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"<": [input, 5.3]}
""").head

    val NE = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"!=": [input, 5.3]}
""").head

    val EQ = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {"==": [input, 5.3]}
""").head

    val GE = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {">=": [input, 5.3]}
""").head

    val GT = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {">": [input, 5.3]}
""").head

    LE.action(java.lang.Double.valueOf(5.2)).asInstanceOf[Boolean] should be (true)
    LE.action(java.lang.Double.valueOf(5.3)).asInstanceOf[Boolean] should be (true)
    LE.action(java.lang.Double.valueOf(5.4)).asInstanceOf[Boolean] should be (false)

    LT.action(java.lang.Double.valueOf(5.2)).asInstanceOf[Boolean] should be (true)
    LT.action(java.lang.Double.valueOf(5.3)).asInstanceOf[Boolean] should be (false)
    LT.action(java.lang.Double.valueOf(5.4)).asInstanceOf[Boolean] should be (false)

    NE.action(java.lang.Double.valueOf(5.2)).asInstanceOf[Boolean] should be (true)
    NE.action(java.lang.Double.valueOf(5.3)).asInstanceOf[Boolean] should be (false)
    NE.action(java.lang.Double.valueOf(5.4)).asInstanceOf[Boolean] should be (true)

    EQ.action(java.lang.Double.valueOf(5.2)).asInstanceOf[Boolean] should be (false)
    EQ.action(java.lang.Double.valueOf(5.3)).asInstanceOf[Boolean] should be (true)
    EQ.action(java.lang.Double.valueOf(5.4)).asInstanceOf[Boolean] should be (false)

    GE.action(java.lang.Double.valueOf(5.2)).asInstanceOf[Boolean] should be (false)
    GE.action(java.lang.Double.valueOf(5.3)).asInstanceOf[Boolean] should be (true)
    GE.action(java.lang.Double.valueOf(5.4)).asInstanceOf[Boolean] should be (true)

    GT.action(java.lang.Double.valueOf(5.2)).asInstanceOf[Boolean] should be (false)
    GT.action(java.lang.Double.valueOf(5.3)).asInstanceOf[Boolean] should be (false)
    GT.action(java.lang.Double.valueOf(5.4)).asInstanceOf[Boolean] should be (true)
  }

  it must "do specific non-numeric operators" taggedAs(Lib1, Lib1Core) in {
    val LE = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"<=": [input, [HAL]]}
""").head

    val LT = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"<": [input, [HAL]]}
""").head

    val NE = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"!=": [input, [HAL]]}
""").head

    val EQ = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {"==": [input, [HAL]]}
""").head

    val GE = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {">=": [input, [HAL]]}
""").head

    val GT = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {">": [input, [HAL]]}
""").head

    LE.action("GZK").asInstanceOf[Boolean] should be (true)
    LE.action("HAL").asInstanceOf[Boolean] should be (true)
    LE.action("IBM").asInstanceOf[Boolean] should be (false)

    LT.action("GZK").asInstanceOf[Boolean] should be (true)
    LT.action("HAL").asInstanceOf[Boolean] should be (false)
    LT.action("IBM").asInstanceOf[Boolean] should be (false)

    NE.action("GZK").asInstanceOf[Boolean] should be (true)
    NE.action("HAL").asInstanceOf[Boolean] should be (false)
    NE.action("IBM").asInstanceOf[Boolean] should be (true)

    EQ.action("GZK").asInstanceOf[Boolean] should be (false)
    EQ.action("HAL").asInstanceOf[Boolean] should be (true)
    EQ.action("IBM").asInstanceOf[Boolean] should be (false)

    GE.action("GZK").asInstanceOf[Boolean] should be (false)
    GE.action("HAL").asInstanceOf[Boolean] should be (true)
    GE.action("IBM").asInstanceOf[Boolean] should be (true)

    GT.action("GZK").asInstanceOf[Boolean] should be (false)
    GT.action("HAL").asInstanceOf[Boolean] should be (false)
    GT.action("IBM").asInstanceOf[Boolean] should be (true)
  }

  it must "reject comparisons between different types, even if they have the same structure" taggedAs(Lib1, Lib1Core) in {
    val engine = PFAEngine.fromYaml("""
input: {type: enum, name: Category1, symbols: [z, y, x, w]}
output: boolean
action:
  - {"==": [input, {type: Category1, value: x}]}
""").head
    engine.action(engine.jsonInput(""""z"""")).asInstanceOf[Boolean] should be (false)
    engine.action(engine.jsonInput(""""y"""")).asInstanceOf[Boolean] should be (false)
    engine.action(engine.jsonInput(""""x"""")).asInstanceOf[Boolean] should be (true)
    engine.action(engine.jsonInput(""""w"""")).asInstanceOf[Boolean] should be (false)

    evaluating { PFAEngine.fromYaml("""
input: {type: enum, name: Category1, symbols: [z, y, x, w]}
output: boolean
action:
  - {"==": [input, {type: {type: enum, name: Category2, symbols: [w, x, y, z]}, value: x}]}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: {type: enum, name: Category1, symbols: [z, y, x, w]}
output: boolean
action:
  - {"==": [input, {type: {type: enum, name: Category2, symbols: [w, x, y, whatever]}, value: x}]}
""").head } should produce [PFASemanticException]
  }

  it must "do max" taggedAs(Lib1, Lib1Math) in {
    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {max: [input, 3.2]}
""").head
    doubleEngine.action(java.lang.Double.valueOf(2.2)) should be (3.2)
    doubleEngine.action(java.lang.Double.valueOf(4.2)) should be (4.2)

    val intEngine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {max: [input, 3]}
""").head
    intEngine.action(java.lang.Integer.valueOf(2)) should be (3)
    intEngine.action(java.lang.Integer.valueOf(4)) should be (4)

    val stringEngine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {max: [input, [HAL]]}
""").head
    stringEngine.action("GZK") should be ("HAL")
    stringEngine.action("IBM") should be ("IBM")
  }

  it must "do min" taggedAs(Lib1, Lib1Math) in {
    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {min: [input, 3.2]}
""").head
    doubleEngine.action(java.lang.Double.valueOf(2.2)) should be (2.2)
    doubleEngine.action(java.lang.Double.valueOf(4.2)) should be (3.2)

    val intEngine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {min: [input, 3]}
""").head
    intEngine.action(java.lang.Integer.valueOf(2)) should be (2)
    intEngine.action(java.lang.Integer.valueOf(4)) should be (3)

    val stringEngine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {min: [input, [HAL]]}
""").head
    stringEngine.action("GZK") should be ("GZK")
    stringEngine.action("IBM") should be ("HAL")
  }

  "logical operators" must "work" taggedAs(Lib1, Lib1Core) in {
    val and = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log:
      - {"&&": [true, true]}
      - {"&&": [true, false]}
      - {"&&": [false, true]}
      - {"&&": [false, false]}
""").head
    and.log = (message: String, ns: Option[String]) => message should be ("true false false false")
    and.action(null)

    val or = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log:
      - {"||": [true, true]}
      - {"||": [true, false]}
      - {"||": [false, true]}
      - {"||": [false, false]}
""").head
    or.log = (message: String, ns: Option[String]) => message should be ("true true true false")
    or.action(null)

    val xor = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log:
      - {"^^": [true, true]}
      - {"^^": [true, false]}
      - {"^^": [false, true]}
      - {"^^": [false, false]}
""").head
    xor.log = (message: String, ns: Option[String]) => message should be ("false true true false")
    xor.action(null)

    val not = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log:
      - {"!": [true]}
      - {"!": [false]}
""").head
    not.log = (message: String, ns: Option[String]) => message should be ("false true")
    not.action(null)
  }

  "bitwise operators" must "work" taggedAs(Lib1, Lib1Core) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {"&": [85, 15]}
""").head.action(null).asInstanceOf[Int] should be (5)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {"|": [85, 15]}
""").head.action(null).asInstanceOf[Int] should be (95)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {"^": [85, 15]}
""").head.action(null).asInstanceOf[Int] should be (90)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {"~": [85]}
""").head.action(null).asInstanceOf[Int] should be (-86)
  }
}
