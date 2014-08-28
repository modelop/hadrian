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

package test.scala.lib1.math

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1MathSuite extends FlatSpec with Matchers {
  "math library" must "provide constants" taggedAs(Lib1, Lib1Math) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {m.pi: []}
""").head.action(null).asInstanceOf[Double] should be (3.141592653589793 +- 0.000000000000010)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {m.e: []}
""").head.action(null).asInstanceOf[Double] should be (2.718281828459045 +- 0.000000000000010)
  }

  it must "do abs" taggedAs(Lib1, Lib1Math) in {
    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.abs: input}
""").head.action(java.lang.Double.valueOf(-3.14)).asInstanceOf[Double] should be (3.14 +- 0.01)

    val intEngine = PFAEngine.fromYaml("""
input: int
output: int
action:
  - {m.abs: input}
""").head
    intEngine.action(java.lang.Integer.valueOf(2147483647)) should be (2147483647)
    intEngine.action(java.lang.Integer.valueOf(-2147483647)) should be (2147483647)
    evaluating { intEngine.action(java.lang.Integer.valueOf(-2147483648)) } should produce [PFARuntimeException]

    val longEngine = PFAEngine.fromYaml("""
input: long
output: long
action:
  - {m.abs: input}
""").head
    longEngine.action(java.lang.Long.valueOf(9223372036854775807L)) should be (9223372036854775807L)
    longEngine.action(java.lang.Long.valueOf(-9223372036854775807L)) should be (9223372036854775807L)
    evaluating { longEngine.action(java.lang.Long.valueOf(-9223372036854775808L)) } should produce [PFARuntimeException]
  }

  it must "do acos" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.acos: input}
""").head
    engine.action(java.lang.Double.valueOf(-10)).toString should be ("NaN")
    engine.action(java.lang.Double.valueOf(-1)).asInstanceOf[Double] should be (3.14 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.8)).asInstanceOf[Double] should be (2.50 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (1.57 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[Double] should be (0.64 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (0.00 +- 0.01)
  }

  it must "do asin" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.asin: input}
""").head
    engine.action(java.lang.Double.valueOf(-10)).toString should be ("NaN")
    engine.action(java.lang.Double.valueOf(-1)).asInstanceOf[Double] should be (-1.57 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.8)).asInstanceOf[Double] should be (-0.93 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[Double] should be (0.93 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (1.57 +- 0.01)
  }

  it must "do atan" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.atan: input}
""").head
    engine.action(java.lang.Double.valueOf(-1)).asInstanceOf[Double] should be (-0.79 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.8)).asInstanceOf[Double] should be (-0.67 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[Double] should be (0.67 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (0.79 +- 0.01)
  }

  it must "do atan2" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.atan2: [input, 1]}
""").head
    engine.action(java.lang.Double.valueOf(-1)).asInstanceOf[Double] should be (-0.79 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.8)).asInstanceOf[Double] should be (-0.67 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.8)).asInstanceOf[Double] should be (0.67 +- 0.01)
    engine.action(java.lang.Double.valueOf(1)).asInstanceOf[Double] should be (0.79 +- 0.01)
  }

  it must "do ceil" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.ceil: input}
""").head
    engine.action(java.lang.Double.valueOf(-3.2)) should be (-3)
    engine.action(java.lang.Double.valueOf(0)) should be (0)
    engine.action(java.lang.Double.valueOf(3.2)) should be (4)
  }

  it must "do copySign" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.copysign: [5, input]}
""").head
    engine.action(java.lang.Double.valueOf(-3.2)) should be (-5)
    engine.action(java.lang.Double.valueOf(0)) should be (5)
    engine.action(java.lang.Double.valueOf(3.2)) should be (5)
  }

  it must "do cos" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.cos: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (-0.87 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (0.88 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.88 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (-0.87 +- 0.01)
  }

  it must "do cosh" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.cosh: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (2955261031.51 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (1.13 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (1.13 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (2955261031.51 +- 0.01)
  }

  it must "do exp" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.exp: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (0.61 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (1.65 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (5910522063.02 +- 0.01)
  }

  it must "do expm1" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.expm1: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (-1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (-0.39 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.65 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (5910522062.02 +- 0.01)
  }

  it must "do floor" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.floor: input}
""").head
    engine.action(java.lang.Double.valueOf(-3.2)) should be (-4)
    engine.action(java.lang.Double.valueOf(0)) should be (0)
    engine.action(java.lang.Double.valueOf(3.2)) should be (3)
  }

  it must "do hypot" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.hypot: [input, 3.5]}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (22.77 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (3.54 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (3.50 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (3.54 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (22.77 +- 0.01)
  }

  it must "do ln" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.ln: input}
""").head
    engine.action(java.lang.Double.valueOf(-1)).toString should be ("NaN")
    engine.action(java.lang.Double.valueOf(0)).toString should be ("-Infinity")
    engine.action(java.lang.Double.valueOf(0.00001)).asInstanceOf[Double] should be (-11.51 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (-0.69 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (3.11 +- 0.01)
  }

  it must "do log10" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.log10: input}
""").head
    engine.action(java.lang.Double.valueOf(-1)).toString should be ("NaN")
    engine.action(java.lang.Double.valueOf(0)).toString should be ("-Infinity")
    engine.action(java.lang.Double.valueOf(0.00001)).asInstanceOf[Double] should be (-5.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (-0.30 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (1.35 +- 0.01)
  }

  it must "do arbitrary-base log" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {m.log: [5.5, input]}
""").head
    engine.action(java.lang.Integer.valueOf(2)).asInstanceOf[Double] should be (2.46 +- 0.01)
    engine.action(java.lang.Integer.valueOf(5)).asInstanceOf[Double] should be (1.06 +- 0.01)
    engine.action(java.lang.Integer.valueOf(10)).asInstanceOf[Double] should be (0.74 +- 0.01)
    engine.action(java.lang.Integer.valueOf(16)).asInstanceOf[Double] should be (0.61 +- 0.01)

    evaluating { engine.action(java.lang.Integer.valueOf(0)) } should produce [PFARuntimeException]
    evaluating { engine.action(java.lang.Integer.valueOf(-1)) } should produce [PFARuntimeException]
  }

  it must "do ln1p" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.ln1p: input}
""").head
    engine.action(java.lang.Double.valueOf(-2)).toString should be ("NaN")
    engine.action(java.lang.Double.valueOf(-1)).toString should be ("-Infinity")
    engine.action(java.lang.Double.valueOf(-0.99999)).asInstanceOf[Double] should be (-11.51 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.00001)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.41 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (3.16 +- 0.01)
  }

  it must "do round" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: long
action:
  - {m.round: input}
""").head
    engine.action(java.lang.Double.valueOf(-3.8)) should be (-4)
    engine.action(java.lang.Double.valueOf(-3.5)) should be (-3)
    engine.action(java.lang.Double.valueOf(-3.2)) should be (-3)
    engine.action(java.lang.Double.valueOf(0)) should be (0)
    engine.action(java.lang.Double.valueOf(3.2)) should be (3)
    engine.action(java.lang.Double.valueOf(3.5)) should be (4)
    engine.action(java.lang.Double.valueOf(3.8)) should be (4)
    engine.action(java.lang.Double.valueOf(9.223372036854776e+18)) should be (9223372036854775807L)
    evaluating { engine.action(java.lang.Double.valueOf(9.223372036854777e+18)) } should produce [PFARuntimeException]
    engine.action(java.lang.Double.valueOf(-9.223372036854776e+18)) should be (-9223372036854775808L)
    evaluating { engine.action(java.lang.Double.valueOf(-9.223372036854777e+18)) } should produce [PFARuntimeException]
  }

  it must "do rint" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.rint: input}
""").head
    engine.action(java.lang.Double.valueOf(-3.8)) should be (-4)
    engine.action(java.lang.Double.valueOf(-3.5)) should be (-4)
    engine.action(java.lang.Double.valueOf(-3.2)) should be (-3)
    engine.action(java.lang.Double.valueOf(0)) should be (0)
    engine.action(java.lang.Double.valueOf(3.2)) should be (3)
    engine.action(java.lang.Double.valueOf(3.5)) should be (4)
    engine.action(java.lang.Double.valueOf(3.8)) should be (4)
  }

  it must "do signum" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {m.signum: input}
""").head
    engine.action(java.lang.Double.valueOf(-3.2)) should be (-1)
    engine.action(java.lang.Double.valueOf(0)) should be (0)
    engine.action(java.lang.Double.valueOf(3.2)) should be (1)
  }

  it must "do sin" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.sin: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (0.49 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (-0.48 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.48 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (-0.49 +- 0.01)
  }

  it must "do sinh" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.sinh: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (-2955261031.51 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (-0.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (2955261031.51 +- 0.01)
  }

  it must "do sqrt" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.sqrt: input}
""").head
    engine.action(java.lang.Double.valueOf(-1)).toString should be ("NaN")
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.71 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (4.74 +- 0.01)
  }

  it must "do tan" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.tan: input}
""").head
    engine.action(java.lang.Double.valueOf(-10.5)).asInstanceOf[Double] should be (-1.85 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (-0.55 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.55 +- 0.01)
    engine.action(java.lang.Double.valueOf(10.5)).asInstanceOf[Double] should be (1.85 +- 0.01)
  }

  it must "do tanh" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.tanh: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (-1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (-0.46 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.46 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (1.00 +- 0.01)
  }

  it must "do erf" taggedAs(Lib1, Lib1Math) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {m.special.erf: input}
""").head
    engine.action(java.lang.Double.valueOf(-22.5)).asInstanceOf[Double] should be (-1.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(-0.5)).asInstanceOf[Double] should be (-0.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (0.00 +- 0.01)
    engine.action(java.lang.Double.valueOf(0.5)).asInstanceOf[Double] should be (0.52 +- 0.01)
    engine.action(java.lang.Double.valueOf(22.5)).asInstanceOf[Double] should be (1.00 +- 0.01)
  }

}
