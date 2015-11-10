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

package test.scala.lib.string

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
class LibStringSuite extends FlatSpec with Matchers {
  "basic access" must "get length" taggedAs(Lib, LibString) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {s.len: [input]}
""").head.action("hello") should be (5)
  }

  it must "get substring" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: string
action:
  - {s.substr: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], 5, input]}
""").head
    engine.action(java.lang.Integer.valueOf(10)) should be ("FGHIJ")
    engine.action(java.lang.Integer.valueOf(-10)) should be ("FGHIJKLMNOP")
    engine.action(java.lang.Integer.valueOf(0)) should be ("")
    engine.action(java.lang.Integer.valueOf(1)) should be ("")
    engine.action(java.lang.Integer.valueOf(100)) should be ("FGHIJKLMNOPQRSTUVWXYZ")
  }

  it must "set substring" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: string
action:
  - {s.substrto: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], 5, input, [...]]}
""").head
    engine.action(java.lang.Integer.valueOf(10)) should be ("ABCDE...KLMNOPQRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(-10)) should be ("ABCDE...QRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(0)) should be ("ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(1)) should be ("ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(100)) should be ("ABCDE...")
  }

  "searching" must "do contains" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {s.contains: [input, [DEFG]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ").asInstanceOf[Boolean] should be (true)
    engine.action("ack! ack! ack!").asInstanceOf[Boolean] should be (false)
  }

  it must "do count" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {s.count: [input, [ack!]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ") should be (0)
    engine.action("ack! ack! ack!") should be (3)
    engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf") should be (2)
    engine.action("adfasdfadack!asdfasdfasdf") should be (1)
  }

  it must "do index" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {s.index: [input, [ack!]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ") should be (-1)
    engine.action("ack! ack! ack!") should be (0)
    engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf") should be (10)
    engine.action("adfasdfadack!asdfasdfasdf") should be (9)
  }

  it must "do rindex" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {s.rindex: [input, [ack!]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ") should be (-1)
    engine.action("ack! ack! ack!") should be (10)
    engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf") should be (36)
    engine.action("adfasdfadack!asdfasdfasdf") should be (9)
  }

  it must "do startswith" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {s.startswith: [input, [ack!]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ").asInstanceOf[Boolean] should be (false)
    engine.action("ack! ack! ack!").asInstanceOf[Boolean] should be (true)
    engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf").asInstanceOf[Boolean] should be (false)
    engine.action("adfasdfadack!asdfasdfasdf").asInstanceOf[Boolean] should be (false)
  }

  it must "do endswith" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {s.endswith: [input, [ack!]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ").asInstanceOf[Boolean] should be (false)
    engine.action("ack! ack! ack!").asInstanceOf[Boolean] should be (true)
    engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsfack!").asInstanceOf[Boolean] should be (true)
    engine.action("adfasdfadack!asdfasdfasdf").asInstanceOf[Boolean] should be (false)
  }

  "conversions to/from other types" must "do join" taggedAs(Lib, LibString) in {
    PFAEngine.fromYaml("""
input: {type: array, items: string}
output: string
action:
  - {s.join: [input, [", "]]}
""").head.action(PFAArray.fromVector(Vector("one", "two", "three"))) should be ("one, two, three")
  }

  it must "do split" taggedAs(Lib, LibString) in {
    PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {s.split: [input, [", "]]}
""").head.action("one, two, three") should be (PFAArray.fromVector(Vector("one", "two", "three")))
  }

  it must "do hex" taggedAs(Lib, LibString) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.hex: [input]}, {string: "|"}]
""").head

    engine1.action(java.lang.Integer.valueOf(0))      should be ("0|")
    engine1.action(java.lang.Integer.valueOf(1))      should be ("1|")
    engine1.action(java.lang.Integer.valueOf(15))     should be ("f|")
    engine1.action(java.lang.Integer.valueOf(16))     should be ("10|")
    engine1.action(java.lang.Integer.valueOf(255))    should be ("ff|")
    engine1.action(java.lang.Integer.valueOf(256))    should be ("100|")
    engine1.action(java.lang.Integer.valueOf(65535))  should be ("ffff|")
    engine1.action(java.lang.Integer.valueOf(65536))  should be ("10000|")
    evaluating { engine1.action(java.lang.Integer.valueOf(-1)) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.hex: [input, 8, false]}, {string: "|"}]
""").head

    engine2.action(java.lang.Integer.valueOf(0))      should be ("       0|")
    engine2.action(java.lang.Integer.valueOf(1))      should be ("       1|")
    engine2.action(java.lang.Integer.valueOf(15))     should be ("       f|")
    engine2.action(java.lang.Integer.valueOf(16))     should be ("      10|")
    engine2.action(java.lang.Integer.valueOf(255))    should be ("      ff|")
    engine2.action(java.lang.Integer.valueOf(256))    should be ("     100|")
    engine2.action(java.lang.Integer.valueOf(65535))  should be ("    ffff|")
    engine2.action(java.lang.Integer.valueOf(65536))  should be ("   10000|")
    evaluating { engine2.action(java.lang.Integer.valueOf(-1)) } should produce [PFARuntimeException]

    val engine2a = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.hex: [input, 8, true]}, {string: "|"}]
""").head

    engine2a.action(java.lang.Integer.valueOf(0))      should be ("00000000|")
    engine2a.action(java.lang.Integer.valueOf(1))      should be ("00000001|")
    engine2a.action(java.lang.Integer.valueOf(15))     should be ("0000000f|")
    engine2a.action(java.lang.Integer.valueOf(16))     should be ("00000010|")
    engine2a.action(java.lang.Integer.valueOf(255))    should be ("000000ff|")
    engine2a.action(java.lang.Integer.valueOf(256))    should be ("00000100|")
    engine2a.action(java.lang.Integer.valueOf(65535))  should be ("0000ffff|")
    engine2a.action(java.lang.Integer.valueOf(65536))  should be ("00010000|")
    evaluating { engine2a.action(java.lang.Integer.valueOf(-1)) } should produce [PFARuntimeException]

    val engine3 = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.hex: [input, -8, false]}, {string: "|"}]
""").head

    engine3.action(java.lang.Integer.valueOf(0))      should be ("0       |")
    engine3.action(java.lang.Integer.valueOf(1))      should be ("1       |")
    engine3.action(java.lang.Integer.valueOf(15))     should be ("f       |")
    engine3.action(java.lang.Integer.valueOf(16))     should be ("10      |")
    engine3.action(java.lang.Integer.valueOf(255))    should be ("ff      |")
    engine3.action(java.lang.Integer.valueOf(256))    should be ("100     |")
    engine3.action(java.lang.Integer.valueOf(65535))  should be ("ffff    |")
    engine3.action(java.lang.Integer.valueOf(65536))  should be ("10000   |")
    evaluating { engine3.action(java.lang.Integer.valueOf(-1)) } should produce [PFARuntimeException]
  }

  it must "do number" taggedAs(Lib, LibString) in {
    val engine1 = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.int: [input]}, {string: "|"}]
""").head

    engine1.action(java.lang.Integer.valueOf(0)) should be ("0|")
    engine1.action(java.lang.Integer.valueOf(1)) should be ("1|")
    engine1.action(java.lang.Integer.valueOf(10)) should be ("10|")
    engine1.action(java.lang.Integer.valueOf(100)) should be ("100|")
    engine1.action(java.lang.Integer.valueOf(1000)) should be ("1000|")
    engine1.action(java.lang.Integer.valueOf(10000)) should be ("10000|")
    engine1.action(java.lang.Integer.valueOf(100000)) should be ("100000|")
    engine1.action(java.lang.Integer.valueOf(1000000)) should be ("1000000|")
    engine1.action(java.lang.Integer.valueOf(10000000)) should be ("10000000|")
    engine1.action(java.lang.Integer.valueOf(100000000)) should be ("100000000|")
    engine1.action(java.lang.Integer.valueOf(1000000000)) should be ("1000000000|")
    engine1.action(java.lang.Integer.valueOf(-1)) should be ("-1|")
    engine1.action(java.lang.Integer.valueOf(-10)) should be ("-10|")
    engine1.action(java.lang.Integer.valueOf(-100)) should be ("-100|")
    engine1.action(java.lang.Integer.valueOf(-1000)) should be ("-1000|")
    engine1.action(java.lang.Integer.valueOf(-10000)) should be ("-10000|")
    engine1.action(java.lang.Integer.valueOf(-100000)) should be ("-100000|")
    engine1.action(java.lang.Integer.valueOf(-1000000)) should be ("-1000000|")
    engine1.action(java.lang.Integer.valueOf(-10000000)) should be ("-10000000|")
    engine1.action(java.lang.Integer.valueOf(-100000000)) should be ("-100000000|")
    engine1.action(java.lang.Integer.valueOf(-1000000000)) should be ("-1000000000|")

    val engine2 = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.int: [input, 8, false]}, {string: "|"}]
""").head

    engine2.action(java.lang.Integer.valueOf(0)) should be ("       0|")
    engine2.action(java.lang.Integer.valueOf(1)) should be ("       1|")
    engine2.action(java.lang.Integer.valueOf(10)) should be ("      10|")
    engine2.action(java.lang.Integer.valueOf(100)) should be ("     100|")
    engine2.action(java.lang.Integer.valueOf(1000)) should be ("    1000|")
    engine2.action(java.lang.Integer.valueOf(10000)) should be ("   10000|")
    engine2.action(java.lang.Integer.valueOf(100000)) should be ("  100000|")
    engine2.action(java.lang.Integer.valueOf(1000000)) should be (" 1000000|")
    engine2.action(java.lang.Integer.valueOf(10000000)) should be ("10000000|")
    engine2.action(java.lang.Integer.valueOf(100000000)) should be ("100000000|")
    engine2.action(java.lang.Integer.valueOf(1000000000)) should be ("1000000000|")
    engine2.action(java.lang.Integer.valueOf(-1)) should be ("      -1|")
    engine2.action(java.lang.Integer.valueOf(-10)) should be ("     -10|")
    engine2.action(java.lang.Integer.valueOf(-100)) should be ("    -100|")
    engine2.action(java.lang.Integer.valueOf(-1000)) should be ("   -1000|")
    engine2.action(java.lang.Integer.valueOf(-10000)) should be ("  -10000|")
    engine2.action(java.lang.Integer.valueOf(-100000)) should be (" -100000|")
    engine2.action(java.lang.Integer.valueOf(-1000000)) should be ("-1000000|")
    engine2.action(java.lang.Integer.valueOf(-10000000)) should be ("-10000000|")
    engine2.action(java.lang.Integer.valueOf(-100000000)) should be ("-100000000|")
    engine2.action(java.lang.Integer.valueOf(-1000000000)) should be ("-1000000000|")

    val engine2a = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.int: [input, 8, true]}, {string: "|"}]
""").head

    engine2a.action(java.lang.Integer.valueOf(0)) should be ("00000000|")
    engine2a.action(java.lang.Integer.valueOf(1)) should be ("00000001|")
    engine2a.action(java.lang.Integer.valueOf(10)) should be ("00000010|")
    engine2a.action(java.lang.Integer.valueOf(100)) should be ("00000100|")
    engine2a.action(java.lang.Integer.valueOf(1000)) should be ("00001000|")
    engine2a.action(java.lang.Integer.valueOf(10000)) should be ("00010000|")
    engine2a.action(java.lang.Integer.valueOf(100000)) should be ("00100000|")
    engine2a.action(java.lang.Integer.valueOf(1000000)) should be ("01000000|")
    engine2a.action(java.lang.Integer.valueOf(10000000)) should be ("10000000|")
    engine2a.action(java.lang.Integer.valueOf(100000000)) should be ("100000000|")
    engine2a.action(java.lang.Integer.valueOf(1000000000)) should be ("1000000000|")
    engine2a.action(java.lang.Integer.valueOf(-1)) should be ("-0000001|")
    engine2a.action(java.lang.Integer.valueOf(-10)) should be ("-0000010|")
    engine2a.action(java.lang.Integer.valueOf(-100)) should be ("-0000100|")
    engine2a.action(java.lang.Integer.valueOf(-1000)) should be ("-0001000|")
    engine2a.action(java.lang.Integer.valueOf(-10000)) should be ("-0010000|")
    engine2a.action(java.lang.Integer.valueOf(-100000)) should be ("-0100000|")
    engine2a.action(java.lang.Integer.valueOf(-1000000)) should be ("-1000000|")
    engine2a.action(java.lang.Integer.valueOf(-10000000)) should be ("-10000000|")
    engine2a.action(java.lang.Integer.valueOf(-100000000)) should be ("-100000000|")
    engine2a.action(java.lang.Integer.valueOf(-1000000000)) should be ("-1000000000|")

    val engine3 = PFAEngine.fromYaml("""
input: int
output: string
action:
  s.concat: [{s.int: [input, -8, false]}, {string: "|"}]
""").head

    engine3.action(java.lang.Integer.valueOf(0)) should be ("0       |")
    engine3.action(java.lang.Integer.valueOf(1)) should be ("1       |")
    engine3.action(java.lang.Integer.valueOf(10)) should be ("10      |")
    engine3.action(java.lang.Integer.valueOf(100)) should be ("100     |")
    engine3.action(java.lang.Integer.valueOf(1000)) should be ("1000    |")
    engine3.action(java.lang.Integer.valueOf(10000)) should be ("10000   |")
    engine3.action(java.lang.Integer.valueOf(100000)) should be ("100000  |")
    engine3.action(java.lang.Integer.valueOf(1000000)) should be ("1000000 |")
    engine3.action(java.lang.Integer.valueOf(10000000)) should be ("10000000|")
    engine3.action(java.lang.Integer.valueOf(100000000)) should be ("100000000|")
    engine3.action(java.lang.Integer.valueOf(1000000000)) should be ("1000000000|")
    engine3.action(java.lang.Integer.valueOf(-1)) should be ("-1      |")
    engine3.action(java.lang.Integer.valueOf(-10)) should be ("-10     |")
    engine3.action(java.lang.Integer.valueOf(-100)) should be ("-100    |")
    engine3.action(java.lang.Integer.valueOf(-1000)) should be ("-1000   |")
    engine3.action(java.lang.Integer.valueOf(-10000)) should be ("-10000  |")
    engine3.action(java.lang.Integer.valueOf(-100000)) should be ("-100000 |")
    engine3.action(java.lang.Integer.valueOf(-1000000)) should be ("-1000000|")
    engine3.action(java.lang.Integer.valueOf(-10000000)) should be ("-10000000|")
    engine3.action(java.lang.Integer.valueOf(-100000000)) should be ("-100000000|")
    engine3.action(java.lang.Integer.valueOf(-1000000000)) should be ("-1000000000|")

    val engine4 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, null, null]}, {string: "|"}]
""").head

    engine4.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("nan|")
    engine4.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("inf|")
    engine4.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("-inf|")
    engine4.action(java.lang.Double.valueOf(0.0)) should be ("0.0|")
    engine4.action(java.lang.Double.valueOf(-0.0)) should be ("0.0|")
    engine4.action(java.lang.Double.valueOf(Math.PI)) should be ("3.141593|")
    engine4.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.0e-10|")
    engine4.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.0e-09|")
    engine4.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.0e-08|")
    engine4.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.0e-07|")
    engine4.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.0e-06|")
    engine4.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.0e-05|")
    engine4.action(java.lang.Double.valueOf(0.0001)) should be ("0.0001|")
    engine4.action(java.lang.Double.valueOf(0.001)) should be ("0.001|")
    engine4.action(java.lang.Double.valueOf(0.01)) should be ("0.01|")
    engine4.action(java.lang.Double.valueOf(0.1)) should be ("0.1|")
    engine4.action(java.lang.Double.valueOf(1.0)) should be ("1.0|")
    engine4.action(java.lang.Double.valueOf(10.0)) should be ("10.0|")
    engine4.action(java.lang.Double.valueOf(100.0)) should be ("100.0|")
    engine4.action(java.lang.Double.valueOf(1000.0)) should be ("1000.0|")
    engine4.action(java.lang.Double.valueOf(10000.0)) should be ("10000.0|")
    engine4.action(java.lang.Double.valueOf(100000.0)) should be ("100000.0|")
    engine4.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.0e+06|")
    engine4.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.0e+07|")
    engine4.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.0e+08|")
    engine4.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.0e+09|")
    engine4.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.0e+10|")
    engine4.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.0e-10|")
    engine4.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.0e-09|")
    engine4.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.0e-08|")
    engine4.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.0e-07|")
    engine4.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.0e-06|")
    engine4.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.0e-05|")
    engine4.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.0001|")
    engine4.action(java.lang.Double.valueOf(-0.001)) should be ("-0.001|")
    engine4.action(java.lang.Double.valueOf(-0.01)) should be ("-0.01|")
    engine4.action(java.lang.Double.valueOf(-0.1)) should be ("-0.1|")
    engine4.action(java.lang.Double.valueOf(-1.0)) should be ("-1.0|")
    engine4.action(java.lang.Double.valueOf(-10.0)) should be ("-10.0|")
    engine4.action(java.lang.Double.valueOf(-100.0)) should be ("-100.0|")
    engine4.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.0|")
    engine4.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.0|")
    engine4.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.0|")
    engine4.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.0e+06|")
    engine4.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.0e+07|")
    engine4.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.0e+08|")
    engine4.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.0e+09|")
    engine4.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.0e+10|")

    val engine5 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, 10, null]}, {string: "|"}]
""").head

    engine5.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("       nan|")
    engine5.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("       inf|")
    engine5.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("      -inf|")
    engine5.action(java.lang.Double.valueOf(0.0)) should be ("       0.0|")
    engine5.action(java.lang.Double.valueOf(-0.0)) should be ("       0.0|")
    engine5.action(java.lang.Double.valueOf(Math.PI)) should be ("  3.141593|")
    engine5.action(java.lang.Double.valueOf(1.0e-10)) should be ("   1.0e-10|")
    engine5.action(java.lang.Double.valueOf(1.0e-09)) should be ("   1.0e-09|")
    engine5.action(java.lang.Double.valueOf(1.0e-08)) should be ("   1.0e-08|")
    engine5.action(java.lang.Double.valueOf(1.0e-07)) should be ("   1.0e-07|")
    engine5.action(java.lang.Double.valueOf(1.0e-06)) should be ("   1.0e-06|")
    engine5.action(java.lang.Double.valueOf(1.0e-05)) should be ("   1.0e-05|")
    engine5.action(java.lang.Double.valueOf(0.0001)) should be ("    0.0001|")
    engine5.action(java.lang.Double.valueOf(0.001)) should be ("     0.001|")
    engine5.action(java.lang.Double.valueOf(0.01)) should be ("      0.01|")
    engine5.action(java.lang.Double.valueOf(0.1)) should be ("       0.1|")
    engine5.action(java.lang.Double.valueOf(1.0)) should be ("       1.0|")
    engine5.action(java.lang.Double.valueOf(10.0)) should be ("      10.0|")
    engine5.action(java.lang.Double.valueOf(100.0)) should be ("     100.0|")
    engine5.action(java.lang.Double.valueOf(1000.0)) should be ("    1000.0|")
    engine5.action(java.lang.Double.valueOf(10000.0)) should be ("   10000.0|")
    engine5.action(java.lang.Double.valueOf(100000.0)) should be ("  100000.0|")
    engine5.action(java.lang.Double.valueOf(1.0e+06)) should be ("   1.0e+06|")
    engine5.action(java.lang.Double.valueOf(1.0e+07)) should be ("   1.0e+07|")
    engine5.action(java.lang.Double.valueOf(1.0e+08)) should be ("   1.0e+08|")
    engine5.action(java.lang.Double.valueOf(1.0e+09)) should be ("   1.0e+09|")
    engine5.action(java.lang.Double.valueOf(1.0e+10)) should be ("   1.0e+10|")
    engine5.action(java.lang.Double.valueOf(-1.0e-10)) should be ("  -1.0e-10|")
    engine5.action(java.lang.Double.valueOf(-1.0e-09)) should be ("  -1.0e-09|")
    engine5.action(java.lang.Double.valueOf(-1.0e-08)) should be ("  -1.0e-08|")
    engine5.action(java.lang.Double.valueOf(-1.0e-07)) should be ("  -1.0e-07|")
    engine5.action(java.lang.Double.valueOf(-1.0e-06)) should be ("  -1.0e-06|")
    engine5.action(java.lang.Double.valueOf(-1.0e-05)) should be ("  -1.0e-05|")
    engine5.action(java.lang.Double.valueOf(-0.0001)) should be ("   -0.0001|")
    engine5.action(java.lang.Double.valueOf(-0.001)) should be ("    -0.001|")
    engine5.action(java.lang.Double.valueOf(-0.01)) should be ("     -0.01|")
    engine5.action(java.lang.Double.valueOf(-0.1)) should be ("      -0.1|")
    engine5.action(java.lang.Double.valueOf(-1.0)) should be ("      -1.0|")
    engine5.action(java.lang.Double.valueOf(-10.0)) should be ("     -10.0|")
    engine5.action(java.lang.Double.valueOf(-100.0)) should be ("    -100.0|")
    engine5.action(java.lang.Double.valueOf(-1000.0)) should be ("   -1000.0|")
    engine5.action(java.lang.Double.valueOf(-10000.0)) should be ("  -10000.0|")
    engine5.action(java.lang.Double.valueOf(-100000.0)) should be (" -100000.0|")
    engine5.action(java.lang.Double.valueOf(-1.0e+06)) should be ("  -1.0e+06|")
    engine5.action(java.lang.Double.valueOf(-1.0e+07)) should be ("  -1.0e+07|")
    engine5.action(java.lang.Double.valueOf(-1.0e+08)) should be ("  -1.0e+08|")
    engine5.action(java.lang.Double.valueOf(-1.0e+09)) should be ("  -1.0e+09|")
    engine5.action(java.lang.Double.valueOf(-1.0e+10)) should be ("  -1.0e+10|")

    val engine6 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, -10, null]}, {string: "|"}]
""").head

    engine6.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("nan       |")
    engine6.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("inf       |")
    engine6.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("-inf      |")
    engine6.action(java.lang.Double.valueOf(0.0)) should be ("0.0       |")
    engine6.action(java.lang.Double.valueOf(-0.0)) should be ("0.0       |")
    engine6.action(java.lang.Double.valueOf(Math.PI)) should be ("3.141593  |")
    engine6.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.0e-10   |")
    engine6.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.0e-09   |")
    engine6.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.0e-08   |")
    engine6.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.0e-07   |")
    engine6.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.0e-06   |")
    engine6.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.0e-05   |")
    engine6.action(java.lang.Double.valueOf(0.0001)) should be ("0.0001    |")
    engine6.action(java.lang.Double.valueOf(0.001)) should be ("0.001     |")
    engine6.action(java.lang.Double.valueOf(0.01)) should be ("0.01      |")
    engine6.action(java.lang.Double.valueOf(0.1)) should be ("0.1       |")
    engine6.action(java.lang.Double.valueOf(1.0)) should be ("1.0       |")
    engine6.action(java.lang.Double.valueOf(10.0)) should be ("10.0      |")
    engine6.action(java.lang.Double.valueOf(100.0)) should be ("100.0     |")
    engine6.action(java.lang.Double.valueOf(1000.0)) should be ("1000.0    |")
    engine6.action(java.lang.Double.valueOf(10000.0)) should be ("10000.0   |")
    engine6.action(java.lang.Double.valueOf(100000.0)) should be ("100000.0  |")
    engine6.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.0e+06   |")
    engine6.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.0e+07   |")
    engine6.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.0e+08   |")
    engine6.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.0e+09   |")
    engine6.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.0e+10   |")
    engine6.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.0e-10  |")
    engine6.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.0e-09  |")
    engine6.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.0e-08  |")
    engine6.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.0e-07  |")
    engine6.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.0e-06  |")
    engine6.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.0e-05  |")
    engine6.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.0001   |")
    engine6.action(java.lang.Double.valueOf(-0.001)) should be ("-0.001    |")
    engine6.action(java.lang.Double.valueOf(-0.01)) should be ("-0.01     |")
    engine6.action(java.lang.Double.valueOf(-0.1)) should be ("-0.1      |")
    engine6.action(java.lang.Double.valueOf(-1.0)) should be ("-1.0      |")
    engine6.action(java.lang.Double.valueOf(-10.0)) should be ("-10.0     |")
    engine6.action(java.lang.Double.valueOf(-100.0)) should be ("-100.0    |")
    engine6.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.0   |")
    engine6.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.0  |")
    engine6.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.0 |")
    engine6.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.0e+06  |")
    engine6.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.0e+07  |")
    engine6.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.0e+08  |")
    engine6.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.0e+09  |")
    engine6.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.0e+10  |")

    val engine7 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, null, 3]}, {string: "|"}]
""").head

    engine7.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("nan|")
    engine7.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("inf|")
    engine7.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("-inf|")
    engine7.action(java.lang.Double.valueOf(0.0)) should be ("0.000|")
    engine7.action(java.lang.Double.valueOf(-0.0)) should be ("0.000|")
    engine7.action(java.lang.Double.valueOf(Math.PI)) should be ("3.142|")
    engine7.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.000e-10|")
    engine7.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.000e-09|")
    engine7.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.000e-08|")
    engine7.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.000e-07|")
    engine7.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.000e-06|")
    engine7.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.000e-05|")
    engine7.action(java.lang.Double.valueOf(0.0001)) should be ("0.000|")
    engine7.action(java.lang.Double.valueOf(0.001)) should be ("0.001|")
    engine7.action(java.lang.Double.valueOf(0.01)) should be ("0.010|")
    engine7.action(java.lang.Double.valueOf(0.1)) should be ("0.100|")
    engine7.action(java.lang.Double.valueOf(1.0)) should be ("1.000|")
    engine7.action(java.lang.Double.valueOf(10.0)) should be ("10.000|")
    engine7.action(java.lang.Double.valueOf(100.0)) should be ("100.000|")
    engine7.action(java.lang.Double.valueOf(1000.0)) should be ("1000.000|")
    engine7.action(java.lang.Double.valueOf(10000.0)) should be ("10000.000|")
    engine7.action(java.lang.Double.valueOf(100000.0)) should be ("100000.000|")
    engine7.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.000e+06|")
    engine7.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.000e+07|")
    engine7.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.000e+08|")
    engine7.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.000e+09|")
    engine7.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.000e+10|")
    engine7.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.000e-10|")
    engine7.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.000e-09|")
    engine7.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.000e-08|")
    engine7.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.000e-07|")
    engine7.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.000e-06|")
    engine7.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.000e-05|")
    engine7.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.000|")
    engine7.action(java.lang.Double.valueOf(-0.001)) should be ("-0.001|")
    engine7.action(java.lang.Double.valueOf(-0.01)) should be ("-0.010|")
    engine7.action(java.lang.Double.valueOf(-0.1)) should be ("-0.100|")
    engine7.action(java.lang.Double.valueOf(-1.0)) should be ("-1.000|")
    engine7.action(java.lang.Double.valueOf(-10.0)) should be ("-10.000|")
    engine7.action(java.lang.Double.valueOf(-100.0)) should be ("-100.000|")
    engine7.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.000|")
    engine7.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.000|")
    engine7.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.000|")
    engine7.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.000e+06|")
    engine7.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.000e+07|")
    engine7.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.000e+08|")
    engine7.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.000e+09|")
    engine7.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.000e+10|")

    val engine8 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, 10, 3]}, {string: "|"}]
""").head

    engine8.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("       nan|")
    engine8.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("       inf|")
    engine8.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("      -inf|")
    engine8.action(java.lang.Double.valueOf(0.0)) should be ("     0.000|")
    engine8.action(java.lang.Double.valueOf(-0.0)) should be ("     0.000|")
    engine8.action(java.lang.Double.valueOf(Math.PI)) should be ("     3.142|")
    engine8.action(java.lang.Double.valueOf(1.0e-10)) should be (" 1.000e-10|")
    engine8.action(java.lang.Double.valueOf(1.0e-09)) should be (" 1.000e-09|")
    engine8.action(java.lang.Double.valueOf(1.0e-08)) should be (" 1.000e-08|")
    engine8.action(java.lang.Double.valueOf(1.0e-07)) should be (" 1.000e-07|")
    engine8.action(java.lang.Double.valueOf(1.0e-06)) should be (" 1.000e-06|")
    engine8.action(java.lang.Double.valueOf(1.0e-05)) should be (" 1.000e-05|")
    engine8.action(java.lang.Double.valueOf(0.0001)) should be ("     0.000|")
    engine8.action(java.lang.Double.valueOf(0.001)) should be ("     0.001|")
    engine8.action(java.lang.Double.valueOf(0.01)) should be ("     0.010|")
    engine8.action(java.lang.Double.valueOf(0.1)) should be ("     0.100|")
    engine8.action(java.lang.Double.valueOf(1.0)) should be ("     1.000|")
    engine8.action(java.lang.Double.valueOf(10.0)) should be ("    10.000|")
    engine8.action(java.lang.Double.valueOf(100.0)) should be ("   100.000|")
    engine8.action(java.lang.Double.valueOf(1000.0)) should be ("  1000.000|")
    engine8.action(java.lang.Double.valueOf(10000.0)) should be (" 10000.000|")
    engine8.action(java.lang.Double.valueOf(100000.0)) should be ("100000.000|")
    engine8.action(java.lang.Double.valueOf(1.0e+06)) should be (" 1.000e+06|")
    engine8.action(java.lang.Double.valueOf(1.0e+07)) should be (" 1.000e+07|")
    engine8.action(java.lang.Double.valueOf(1.0e+08)) should be (" 1.000e+08|")
    engine8.action(java.lang.Double.valueOf(1.0e+09)) should be (" 1.000e+09|")
    engine8.action(java.lang.Double.valueOf(1.0e+10)) should be (" 1.000e+10|")
    engine8.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.000e-10|")
    engine8.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.000e-09|")
    engine8.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.000e-08|")
    engine8.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.000e-07|")
    engine8.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.000e-06|")
    engine8.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.000e-05|")
    engine8.action(java.lang.Double.valueOf(-0.0001)) should be ("    -0.000|")
    engine8.action(java.lang.Double.valueOf(-0.001)) should be ("    -0.001|")
    engine8.action(java.lang.Double.valueOf(-0.01)) should be ("    -0.010|")
    engine8.action(java.lang.Double.valueOf(-0.1)) should be ("    -0.100|")
    engine8.action(java.lang.Double.valueOf(-1.0)) should be ("    -1.000|")
    engine8.action(java.lang.Double.valueOf(-10.0)) should be ("   -10.000|")
    engine8.action(java.lang.Double.valueOf(-100.0)) should be ("  -100.000|")
    engine8.action(java.lang.Double.valueOf(-1000.0)) should be (" -1000.000|")
    engine8.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.000|")
    engine8.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.000|")
    engine8.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.000e+06|")
    engine8.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.000e+07|")
    engine8.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.000e+08|")
    engine8.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.000e+09|")
    engine8.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.000e+10|")

    val engine9 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, -10, 3]}, {string: "|"}]
""").head

    engine9.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("nan       |")
    engine9.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("inf       |")
    engine9.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("-inf      |")
    engine9.action(java.lang.Double.valueOf(0.0)) should be ("0.000     |")
    engine9.action(java.lang.Double.valueOf(-0.0)) should be ("0.000     |")
    engine9.action(java.lang.Double.valueOf(Math.PI)) should be ("3.142     |")
    engine9.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.000e-10 |")
    engine9.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.000e-09 |")
    engine9.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.000e-08 |")
    engine9.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.000e-07 |")
    engine9.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.000e-06 |")
    engine9.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.000e-05 |")
    engine9.action(java.lang.Double.valueOf(0.0001)) should be ("0.000     |")
    engine9.action(java.lang.Double.valueOf(0.001)) should be ("0.001     |")
    engine9.action(java.lang.Double.valueOf(0.01)) should be ("0.010     |")
    engine9.action(java.lang.Double.valueOf(0.1)) should be ("0.100     |")
    engine9.action(java.lang.Double.valueOf(1.0)) should be ("1.000     |")
    engine9.action(java.lang.Double.valueOf(10.0)) should be ("10.000    |")
    engine9.action(java.lang.Double.valueOf(100.0)) should be ("100.000   |")
    engine9.action(java.lang.Double.valueOf(1000.0)) should be ("1000.000  |")
    engine9.action(java.lang.Double.valueOf(10000.0)) should be ("10000.000 |")
    engine9.action(java.lang.Double.valueOf(100000.0)) should be ("100000.000|")
    engine9.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.000e+06 |")
    engine9.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.000e+07 |")
    engine9.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.000e+08 |")
    engine9.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.000e+09 |")
    engine9.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.000e+10 |")
    engine9.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.000e-10|")
    engine9.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.000e-09|")
    engine9.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.000e-08|")
    engine9.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.000e-07|")
    engine9.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.000e-06|")
    engine9.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.000e-05|")
    engine9.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.000    |")
    engine9.action(java.lang.Double.valueOf(-0.001)) should be ("-0.001    |")
    engine9.action(java.lang.Double.valueOf(-0.01)) should be ("-0.010    |")
    engine9.action(java.lang.Double.valueOf(-0.1)) should be ("-0.100    |")
    engine9.action(java.lang.Double.valueOf(-1.0)) should be ("-1.000    |")
    engine9.action(java.lang.Double.valueOf(-10.0)) should be ("-10.000   |")
    engine9.action(java.lang.Double.valueOf(-100.0)) should be ("-100.000  |")
    engine9.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.000 |")
    engine9.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.000|")
    engine9.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.000|")
    engine9.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.000e+06|")
    engine9.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.000e+07|")
    engine9.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.000e+08|")
    engine9.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.000e+09|")
    engine9.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.000e+10|")

    val engine10 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, null, 10]}, {string: "|"}]
""").head

    engine10.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("nan|")
    engine10.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("inf|")
    engine10.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("-inf|")
    engine10.action(java.lang.Double.valueOf(0.0)) should be ("0.0000000000|")
    engine10.action(java.lang.Double.valueOf(-0.0)) should be ("0.0000000000|")
    engine10.action(java.lang.Double.valueOf(Math.PI)) should be ("3.1415926536|")
    engine10.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.0000000000e-10|")
    engine10.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.0000000000e-09|")
    engine10.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.0000000000e-08|")
    engine10.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.0000000000e-07|")
    engine10.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.0000000000e-06|")
    engine10.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.0000000000e-05|")
    engine10.action(java.lang.Double.valueOf(0.0001)) should be ("0.0001000000|")
    engine10.action(java.lang.Double.valueOf(0.001)) should be ("0.0010000000|")
    engine10.action(java.lang.Double.valueOf(0.01)) should be ("0.0100000000|")
    engine10.action(java.lang.Double.valueOf(0.1)) should be ("0.1000000000|")
    engine10.action(java.lang.Double.valueOf(1.0)) should be ("1.0000000000|")
    engine10.action(java.lang.Double.valueOf(10.0)) should be ("10.0000000000|")
    engine10.action(java.lang.Double.valueOf(100.0)) should be ("100.0000000000|")
    engine10.action(java.lang.Double.valueOf(1000.0)) should be ("1000.0000000000|")
    engine10.action(java.lang.Double.valueOf(10000.0)) should be ("10000.0000000000|")
    engine10.action(java.lang.Double.valueOf(100000.0)) should be ("100000.0000000000|")
    engine10.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.0000000000e+06|")
    engine10.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.0000000000e+07|")
    engine10.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.0000000000e+08|")
    engine10.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.0000000000e+09|")
    engine10.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.0000000000e+10|")
    engine10.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.0000000000e-10|")
    engine10.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.0000000000e-09|")
    engine10.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.0000000000e-08|")
    engine10.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.0000000000e-07|")
    engine10.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.0000000000e-06|")
    engine10.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.0000000000e-05|")
    engine10.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.0001000000|")
    engine10.action(java.lang.Double.valueOf(-0.001)) should be ("-0.0010000000|")
    engine10.action(java.lang.Double.valueOf(-0.01)) should be ("-0.0100000000|")
    engine10.action(java.lang.Double.valueOf(-0.1)) should be ("-0.1000000000|")
    engine10.action(java.lang.Double.valueOf(-1.0)) should be ("-1.0000000000|")
    engine10.action(java.lang.Double.valueOf(-10.0)) should be ("-10.0000000000|")
    engine10.action(java.lang.Double.valueOf(-100.0)) should be ("-100.0000000000|")
    engine10.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.0000000000|")
    engine10.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.0000000000|")
    engine10.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.0000000000|")
    engine10.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.0000000000e+06|")
    engine10.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.0000000000e+07|")
    engine10.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.0000000000e+08|")
    engine10.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.0000000000e+09|")
    engine10.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.0000000000e+10|")

    val engine11 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, 10, 10]}, {string: "|"}]
""").head

    engine11.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("       nan|")
    engine11.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("       inf|")
    engine11.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("      -inf|")
    engine11.action(java.lang.Double.valueOf(0.0)) should be ("0.0000000000|")
    engine11.action(java.lang.Double.valueOf(-0.0)) should be ("0.0000000000|")
    engine11.action(java.lang.Double.valueOf(Math.PI)) should be ("3.1415926536|")
    engine11.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.0000000000e-10|")
    engine11.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.0000000000e-09|")
    engine11.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.0000000000e-08|")
    engine11.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.0000000000e-07|")
    engine11.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.0000000000e-06|")
    engine11.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.0000000000e-05|")
    engine11.action(java.lang.Double.valueOf(0.0001)) should be ("0.0001000000|")
    engine11.action(java.lang.Double.valueOf(0.001)) should be ("0.0010000000|")
    engine11.action(java.lang.Double.valueOf(0.01)) should be ("0.0100000000|")
    engine11.action(java.lang.Double.valueOf(0.1)) should be ("0.1000000000|")
    engine11.action(java.lang.Double.valueOf(1.0)) should be ("1.0000000000|")
    engine11.action(java.lang.Double.valueOf(10.0)) should be ("10.0000000000|")
    engine11.action(java.lang.Double.valueOf(100.0)) should be ("100.0000000000|")
    engine11.action(java.lang.Double.valueOf(1000.0)) should be ("1000.0000000000|")
    engine11.action(java.lang.Double.valueOf(10000.0)) should be ("10000.0000000000|")
    engine11.action(java.lang.Double.valueOf(100000.0)) should be ("100000.0000000000|")
    engine11.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.0000000000e+06|")
    engine11.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.0000000000e+07|")
    engine11.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.0000000000e+08|")
    engine11.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.0000000000e+09|")
    engine11.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.0000000000e+10|")
    engine11.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.0000000000e-10|")
    engine11.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.0000000000e-09|")
    engine11.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.0000000000e-08|")
    engine11.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.0000000000e-07|")
    engine11.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.0000000000e-06|")
    engine11.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.0000000000e-05|")
    engine11.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.0001000000|")
    engine11.action(java.lang.Double.valueOf(-0.001)) should be ("-0.0010000000|")
    engine11.action(java.lang.Double.valueOf(-0.01)) should be ("-0.0100000000|")
    engine11.action(java.lang.Double.valueOf(-0.1)) should be ("-0.1000000000|")
    engine11.action(java.lang.Double.valueOf(-1.0)) should be ("-1.0000000000|")
    engine11.action(java.lang.Double.valueOf(-10.0)) should be ("-10.0000000000|")
    engine11.action(java.lang.Double.valueOf(-100.0)) should be ("-100.0000000000|")
    engine11.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.0000000000|")
    engine11.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.0000000000|")
    engine11.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.0000000000|")
    engine11.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.0000000000e+06|")
    engine11.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.0000000000e+07|")
    engine11.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.0000000000e+08|")
    engine11.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.0000000000e+09|")
    engine11.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.0000000000e+10|")

    val engine12 = PFAEngine.fromYaml("""
input: double
output: string
action:
  s.concat: [{s.number: [input, -10, 10]}, {string: "|"}]
""").head

    engine12.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be ("nan       |")
    engine12.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be ("inf       |")
    engine12.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be ("-inf      |")
    engine12.action(java.lang.Double.valueOf(0.0)) should be ("0.0000000000|")
    engine12.action(java.lang.Double.valueOf(-0.0)) should be ("0.0000000000|")
    engine12.action(java.lang.Double.valueOf(Math.PI)) should be ("3.1415926536|")
    engine12.action(java.lang.Double.valueOf(1.0e-10)) should be ("1.0000000000e-10|")
    engine12.action(java.lang.Double.valueOf(1.0e-09)) should be ("1.0000000000e-09|")
    engine12.action(java.lang.Double.valueOf(1.0e-08)) should be ("1.0000000000e-08|")
    engine12.action(java.lang.Double.valueOf(1.0e-07)) should be ("1.0000000000e-07|")
    engine12.action(java.lang.Double.valueOf(1.0e-06)) should be ("1.0000000000e-06|")
    engine12.action(java.lang.Double.valueOf(1.0e-05)) should be ("1.0000000000e-05|")
    engine12.action(java.lang.Double.valueOf(0.0001)) should be ("0.0001000000|")
    engine12.action(java.lang.Double.valueOf(0.001)) should be ("0.0010000000|")
    engine12.action(java.lang.Double.valueOf(0.01)) should be ("0.0100000000|")
    engine12.action(java.lang.Double.valueOf(0.1)) should be ("0.1000000000|")
    engine12.action(java.lang.Double.valueOf(1.0)) should be ("1.0000000000|")
    engine12.action(java.lang.Double.valueOf(10.0)) should be ("10.0000000000|")
    engine12.action(java.lang.Double.valueOf(100.0)) should be ("100.0000000000|")
    engine12.action(java.lang.Double.valueOf(1000.0)) should be ("1000.0000000000|")
    engine12.action(java.lang.Double.valueOf(10000.0)) should be ("10000.0000000000|")
    engine12.action(java.lang.Double.valueOf(100000.0)) should be ("100000.0000000000|")
    engine12.action(java.lang.Double.valueOf(1.0e+06)) should be ("1.0000000000e+06|")
    engine12.action(java.lang.Double.valueOf(1.0e+07)) should be ("1.0000000000e+07|")
    engine12.action(java.lang.Double.valueOf(1.0e+08)) should be ("1.0000000000e+08|")
    engine12.action(java.lang.Double.valueOf(1.0e+09)) should be ("1.0000000000e+09|")
    engine12.action(java.lang.Double.valueOf(1.0e+10)) should be ("1.0000000000e+10|")
    engine12.action(java.lang.Double.valueOf(-1.0e-10)) should be ("-1.0000000000e-10|")
    engine12.action(java.lang.Double.valueOf(-1.0e-09)) should be ("-1.0000000000e-09|")
    engine12.action(java.lang.Double.valueOf(-1.0e-08)) should be ("-1.0000000000e-08|")
    engine12.action(java.lang.Double.valueOf(-1.0e-07)) should be ("-1.0000000000e-07|")
    engine12.action(java.lang.Double.valueOf(-1.0e-06)) should be ("-1.0000000000e-06|")
    engine12.action(java.lang.Double.valueOf(-1.0e-05)) should be ("-1.0000000000e-05|")
    engine12.action(java.lang.Double.valueOf(-0.0001)) should be ("-0.0001000000|")
    engine12.action(java.lang.Double.valueOf(-0.001)) should be ("-0.0010000000|")
    engine12.action(java.lang.Double.valueOf(-0.01)) should be ("-0.0100000000|")
    engine12.action(java.lang.Double.valueOf(-0.1)) should be ("-0.1000000000|")
    engine12.action(java.lang.Double.valueOf(-1.0)) should be ("-1.0000000000|")
    engine12.action(java.lang.Double.valueOf(-10.0)) should be ("-10.0000000000|")
    engine12.action(java.lang.Double.valueOf(-100.0)) should be ("-100.0000000000|")
    engine12.action(java.lang.Double.valueOf(-1000.0)) should be ("-1000.0000000000|")
    engine12.action(java.lang.Double.valueOf(-10000.0)) should be ("-10000.0000000000|")
    engine12.action(java.lang.Double.valueOf(-100000.0)) should be ("-100000.0000000000|")
    engine12.action(java.lang.Double.valueOf(-1.0e+06)) should be ("-1.0000000000e+06|")
    engine12.action(java.lang.Double.valueOf(-1.0e+07)) should be ("-1.0000000000e+07|")
    engine12.action(java.lang.Double.valueOf(-1.0e+08)) should be ("-1.0000000000e+08|")
    engine12.action(java.lang.Double.valueOf(-1.0e+09)) should be ("-1.0000000000e+09|")
    engine12.action(java.lang.Double.valueOf(-1.0e+10)) should be ("-1.0000000000e+10|")
  }

  "conversions to/from other strings" must "do join" taggedAs(Lib, LibString) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.concat: [[one], input]}
""").head.action("two") should be ("onetwo")
  }

  it must "do repeat" taggedAs(Lib, LibString) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.repeat: [input, 5]}
""").head.action("hey") should be ("heyheyheyheyhey")
  }

  it must "do lower" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.lower: [input]}
""").head
    engine.action("hey") should be ("hey")
    engine.action("Hey") should be ("hey")
    engine.action("HEY") should be ("hey")
    engine.action("hEy") should be ("hey")
    engine.action("heY") should be ("hey")
  }

  it must "do upper" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.upper: [input]}
""").head
    engine.action("hey") should be ("HEY")
    engine.action("Hey") should be ("HEY")
    engine.action("HEY") should be ("HEY")
    engine.action("hEy") should be ("HEY")
    engine.action("heY") should be ("HEY")
  }

  it must "do lstrip" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.lstrip: [input, ["h "]]}
""").head
    engine.action("hey") should be ("ey")
    engine.action(" hey") should be ("ey")
    engine.action("  hey") should be ("ey")
    engine.action("hey ") should be ("ey ")
    engine.action("Hey") should be ("Hey")
    engine.action(" Hey") should be ("Hey")
    engine.action("  Hey") should be ("Hey")
    engine.action("Hey ") should be ("Hey ")
  }

  it must "do rstrip" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.rstrip: [input, ["y "]]}
""").head
    engine.action("hey") should be ("he")
    engine.action("hey ") should be ("he")
    engine.action("hey  ") should be ("he")
    engine.action(" hey") should be (" he")
    engine.action("heY") should be ("heY")
    engine.action("heY ") should be ("heY")
    engine.action("heY  ") should be ("heY")
    engine.action(" heY") should be (" heY")
  }

  it must "do strip" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.strip: [input, ["hy "]]}
""").head
    engine.action("hey") should be ("e")
    engine.action("hey ") should be ("e")
    engine.action("hey  ") should be ("e")
    engine.action(" hey") should be ("e")
    engine.action("HEY") should be ("HEY")
    engine.action("HEY ") should be ("HEY")
    engine.action("HEY  ") should be ("HEY")
    engine.action(" HEY") should be ("HEY")
  }

  it must "do replaceall" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.replaceall: [input, [ey], [EY]]}
""").head
    engine.action("hey") should be ("hEY")
    engine.action("hey hey hey") should be ("hEY hEY hEY")
    engine.action("abc") should be ("abc")
    engine.action("yeh yeh yeh") should be ("yeh yeh yeh")
  }

  it must "do replacefirst" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.replacefirst: [input, [ey], [EY]]}
""").head
    engine.action("hey") should be ("hEY")
    engine.action("hey hey hey") should be ("hEY hey hey")
    engine.action("abc") should be ("abc")
    engine.action("yeh yeh yeh") should be ("yeh yeh yeh")
  }

  it must "do replacelast" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.replacelast: [input, [ey], [EY]]}
""").head
    engine.action("hey") should be ("hEY")
    engine.action("hey hey hey") should be ("hey hey hEY")
    engine.action("abc") should be ("abc")
    engine.action("yeh yeh yeh") should be ("yeh yeh yeh")
  }

  it must "do translate" taggedAs(Lib, LibString) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.translate: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], [AEIOU], input]}
""").head
    engine.action("aeiou") should be ("aBCDeFGHiJKLMNoPQRSTuVWXYZ")
    engine.action("aeio") should be ("aBCDeFGHiJKLMNoPQRSTVWXYZ")
    engine.action("") should be ("BCDFGHJKLMNPQRSTVWXYZ")
    engine.action("aeiouuuu") should be ("aBCDeFGHiJKLMNoPQRSTuVWXYZ")
  }
}
