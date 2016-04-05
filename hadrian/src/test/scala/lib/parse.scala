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

package test.scala.lib.parse

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class LibParseSuite extends FlatSpec with Matchers {
  "parse" must "parse int" taggedAs(Lib, LibParse) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
action: {parse.int: [input, 10]}
""").head
    engine.action("   123   ") should be (123)
    engine.action("   +123   ") should be (123)
    engine.action("   -123   ") should be (-123)
    engine.action("   2147483647   ") should be (2147483647)
    evaluating { engine.action("   2147483648   ") } should produce [PFARuntimeException]
    engine.action("   -2147483648   ") should be (-2147483648)
    evaluating { engine.action("   -2147483649   ") } should produce [PFARuntimeException]
  }

  it must "parse long" taggedAs(Lib, LibParse) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: long
action: {parse.long: [input, 10]}
""").head
    engine.action("   123   ") should be (123L)
    engine.action("   +123   ") should be (123L)
    engine.action("   -123   ") should be (-123L)
    engine.action("   9223372036854775807   ") should be (9223372036854775807L)
    evaluating { engine.action("   9223372036854775808   ") } should produce [PFARuntimeException]
    engine.action("   -9223372036854775808   ") should be (-9223372036854775808L)
    evaluating { engine.action("   -9223372036854775809   ") } should produce [PFARuntimeException]
  }

  it must "parse float" taggedAs(Lib, LibParse) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: float
action: {parse.float: input}
""").head
    engine.action("   123   ") should be (123.0F)
    engine.action("   -123   ") should be (-123.0F)
    engine.action("   3.4028234e38   ") should be (3.4028234e38F)
    engine.action("   -3.4028234e38   ") should be (-3.4028234e38F)
    engine.action("   3.4028236e38   ") should be (java.lang.Float.POSITIVE_INFINITY)
    engine.action("   -3.4028236e38   ") should be (java.lang.Float.NEGATIVE_INFINITY)
    engine.action("   1.4e-45   ") should be (1.4e-45F)
    engine.action("   -1.4e-45   ") should be (-1.4e-45F)
    engine.action("   1e-46   ") should be (0.0F)
    engine.action("   -1e-46   ") should be (0.0F)
    java.lang.Float.isNaN(engine.action("   nAN   ").asInstanceOf[java.lang.Float]) should be (true)
    engine.action("   inf   ") should be (java.lang.Float.POSITIVE_INFINITY)
    engine.action("   +inf   ") should be (java.lang.Float.POSITIVE_INFINITY)
    engine.action("   -inf   ") should be (java.lang.Float.NEGATIVE_INFINITY)
  }

  it must "parse double" taggedAs(Lib, LibParse) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: double
action: {parse.double: input}
""").head
    engine.action("   123   ") should be (123.0)
    engine.action("   -123   ") should be (-123.0)
    engine.action("   1.7976931348623157e308   ") should be (1.7976931348623157e308)
    engine.action("   -1.7976931348623157e308   ") should be (-1.7976931348623157e308)
    engine.action("   1.7976931348623159e308   ") should be (java.lang.Double.POSITIVE_INFINITY)
    engine.action("   -1.7976931348623159e308   ") should be (java.lang.Double.NEGATIVE_INFINITY)
    engine.action("   4.9e-324   ") should be (4.9e-324)
    engine.action("   -4.9e-324   ") should be (-4.9e-324)
    engine.action("   1e-324   ") should be (0.0)
    engine.action("   1e-324   ") should be (0.0)
    java.lang.Double.isNaN(engine.action("   nAN   ").asInstanceOf[java.lang.Double]) should be (true)
    engine.action("   inf   ") should be (java.lang.Double.POSITIVE_INFINITY)
    engine.action("   +inf   ") should be (java.lang.Double.POSITIVE_INFINITY)
    engine.action("   -inf   ") should be (java.lang.Double.NEGATIVE_INFINITY)
  }

}
