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

package test.scala.lib1.parse

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1CastSuite extends FlatSpec with Matchers {
  "cast" must "do toInt" taggedAs(Lib1, Lib1Cast) in {
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

  it must "do toLong" taggedAs(Lib1, Lib1Cast) in {
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

  it must "do toFloat" taggedAs(Lib1, Lib1Cast) in {
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

  it must "do toDouble" taggedAs(Lib1, Lib1Cast) in {
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
}
