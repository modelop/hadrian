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

package test.scala.lib.impute

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
class LibImputeSuite extends FlatSpec with Matchers {
  "impute" must "errorOnNull" taggedAs(Lib, LibImpute) in {
    val intEngine = PFAEngine.fromYaml("""
input: [int, "null"]
output: int
action:
  - {impute.errorOnNull: [input]}
""").head
    intEngine.action(java.lang.Integer.valueOf(3)) should be (3)
    intEngine.action(java.lang.Integer.valueOf(12)) should be (12)
    evaluating { intEngine.action(null) } should produce [PFARuntimeException]
    intEngine.action(java.lang.Integer.valueOf(5)) should be (5)
  }

  it must "defaultOnNull" taggedAs(Lib, LibImpute) in {
    val intEngine = PFAEngine.fromYaml("""
input: [int, "null"]
output: int
action:
  - {impute.defaultOnNull: [input, 12]}
""").head
    intEngine.action(java.lang.Integer.valueOf(3)) should be (3)
    intEngine.action(java.lang.Integer.valueOf(12)) should be (12)
    intEngine.action(null) should be (12)
    intEngine.action(java.lang.Integer.valueOf(5)) should be (5)

    val stringEngine = PFAEngine.fromYaml("""
input: [string, "null"]
output: string
action:
  - {impute.defaultOnNull: [input, [oops]]}
""").head
    stringEngine.action("one") should be ("one")
    stringEngine.action("two") should be ("two")
    stringEngine.action(null) should be ("oops")
    stringEngine.action("four") should be ("four")

    evaluating { PFAEngine.fromYaml("""
input: ["null", "null"]
output: "null"
action:
  - {impute.defaultOnNull: [input, "null"]}
""").head } should produce [org.apache.avro.AvroRuntimeException]

    evaluating { PFAEngine.fromYaml("""
input: [[int, string], "null"]
output: [int, string]
action:
  - {impute.defaultOnNull: [input, 12]}
""").head } should produce [org.apache.avro.AvroRuntimeException]

    evaluating { PFAEngine.fromYaml("""
input: [int, string, "null"]
output: int
action:
  - {impute.defaultOnNull: [input, 12]}
""").head } should produce [PFASemanticException]
  }

  it must "isnan" taggedAs(Lib, LibImpute) in {
    val floatEngine = PFAEngine.fromYaml("""
input: float
output: boolean
action: {impute.isnan: input}
""").head
    floatEngine.action(java.lang.Float.valueOf(123.4F)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NaN)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.POSITIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NEGATIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: boolean
action: {impute.isnan: input}
""").head
    doubleEngine.action(java.lang.Double.valueOf(123.4F)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "isinf" taggedAs(Lib, LibImpute) in {
    val floatEngine = PFAEngine.fromYaml("""
input: float
output: boolean
action: {impute.isinf: input}
""").head
    floatEngine.action(java.lang.Float.valueOf(123.4F)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NaN)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.POSITIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NEGATIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: boolean
action: {impute.isinf: input}
""").head
    doubleEngine.action(java.lang.Double.valueOf(123.4F)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "isnum" taggedAs(Lib, LibImpute) in {
    val floatEngine = PFAEngine.fromYaml("""
input: float
output: boolean
action: {impute.isnum: input}
""").head
    floatEngine.action(java.lang.Float.valueOf(123.4F)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NaN)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.POSITIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NEGATIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: boolean
action: {impute.isnum: input}
""").head
    doubleEngine.action(java.lang.Double.valueOf(123.4F)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NaN)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "errorOnNonNum" taggedAs(Lib, LibImpute) in {
    val floatEngine = PFAEngine.fromYaml("""
input: float
output: float
action: {impute.errorOnNonNum: input}
""").head
    floatEngine.action(java.lang.Float.valueOf(123.4F)) should be (123.4F)
    evaluating { floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NaN)) } should produce [PFARuntimeException]
    evaluating { floatEngine.action(java.lang.Float.valueOf(java.lang.Float.POSITIVE_INFINITY)) } should produce [PFARuntimeException]
    evaluating { floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NEGATIVE_INFINITY)) } should produce [PFARuntimeException]

    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: double
action: {impute.errorOnNonNum: input}
""").head
    doubleEngine.action(java.lang.Double.valueOf(123.4F)) should be (123.4F)
    evaluating { doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NaN)) } should produce [PFARuntimeException]
    evaluating { doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) } should produce [PFARuntimeException]
    evaluating { doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) } should produce [PFARuntimeException]
  }

  it must "defaultOnNonNum" taggedAs(Lib, LibImpute) in {
    val floatEngine = PFAEngine.fromYaml("""
input: float
output: float
action: {impute.defaultOnNonNum: [input, {float: 999.0}]}
""").head
    floatEngine.action(java.lang.Float.valueOf(123.4F)) should be (123.4F)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NaN)) should be (999.0F)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.POSITIVE_INFINITY)) should be (999.0F)
    floatEngine.action(java.lang.Float.valueOf(java.lang.Float.NEGATIVE_INFINITY)) should be (999.0F)

    val doubleEngine = PFAEngine.fromYaml("""
input: double
output: double
action: {impute.defaultOnNonNum: [input, 999.0]}
""").head
    doubleEngine.action(java.lang.Double.valueOf(123.4)) should be (123.4)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NaN)) should be (999.0)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)) should be (999.0)
    doubleEngine.action(java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)) should be (999.0)
  }

}
