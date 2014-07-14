package test.scala.lib1.impute

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
class Lib1ImputeSuite extends FlatSpec with Matchers {
  "impute" must "errorOnNull" taggedAs(Lib1, Lib1Impute) in {
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

  it must "defaultOnNull" taggedAs(Lib1, Lib1Impute) in {
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

}
