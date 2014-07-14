package test.scala.lib1.string

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
class Lib1StringSuite extends FlatSpec with Matchers {
  "basic access" must "get length" taggedAs(Lib1, Lib1String) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {s.len: [input]}
""").head.action("hello") should be (5)
  }

  it must "get substring" taggedAs(Lib1, Lib1String) in {
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

  it must "set substring" taggedAs(Lib1, Lib1String) in {
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

  "searching" must "do contains" taggedAs(Lib1, Lib1String) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {s.contains: [input, [DEFG]]}
""").head
    engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ").asInstanceOf[Boolean] should be (true)
    engine.action("ack! ack! ack!").asInstanceOf[Boolean] should be (false)
  }

  it must "do count" taggedAs(Lib1, Lib1String) in {
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

  it must "do index" taggedAs(Lib1, Lib1String) in {
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

  it must "do rindex" taggedAs(Lib1, Lib1String) in {
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

  it must "do startswith" taggedAs(Lib1, Lib1String) in {
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

  it must "do endswith" taggedAs(Lib1, Lib1String) in {
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

  "conversions to/from other types" must "do join" taggedAs(Lib1, Lib1String) in {
    PFAEngine.fromYaml("""
input: {type: array, items: string}
output: string
action:
  - {s.join: [input, [", "]]}
""").head.action(PFAArray.fromVector(Vector("one", "two", "three"))) should be ("one, two, three")
  }

  it must "do split" taggedAs(Lib1, Lib1String) in {
    PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {s.split: [input, [", "]]}
""").head.action("one, two, three") should be (PFAArray.fromVector(Vector("one", "two", "three")))
  }

  "conversions to/from other strings" must "do join" taggedAs(Lib1, Lib1String) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.concat: [[one], input]}
""").head.action("two") should be ("onetwo")
  }

  it must "do repeat" taggedAs(Lib1, Lib1String) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {s.repeat: [input, 5]}
""").head.action("hey") should be ("heyheyheyheyhey")
  }

  it must "do lower" taggedAs(Lib1, Lib1String) in {
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

  it must "do upper" taggedAs(Lib1, Lib1String) in {
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

  it must "do lstrip" taggedAs(Lib1, Lib1String) in {
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

  it must "do rstrip" taggedAs(Lib1, Lib1String) in {
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

  it must "do strip" taggedAs(Lib1, Lib1String) in {
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

  it must "do replaceall" taggedAs(Lib1, Lib1String) in {
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

  it must "do replacefirst" taggedAs(Lib1, Lib1String) in {
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

  it must "do replacelast" taggedAs(Lib1, Lib1String) in {
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

  it must "do translate" taggedAs(Lib1, Lib1String) in {
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
