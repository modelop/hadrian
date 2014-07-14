package test.scala.jvmcompiler

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class JVMCompilationSuite extends FlatSpec with Matchers {
  def compileExpressionEngine(yaml: String, outputType: String, debug: Boolean = false): PFAEngine[AnyRef, AnyRef] =
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  test:
    params: []
    ret: %s
    do:
      - %s
""".format(outputType, yaml.replace("\n", "\n        ")), debug = debug).head

  def compileExpression(yaml: String, outputType: String, debug: Boolean = false): Function0[AnyRef] =
    compileExpressionEngine(yaml, outputType, debug).fcn0("test")

  def collectLogs(pfaEngine: PFAEngine[AnyRef, AnyRef]): Seq[(Option[String], String)] = {
    val allLogs = mutable.ListBuffer[(Option[String], String)]()
    pfaEngine.log = (str, namespace) => allLogs.append((namespace, str))
    pfaEngine.fcn0("test").apply
    allLogs.toSeq
  }

  "JVM expression compilation" must "do simple literals" taggedAs(JVMCompilation) in {
    compileExpression("""null""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])
    compileExpression("""true""", """"boolean"""").apply should be (java.lang.Boolean.valueOf(true))
    compileExpression("""false""", """"boolean"""").apply should be (java.lang.Boolean.valueOf(false))

    compileExpression("""1""", """"int"""").apply should be (1)
    compileExpression("""{"long": 1}""", """"long"""").apply should be (1L)
    compileExpression("""{"float": 2.5}""", """"float"""").apply should be (2.5F)
    compileExpression("""2.5""", """"double"""").apply should be (2.5)

    evaluating { compileExpression("""{"int": "string"}""", """"int"""") } should produce [PFASyntaxException]
    evaluating { compileExpression("""{"float": "string"}""", """"float"""") } should produce [PFASyntaxException]

    compileExpression("""{"string": "hello"}""", """"string"""").apply should be ("hello")
    compileExpression("""{"base64": "aGVsbG8="}""", """"bytes"""").apply should be ("hello".getBytes)
  }

  it must "do complex literals" taggedAs(JVMCompilation) in {
    compileExpression("""
type: {type: array, items: string}
value: [one, two, three]
""", """{type: array, items: string}""").apply.asInstanceOf[java.util.List[_]].toList should be (List("one", "two", "three"))
  }

  it must "do new record" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: 1, two: 2.2, three: ["THREE"]}, type: SimpleRecord}
""").head

    val out = engine.action(java.lang.Double.valueOf(5))
    out.asInstanceOf[PFARecord].get("one") should be (1)
    out.asInstanceOf[PFARecord].get("two") should be (2.2)
    out.asInstanceOf[PFARecord].get("three") should be ("THREE")

    evaluating { PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: {long: 1}, two: 2.2, three: ["THREE"]}, type: SimpleRecord}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: 1, two: 2.2, three: ["THREE"], four: 444}, type: SimpleRecord}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: 1, two: 2.2}, type: SimpleRecord}
""").head } should produce [PFASemanticException]
  }

  it must "do new record with inline types" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: 1, two: 2.2, three: ["THREE"]}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head

    val out = engine.action(java.lang.Double.valueOf(5))
    out.asInstanceOf[PFARecord].get("one") should be (1)
    out.asInstanceOf[PFARecord].get("two") should be (2.2)
    out.asInstanceOf[PFARecord].get("three") should be ("THREE")

    evaluating { PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: {long: 1}, two: 2.2, three: ["THREE"]}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: 1, two: 2.2, three: ["THREE"], four: 444}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: 1, two: 2.2}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head } should produce [PFASemanticException]
  }

  it must "do new map" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: {type: map, values: int}
action:
  - {new: {one: 1, two: 2, three: input}, type: {type: map, values: int}}
""").head

    val out = engine.action(java.lang.Integer.valueOf(5))
    out.asInstanceOf[PFAMap[java.lang.Integer]].toMap("one") should be (1)
    out.asInstanceOf[PFAMap[java.lang.Integer]].toMap("two") should be (2)
    out.asInstanceOf[PFAMap[java.lang.Integer]].toMap("three") should be (5)
  }

  it must "do new array" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: {type: array, items: int}
action:
  - {new: [1, 2, input], type: {type: array, items: int}}
""").head

    val out = engine.action(java.lang.Integer.valueOf(5))
    out.asInstanceOf[PFAArray[java.lang.Integer]].toVector(0) should be (1)
    out.asInstanceOf[PFAArray[java.lang.Integer]].toVector(1) should be (2)
    out.asInstanceOf[PFAArray[java.lang.Integer]].toVector(2) should be (5)
  }

  it must "write to logs" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""{log: [{string: "hello"}]}""", """"null"""")) should be ((None, """"hello"""") :: Nil)
    collectLogs(compileExpressionEngine("""{log: [1, 2, 3]}""", """"null"""")) should be ((None, "1 2 3") :: Nil)
    collectLogs(compileExpressionEngine("""{log: [[hello]]}""", """"null"""")) should be ((None, """"hello"""") :: Nil)
    collectLogs(compileExpressionEngine("""{log: [[hello]], namespace: filter-me}""", """"null"""")) should be ((Some("filter-me"), """"hello"""") :: Nil)
  }

  it must "do variable declarations" taggedAs(JVMCompilation) in {
    compileExpression("""
do:
  - let: {x: [hello]}
  - x
""", """"string"""").apply should be ("hello")

    evaluating { compileExpression("""
do:
  - x
  - let: {x: [hello]}
""", """"null"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - let: {x: [hello], y: 12}
  - y
""", """"int"""").apply should be (12)

    evaluating { compileExpression("""
do:
  - let: {x: [hello], y: x}
  - y
""", """"string"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - let: {x: {let: {y: [stuff]}}}
  - x
""", """"null"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - let:
      x:
        do:
          - {let: {y: [stuff]}}
          - y
  - x
""", """"string"""").apply should be ("stuff")
  }

  it must "do variable reassignment" taggedAs(JVMCompilation) in {
    compileExpression("""
do:
  - let: {x: [hello]}
  - set: {x: [there]}
  - x
""", """"string"""").apply should be ("there")

    evaluating { compileExpression("""
do:
  - set: {x: [there]}
  - let: {x: [hello]}
  - x
""", """"string"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - let: {x: [hello]}
  - set: {x: 12}
  - x
""", """"int"""").apply } should produce [PFASemanticException]
  }

  it must "call functions" taggedAs(JVMCompilation) in {
    compileExpression("""+: [2, 2]""", """"int"""").apply should be (4)

    evaluating { compileExpression("""+: [2, [hello]]""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
"+":
  - "+": [2, 2]
  - 2
""", """"int"""").apply should be (6)

    evaluating { compileExpression("""
"+":
  - {let: {x: 5}}
  - 2
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
"+":
  - {do: [{let: {x: 5}}, x]}
  - 2
""", """"int"""").apply should be (7)

    compileExpression("""
do:
  - {let: {x: 5}}
  - "+":
      - x
      - 2
""", """"int"""").apply should be (7)

    evaluating { compileExpression("""
do:
  - {let: {x: 5}}
  - "+":
      - {do: [{let: {x: 5}}, x]}
      - 2
""", """"int"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - {let: {x: 5}}
  - "+":
      - {do: [{set: {x: 10}}, x]}
      - 2
""", """"int"""") } should produce [PFASemanticException]
  }

  it must "do if expressions" taggedAs(JVMCompilation) in {
    compileExpression("""{if: true, then: [3]}""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    evaluating { compileExpression("""{if: [hello], then: [3]}""", """"string"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {x: 3}}
  - if: true
    then:
      - {set: {x: 99}}
  - x
""", """"int"""").apply should be (99)

    evaluating { compileExpression("""
do:
  - {let: {x: 3}}
  - if: true
    then:
      - {let: {x: 99}}
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {x: 3}}
  - if: true
    then:
      - {set: {x: 99}}
    else:
      - {set: {x: 55}}
  - x
""", """"int"""").apply should be (99)

    compileExpression("""
do:
  - {let: {x: 3}}
  - if: false
    then:
      - {set: {x: 99}}
    else:
      - {set: {x: 55}}
  - x
""", """"int"""").apply should be (55)

    compileExpression("""
if: true
then:
  - 20
else:
  - 30
""", """"int"""").apply should be (20)

    compileExpression("""
if: false
then:
  - 20
else:
  - 30
""", """"int"""").apply should be (30)

    compileExpression("""
if: false
then:
  - 20
else:
  - [string]
""", """["string", "int"]""").apply should be ("string")

    compileExpression("""
if: true
then:
  - {let: {x: 999}}
  - x
else:
  - 50
""", """"int"""").apply should be (999)

    evaluating { compileExpression("""
if: true
then:
  - {let: {x: 999}}
  - x
else:
  - x
""", """"int"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
if: true
then:
  - x
else:
  - {let: {x: 999}}
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
if: {"!=": [2, 3]}
then:
  - {+: [5, 5]}
else:
  - {+: [123, 456]}
""", """"int"""").apply should be (10)

    compileExpression("""
"+":
  - if: true
    then: [5]
    else: [2]
  - 100
""", """"int"""").apply should be (105)

    collectLogs(compileExpressionEngine("""
do:
  - {log: [{string: "one"}]}
  - if: true
    then: [{log: [{string: "two"}]}]
    else: [{log: [{string: "ARG!"}]}]
  - if: false
    then: [{log: [{string: "ARGY-ARG-ARG!"}]}]
    else: [{log: [{string: "three"}]}]
  - if: true
    then: [{log: [{string: "four"}]}]
  - if: false
    then: [{log: [{string: "AAAAAAAAARG!"}]}]
""", """"null"""")) should be ((None, """"one"""") :: (None, """"two"""") :: (None, """"three"""") :: (None, """"four"""") :: Nil)

    collectLogs(compileExpressionEngine("""
if: true
then:
  - if: true
    then:
      - if: true
        then:
          - {log: [{string: "HERE"}]}
""", """"null"""")) should be ((None, """"HERE"""") :: Nil)

    collectLogs(compileExpressionEngine("""
if: true
then:
  - if: true
    then:
      - if: true
        then:
          - {log: [{string: "HERE"}]}
        else:
          - {log: [{string: "AAAARG!"}]}
""", """"null"""")) should be ((None, """"HERE"""") :: Nil)

    collectLogs(compileExpressionEngine("""
if: true
then:
  - if: true
    then:
      - if: true
        then:
          - {log: [{string: "HERE"}]}
    else:
      - {log: [{string: "BOO!"}]}
""", """"null"""")) should be ((None, """"HERE"""") :: Nil)

    compileExpression("""
do:
  - if: true
    then:
      - {let: {x: 99}}
    else:
      - {let: {x: 99}}
  - 123
""", """"int"""").apply should be (123)

    compileExpression("""
if: true
then: [1]
else: [2]
""", """"int"""").apply should be (1)
  }

  it must "do cond expressions" taggedAs(JVMCompilation) in {
    compileExpression("""
cond:
  - {if: false, then: [1]}
  - {if: true, then: [2]}
  - {if: true, then: [3]}
else: [4]
""", """"int"""").apply should be (2)

    compileExpression("""
cond:
  - {if: false, then: [1]}
  - {if: false, then: [2]}
  - {if: false, then: [3]}
else: [4]
""", """"int"""").apply should be (4)

    compileExpression("""
cond:
  - {if: false, then: [1]}
  - {if: false, then: [2]}
  - {if: false, then: [3]}
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
cond:
  - {if: false, then: [{let: {x: 5}}, 1]}
  - {if: false, then: [{let: {x: 5}}, 2]}
  - {if: false, then: [{let: {x: 5}}, 3]}
else: [{let: {x: 5}}, 4]
""", """"int"""").apply should be (4)

    compileExpression("""
cond:
  - {if: false, then: [{let: {x: 5}}, 1]}
  - {if: false, then: [{let: {x: 5}}, 2]}
  - {if: false, then: [{let: {x: 5}}, 3]}
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    evaluating { compileExpression("""
cond:
  - {if: false, then: [{let: {x: 5}}, 1]}
  - {if: false, then: [{let: {x: 5}}, 2]}
  - {if: false, then: [{let: {x: 5}}, 3]}
else: [{set: {x: 5}}, 4]
""", """"int"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
cond:
  - {if: false, then: [{let: {x: 5}}, 1]}
  - {if: false, then: [{set: {x: 5}}, 2]}
  - {if: false, then: [{set: {x: 5}}, 3]}
""", """"null"""") } should produce [PFASemanticException]

    compileExpression("""
cond:
  - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 1]}
  - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 2]}
  - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 3]}
else: [{let: {x: 5}}, 4]
""", """"int"""").apply should be (4)

    compileExpression("""
cond:
  - {if: {do: [{let: {x: 5}}, true]}, then: [{let: {x: 5}}, 1]}
  - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 2]}
  - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 3]}
else: [{let: {x: 5}}, 4]
""", """"int"""").apply should be (1)

    evaluating { compileExpression("""
do:
  - {let: {x: 3}}
  - cond:
      - {if: {do: [{let: {x: 5}}, true]}, then: [1]}
      - {if: {do: [{let: {x: 5}}, false]}, then: [2]}
      - {if: {do: [{let: {x: 5}}, false]}, then: [3]}
    else: [{let: {x: 5}}, 4]
""", """"int"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - {let: {x: 3}}
  - cond:
      - {if: {do: [{set: {x: 1}}, true]}, then: [1]}
      - {if: {do: [{set: {x: 2}}, false]}, then: [2]}
      - {if: {do: [{set: {x: 3}}, false]}, then: [3]}
    else: [4]
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
cond:
  - {if: false, then: [1]}
  - {if: true, then: [[two]]}
  - {if: true, then: [3.0]}
else: [4]
""", """["string", "int", "double"]""").apply should be ("two")
  }

  it must "do while loops" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""
do:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
""", """"null"""")) should be ((None, "0") :: (None, "1") :: (None, "2") :: (None, "3") :: (None, "4") :: Nil)

    compileExpression("""
do:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""").apply should be (5)

    evaluating { compileExpression("""
do:
  - {let: {x: 0}}
  - while: {+: [2, 2]}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - {let: {x: 0}}
  - while: {let: {y: 12}}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {x: 0}}
  - while: {do: [{+: [2, 2]}, {"!=": [x, 5]}]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""").apply should be (5)

    evaluating { compileExpression("""
do:
  - {let: {x: 0}}
  - {let: {y: 0}}
  - while: {do: [{set: {y: 5}}, {"!=": [x, y]}]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {set: {x: {+: [x, 1]}}}
  - x
""", """"int"""").apply should be (5)

    evaluating { compileExpression("""
do:
  - {let: {x: 0}}
  - while: {"!=": [y, 5]}
    do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {y: [before], x: 0}}
  - while: {"!=": [x, 0]}
    do:
      - {set: {y: [after]}}
  - y
""", """"string"""").apply should be ("before")
  }

  it must "do until loops" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""
do:
  - {let: {x: 0}}
  - do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
    until: {==: [x, 5]}
""", """"null"""")) should be ((None, "0") :: (None, "1") :: (None, "2") :: (None, "3") :: (None, "4") :: Nil)

    compileExpression("""
do:
  - {let: {x: 0}}
  - do:
      - {set: {x: {+: [x, 1]}}}
      - x
    until: {==: [x, 5]}
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
do:
  - {let: {x: 0}}
  - do:
      - {set: {x: {+: [x, 1]}}}
      - x
    until: {==: [x, 5]}
  - x
""", """"int"""").apply should be (5)

    compileExpression("""
do:
  - {let: {x: 0}}
  - do:
      - {set: {x: {+: [x, 1]}}}
    until: {==: [x, 5]}
  - x
""", """"int"""").apply should be (5)

    compileExpression("""
do:
  - {let: {x: 0}}
  - do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
    until: {==: [y, 5]}
  - x
""", """"int"""").apply should be (5)

    evaluating { compileExpression("""
do:
  - {let: {x: 0}}
  - do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
    until: {==: [y, 5]}
  - y
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {y: [before], x: 0}}
  - do:
      - {set: {y: [after]}}
    until: {==: [x, 0]}
  - y
""", """"string"""").apply should be ("after")
  }

  it must "do for loops" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""
for: {x: 0}
while: {"!=": [x, 5]}
step: {x: {+: [x, 1]}}
do:
  - {log: [x]}
""", """"null"""")) should be ((None, "0") :: (None, "1") :: (None, "2") :: (None, "3") :: (None, "4") :: Nil)

    compileExpression("""
do:
  - for: {x: 0}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    evaluating { compileExpression("""
do:
  - for: {x: 0}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
  - x
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {x: 0}}
  - for: {dummy: null}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
  - x
""", """"int"""").apply should be (5)

    compileExpression("""
for: {x: {+: [99, -99]}}
while: {"!=": [x, {+: [2, 3]}]}
step: {x: {+: [x, {-: [3, 2]}]}}
do:
  - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    evaluating { compileExpression("""
for: {x: {let: {y: 0}}}
while: {"!=": [x, {+: [2, 3]}]}
step: {x: {+: [x, {-: [3, 2]}]}}
do:
  - x
""", """"null"""") } should produce [PFASemanticException]

    compileExpression("""
for: {x: {do: [{let: {y: 0}}, y]}}
while: {"!=": [x, {+: [2, 3]}]}
step: {x: {+: [x, {-: [3, 2]}]}}
do:
  - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
for: {x: 0}
while: {"!=": [x, 5]}
step: {x: {+: [x, 1]}}
do:
  - {let: {y: x}}
  - y
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    evaluating { compileExpression("""
for: {x: 0}
while: {"!=": [x, 5]}
step: {x: {+: [y, 1]}}
do:
  - {let: {y: x}}
  - y
""", """"int"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
for: {x: 0}
while: {"!=": [y, 5]}
step: {x: {+: [x, 1]}}
do:
  - {let: {y: x}}
  - y
""", """"int"""") } should produce [PFASemanticException]

    compileExpression("""
for: {x: 0}
while: {"!=": [x, 0]}
step: {x: {+: [x, 1]}}
do:
  - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
do:
  - {let: {y: [before]}}
  - for: {x: 0}
    while: {"!=": [x, 0]}
    step: {x: {+: [x, 1]}}
    do:
      - {set: {y: [after]}}
  - y
""", """"string"""").apply should be ("before")
  }

  it must "do foreach loops" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""
foreach: x
in: {type: {type: array, items: string}, value: [one, two, three]}
do:
  - {log: [x]}
""", """"null"""")) should be ((None, """"one"""") :: (None, """"two"""") :: (None, """"three"""") :: Nil)

    compileExpression("""
foreach: x
in: {type: {type: array, items: string}, value: [one, two, three]}
do:
  - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
foreach: x
in: {type: {type: array, items: string}, value: [one, two, three]}
do:
  - {let: {y: x}}
  - y
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    evaluating { compileExpression("""
do:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
      - y
""", """"string"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
      - y
    seq: true
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
do:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
    seq: true
  - y
""", """"string"""").apply should be ("three")

    evaluating { compileExpression("""
do:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {y: x}}
  - y
""", """"string"""") } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {y: x}}
    seq: true
  - y
""", """"string"""").apply should be ("three")

    compileExpression("""
do:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {array: {type: {type: array, items: string}, value: [zero]}}}
      - {set: {y: x}}
    seq: true
  - y
""", """"string"""").apply should be ("three")

    compileExpression("""
foreach: x
in: {type: {type: array, items: string}, value: []}
do:
  - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
foreach: x
in: {type: {type: array, items: double}, value: [1, 2, 3]}
do:
  - x
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
do:
  - {let: {y: 0.0}}
  - foreach: x
    in: {type: {type: array, items: double}, value: [1, 2, 3]}
    do:
      - {set: {y: x}}
    seq: true
  - y
""", """"double"""").apply should be (3.0)
  }

  it must "do forkeyval loops" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""
forkey: n
forval: v
in: {type: {type: map, values: int}, value: {one: 1, two: 2, three: 3}}
do:
  - {log: [n, v]}
""", """"null"""")).toSet should be (Set((None, """"one" 1"""), (None, """"two" 2"""), (None, """"three" 3""")))

    compileExpression("""
do:
  - {let: {x: 0}}
  - forkey: n
    forval: v
    in: {type: {type: map, values: int}, value: {one: 1, two: 2, three: 3}}
    do:
      - {set: {x: v}}
  - x
""", """"int"""")
  }

  it must "do type-cast blocks" taggedAs(JVMCompilation) in {
    collectLogs(compileExpressionEngine("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
      - {as: "string", named: y, do: [{log: [x, y]}]}
""", """"null"""")) should be ((None, """{"double":2.2} 2.2""") :: Nil)

    evaluating { compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
      - {as: "string", named: y, do: [{log: [x, y]}]}
      - {as: "bytes", named: y, do: [{log: [x, y]}]}
""", """"null"""") } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
""", """"null"""") } should produce [PFASemanticException]

    collectLogs(compileExpressionEngine("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
    partial: true
""", """"null"""")) should be ((None, """{"double":2.2} 2.2""") :: Nil)

    compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - {let: {z: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{set: {z: y}}]}
      - {as: "double", named: y, do: [{set: {z: y}}]}
      - {as: "string", named: y, do: [{set: {z: y}}]}
  - z
""", """["double", "string"]""").apply should be (2.2)

    compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - {let: {z: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{set: {z: y}}]}
      - {as: "double", named: y, do: [{set: {z: y}}]}
      - {as: "string", named: y, do: [{set: {z: y}}]}
    partial: true
  - z
""", """["double", "string"]""").apply should be (2.2)

    compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: 888.88}
      - {as: "double", named: y, do: [y]}
      - {as: "string", named: y, do: 999.99}
""", """"double"""").apply should be (2.2)

    compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: 888.88}
      - {as: "double", named: y, do: [y]}
      - {as: "string", named: y, do: 999.99}
    partial: true
""", """"null"""").apply should be (null)

  }

  it must "do upcast" taggedAs(JVMCompilation) in {
    compileExpression("""
{upcast: 3, as: "double"}
""", """"double"""").apply should be (3.0)

    compileExpression("""
{upcast: 3, as: ["double", "string"]}
""", """["string", "double"]""").apply should be (3.0)

    evaluating { compileExpression("""
do:
  - {let: {fred: {upcast: 3, as: "double"}}}
  - {set: {fred: [hello]}}
  - fred
""", """"double"""").apply } should produce [PFASemanticException]

    evaluating { compileExpression("""
do:
  - {let: {fred: {upcast: 3, as: "double"}}}
  - {set: {fred: [hello]}}
  - fred
""", """"string"""").apply } should produce [PFASemanticException]

    compileExpression("""
do:
  - {let: {fred: {upcast: 3, as: ["double", "string"]}}}
  - {set: {fred: [hello]}}
  - fred
""", """["double", "string"]""").apply should be ("hello")
  }

  it must "do ifnotnull" taggedAs(JVMCompilation) in {
    val engine1 = PFAEngine.fromYaml("""
input: [double, "null"]
output: double
action:
  ifnotnull: {x: input}
  then: x
  else: 12
""").head
    engine1.action(java.lang.Double.valueOf(5)) should be (5.0)
    engine1.action(null) should be (12.0)

    val engine2 = PFAEngine.fromYaml("""
input: [double, "null"]
output: double
action:
  ifnotnull: {x: input, y: input}
  then: {+: [x, y]}
  else: 12
""").head
    engine2.action(java.lang.Double.valueOf(5)) should be (10.0)
    engine2.action(null) should be (12.0)

    val engine3 = PFAEngine.fromYaml("""
input: [double, "null"]
output: "null"
action:
  ifnotnull: {x: input, y: input}
  then: {+: [x, y]}
""").head
    engine3.action(java.lang.Double.valueOf(5)) should be (null)
    engine3.action(null) should be (null)

    val engine4 = PFAEngine.fromYaml("""
input: [double, "null"]
output: double
action:
  - let: {z: -3.0}
  - ifnotnull: {x: input, y: input}
    then: {set: {z: {+: [x, y]}}}
  - z
""").head
    engine4.action(java.lang.Double.valueOf(5)) should be (10.0)
    engine4.action(null) should be (-3.0)

    val engine5 = PFAEngine.fromYaml("""
input: [double, "null"]
output: double
action:
  - let: {z: -3.0}
  - ifnotnull: {x: input, y: input}
    then: {set: {z: {+: [x, y]}}}
    else: {set: {z: 999.9}}
  - z
""").head
    engine5.action(java.lang.Double.valueOf(5)) should be (10.0)
    engine5.action(null) should be (999.9)

    val engine6 = PFAEngine.fromYaml("""
input: [double, string, "null"]
output: [double, string]
action:
  - ifnotnull: {x: input}
    then: x
    else: [[whatever]]
""").head
    engine6.action(java.lang.Double.valueOf(5)) should be (5.0)
    engine6.action("hello") should be ("hello")
    engine6.action(null) should be ("whatever")
  }

  it must "do doc" taggedAs(JVMCompilation) in {
    compileExpression("""{doc: "This is very nice"}""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
if: true
then:
  - {doc: "This is very nice"}
else:
  - {+: [5, 5]}
""", """["int", "null"]""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
if: true
then:
  - {doc: "This is very nice"}
  - if: true
    then:
      - {+: [5, 5]}
""", """["null", "int"]""").apply should be (null.asInstanceOf[java.lang.Void])
  }

  it must "do error" taggedAs(JVMCompilation) in {
    evaluating { compileExpression("""{error: "This is bad"}""", """"null"""").apply } should produce [PFAUserException]

    try {
      compileExpression("""{error: "This is bad", code: 12}""", """"null"""").apply
      true should be (false)
    }
    catch {
      case err: PFAUserException => err.code should be (Some(12))
    }

    evaluating { compileExpression("""
if: true
then:
  - {error: "This is bad"}
""", """"null"""").apply } should produce [PFAUserException]

    compileExpression("""
if: false
then:
  - {error: "This is bad"}
""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    compileExpression("""
if: false
then:
  - {error: "This is bad"}
  - [hello]
else:
  - [there]
""", """"string"""").apply should be ("there")
  }

  "JVM engine compilation" must "minimally work" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
""").head.action("hello") should be ("hello")
  }

  it must "handle nested scopes" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - do:
    - input
""").head.action("hello") should be ("hello")
  }

  it must "call functions" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: double
output: double
action:
  - {+: [input, input]}
""").head.action(java.lang.Double.valueOf(2)) should be (java.lang.Double.valueOf(4))
  }

  it must "identify type errors 1" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: double
output: string
action:
  - {+: [input, input]}
""").head } should produce [PFASemanticException]
  }

  it must "identify type errors 2" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {+: [input, input]}
""").head } should produce [PFASemanticException]
  }

  it must "define functions" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {u.plus: [input, input]}
fcns:
  plus:
    params: [{x: double}, {y: double}]
    ret: double
    do:
      - {+: [x, y]}
  minus:
    params: [{x: double}, {y: double}]
    ret: double
    do:
      - {-: [x, y]}
""").head

    engine.action(java.lang.Double.valueOf(2)) should be (java.lang.Double.valueOf(4))

    engine.fcn2("plus")(java.lang.Double.valueOf(3), java.lang.Double.valueOf(4)) should be (java.lang.Double.valueOf(7))
    engine.fcn2("minus")(java.lang.Double.valueOf(3), java.lang.Double.valueOf(4)) should be (java.lang.Double.valueOf(-1))
  }

  it must "minimally work for emit-type engines" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
method: emit
action:
  - {emit: [input]}
  - input
""").head

    engine.action("hello") should be (null.asInstanceOf[String])

    object Counter {
      var count = 0
    }
    engine.asInstanceOf[PFAEmitEngine[_, String]].emit = {x: String => Counter.count += 1;  x should be ("hello")}
    engine.action("hello") should be (null.asInstanceOf[String])
    Counter.count should be (1)
  }

  it must "access emit in user functions" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
method: emit
action:
  - emit: [input]
  - u.callme: [input]
fcns:
  callme:
    params:
      - x: string
    ret: "null"
    do:
      - emit: [x]
""").head

    object Counter {
      var count = 0
    }

    engine.asInstanceOf[PFAEmitEngine[_, String]].emit = {x: String => Counter.count += 1; x should be ("hello")}
    engine.action("hello") should be (null.asInstanceOf[java.lang.Void])
    Counter.count should be (2)
  }

  it must "minimally work for fold-type engines" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
method: fold
zero: 0
action:
  - {+: [input, tally]}
""").head

    engine.action(java.lang.Double.valueOf(5)) should be (java.lang.Double.valueOf(5))
    engine.action(java.lang.Double.valueOf(3)) should be (java.lang.Double.valueOf(8))
    engine.action(java.lang.Double.valueOf(2)) should be (java.lang.Double.valueOf(10))
    engine.action(java.lang.Double.valueOf(20)) should be (java.lang.Double.valueOf(30))

    engine.asInstanceOf[PFAFoldEngine[_, java.lang.Double]].tally = java.lang.Double.valueOf(1)
    engine.action(java.lang.Double.valueOf(5)) should be (java.lang.Double.valueOf(6))
  }

  it must "refuse tally in user functions" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: double
output: double
method: fold
zero: 0
action:
  - u.callme: [input]
fcns:
  callme:
    params:
      - x: double
    ret: double
    do:
      - {+: [x, tally]}
""").head } should produce [PFASemanticException]
  }

  it must "but passing tally in is fine" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
method: fold
zero: 0
action:
  - u.callme: [input, tally]
fcns:
  callme:
    params:
      - x: double
      - t: double
    ret: double
    do:
      - {+: [x, t]}
""").head

    engine.action(java.lang.Double.valueOf(5)) should be (java.lang.Double.valueOf(5))
    engine.action(java.lang.Double.valueOf(3)) should be (java.lang.Double.valueOf(8))
    engine.action(java.lang.Double.valueOf(2)) should be (java.lang.Double.valueOf(10))
    engine.action(java.lang.Double.valueOf(20)) should be (java.lang.Double.valueOf(30))

    engine.asInstanceOf[PFAFoldEngine[_, java.lang.Double]].tally = java.lang.Double.valueOf(1)
    engine.action(java.lang.Double.valueOf(5)) should be (java.lang.Double.valueOf(6))
  }

  "attr-get" must "extract deep within an object" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], 2, x]}
""").head
    engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (2)
  }

  it must "extract from an on-the-fly generated object" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {let: {x: [two]}}
  - attr:
      value: {"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}
      type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
    path: [[one], 2, x]
""").head
    engine.action(null) should be (2)
  }

  it must "not accept bad indexes" taggedAs(JVMCompilation) in {
    evaluating {
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], 3, x]}
""").head
      engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType))
    } should produce [PFARuntimeException]

    evaluating {
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [TWO]}}
  - {attr: input, path: [[one], 2, x]}
""").head
      engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType))
    } should produce [PFARuntimeException]
  }

  it must "not accept bad index types" taggedAs(JVMCompilation) in {
    evaluating {
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[ONE], 2, x]}
""").head
      engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (2)
    } should produce [PFASemanticException]

    evaluating {
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], x, x]}
""").head
      engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (2)
    } should produce [PFASemanticException]

   evaluating {
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {attr: input, path: [[one], 2, 2]}
""").head
      engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (2)
   } should produce [PFASemanticException]
  }

  "attr-set" must "change a deep object" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: 999}}}
  - {attr: something, path: [[one], 2, x]}
""").head
    engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (999)
  }

  it must "change an on-the-fly generated object" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {let: {x: [two]}}
  - let:
      something:
        attr:
          value: {"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}
          type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
        path: [[one], 2, x]
        to: 999
  - {attr: something, path: [[one], 2, x]}
""").head
    engine.action(null) should be (999)
  }

  it must "change a deep object with fcndef" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}}}
  - {attr: something, path: [[one], 2, x]}
""").head
    engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (3)
  }

  it must "change a deep object with fcnref" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {fcnref: u.inc}}}}
  - {attr: something, path: [[one], 2, x]}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head
    engine.action(engine.fromJson("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""", engine.inputType)) should be (3)
  }

  "cell-get" must "extract private cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - cell: x
cells:
  x: {type: int, init: 12}
""").head.action("whatever") should be (12)
  }

  it must "extract public cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - cell: x
cells:
  x: {type: int, init: 12, shared: true}
""").head.action("whatever") should be (12)
  }

  it must "extract deep private cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") should be (2)
  }

  it must "extract deep public cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
""").head.action("two") should be (2)
  }

  it must "not find non-existent cells" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - cell: y
cells:
  x: {type: int, init: 12}
""").head } should produce [PFASemanticException]
  }

  it must "not accept bad indexes" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 3, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") } should produce [PFARuntimeException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("TWO") } should produce [PFARuntimeException]
  }

  it must "not accept bad index types" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[ONE], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], input, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, 2]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head } should produce [PFASemanticException]
  }

  "cell-set" must "change private cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: 999}
  - {cell: x}
cells:
  x: {type: int, init: 12}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: 999}
cells:
  x: {type: int, init: 12}
""").head.action("whatever") should be (999)
  }

  it must "change private cells with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x}
cells:
  x: {type: int, init: 12}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
cells:
  x: {type: int, init: 12}
""").head.action("whatever") should be (13)
  }

  it must "change private cells with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {fcnref: u.inc}}
  - {cell: x}
cells:
  x: {type: int, init: 12}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {fcnref: u.inc}}
cells:
  x: {type: int, init: 12}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)
  }

  it must "change deep private cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: 999}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: 999}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") should be (999)
  }

  it must "change deep private cells with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") should be (3)
  }

  it must "change deep private cells with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {fcnref: u.inc}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {fcnref: u.inc}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)
  }

  "cell-set" must "change public cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: 999}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: 999}
cells:
  x: {type: int, init: 12, shared: true}
""").head.action("whatever") should be (999)
  }

  it must "change public cells with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
cells:
  x: {type: int, init: 12, shared: true}
""").head.action("whatever") should be (13)
  }

  it must "change public cells with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {fcnref: u.inc}}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {fcnref: u.inc}}
cells:
  x: {type: int, init: 12, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)
  }

  it must "change deep public cells" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: 999}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: 999}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
""").head.action("two") should be (999)
  }

  it must "change deep public cells with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
""").head.action("two") should be (3)
  }

  it must "change deep public cells with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {fcnref: u.inc}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {fcnref: u.inc}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)
  }

  "pool-get" must "extract private pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (12)
  }

  it must "extract public pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (12)
  }

  it must "extract deep private pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (2)
  }

  it must "extract deep public pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (2)
  }

  it must "not find non-existent pools" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: y, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
""").head } should produce [PFASemanticException]
  }

  it must "not accept bad indexes" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 3, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") } should produce [PFARuntimeException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("TWO") } should produce [PFARuntimeException]
  }

  it must "not accept bad index types" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [ONE], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], input, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head } should produce [PFASemanticException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, 2]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head } should produce [PFASemanticException]
  }

  "pool-set" must "change private pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (999)
  }

  it must "change private pools with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (13)
  }

  it must "change private pools with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcnref: u.inc}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcnref: u.inc}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)
  }

  it must "change deep private pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: 999}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: 999}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (999)
  }

  it must "change deep private pools with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (3)
  }

  it must "change deep private pools with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcnref: u.inc}, init: 0}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {fcnref: u.inc}, init: 0}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)
  }

  "pool-set" must "change public pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (999)
  }

  it must "change public pools with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (13)
  }

  it must "change public pools with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcnref: u.inc}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcnref: u.inc}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("whatever") should be (13)
  }

  it must "change deep public pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: 999}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: 999}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (999)
  }

  it must "change deep public pools with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (3)
  }

  it must "change deep public pools with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcnref: u.inc}, init: 0}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {fcnref: u.inc}, init: 0}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)
  }

  "unshared cells" must "handle null cases" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - input
cells:
  notempty: {type: "null", init: null}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: empty}
  - input
cells:
  notempty: {type: "null", init: null}
""").head } should produce [PFASemanticException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[set]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[notinset]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}}
""").head.action("hey") } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [0]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null]}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [999]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null]}
""").head.action("hey") } should produce [PFARuntimeException]
  }

  "unshared pools" must "handle null cases" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: []}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
""").head.action("hey") } should produce [PFASyntaxException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[shouldfail]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
""").head.action("hey") } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [set]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [notinset]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}}
""").head.action("hey") } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 0]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 999]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}}
""").head.action("hey") } should produce [PFARuntimeException]
  }

  "shared cells" must "handle null cases" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - input
cells:
  notempty: {type: "null", init: null, shared: true}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: empty}
  - input
cells:
  notempty: {type: "null", init: null, shared: true}
""").head } should produce [PFASemanticException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[set]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}, shared: true}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[notinset]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}, shared: true}
""").head.action("hey") } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [0]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null], shared: true}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [999]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null], shared: true}
""").head.action("hey") } should produce [PFARuntimeException]
  }

  "shared pools" must "handle null cases" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: []}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
""").head.action("hey") } should produce [PFASyntaxException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[shouldfail]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
""").head.action("hey") } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [set]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}, shared: true}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [notinset]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}, shared: true}
""").head.action("hey") } should produce [PFARuntimeException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 0]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}, shared: true}
""").head.action("hey")

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 999]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}, shared: true}
""").head.action("hey") } should produce [PFARuntimeException]
  }

  "pool to and init" must "handle a typical use-case" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
method: map
action:
  - {pool: tally, path: [[x]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[x]]}
pools:
  tally: {type: int, init: {}}
fcns:
  inc: {params: [{i: int}], ret: int, do: [{+: [i, 1]}]}
""").head.action("hey") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
method: map
action:
  - {pool: tally, path: [[x]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcnref: u.inc}, init: 0}
  - {pool: tally, path: [[y]]}
pools:
  tally: {type: int, init: {}}
fcns:
  inc: {params: [{i: int}], ret: int, do: [{+: [i, 1]}]}
""").head.action("hey") should be (2)
  }

  "execution clock" must "stop int-running processes" taggedAs(JVMCompilation) in {
    evaluating { PFAEngine.fromYaml("""
input: string
output: "null"
action:
  - for: {x: 0}
    while: {"!=": [x, -5]}
    step: {x: {+: [x, 1]}}
    do: [x]
options:
  timeout: 1000
""").head.action("hey") } should produce [PFATimeoutException]
  }

  "begin and end" must "work" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
begin:
  - {cell: x, to: {params: [{in: int}], ret: int, do: [{+: [in, 100]}]}}
action:
  - {cell: x, to: {params: [{in: int}], ret: int, do: [{+: [in, 33]}]}}
  - {cell: x}
end:
  - {cell: x, to: {params: [{in: int}], ret: int, do: [{+: [in, 40]}]}}
cells:
  x: {type: int, init: 0}
""").head

    engine.begin()
    engine.action("hey")
    engine.end()
    engine.action("hey") should be (206)
  }

  "call graph" must "be generated correctly" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [], ret: int, do: [{+: [1, 1]}]}
  b: {params: [], ret: int, do: [{u.a: []}]}
  c: {params: [], ret: int, do: [{u.c: []}]}
  d: {params: [], ret: int, do: [{+: [{u.b: []}, {u.c: []}]}]}
  c1: {params: [], ret: int, do: [{u.c2: []}]}
  c2: {params: [], ret: int, do: [{u.c1: []}]}
""").head

    engine.callGraph should be (Map("u.a" -> Set("+", "(int)"), "u.b" -> Set("u.a"), "u.c" -> Set("u.c"), "u.d" -> Set("+", "u.b", "u.c"),
      "u.c1" -> Set("u.c2"), "u.c2" -> Set("u.c1"), "(begin)" -> Set(), "(action)" -> Set(), "(end)" -> Set()))
    engine.calledBy("u.d") should be (Set("+", "u.a", "u.b", "u.c", "(int)"))
    engine.isRecursive("u.c") should be (true)
    engine.isRecursive("u.c1") should be (true)
    engine.isRecursive("u.d") should be (false)
    engine.hasRecursive("u.d") should be (true)

    engine.callDepth("u.a") should be (1)
    engine.callDepth("u.b") should be (2)
    engine.callDepth("u.d") should be (java.lang.Double.POSITIVE_INFINITY)
    engine.callDepth("u.c") should be (java.lang.Double.POSITIVE_INFINITY)
  }

  it must "refuse situations that could lead to deadlock" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {fcnref: u.b}}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""").head

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""").head

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {fcnref: u.b}}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [{cell: y, to: null}]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""").head } should produce [PFAInitializationException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}}]}
  c: {params: [{x: "null"}], ret: "null", do: [{cell: y, to: null}]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""").head } should produce [PFAInitializationException]

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {fcnref: u.b}, init: null}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""").head

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}, init: null}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""").head

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {fcnref: u.b}, init: null}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [{pool: y, path: [[whatever]], to: null}]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""").head } should produce [PFAInitializationException]

    evaluating { PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}, init: null}]}
  c: {params: [{x: "null"}], ret: "null", do: [{pool: y, path: [[whatever]], to: null}]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""").head } should produce [PFAInitializationException]

  }

  "roll-backer" must "do cell rollback" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - cell: x
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - cell: x
cells:
  x: {type: int, init: 0, rollback: false}
""").head

    engine.action(java.lang.Boolean.valueOf(false)) should be (1)
    engine.action(java.lang.Boolean.valueOf(false)) should be (2)
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engine.action(java.lang.Boolean.valueOf(false)) should be (4)
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engine.action(java.lang.Boolean.valueOf(false)) should be (8)
    engine.action(java.lang.Boolean.valueOf(false)) should be (9)

    val engineRB = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - cell: x
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - cell: x
cells:
  x: {type: int, init: 0, rollback: true}
""").head

    engineRB.action(java.lang.Boolean.valueOf(false)) should be (1)
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (2)
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (3)
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (4)
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (5)
  }

  it must "do pool rollback" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - pool: x
    path: [[z]]
    init: 0
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - pool: x
    path: [[z]]
pools:
  x: {type: int, init: {z: 0}, rollback: false}
""").head

    engine.action(java.lang.Boolean.valueOf(false)) should be (1)
    engine.action(java.lang.Boolean.valueOf(false)) should be (2)
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engine.action(java.lang.Boolean.valueOf(false)) should be (4)
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engine.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engine.action(java.lang.Boolean.valueOf(false)) should be (8)
    engine.action(java.lang.Boolean.valueOf(false)) should be (9)

    val engineRB = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - pool: x
    path: [[z]]
    init: 0
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - pool: x
    path: [[z]]
pools:
  x: {type: int, init: {z: 0}, rollback: true}
""").head

    engineRB.action(java.lang.Boolean.valueOf(false)) should be (1)
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (2)
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (3)
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    try { engineRB.action(java.lang.Boolean.valueOf(true)) } catch { case err: PFAUserException => }
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (4)
    engineRB.action(java.lang.Boolean.valueOf(false)) should be (5)
  }

}
