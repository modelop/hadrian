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

package test.scala.jvmcompiler

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.map.ObjectMapper

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.yaml._
import test.scala._

case class MyTestRecord(one: Int, two: Double, three: String)
case class MyTestRecord2(one: Seq[Int], two: Map[String, Double])
case class MyTestRecord3(first: MyTestRecord, second: MyTestRecord2)

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

  "JVM metadata access" must "access name" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: string
method: map
action: {s.concat: [name, {string: "!!!"}]}
""").head.action(null) should be ("ThisIsMyName!!!")

    val emitEngine = PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: string
method: emit
action: {emit: {s.concat: [name, {string: "!!!"}]}}
""").head
    emitEngine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef => x should be ("ThisIsMyName!!!")}
    emitEngine.action(null)

    PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: string
method: fold
zero: "!!!"
action: {s.concat: [name, tally]}
merge: {s.concat: [tallyOne, tallyTwo]}
""").head.action(null) should be ("ThisIsMyName!!!")

    val beginEngine = PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
action: null
begin: {log: {s.concat: [name, {string: "!!!"}]}}
""").head
    beginEngine.log = {(x: String, ns: Option[String]) => x should be (""""ThisIsMyName!!!"""")}
    beginEngine.begin()

    val endEngine = PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
action: null
end: {log: {s.concat: [name, {string: "!!!"}]}}
""").head
    endEngine.log = {(x: String, ns: Option[String]) => x should be (""""ThisIsMyName!!!"""")}
    endEngine.end()

    val engines = PFAEngine.fromYaml("""
name: WeAllHaveTheSameName
input: "null"
output: {type: array, items: string}
action:
  - let: {out: {type: {type: array, items: string}, value: []}}
  - for: {i: 0}
    while: {"<": [i, instance]}
    step: {i: {+: [i, 1]}}
    do:
      set: {out: {a.append: [out, name]}}
  - out
""", multiplicity = 10)
    for ((engine, index) <- engines.zipWithIndex) {
      val output = engine.action(null).asInstanceOf[PFAArray[String]].toVector
      output.size should be (index)
      output.headOption foreach {_ should be ("WeAllHaveTheSameName")}
    }
  }

  it must "refuse to change name" taggedAs(JVMCompilation) in {
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
method: map
action:
  - set: {name: {string: SomethingElse}}
  - null
""").head }

    intercept [PFASemanticException] { PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
method: emit
action:
  - set: {name: {string: SomethingElse}}
""").head }

    intercept [PFASemanticException] { PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
method: fold
zero: null
action:
  - set: {name: {string: SomethingElse}}
  - null
merge: null
""").head }

    intercept [PFASemanticException] { PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
action: null
begin:
  - set: {name: {string: SomethingElse}}
""").head }

    intercept[PFASemanticException] { PFAEngine.fromYaml("""
name: ThisIsMyName
input: "null"
output: "null"
action: null
end:
  - set: {name: {string: SomethingElse}}
""").head }
  }

  it must "access version" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
version: 123
input: "null"
output: int
method: map
action: {+: [version, 1000]}
""").head.action(null) should be (1123)

    val emitEngine = PFAEngine.fromYaml("""
version: 123
input: "null"
output: int
method: emit
action: {emit: {+: [version, 1000]}}
""").head
    emitEngine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef => x should be (1123)}
    emitEngine.action(null)

    PFAEngine.fromYaml("""
version: 123
input: "null"
output: int
method: fold
zero: 1000
action: {+: [version, tally]}
merge: {+: [tallyOne, tallyTwo]}
""").head.action(null) should be (1123)

    intercept[PFASemanticException] { PFAEngine.fromYaml("""
input: "null"
output: int
method: map
action: {+: [version, 1000]}
""").head }

    val beginEngine = PFAEngine.fromYaml("""
version: 123
input: "null"
output: "null"
action: null
begin: {log: {+: [version, 1000]}}
""").head
    beginEngine.log = {(x: String, ns: Option[String]) => x should be ("1123")}
    beginEngine.begin()

    intercept[PFASemanticException] { PFAEngine.fromYaml("""
input: "null"
output: "null"
action: null
begin: {+: [version, 1000]}
""").head }

    val endEngine = PFAEngine.fromYaml("""
version: 123
input: "null"
output: "null"
action: null
end: {log: {+: [version, 1000]}}
""").head
    endEngine.log = {(x: String, ns: Option[String]) => x should be ("1123")}
    endEngine.end()

    intercept[PFASemanticException] { PFAEngine.fromYaml("""
input: "null"
output: "null"
action: null
end: {+: [version, 1000]}
""").head }
  }

  it must "access metadata" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
metadata: {hello: there}
input: "null"
output: string
method: map
action: metadata.hello
""").head.action(null) should be ("there")

    val emitEngine = PFAEngine.fromYaml("""
metadata: {hello: there}
input: "null"
output: string
method: emit
action: {emit: metadata.hello}
""").head
    emitEngine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef => x should be ("there")}
    emitEngine.action(null)

    PFAEngine.fromYaml("""
metadata: {hello: there}
input: "null"
output: string
method: fold
zero: "!!!"
action: {s.concat: [metadata.hello, tally]}
merge: {s.concat: [tallyOne, tallyTwo]}
""").head.action(null) should be ("there!!!")

    val beginEngine = PFAEngine.fromYaml("""
metadata: {hello: there}
input: "null"
output: "null"
action: null
begin: {log: metadata.hello}
""").head
    beginEngine.log = {(x: String, ns: Option[String]) => x should be (""""there"""")}
    beginEngine.begin()

    val endEngine = PFAEngine.fromYaml("""
metadata: {hello: there}
input: "null"
output: "null"
action: null
end: {log: metadata.hello}
""").head
    endEngine.log = {(x: String, ns: Option[String]) => x should be (""""there"""")}
    endEngine.end()
  }

  it must "count successful and unsuccessful actions in a map-type engine" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: boolean
output: {type: array, items: long}
method: map
action:
  if: input
  then:
    new: [actionsStarted, actionsFinished]
    type: {type: array, items: long}
  else:
    - error: "yowzers!"
    - {new: [], type: {type: array, items: long}}
""").head

    try {
      engine.action(java.lang.Boolean.valueOf(true)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(1L, 0L))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(true)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(2L, 1L))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(false))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(false))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(true)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(5L, 2L))
    }
    catch {case err: Exception =>}
  }

  it must "count successful and unsuccessful actions in an emit-type engine" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: boolean
output: {type: array, items: long}
method: emit
action:
  if: input
  then:
    emit:
      new: [actionsStarted, actionsFinished]
      type: {type: array, items: long}
  else:
    - error: "yowzers!"
""").head

    engine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef => x.asInstanceOf[PFAArray[Long]].toVector should be (Vector(1L, 0L))}
    try {
      engine.action(java.lang.Boolean.valueOf(true))
    }
    catch {case err: Exception =>}

    engine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef => x.asInstanceOf[PFAArray[Long]].toVector should be (Vector(2L, 1L))}
    try {
      engine.action(java.lang.Boolean.valueOf(true))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(false))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(false))
    }
    catch {case err: Exception =>}

    engine.asInstanceOf[PFAEmitEngine[AnyRef, AnyRef]].emit = {x: AnyRef => x.asInstanceOf[PFAArray[Long]].toVector should be (Vector(5L, 2L))}
    try {
      engine.action(java.lang.Boolean.valueOf(true))
    }
    catch {case err: Exception =>}
  }

  it must "count successful and unsuccessful actions in a fold-type engine" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: boolean
output: {type: array, items: long}
method: fold
zero: []
action:
  if: input
  then:
    new: [actionsStarted, actionsFinished]
    type: {type: array, items: long}
  else:
    - error: "yowzers!"
    - {new: [], type: {type: array, items: long}}
merge: {a.concat: [tallyOne, tallyTwo]}
""").head

    try {
      engine.action(java.lang.Boolean.valueOf(true)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(1L, 0L))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(true)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(2L, 1L))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(false))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(false))
    }
    catch {case err: Exception =>}

    try {
      engine.action(java.lang.Boolean.valueOf(true)).asInstanceOf[PFAArray[Long]].toVector should be (Vector(5L, 2L))
    }
    catch {case err: Exception =>}
  }

  it must "correctly use merge" taggedAs(JVMCompilation) in {
    val Seq(engineOne, engineTwo) = PFAEngine.fromYaml("""
input: int
output: string
method: fold
zero: "-"
action: {s.concat: [tally, {s.int: input}]}
merge: {s.concat: [tallyOne, tallyTwo]}
""", multiplicity = 2) map {_.asInstanceOf[PFAFoldEngine[AnyRef, AnyRef]]}

    engineOne.action(java.lang.Integer.valueOf(1)) should be ("-1")
    engineOne.action(java.lang.Integer.valueOf(2)) should be ("-12")
    engineOne.action(java.lang.Integer.valueOf(3)) should be ("-123")
    engineOne.action(java.lang.Integer.valueOf(4)) should be ("-1234")

    engineTwo.action(java.lang.Integer.valueOf(9)) should be ("-9")
    engineTwo.action(java.lang.Integer.valueOf(8)) should be ("-98")
    engineTwo.action(java.lang.Integer.valueOf(7)) should be ("-987")
    engineTwo.action(java.lang.Integer.valueOf(6)) should be ("-9876")

    engineOne.merge(engineOne.tally, engineTwo.tally) should be ("-1234-9876")
    engineOne.action(java.lang.Integer.valueOf(5)) should be ("-1234-98765")

    engineTwo.action(java.lang.Integer.valueOf(5)) should be ("-98765")
  }

  "JVM expression compilation" must "do simple literals" taggedAs(JVMCompilation) in {
    compileExpression("""null""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])
    compileExpression("""true""", """"boolean"""").apply should be (java.lang.Boolean.valueOf(true))
    compileExpression("""false""", """"boolean"""").apply should be (java.lang.Boolean.valueOf(false))

    compileExpression("""1""", """"int"""").apply should be (1)
    compileExpression("""{"long": 1}""", """"long"""").apply should be (1L)
    compileExpression("""{"float": 2.5}""", """"float"""").apply should be (2.5F)
    compileExpression("""2.5""", """"double"""").apply should be (2.5)

    intercept[PFASyntaxException] { compileExpression("""{"int": "string"}""", """"int"""") }
    intercept [PFASyntaxException]{ compileExpression("""{"float": "string"}""", """"float"""") }

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

    intercept  [PFASemanticException]{ PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: {long: 1}, two: 2.2, three: ["THREE"]}, type: SimpleRecord}
""").head }

    intercept [PFASemanticException] { PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: 1, two: 2.2, three: ["THREE"], four: 444}, type: SimpleRecord}
""").head }

    intercept [PFASemanticException] { PFAEngine.fromYaml("""
input: double
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {new: {one: 1, two: 2.2}, type: SimpleRecord}
""").head }
  }

  it must "promote types in a new record" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: long}, {name: three, type: float}, {name: four, type: double}]}
action:
  - {new: {one: 1, two: 2, three: 3, four: 4}, type: SimpleRecord}
""").head
    val out = engine.action(null).asInstanceOf[PFARecord]
    out.get("one").getClass.getName should be ("java.lang.Integer")
    out.get("two").getClass.getName should be ("java.lang.Long")
    out.get("three").getClass.getName should be ("java.lang.Float")
    out.get("four").getClass.getName should be ("java.lang.Double")

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: {type: record, name: SimpleRecord, fields: [{name: one, type: [double, "null"]}, {name: two, type: [double, "null"]}]}
action:
  - {new: {one: 1, two: null}, type: SimpleRecord}
""").head
    val out2 = engine2.action(null).asInstanceOf[PFARecord]
    out2.get("one").getClass.getName should be ("java.lang.Double")
    out2.get("one").asInstanceOf[java.lang.Double].doubleValue should be (1.0)
    out2.get("two") should be (null)
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

    intercept  [PFASemanticException]{ PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: {long: 1}, two: 2.2, three: ["THREE"]}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head }

    intercept  [PFASemanticException]{ PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: 1, two: 2.2, three: ["THREE"], four: 444}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head }

    intercept  [PFASemanticException]{ PFAEngine.fromYaml("""
input: double
output: SimpleRecord
action:
  - {new: {one: 1, two: 2.2}, type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}}
""").head }
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

    intercept  [PFASemanticException]{ compileExpression("""
do:
  - x
  - let: {x: [hello]}
""", """"null"""") }

    compileExpression("""
do:
  - let: {x: [hello], y: 12}
  - y
""", """"int"""").apply should be (12)

    intercept [PFASemanticException] { compileExpression("""
do:
  - let: {x: [hello], y: x}
  - y
""", """"string"""") }

    intercept  [PFASemanticException]{ compileExpression("""
do:
  - let: {x: {let: {y: [stuff]}}}
  - x
""", """"null"""") }

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

    intercept  [PFASemanticException]{ compileExpression("""
do:
  - set: {x: [there]}
  - let: {x: [hello]}
  - x
""", """"string"""") }

    intercept  [PFASemanticException]{ compileExpression("""
do:
  - let: {x: [hello]}
  - set: {x: 12}
  - x
""", """"int"""").apply }
  }

  it must "call functions" taggedAs(JVMCompilation) in {
    compileExpression("""+: [2, 2]""", """"int"""").apply should be (4)

    intercept  [PFASemanticException]{ compileExpression("""+: [2, [hello]]""", """"int"""") }

    compileExpression("""
"+":
  - "+": [2, 2]
  - 2
""", """"int"""").apply should be (6)

    intercept  [PFASemanticException]{ compileExpression("""
"+":
  - {let: {x: 5}}
  - 2
""", """"int"""") }

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

    intercept [PFASemanticException] { compileExpression("""
do:
  - {let: {x: 5}}
  - "+":
      - {do: [{let: {x: 5}}, x]}
      - 2
""", """"int"""") }

    intercept  [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 5}}
  - "+":
      - {do: [{set: {x: 10}}, x]}
      - 2
""", """"int"""") }
  }

  it must "do if expressions" taggedAs(JVMCompilation) in {
    compileExpression("""{if: true, then: [3]}""", """"null"""").apply should be (null.asInstanceOf[java.lang.Void])

    intercept  [PFASemanticException]{ compileExpression("""{if: [hello], then: [3]}""", """"string"""") }

    compileExpression("""
do:
  - {let: {x: 3}}
  - if: true
    then:
      - {set: {x: 99}}
  - x
""", """"int"""").apply should be (99)

    intercept [PFASemanticException] { compileExpression("""
do:
  - {let: {x: 3}}
  - if: true
    then:
      - {let: {x: 99}}
  - x
""", """"int"""") }

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

    intercept [PFASemanticException] { compileExpression("""
if: true
then:
  - {let: {x: 999}}
  - x
else:
  - x
""", """"int"""") }

    intercept  [PFASemanticException]{ compileExpression("""
if: true
then:
  - x
else:
  - {let: {x: 999}}
  - x
""", """"int"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
cond:
  - {if: false, then: [{let: {x: 5}}, 1]}
  - {if: false, then: [{let: {x: 5}}, 2]}
  - {if: false, then: [{let: {x: 5}}, 3]}
else: [{set: {x: 5}}, 4]
""", """"int"""") }

    intercept [PFASemanticException]{ compileExpression("""
cond:
  - {if: false, then: [{let: {x: 5}}, 1]}
  - {if: false, then: [{set: {x: 5}}, 2]}
  - {if: false, then: [{set: {x: 5}}, 3]}
""", """"null"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 3}}
  - cond:
      - {if: {do: [{let: {x: 5}}, true]}, then: [1]}
      - {if: {do: [{let: {x: 5}}, false]}, then: [2]}
      - {if: {do: [{let: {x: 5}}, false]}, then: [3]}
    else: [{let: {x: 5}}, 4]
""", """"int"""") }

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 3}}
  - cond:
      - {if: {do: [{set: {x: 1}}, true]}, then: [1]}
      - {if: {do: [{set: {x: 2}}, false]}, then: [2]}
      - {if: {do: [{set: {x: 3}}, false]}, then: [3]}
    else: [4]
  - x
""", """"int"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 0}}
  - while: {+: [2, 2]}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""") }

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 0}}
  - while: {let: {y: 12}}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""") }

    compileExpression("""
do:
  - {let: {x: 0}}
  - while: {do: [{+: [2, 2]}, {"!=": [x, 5]}]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""").apply should be (5)

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 0}}
  - {let: {y: 0}}
  - while: {do: [{set: {y: 5}}, {"!=": [x, y]}]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
""", """"int"""") }

    compileExpression("""
do:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {set: {x: {+: [x, 1]}}}
  - x
""", """"int"""").apply should be (5)

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 0}}
  - while: {"!=": [y, 5]}
    do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
  - x
""", """"int"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: 0}}
  - do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
    until: {==: [y, 5]}
  - y
""", """"int"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - for: {x: 0}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
  - x
""", """"int"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
for: {x: {let: {y: 0}}}
while: {"!=": [x, {+: [2, 3]}]}
step: {x: {+: [x, {-: [3, 2]}]}}
do:
  - x
""", """"null"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
for: {x: 0}
while: {"!=": [x, 5]}
step: {x: {+: [y, 1]}}
do:
  - {let: {y: x}}
  - y
""", """"int"""") }

    intercept [PFASemanticException]{ compileExpression("""
for: {x: 0}
while: {"!=": [y, 5]}
step: {x: {+: [x, 1]}}
do:
  - {let: {y: x}}
  - y
""", """"int"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
      - y
""", """"string"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {y: x}}
    seq: false
  - y
""", """"string"""") }

    compileExpression("""
do:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {y: x}}
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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
      - {as: "string", named: y, do: [{log: [x, y]}]}
      - {as: "bytes", named: y, do: [{log: [x, y]}]}
""", """"null"""") }

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
""", """"null"""") }

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

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {fred: {upcast: 3, as: "double"}}}
  - {set: {fred: [hello]}}
  - fred
""", """"double"""").apply }

    intercept [PFASemanticException]{ compileExpression("""
do:
  - {let: {fred: {upcast: 3, as: "double"}}}
  - {set: {fred: [hello]}}
  - fred
""", """"string"""").apply }

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

  "Pack binary" must "pack signed big-endian" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: bytes
action:
  pack: [{pad: null}, {boolean: true}, {boolean: false}, {byte: 12}, {short: 12}, {int: 12}, {long: 12}, {float: 12}, {double: 12}]
""").head
    engine.action(null).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 0, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 12, 65, 64, 0, 0, 64, 40, 0, 0, 0, 0, 0, 0))
  }

  it must "pack unsigned big-endian" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 12}, {"unsigned short": 12}, {"unsigned int": 12}, {"unsigned long": 12}, {"float": 12}, {"double": 12}]
""").head
    engine.action(null).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 0, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 12, 65, 64, 0, 0, 64, 40, 0, 0, 0, 0, 0, 0))

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 255}, {"unsigned short": 65535}, {"unsigned int": 4294967295}, {"unsigned long": {double: 1000000000000000000}}, {"float": 12}, {"double": 12}]
""").head
    engine2.action(null).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 0, -1, -1, -1, -1, -1, -1, -1, 13, -32, -74, -77, -89, 100, 0, 0, 65, 64, 0, 0, 64, 40, 0, 0, 0, 0, 0, 0))
  }

  it must "pack signed little-endian" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: bytes
action:
  pack: [{pad: null}, {boolean: true}, {boolean: false}, {byte: 12}, {"little short": 12}, {"little int": 12}, {"little long": 12}, {"little float": 12}, {"little double": 12}]
""").head
    engine.action(null).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 0, 12, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 65, 0, 0, 0, 0, 0, 0, 40, 64))
  }

  it must "pack unsigned little-endian" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 12}, {"little unsigned short": 12}, {"little unsigned int": 12}, {"little unsigned long": 12}, {"little float": 12}, {"little double": 12}]
""").head
    engine.action(null).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 0, 12, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 65, 0, 0, 0, 0, 0, 0, 40, 64))

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 255}, {"little unsigned short": 65535}, {"little unsigned int": 4294967295}, {"little unsigned long": {double: 1000000000000000000}}, {"little float": 12}, {"little double": 12}]
""").head
    engine2.action(null).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 0, -1, -1, -1, -1, -1, -1, -1, 0, 0, 100, -89, -77, -74, -32, 13, 0, 0, 64, 65, 0, 0, 0, 0, 0, 0, 40, 64))
  }

  it must "pack byte arrays as raw" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{raw: tally}, {raw: input}]
merge:
  pack: [{raw: tallyOne}, {raw: tallyTwo}]
""").head
    engine.action(Array[Byte](1, 2, 3)).asInstanceOf[Array[Byte]].toList should be (List[Byte](1, 2, 3))
    engine.action(Array[Byte](4, 5, 6)).asInstanceOf[Array[Byte]].toList should be (List[Byte](1, 2, 3, 4, 5, 6))
    engine.action(Array[Byte](7, 8, 9)).asInstanceOf[Array[Byte]].toList should be (List[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9))
  }

  it must "pack byte arrays as raw3" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{raw: tally}, {raw3: input}]
merge:
  pack: [{raw: tallyOne}, {raw: tallyTwo}]
""").head
    engine.action(Array[Byte](1, 2, 3)).asInstanceOf[Array[Byte]].toList should be (List[Byte](1, 2, 3))
    engine.action(Array[Byte](4, 5, 6)).asInstanceOf[Array[Byte]].toList should be (List[Byte](1, 2, 3, 4, 5, 6))
    engine.action(Array[Byte](7, 8, 9)).asInstanceOf[Array[Byte]].toList should be (List[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9))
    intercept [PFARuntimeException]{ engine.action(Array[Byte](0, 0)) }
  }

  it must "pack byte arrays as nullterminated" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{"null terminated": tally}, {"null terminated": input}]
merge:
  pack: [{"null terminated": tallyOne}, {"null terminated": tallyTwo}]
""").head
    engine.action(Array[Byte](1, 2, 3)).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 2, 3, 0))
    engine.action(Array[Byte](4, 5, 6)).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 2, 3, 0, 0, 4, 5, 6, 0))
    engine.action(Array[Byte](7, 8, 9)).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 1, 2, 3, 0, 0, 4, 5, 6, 0, 0, 7, 8, 9, 0))
  }

  it must "pack byte arrays as lengthprefixed" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{"length prefixed": tally}, {"length prefixed": input}]
merge:
  pack: [{"length prefixed": tallyOne}, {"length prefixed": tallyTwo}]
""").head
    engine.action(Array[Byte](1, 2, 3)).asInstanceOf[Array[Byte]].toList should be (List[Byte](0, 3, 1, 2, 3))
    engine.action(Array[Byte](4, 5, 6)).asInstanceOf[Array[Byte]].toList should be (List[Byte](5, 0, 3, 1, 2, 3, 3, 4, 5, 6))
    engine.action(Array[Byte](7, 8, 9)).asInstanceOf[Array[Byte]].toList should be (List[Byte](10, 5, 0, 3, 1, 2, 3, 3, 4, 5, 6, 3, 7, 8, 9))
  }

  "Unpack binary" must "unpack null" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: "null"
action:
  unpack: input
  format: [{x: pad}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](10)) should be (null)
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2)) }
  }

  it must "unpack boolean" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  unpack: input
  format: [{x: boolean}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](0)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(Array[Byte](1)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(Array[Byte](8)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2)) }
  }

  it must "unpack byte" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: byte}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](10)).asInstanceOf[java.lang.Integer].intValue should be (10)
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2)) }

    val engineUnsigned = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: "unsigned byte"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsigned.action(Array[Byte](127)).asInstanceOf[java.lang.Integer].intValue should be (127)
    engineUnsigned.action(Array[Byte](-128)).asInstanceOf[java.lang.Integer].intValue should be (128)
    engineUnsigned.action(Array[Byte](-1)).asInstanceOf[java.lang.Integer].intValue should be (255)
  }

  it must "unpack short" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: short}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](0, 4)).asInstanceOf[java.lang.Integer].intValue should be (4)
    intercept [PFAUserException]{ engine.action(Array[Byte](1)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3)) }

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: "little short"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](4, 0)).asInstanceOf[java.lang.Integer].intValue should be (4)

    val engineUnsigned = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: "unsigned short"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsigned.action(Array[Byte](127, -1)).asInstanceOf[java.lang.Integer].intValue should be (32767)
    engineUnsigned.action(Array[Byte](-128, 0)).asInstanceOf[java.lang.Integer].intValue should be (32768)
    engineUnsigned.action(Array[Byte](-1, -1)).asInstanceOf[java.lang.Integer].intValue should be (65535)

    val engineUnsignedLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: "little unsigned short"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsignedLittleEndian.action(Array[Byte](-1, 127)).asInstanceOf[java.lang.Integer].intValue should be (32767)
    engineUnsignedLittleEndian.action(Array[Byte](0, -128)).asInstanceOf[java.lang.Integer].intValue should be (32768)
    engineUnsignedLittleEndian.action(Array[Byte](-1, -1)).asInstanceOf[java.lang.Integer].intValue should be (65535)
  }

  it must "unpack int" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: int}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](0, 0, 0, 4)).asInstanceOf[java.lang.Integer].intValue should be (4)
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5)) }

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  unpack: input
  format: [{x: "little int"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](4, 0, 0, 0)).asInstanceOf[java.lang.Integer].intValue should be (4)

    val engineUnsigned = PFAEngine.fromYaml("""
input: bytes
output: long
action:
  unpack: input
  format: [{x: "unsigned int"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsigned.action(Array[Byte](127, -1, -1, -1)).asInstanceOf[java.lang.Long].longValue should be (2147483647L)
    engineUnsigned.action(Array[Byte](-128, 0, 0, 0)).asInstanceOf[java.lang.Long].longValue should be (2147483648L)
    engineUnsigned.action(Array[Byte](-1, -1, -1, -1)).asInstanceOf[java.lang.Long].longValue should be (4294967295L)

    val engineUnsignedLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: long
action:
  unpack: input
  format: [{x: "little unsigned int"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsignedLittleEndian.action(Array[Byte](-1, -1, -1, 127)).asInstanceOf[java.lang.Long].longValue should be (2147483647L)
    engineUnsignedLittleEndian.action(Array[Byte](0, 0, 0, -128)).asInstanceOf[java.lang.Long].longValue should be (2147483648L)
    engineUnsignedLittleEndian.action(Array[Byte](-1, -1, -1, -1)).asInstanceOf[java.lang.Long].longValue should be (4294967295L)
  }

  it must "unpack long" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: long
action:
  unpack: input
  format: [{x: long}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](0, 0, 0, 0, 0, 0, 0, 4)).asInstanceOf[java.lang.Long].longValue should be (4)
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5, 6, 7)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)) } 

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: long
action:
  unpack: input
  format: [{x: "little long"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](4, 0, 0, 0, 0, 0, 0, 0)).asInstanceOf[java.lang.Long].longValue should be (4)

    val engineUnsigned = PFAEngine.fromYaml("""
input: bytes
output: double
action:
  unpack: input
  format: [{x: "unsigned long"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsigned.action(Array[Byte](0, 0, 0, 0, 0, 0, 0, 4)).asInstanceOf[java.lang.Double].doubleValue should be (4.0 +- 0.0001)

    val engineUnsignedLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: double
action:
  unpack: input
  format: [{x: "little unsigned long"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineUnsignedLittleEndian.action(Array[Byte](4, 0, 0, 0, 0, 0, 0, 0)).asInstanceOf[java.lang.Double].doubleValue should be (4.0 +- 0.0001)
  }

  it must "unpack float" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: float
action:
  unpack: input
  format: [{x: float}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](64, 72, -11, -61)).asInstanceOf[java.lang.Float].doubleValue should be (3.14 +- 0.000001)
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3)) } 
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5)) } 

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: float
action:
  unpack: input
  format: [{x: "little float"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](-61, -11, 72, 64)).asInstanceOf[java.lang.Float].doubleValue should be (3.14 +- 0.000001)
  }

  it must "unpack double" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: double
action:
  unpack: input
  format: [{x: double}]
  then: x
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](64, 9, 30, -72, 81, -21, -123, 31)).asInstanceOf[java.lang.Double].doubleValue should be (3.14 +- 0.000001)
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5, 6, 7)) } 
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)) } 

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: double
action:
  unpack: input
  format: [{x: "little double"}]
  then: x
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](31, -123, -21, 81, -72, 30, 9, 64)).asInstanceOf[java.lang.Double].doubleValue should be (3.14 +- 0.000001)
  }

  it must "unpack raw" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: raw5}]
  then: {bytes.decodeAscii: x}
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)) should be ("hello")
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4)) } 
    intercept [PFAUserException]{ engine.action(Array[Byte](1, 2, 3, 4, 5, 6)) } 
  }

  it must "unpack tonull" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: nullterminated}]
  then: {bytes.decodeAscii: x}
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111, 0)) should be ("hello")
    intercept [PFAUserException]{ engine.action(Array[Byte](104, 101, 108, 108, 111)) } 
    intercept [PFAUserException]{ engine.action(Array[Byte]()) } 
  }

  it must "unpack lengthprefixed" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: lengthprefixed}]
  then: {bytes.decodeAscii: x}
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](5, 104, 101, 108, 108, 111)) should be ("hello")
    intercept [PFAUserException]{ engine.action(Array[Byte](5, 104, 101, 108, 108, 111, 99)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](5, 104, 101, 108, 108)) }
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }
  }

  it must "unpack multiple with raw" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: pad}, {y: raw5}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](99, 104, 101, 108, 108, 111, 0, 0, 0, 4)) should be ("hello")
    intercept [PFAUserException]{ engine.action(Array[Byte](99, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](99, 104, 101, 108, 108, 111, 0, 0, 0)) }
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: "little int"}, {y: raw5}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 4)) should be ("hello")
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1)) }
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0)) }
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte]()) }
  }

  it must "unpack multiple with tonull" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: pad}, {y: nullterminated}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](99, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4)) should be ("hello")
    intercept [PFAUserException]{ engine.action(Array[Byte](99, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4, 1)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](99, 104, 101, 108, 108, 111, 0, 0, 0, 0)) }
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: "little int"}, {y: nullterminated}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4)) should be ("hello")
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4, 1)) }
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 0)) }
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte]()) }
  }

  it must "unpack multiple with lengthprefixed" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: pad}, {y: prefixed}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
""").head
    engine.action(Array[Byte](99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4)) should be ("hello")
    intercept [PFAUserException]{ engine.action(Array[Byte](99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1)) }
    intercept [PFAUserException]{ engine.action(Array[Byte](99, 5, 104, 101, 108, 108, 111, 0, 0, 0)) }
    intercept [PFAUserException]{ engine.action(Array[Byte]()) }

    val engineLittleEndian = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  unpack: input
  format: [{x: "little int"}, {y: prefixed}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
""").head
    engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4)) should be ("hello")
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1)) }
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte](0, 0, 0, 4, 5, 104, 101, 108, 108, 111, 0, 0, 0)) }
    intercept [PFAUserException]{ engineLittleEndian.action(Array[Byte]()) }
  }

  it must "not break other variables" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  - let: {successful: false}
  - unpack: input
    format: [{x: pad}, {y: prefixed}, {z: int}]
    then: {bytes.decodeAscii: y}
    else: {error: "Ack!"}
  - successful
""").head
    engine.action(Array[Byte](99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    val engine2 = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  - let: {tmpInteger: 12}
  - unpack: input
    format: [{x: pad}, {y: prefixed}, {z: int}]
    then: {bytes.decodeAscii: y}
    else: {error: "Ack!"}
  - tmpInteger
""").head
    engine2.action(Array[Byte](99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4)).asInstanceOf[java.lang.Integer].intValue should be (12)
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
    intercept [PFAUserException]{ compileExpression("""{error: "This is bad"}""", """"null"""").apply } 

    try {
      compileExpression("""{error: "This is bad", code: -12}""", """"null"""").apply
      true should be (false)
    }
    catch {
      case err: PFAUserException => err.code should be (Some(-12))
    }

    intercept [PFAUserException]{ compileExpression("""
if: true
then:
  - {error: "This is bad"}
""", """"null"""").apply } 

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

  it must "properly deal with the exception type" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  if: true
  then: {string: "hello"}
  else: {error: "This is bad"}
""").head.action(null) should be ("hello")

    PFAEngine.fromYaml("""
input: "null"
output: ["null", string]
action:
  if: true
  then: {string: "hello"}
  else: {do: {error: "This is bad"}}
""").head.action(null) should be ("hello")

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  if: true
  then: {error: "err 1"}
  else: {error: "err 2"}
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: string
action:
  if: false
  then: {string: "hello"}
  else: {error: "This is bad"}
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  if: true
  then: {error: "This is bad"}
""").head.action(null) }

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  cond:
    - if: false
      then: {error: "err 1"}
    - if: false
      then: {error: "err 2"}
    - if: true
      then: {string: hey}
    - if: false
      then: {error: "err 3"}
  else:
    {error: "err 4"}
""").head.action(null) should be ("hey")

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  cond:
    - if: false
      then: {error: "err 1"}
    - if: false
      then: {error: "err 2"}
    - if: false
      then: {error: "err 3"}
  else:
    {error: "err 4"}
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - let:
      x: {error: "should not be able to assign the bottom type"}
  - 123
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - let:
      x: null
  - set:
      x: {error: "even if it's a null object"}
  - 123
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
cells:
  someobj:
    type:
      type: record
      name: Whatever
      fields:
        - {name: x, type: "null"}
    init:
      {x: null}
action:
  - attr: {cell: someobj}
    path: [{string: x}]
    to: {error: "even if it's a null object"}
  - 123
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
cells:
  someobj:
    type:
      type: record
      name: Whatever
      fields:
        - {name: x, type: "null"}
    init:
      {x: null}
action:
  - cell: someobj
    path: [{string: x}]
    to: {error: "even if it's a null object"}
  - 123
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
pools:
  someobj:
    type:
      type: record
      name: Whatever
      fields:
        - {name: x, type: "null"}
    init:
      whatev:
        {x: null}
action:
  - pool: someobj
    path: [{string: whatev}, {string: x}]
    to: {error: "even if it's a null object"}
    init:
      type: Whatever
      value: {x: null}
  - 123
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action: {u.somefcn: []}
fcns:
  somefcn:
    params: []
    ret: "null"
    do: {error: "but this is okay"}
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: int
action: {u.somefcn: []}
fcns:
  somefcn:
    params: []
    ret: int
    do: {error: "this, too"}
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
action:
  u.callme: {error: "not in argument lists, no!"}
fcns:
  callme:
    params: [{x: "null"}]
    ret: int
    do: 12
""").head }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: int
action:
  u.callme: {do: {error: "but do sanitizes it"}}
fcns:
  callme:
    params: [{x: "null"}]
    ret: int
    do: 12
""").head.action(null) }

    PFAEngine.fromYaml("""
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {int: 12}
output: int
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
""").head.action(null) should be (12)

    PFAEngine.fromYaml("""
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {int: 12}
output: "null"
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
  partial: true
""").head.action(null) should be (null)

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {string: woohoo}
output: int
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {string: woohoo}
output: "null"
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
  partial: true
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  upcast: {error: "wtf?"}
  as: "null"
""").head }

    PFAEngine.fromYaml("""
input: "null"
output: int
cells:
  someobj:
    type: [int, "null"]
    init: {int: 12}
action:
  ifnotnull: {x: {cell: someobj}}
  then: x
  else: {error: "I really wanted an int"}
""").head.action(null) should be (12)

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: int
cells:
  someobj:
    type: [int, "null"]
    init: null
action:
  ifnotnull: {x: {cell: someobj}}
  then: x
  else: {error: "I really wanted an int"}
""").head.action(null) }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: int
action:
  ifnotnull: {x: {error: "wtf?"}}
  then: x
""").head }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: {error: "wtf?"}
  - null
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: [12, {error: "wtf?"}]
  - null
""").head.action(null) }

    intercept [PFAUserException]{ PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: [{error: "wtf?"}, 12]
  - null
""").head.action(null) }
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
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: double
output: string
action:
  - {+: [input, input]}
""").head }
  }

  it must "identify type errors 2" taggedAs(JVMCompilation) in {
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {+: [input, input]}
""").head }
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
action: {+: [input, tally]}
merge: {+: [tallyOne, tallyTwo]}
""").head

    engine.action(java.lang.Double.valueOf(5)) should be (java.lang.Double.valueOf(5))
    engine.action(java.lang.Double.valueOf(3)) should be (java.lang.Double.valueOf(8))
    engine.action(java.lang.Double.valueOf(2)) should be (java.lang.Double.valueOf(10))
    engine.action(java.lang.Double.valueOf(20)) should be (java.lang.Double.valueOf(30))

    engine.asInstanceOf[PFAFoldEngine[_, java.lang.Double]].tally = java.lang.Double.valueOf(1)
    engine.action(java.lang.Double.valueOf(5)) should be (java.lang.Double.valueOf(6))
  }

  it must "refuse tally in user functions" taggedAs(JVMCompilation) in {
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
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
merge: {+: [tallyOne, tallyTwo]}
""").head }
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
merge: {+: [tallyOne, tallyTwo]}
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
    engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (2)
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
    intercept [PFARuntimeException]{
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], 3, x]}
""").head
      engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}"""))
    }

    intercept [PFARuntimeException]{
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [TWO]}}
  - {attr: input, path: [[one], 2, x]}
""").head
      engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}"""))
    }
  }

  it must "not accept bad index types" taggedAs(JVMCompilation) in {
    intercept [PFASemanticException]{
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[ONE], 2, x]}
""").head
      engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (2)
    }

    intercept [PFASemanticException]{
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], x, x]}
""").head
      engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (2)
    }

   intercept [PFASemanticException]{
      val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {attr: input, path: [[one], 2, 2]}
""").head
      engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (2)
   }
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
    engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (999)
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
    engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (3)
  }

  it must "change a deep object with fcnref" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {fcn: u.inc}}}}
  - {attr: something, path: [[one], 2, x]}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head
    engine.action(engine.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (3)

    val engine2 = PFAEngine.fromYaml("""
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {fcn: u.inc, fill: {uno: 1}}}}}
  - {attr: something, path: [[one], 2, x]}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head
    engine2.action(engine2.jsonInput("""{"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}""")) should be (3)
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
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - cell: y
cells:
  x: {type: int, init: 12}
""").head }
  }

  it must "not accept bad indexes" taggedAs(JVMCompilation) in {
    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 3, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("two") }

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head.action("TWO") }
  }

  it must "not accept bad index types" taggedAs(JVMCompilation) in {
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[ONE], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], input, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, path: [[one], 2, 2]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
""").head }
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
  - {cell: x, to: {fcn: u.inc}}
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
  - {cell: x, to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x}
cells:
  x: {type: int, init: 12}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc}}
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
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
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
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
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
  - {cell: x, to: {fcn: u.inc}}
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
  - {cell: x, to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc}}
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
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
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
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
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
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: y, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
""").head }
  }

  it must "not accept bad indexes" taggedAs(JVMCompilation) in {
    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 3, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") }

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("TWO") }
  }

  it must "not accept bad index types" taggedAs(JVMCompilation) in {
    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [ONE], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], input, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, 2]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head }
  }

  "pool-set" must "change private pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}}
""").head.action("somethingelse") should be (999)
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
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
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
  - {pool: x, path: [input], to: {fcn: u.inc, fill: {uno: 1}}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
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
  - {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: {type: map, values: int}
action:
  - attr: {pool: x, path: [[somethingelse], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102, glue: 103}]}, type: SimpleRecord}}
    path: [[one], 2]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two").asInstanceOf[PFAMap[java.lang.Integer]].toMap should be (Map("two" -> java.lang.Integer.valueOf(999), "glue" -> java.lang.Integer.valueOf(103)))
  }

  it must "change deep private pools with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
""").head.action("two") should be (103)
  }

  it must "change deep private pools with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
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
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (103)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc, fill: {uno: 1}}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("two") should be (103)
  }

  "pool-set" must "change public pools" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("whatever") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
""").head.action("somethingelse") should be (999)
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
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
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
  - {pool: x, path: [input], to: {fcn: u.inc, fill: {uno: 1}}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("whatever") should be (13)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
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
  - {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (999)

    PFAEngine.fromYaml("""
input: string
output: {type: map, values: int}
action:
  - attr: {pool: x, path: [[somethingelse], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102, glue: 103}]}, type: SimpleRecord}}
    path: [[one], 2]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two").asInstanceOf[PFAMap[java.lang.Integer]].toMap should be (Map("two" -> java.lang.Integer.valueOf(999), "glue" -> java.lang.Integer.valueOf(103)))
  }

  it must "change deep public pools with fcndef" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
""").head.action("two") should be (103)
  }

  it must "change deep public pools with fcnref" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
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
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (3)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
""").head.action("two") should be (103)

    PFAEngine.fromYaml("""
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc, fill: {uno: 1}}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
""").head.action("two") should be (103)
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

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: empty}
  - input
cells:
  notempty: {type: "null", init: null}
""").head }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[notinset]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}}
""").head.action("hey") }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [999]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null]}
""").head.action("hey") }
  }

  "unshared pools" must "handle null cases" taggedAs(JVMCompilation) in {
    intercept [PFASyntaxException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: []}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
""").head.action("hey") }

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
""").head.action("hey")

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[shouldfail]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
""").head.action("hey") }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [notinset]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}}
""").head.action("hey") }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 999]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}}
""").head.action("hey") }
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

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: empty}
  - input
cells:
  notempty: {type: "null", init: null, shared: true}
""").head }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[notinset]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}, shared: true}
""").head.action("hey") }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [999]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null], shared: true}
""").head.action("hey") }
  }

  "shared pools" must "handle null cases" taggedAs(JVMCompilation) in {
    intercept [PFASyntaxException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: []}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
""").head.action("hey") }

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
""").head.action("hey")

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[shouldfail]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
""").head.action("hey") }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [notinset]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}, shared: true}
""").head.action("hey") }

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

    intercept [PFARuntimeException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 999]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}, shared: true}
""").head.action("hey") }
  }

  "pool to and init" must "handle a typical use-case" taggedAs(JVMCompilation) in {
    PFAEngine.fromYaml("""
input: string
output: int
method: map
action:
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
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
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]]}
pools:
  tally: {type: int, init: {}}
fcns:
  inc: {params: [{i: int}], ret: int, do: [{+: [i, 1]}]}
""").head.action("hey") should be (2)
  }

  "pool-del" must "remove private pool items" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
pools:
  x: {type: int, init: {whatever: 12}}
action:
  - {pool: x, del: input}
  - {pool: x, path: [{string: "whatever"}]}
""").head

    engine.action("somesuch") should be (12)
    intercept [PFARuntimeException]{ engine.action("whatever") }
  }

  it must "remove public pool items" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
action:
  - {pool: x, del: input}
  - {pool: x, path: [{string: "whatever"}]}
""").head

    engine.action("somesuch") should be (12)
    intercept [PFARuntimeException]{ engine.action("whatever") }
  }

  "execution clock" must "stop int-running processes" taggedAs(JVMCompilation) in {
    intercept [PFATimeoutException]{ PFAEngine.fromYaml("""
input: string
output: "null"
action:
  - for: {x: 0}
    while: {"!=": [x, -5]}
    step: {x: {+: [x, 1]}}
    do: [x]
options:
  timeout: 1000
""").head.action("hey") }
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
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {fcn: u.b}}]}
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

    intercept [PFAInitializationException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {fcn: u.b}}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [{cell: y, to: null}]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""").head }

    intercept [PFAInitializationException]{ PFAEngine.fromYaml("""
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
""").head }

    PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {fcn: u.b}, init: null}]}
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

    intercept [PFAInitializationException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {fcn: u.b}, init: null}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [{pool: y, path: [[whatever]], to: null, init: null}]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""").head }

    intercept [PFAInitializationException]{ PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}, init: null}]}
  c: {params: [{x: "null"}], ret: "null", do: [{pool: y, path: [[whatever]], to: null, init: null}]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""").head }

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

  "call user function" must "work" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: double
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.2]}
  three:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.3]}
""").head
    engine.action(engine.jsonInput(""""one"""")).asInstanceOf[java.lang.Double].doubleValue should be (100.1 +- 0.01)
    engine.action(engine.jsonInput(""""two"""")).asInstanceOf[java.lang.Double].doubleValue should be (100.2 +- 0.01)
    engine.action(engine.jsonInput(""""three"""")).asInstanceOf[java.lang.Double].doubleValue should be (100.3 +- 0.01)

    engine.callGraph("(action)") should be (Set("u.one", "u.two", "u.three"))

    val engine2 = PFAEngine.fromYaml("""
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: [double, string]
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.2]}
  three:
    params: [{x: double}]
    ret: string
    do: {string: hello}
""").head
    engine2.action(engine2.jsonInput(""""one"""")).asInstanceOf[java.lang.Double].doubleValue should be (100.1 +- 0.01)
    engine2.action(engine2.jsonInput(""""two"""")).asInstanceOf[java.lang.Double].doubleValue should be (100.2 +- 0.01)
    engine2.action(engine2.jsonInput(""""three"""")) should be ("hello")

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: double
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.2]}
  three:
    params: [{x: double}]
    ret: string
    do: {string: hello}
""") }

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: double
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: string}]
    ret: double
    do: 999.9
  three:
    params: [{x: double}]
    ret: string
    do: {string: hello}
""") }

  }

  "FcnRefFill" must "work with user functions" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {y: 2}
fcns:
  callme:
    params: [{x: int}, {y: int}]
    ret: int
    do: {"*": [{+: [x, 1000]}, y]}
""").head
    engine.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(2002, 2004, 2006, 2008, 2010))

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {x: 2}
fcns:
  callme:
    params: [{x: int}, {y: int}]
    ret: int
    do: {"*": [{+: [x, 1000]}, y]}
""").head
    engine2.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(1002, 2004, 3006, 4008, 5010))

    val engine3 = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {x: 10, y: 100}
fcns:
  callme:
    params: [{x: int}, {y: int}, {z: int}]
    ret: int
    do: {+: [x, {+: [y, z]}]}
""").head
    engine3.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(111, 112, 113, 114, 115))

    intercept [PFASemanticException]{ PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {x: 10, yy: 100}
fcns:
  callme:
    params: [{x: int}, {y: int}, {z: int}]
    ret: int
    do: {+: [x, {+: [y, z]}]}
""") }

    intercept [PFASyntaxException]{ PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {}
fcns:
  callme:
    params: [{x: int}, {y: int}, {z: int}]
    ret: int
    do: {+: [x, {+: [y, z]}]}
""") }
  }

  it must "work with library functions" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: {type: array, items: string}
action:
  a.map:
    - value: ["abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
      type: {type: array, items: string}
    - fcn: s.substr
      fill: {start: 0, end: input}
""").head
    engine.action(java.lang.Integer.valueOf(1)).asInstanceOf[PFAArray[String]].toVector should be (Vector("a", "A"))
    engine.action(java.lang.Integer.valueOf(3)).asInstanceOf[PFAArray[String]].toVector should be (Vector("abc", "ABC"))
    engine.action(java.lang.Integer.valueOf(5)).asInstanceOf[PFAArray[String]].toVector should be (Vector("abcde", "ABCDE"))
    engine.action(java.lang.Integer.valueOf(10)).asInstanceOf[PFAArray[String]].toVector should be (Vector("abcdefghij", "ABCDEFGHIJ"))
    engine.action(java.lang.Integer.valueOf(15)).asInstanceOf[PFAArray[String]].toVector should be (Vector("abcdefghijklmno", "ABCDEFGHIJKLMNO"))
  }

  "try-catch" must "work without filtering" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: [int, "null"]
action:
  try:
    if: {"==": [input, 3]}
    then: {error: "ouch"}
    else: input
""").head
    engine.action(java.lang.Integer.valueOf(1)) should be (java.lang.Integer.valueOf(1))
    engine.action(java.lang.Integer.valueOf(2)) should be (java.lang.Integer.valueOf(2))
    engine.action(java.lang.Integer.valueOf(3)) should be (null)
    engine.action(java.lang.Integer.valueOf(4)) should be (java.lang.Integer.valueOf(4))
    engine.action(java.lang.Integer.valueOf(5)) should be (java.lang.Integer.valueOf(5))
  }

  it must "work with filtering" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: [int, "null"]
action:
  try:
    cond:
      - if: {"==": [input, 3]}
        then: {error: "ouch"}
      - if: {"==": [input, 4]}
        then: {error: "yowzers"}
    else: input
  filter:
    - ouch
""").head
    engine.action(java.lang.Integer.valueOf(1)) should be (java.lang.Integer.valueOf(1))
    engine.action(java.lang.Integer.valueOf(2)) should be (java.lang.Integer.valueOf(2))
    engine.action(java.lang.Integer.valueOf(3)) should be (null)
    intercept [PFAUserException]{ engine.action(java.lang.Integer.valueOf(4)) }
    engine.action(java.lang.Integer.valueOf(5)) should be (java.lang.Integer.valueOf(5))
  }

  it must "work with filtering 2" taggedAs(JVMCompilation) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: [int, "null"]
action:
  try:
    cond:
      - if: {"==": [input, 3]}
        then: {error: "ouch", code: -99}
      - if: {"==": [input, 4]}
        then: {error: "yowzers", code: -100}
    else: input
  filter:
    - -99
""").head
    engine.action(java.lang.Integer.valueOf(1)) should be (java.lang.Integer.valueOf(1))
    engine.action(java.lang.Integer.valueOf(2)) should be (java.lang.Integer.valueOf(2))
    engine.action(java.lang.Integer.valueOf(3)) should be (null)
    intercept [PFAUserException]{ engine.action(java.lang.Integer.valueOf(4)) }
    engine.action(java.lang.Integer.valueOf(5)) should be (java.lang.Integer.valueOf(5))
  }

  "translator" must "translate PFA data" taggedAs(JVMCompilation) in {
    val engine1 = PFAEngine.fromYaml("""
name: TestMe
input: "null"
output:
  type: array
  items:
    type: record
    name: Whatever
    fields:
      - {name: a, type: int}
      - {name: b, type: double}
      - {name: c, type: string}
action:
  type:
    type: array
    items: Whatever
  value:
    [{a: 1, b: 1.1, c: ONE}, {a: 2, b: 2.2, c: TWO}, {a: 3, b: 3.3, c: THREE}]
""").head

    val engine2 = PFAEngine.fromYaml("""
name: TestMe
input:
  type: array
  items:
    type: record
    name: Whatever
    fields:
      - {name: a, type: int}
      - {name: b, type: double}
      - {name: c, type: string}
output:
  type: array
  items: Whatever
action:
  type:
    type: array
    items: Whatever
  new:
    - type: Whatever
      new: {a: {+: [input.0.a, 100]}, b: {+: [input.0.b, 100]}, c: {s.concat: [input.0.c, ["-100"]]}}
    - type: Whatever
      new: {a: {+: [input.1.a, 100]}, b: {+: [input.1.b, 100]}, c: {s.concat: [input.1.c, ["-100"]]}}
    - type: Whatever
      new: {a: {+: [input.2.a, 100]}, b: {+: [input.2.b, 100]}, c: {s.concat: [input.2.c, ["-100"]]}}
""").head

    val out1 = engine1.action(null)
    out1.toString should be ("""[{"a": 1, "b": 1.1, "c": "ONE"}, {"a": 2, "b": 2.2, "c": "TWO"}, {"a": 3, "b": 3.3, "c": "THREE"}]""")

    intercept [java.lang.ClassCastException]{ engine2.action(out1) }

    val out2 = engine2.action(engine2.fromPFAData(out1))
    out2.toString should be ("""[{"a": 101, "b": 101.1, "c": "ONE-100"}, {"a": 102, "b": 102.2, "c": "TWO-100"}, {"a": 103, "b": 103.3, "c": "THREE-100"}]""")
  }

  def testGenericLoadAndConvert(avroType: AvroType, jsonData: String, yamlEngine: String, debug: Boolean = false): Unit = {
    val objectMapper = new ObjectMapper

    val datum = fromJson(jsonData, avroType)
    if (debug  &&  datum == null)
      println("null")
    else if (debug)
      println(datum, toJson(datum, avroType), datum.getClass.getName)
    objectMapper.readTree(toJson(datum, avroType)) should be (objectMapper.readTree(jsonData))

    val engine = PFAEngine.fromYaml(yamlEngine).head
    val translated = engine.fromPFAData(datum)
    if (debug  &&  datum == null)
      println("null")
    else if (debug)
      println(translated, toJson(translated, avroType), translated.getClass.getName)
    objectMapper.readTree(toJson(translated, avroType)) should be (objectMapper.readTree(jsonData))

    val result = engine.action(translated)
    if (debug  &&  datum == null)
      println("null")
    else if (debug)
      println(result, toJson(result, avroType), result.getClass.getName)
    objectMapper.readTree(toJson(result, avroType)) should be (objectMapper.readTree(jsonData))
  }

  "generic data" must "load correctly" taggedAs(JVMCompilation) in {
    testGenericLoadAndConvert(AvroNull(), """null""", """
input: "null"
output: "null"
action: input
""")

    testGenericLoadAndConvert(AvroBoolean(), """true""", """
input: boolean
output: boolean
action: input
""")

    testGenericLoadAndConvert(AvroInt(), """3""", """
input: int
output: int
action: input
""")

    testGenericLoadAndConvert(AvroLong(), """3""", """
input: long
output: long
action: input
""")

    testGenericLoadAndConvert(AvroFloat(), """3.14""", """
input: float
output: float
action: input
""")

    testGenericLoadAndConvert(AvroDouble(), """3.14""", """
input: double
output: double
action: input
""")

    testGenericLoadAndConvert(AvroBytes(), """"aGVsbG8="""", """
input: bytes
output: bytes
action: input
""")

    testGenericLoadAndConvert(AvroBytes(), """"hello"""", """
input: bytes
output: bytes
cells:
  tmp:
    type: bytes
    init: "hello"
action: {cell: tmp}
""")

    testGenericLoadAndConvert(AvroFixed(5, "MyType"), """"hello"""", """
input:
  type: fixed
  name: MyType
  size: 5
output: MyType
action: input
""")

    testGenericLoadAndConvert(AvroFixed(5, "MyType"), """"hello"""", """
input:
  type: fixed
  name: MyType
  size: 5
output: MyType
cells:
  tmp:
    type: MyType
    init: "hello"
action: {cell: tmp}
""")

    testGenericLoadAndConvert(AvroString(), """"hello"""", """
input: string
output: string
action: input
""")

    testGenericLoadAndConvert(AvroEnum(Seq("one", "two", "three"), "MyType"), """"three"""", """
input:
  type: enum
  name: MyType
  symbols: [one, two, three]
output: MyType
action: input
""")

    testGenericLoadAndConvert(AvroUnion(List(AvroNull(), AvroEnum(Seq("one", "two", "three"), "MyType"))), """{"MyType": "three"}""", """
input:
  - "null"
  - type: enum
    name: MyType
    symbols: [one, two, three]
output:
  - "null"
  - MyType
action: input
""")

    testGenericLoadAndConvert(AvroUnion(List(AvroNull(), AvroEnum(Seq("one", "two", "three"), "MyType"))), "null", """
input:
  - "null"
  - type: enum
    name: MyType
    symbols: [one, two, three]
output:
  - "null"
  - MyType
action: input
""")

    testGenericLoadAndConvert(AvroArray(AvroInt()), """[1,2,3]""", """
input:
  type: array
  items: int
output:
  type: array
  items: int
action: input
""")

    testGenericLoadAndConvert(AvroArray(AvroEnum(Seq("one", "two", "three"), "MyType")), """["one","two","three"]""", """
input:
  type: array
  items:
    type: enum
    name: MyType
    symbols: [one, two, three]
output:
  type: array
  items: MyType
action: input
""")

    testGenericLoadAndConvert(AvroMap(AvroInt()), """{"uno":1,"dos":2,"tres":3}""", """
input:
  type: map
  values: int
output:
  type: map
  values: int
action: input
""")

    testGenericLoadAndConvert(AvroRecord(Seq(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyType"), """{"one":1,"two":2.2,"three":"THREE"}""", """
input:
  type: record
  name: MyType
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
output: MyType
action: input
""")

    testGenericLoadAndConvert(AvroUnion(Seq(AvroInt(), AvroString(), AvroArray(AvroString()))), """{"int":3}""", """
input: [int, string, {type: array, items: string}]
output: [int, string, {type: array, items: string}]
action: input
""")

    testGenericLoadAndConvert(AvroUnion(Seq(AvroInt(), AvroString(), AvroArray(AvroString()))), """{"string":"THREE"}""", """
input: [int, string, {type: array, items: string}]
output: [int, string, {type: array, items: string}]
action: input
""")


    testGenericLoadAndConvert(AvroUnion(Seq(AvroInt(), AvroString(), AvroArray(AvroString()))), """{"array":["one","two","three"]}""", """
input: [int, string, {type: array, items: string}]
output: [int, string, {type: array, items: string}]
action: input
""")
  }

  "PFA data" must "translate to Scala" taggedAs(JVMCompilation) in {
    val pfaInt = PFAEngine.fromYaml("""
input: "null"
output: int
action: 3
""").head.action(null)
    pfaInt.isInstanceOf[java.lang.Integer] should be (true)
    val translateInt = new ScalaDataTranslator[Int](AvroInt(), getClass.getClassLoader)
    translateInt.toScala(pfaInt).isInstanceOf[Int] should be (true)

    val pfaString = PFAEngine.fromYaml("""
input: "null"
output: string
action: {string: "hello"}
""").head.action(null)
    pfaString.isInstanceOf[String] should be (true)
    val translateString = new ScalaDataTranslator[String](AvroString(), getClass.getClassLoader)
    translateString.toScala(pfaString).isInstanceOf[String] should be (true)

    val pfaArrayString = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  type: {type: array, items: string}
  value: ["one", "two", "three"]
""").head.action(null)
    pfaArrayString.isInstanceOf[PFAArray[_]] should be (true)
    val translateArrayString = new ScalaDataTranslator[Seq[String]](AvroArray(AvroString()), getClass.getClassLoader)
    translateArrayString.toScala(pfaArrayString).isInstanceOf[Seq[_]] should be (true)
    translateArrayString.toScala(pfaArrayString) map {_.isInstanceOf[String] should be (true)}

    val pfaArrayInt = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  type: {type: array, items: int}
  value: [1, 2, 3]
""").head.action(null)
    pfaArrayInt.isInstanceOf[PFAArray[_]] should be (true)
    val translateArrayInt = new ScalaDataTranslator[Seq[Int]](AvroArray(AvroInt()), getClass.getClassLoader)
    translateArrayInt.toScala(pfaArrayInt).isInstanceOf[Seq[_]] should be (true)
    translateArrayInt.toScala(pfaArrayInt) map {_.isInstanceOf[Int] should be (true)}

    val pfaMapString = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: string}
action:
  type: {type: map, values: string}
  value: {one: "ONE", two: "TWO", three: "THREE"}
""").head.action(null)
    pfaMapString.isInstanceOf[PFAMap[_]] should be (true)
    val translateMapString = new ScalaDataTranslator[Map[String, String]](AvroMap(AvroString()), getClass.getClassLoader)
    translateMapString.toScala(pfaMapString).isInstanceOf[Map[_, _]] should be (true)
    translateMapString.toScala(pfaMapString) map {case (k, v) => v.isInstanceOf[String] should be (true)}

    val pfaMapInt = PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: int}
action:
  type: {type: map, values: int}
  value: {one: 1, two: 2, three: 3}
""").head.action(null)
    pfaMapInt.isInstanceOf[PFAMap[_]] should be (true)
    val translateMapInt = new ScalaDataTranslator[Map[String, Int]](AvroMap(AvroInt()), getClass.getClassLoader)
    translateMapInt.toScala(pfaMapInt).isInstanceOf[Map[_, _]] should be (true)
    translateMapInt.toScala(pfaMapInt) map {case (k, v) => v.isInstanceOf[Int] should be (true)}

    val pfaRecord = PFAEngine.fromYaml("""
input: "null"
output:
  type: record
  name: MyRecord
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
action:
  type: MyRecord
  value: {one: 1, two: 2.2, three: "THREE"}
""").head.action(null)
    pfaRecord.isInstanceOf[PFARecord] should be (true)
    val translateRecord = new ScalaDataTranslator[MyTestRecord](AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyTestRecord", Some("test.scala.jvmcompiler")), getClass.getClassLoader)
    translateRecord.toScala(pfaRecord).isInstanceOf[MyTestRecord] should be (true)
    translateRecord.toScala(pfaRecord).one.isInstanceOf[Int] should be (true)
    translateRecord.toScala(pfaRecord).two.isInstanceOf[Double] should be (true)
    translateRecord.toScala(pfaRecord).three.isInstanceOf[String] should be (true)

    val pfaArrayRecord = PFAEngine.fromYaml("""
input: "null"
output:
  type: array
  items:
    type: record
    name: MyRecord
    fields:
      - {name: one, type: int}
      - {name: two, type: double}
      - {name: three, type: string}
action:
  type: {type: array, items: MyRecord}
  value: [{one: 1, two: 1.1, three: "UNO"}, {one: 2, two: 2.2, three: "DOS"}, {one: 3, two: 3.3, three: "TRES"}]
""").head.action(null)
    pfaArrayRecord.isInstanceOf[PFAArray[_]] should be (true)
    val translateArrayRecord = new ScalaDataTranslator[Seq[MyTestRecord]](AvroArray(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyTestRecord", Some("test.scala.jvmcompiler"))), getClass.getClassLoader)
    translateArrayRecord.toScala(pfaArrayRecord).isInstanceOf[Seq[_]] should be (true)
    translateArrayRecord.toScala(pfaArrayRecord).head.isInstanceOf[MyTestRecord] should be (true)
    translateArrayRecord.toScala(pfaArrayRecord).head.one.isInstanceOf[Int] should be (true)
    translateArrayRecord.toScala(pfaArrayRecord).head.two.isInstanceOf[Double] should be (true)
    translateArrayRecord.toScala(pfaArrayRecord).head.three.isInstanceOf[String] should be (true)

    val pfaRecord2 = PFAEngine.fromYaml("""
input: "null"
output:
  type: record
  name: MyRecord
  fields:
    - {name: one, type: {type: array, items: int}}
    - {name: two, type: {type: map, values: double}}
action:
  type: MyRecord
  value: {one: [1, 2, 3], two: {"uno": 1.1, "dos": 2.2, "tres": 3.3}}
""").head.action(null)
    pfaRecord2.isInstanceOf[PFARecord] should be (true)
    val translateRecord2 = new ScalaDataTranslator[MyTestRecord2](AvroRecord(List(AvroField("one", AvroArray(AvroInt())), AvroField("two", AvroMap(AvroDouble()))), "MyTestRecord2", Some("test.scala.jvmcompiler")), getClass.getClassLoader)
    translateRecord2.toScala(pfaRecord2).isInstanceOf[MyTestRecord2] should be (true)
    translateRecord2.toScala(pfaRecord2).one.isInstanceOf[Seq[_]] should be (true)
    translateRecord2.toScala(pfaRecord2).one map {_.isInstanceOf[Int] should be (true)}
    translateRecord2.toScala(pfaRecord2).two.isInstanceOf[Map[_, _]] should be (true)
    translateRecord2.toScala(pfaRecord2).two map {case (k, v) => v.isInstanceOf[Double] should be (true)}

    val pfaRecord3 = PFAEngine.fromYaml("""
input: "null"
output:
  type: record
  name: Outer
  fields:
    - name: first
      type:
        type: record
        name: MyRecord
        fields:
          - {name: one, type: int}
          - {name: two, type: double}
          - {name: three, type: string}
    - name: second
      type:
        type: record
        name: MyRecord2
        fields:
          - {name: one, type: {type: array, items: int}}
          - {name: two, type: {type: map, values: double}}
action:
  type: Outer
  value:
    first: {one: 1, two: 1.1, three: "UNO"}
    second: {one: [1, 2, 3], two: {"uno": 1.1, "dos": 2.2, "tres": 3.3}}
""").head.action(null)
    pfaRecord3.isInstanceOf[PFARecord] should be (true)
    val translateRecord3 = new ScalaDataTranslator[MyTestRecord3](AvroRecord(List(AvroField("first", AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyTestRecord", Some("test.scala.jvmcompiler"))), AvroField("second", AvroRecord(List(AvroField("one", AvroArray(AvroInt())), AvroField("two", AvroMap(AvroDouble()))), "MyTestRecord2", Some("test.scala.jvmcompiler")))), "MyTestRecord3", Some("test.scala.jvmcompiler")), getClass.getClassLoader)
    translateRecord3.toScala(pfaRecord3).isInstanceOf[MyTestRecord3] should be (true)
    translateRecord3.toScala(pfaRecord3).first.isInstanceOf[MyTestRecord] should be (true)
    translateRecord3.toScala(pfaRecord3).first.one.isInstanceOf[Int] should be (true)
    translateRecord3.toScala(pfaRecord3).first.two.isInstanceOf[Double] should be (true)
    translateRecord3.toScala(pfaRecord3).first.three.isInstanceOf[String] should be (true)
    translateRecord3.toScala(pfaRecord3).second.isInstanceOf[MyTestRecord2] should be (true)
    translateRecord3.toScala(pfaRecord3).second.one.isInstanceOf[Seq[_]] should be (true)
    translateRecord3.toScala(pfaRecord3).second.one map {_.isInstanceOf[Int] should be (true)}
    translateRecord3.toScala(pfaRecord3).second.two.isInstanceOf[Map[_, _]] should be (true)
    translateRecord3.toScala(pfaRecord3).second.two map {case (k, v) => v.isInstanceOf[Double] should be (true)}

    // NOTE: haven't tested fixed, enum, or union
  }

  it must "translate from Scala" taggedAs(JVMCompilation) in {
    val scalaInt = 3
    val pfaInt = new ScalaDataTranslator[Int](AvroInt(), getClass.getClassLoader).fromScala(scalaInt)
    val engineInt = PFAEngine.fromYaml("""
input: int
output: int
action: input
""").head
    engineInt.action(engineInt.fromPFAData(pfaInt)).isInstanceOf[java.lang.Integer] should be (true)

    val scalaString = "THREE"
    val pfaString = new ScalaDataTranslator[String](AvroString(), getClass.getClassLoader).fromScala(scalaString)
    val engineString = PFAEngine.fromYaml("""
input: string
output: string
action: input
""").head
    engineString.action(engineString.fromPFAData(pfaString)).isInstanceOf[String] should be (true)

    val scalaArrayString = Seq("one", "two", "three")
    val pfaArrayString = new ScalaDataTranslator[Seq[String]](AvroArray(AvroString()), getClass.getClassLoader).fromScala(scalaArrayString)
    val engineArrayString = PFAEngine.fromYaml("""
input: {type: array, items: string}
output: {type: array, items: string}
action: input
""").head
    engineArrayString.action(engineArrayString.fromPFAData(pfaArrayString)).isInstanceOf[PFAArray[_]] should be (true)
    engineArrayString.action(engineArrayString.fromPFAData(pfaArrayString)).asInstanceOf[PFAArray[String]].toVector map {_.isInstanceOf[String] should be (true)}

    val scalaArrayInt = Seq(1, 2, 3)
    val pfaArrayInt = new ScalaDataTranslator[Seq[Int]](AvroArray(AvroInt()), getClass.getClassLoader).fromScala(scalaArrayInt)
    val engineArrayInt = PFAEngine.fromYaml("""
input: {type: array, items: int}
output: {type: array, items: int}
action: input
""").head
    engineArrayInt.action(engineArrayInt.fromPFAData(pfaArrayInt)).isInstanceOf[PFAArray[_]] should be (true)
    engineArrayInt.action(engineArrayInt.fromPFAData(pfaArrayInt)).asInstanceOf[PFAArray[Int]].toVector map {_.isInstanceOf[Int] should be (true)}

    val scalaMapString = Map("one" -> "ONE", "two" -> "TWO", "three" -> "THREE")
    val pfaMapString = new ScalaDataTranslator[Map[String, String]](AvroMap(AvroString()), getClass.getClassLoader).fromScala(scalaMapString)
    val engineMapString = PFAEngine.fromYaml("""
input: {type: map, values: string}
output: {type: map, values: string}
action: input
""").head
    engineMapString.action(engineMapString.fromPFAData(pfaMapString)).isInstanceOf[PFAMap[_]] should be (true)
    engineMapString.action(engineMapString.fromPFAData(pfaMapString)).asInstanceOf[PFAMap[String]].toMap map {case (k, v) => v.isInstanceOf[String] should be (true)}

    val scalaMapInt = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val pfaMapInt = new ScalaDataTranslator[Map[String, Int]](AvroMap(AvroInt()), getClass.getClassLoader).fromScala(scalaMapInt)
    val engineMapInt = PFAEngine.fromYaml("""
input: {type: map, values: int}
output: {type: map, values: int}
action: input
""").head
    engineMapInt.action(engineMapInt.fromPFAData(pfaMapInt)).isInstanceOf[PFAMap[_]] should be (true)
    engineMapInt.action(engineMapInt.fromPFAData(pfaMapInt)).asInstanceOf[PFAMap[java.lang.Integer]].toMap map {case (k, v) => v.isInstanceOf[java.lang.Integer] should be (true)}

    val scalaRecord = MyTestRecord(1, 2.2, "THREE")
    val pfaRecord = new ScalaDataTranslator[MyTestRecord](AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyTestRecord", Some("test.scala.jvmcompiler")), getClass.getClassLoader).fromScala(scalaRecord)
    val engineRecord = PFAEngine.fromYaml("""
input:
  type: record
  name: MyRecord
  fields:
    - {name: one, type: int}
    - {name: two, type: double}
    - {name: three, type: string}
output: MyRecord
action: input
""").head
    engineRecord.action(engineRecord.fromPFAData(pfaRecord)).isInstanceOf[PFARecord] should be (true)
    engineRecord.action(engineRecord.fromPFAData(pfaRecord)).asInstanceOf[PFARecord].get("one").isInstanceOf[java.lang.Integer] should be (true)
    engineRecord.action(engineRecord.fromPFAData(pfaRecord)).asInstanceOf[PFARecord].get("two").isInstanceOf[java.lang.Double] should be (true)
    engineRecord.action(engineRecord.fromPFAData(pfaRecord)).asInstanceOf[PFARecord].get("three").isInstanceOf[String] should be (true)

    val scalaArrayRecord = Seq(MyTestRecord(1, 1.1, "UNO"), MyTestRecord(2, 2.2, "DOS"), MyTestRecord(3, 3.3, "TRES"))
    val pfaArrayRecord = new ScalaDataTranslator[Seq[MyTestRecord]](AvroArray(AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyTestRecord", Some("test.scala.jvmcompiler"))), getClass.getClassLoader).fromScala(scalaArrayRecord)
    val engineArrayRecord = PFAEngine.fromYaml("""
input:
  type: array
  items:
    type: record
    name: MyRecord
    fields:
      - {name: one, type: int}
      - {name: two, type: double}
      - {name: three, type: string}
output: {type: array, items: MyRecord}
action: input
""").head
    engineArrayRecord.action(engineArrayRecord.fromPFAData(pfaArrayRecord)).isInstanceOf[PFAArray[_]] should be (true)
    engineArrayRecord.action(engineArrayRecord.fromPFAData(pfaArrayRecord)).asInstanceOf[PFAArray[_]].toVector.head.isInstanceOf[PFARecord] should be (true)
    engineArrayRecord.action(engineArrayRecord.fromPFAData(pfaArrayRecord)).asInstanceOf[PFAArray[_]].toVector.head.asInstanceOf[PFARecord].get("one").isInstanceOf[java.lang.Integer] should be (true)
    engineArrayRecord.action(engineArrayRecord.fromPFAData(pfaArrayRecord)).asInstanceOf[PFAArray[_]].toVector.head.asInstanceOf[PFARecord].get("two").isInstanceOf[java.lang.Double] should be (true)
    engineArrayRecord.action(engineArrayRecord.fromPFAData(pfaArrayRecord)).asInstanceOf[PFAArray[_]].toVector.head.asInstanceOf[PFARecord].get("three").isInstanceOf[String] should be (true)

    val scalaRecord2 = MyTestRecord2(Seq(1, 2, 3), Map("uno" -> 1.1, "dos" -> 2.2, "tres" -> 3.3))
    val pfaRecord2 = new ScalaDataTranslator[MyTestRecord2](AvroRecord(List(AvroField("one", AvroArray(AvroInt())), AvroField("two", AvroMap(AvroDouble()))), "MyTestRecord2", Some("test.scala.jvmcompiler")), getClass.getClassLoader).fromScala(scalaRecord2)
    val engineRecord2 = PFAEngine.fromYaml("""
input:
  type: record
  name: MyRecord
  fields:
    - {name: one, type: {type: array, items: int}}
    - {name: two, type: {type: map, values: double}}
output: MyRecord
action: input
""").head
    engineRecord2.action(engineRecord2.fromPFAData(pfaRecord2)).isInstanceOf[PFARecord] should be (true)
    engineRecord2.action(engineRecord2.fromPFAData(pfaRecord2)).asInstanceOf[PFARecord].get("one").isInstanceOf[PFAArray[_]] should be (true)
    engineRecord2.action(engineRecord2.fromPFAData(pfaRecord2)).asInstanceOf[PFARecord].get("one").asInstanceOf[PFAArray[_]].toVector map {_.isInstanceOf[Int] should be (true)}
    engineRecord2.action(engineRecord2.fromPFAData(pfaRecord2)).asInstanceOf[PFARecord].get("two").isInstanceOf[PFAMap[_]] should be (true)
    engineRecord2.action(engineRecord2.fromPFAData(pfaRecord2)).asInstanceOf[PFARecord].get("two").asInstanceOf[PFAMap[_]].toMap map {case (k, v) => v.isInstanceOf[java.lang.Double] should be (true)}

    val scalaRecord3 = MyTestRecord3(MyTestRecord(1, 2.2, "UNO"), MyTestRecord2(Seq(1, 2, 3), Map("uno" -> 1.1, "dos" -> 2.2, "tres" -> 3.3)))
    val pfaRecord3 = new ScalaDataTranslator[MyTestRecord3](AvroRecord(List(AvroField("first", AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "MyTestRecord", Some("test.scala.jvmcompiler"))), AvroField("second", AvroRecord(List(AvroField("one", AvroArray(AvroInt())), AvroField("two", AvroMap(AvroDouble()))), "MyTestRecord2", Some("test.scala.jvmcompiler")))), "MyTestRecord3", Some("test.scala.jvmcompiler")), getClass.getClassLoader).fromScala(scalaRecord3)
    val engineRecord3 = PFAEngine.fromYaml("""
input:
  type: record
  name: Outer
  fields:
    - name: first
      type:
        type: record
        name: MyRecord
        fields:
          - {name: one, type: int}
          - {name: two, type: double}
          - {name: three, type: string}
    - name: second
      type:
        type: record
        name: MyRecord2
        fields:
          - {name: one, type: {type: array, items: int}}
          - {name: two, type: {type: map, values: double}}
output: Outer
action: input
""").head
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).isInstanceOf[PFARecord] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("first").isInstanceOf[PFARecord] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("first").asInstanceOf[PFARecord].get("one").isInstanceOf[java.lang.Integer] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("first").asInstanceOf[PFARecord].get("two").isInstanceOf[java.lang.Double] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("first").asInstanceOf[PFARecord].get("three").isInstanceOf[String] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("second").isInstanceOf[PFARecord] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("second").asInstanceOf[PFARecord].get("one").isInstanceOf[PFAArray[_]] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("second").asInstanceOf[PFARecord].get("one").asInstanceOf[PFAArray[_]].toVector map {_.isInstanceOf[java.lang.Integer] should be (true)}
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("second").asInstanceOf[PFARecord].get("two").isInstanceOf[PFAMap[_]] should be (true)
    engineRecord3.action(engineRecord3.fromPFAData(pfaRecord3)).asInstanceOf[PFARecord].get("second").asInstanceOf[PFARecord].get("two").asInstanceOf[PFAMap[_]].toMap map {case (k, v) => v.isInstanceOf[java.lang.Double] should be (true)}

    // NOTE: haven't tested fixed, enum, or union
  }
}
