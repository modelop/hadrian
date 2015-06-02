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

package test.scala.dumpstate

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.util._
import com.opendatagroup.hadrian.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class DumpStateSuite extends FlatSpec with Matchers {
  "Dump private cells" must "int" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: int
    init: 0
action:
  - cell: test
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("0")
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("3")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("5")
  }

  it must "string" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: "null"
cells:
  test:
    type: string
    init: ""
action:
  - cell: test
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("\"\"")
    engine.action("hey")
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("\"hey\"")
    engine.action("there")
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("\"heythere\"")
  }

  it must "array" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: {type: array, items: int}
    init: []
action:
  - cell: test
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("[]")
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("[3]")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("[3,2]")
  }

  it must "map" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: {type: map, values: int}
    init: {"a": 0, "b": 0}
action:
  - cell: test
    path: [{string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.jsonNode.get("b").getIntValue should be (0)
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.jsonNode.get("b").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.jsonNode.get("b").getIntValue should be (5)
  }

  it must "record" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
    init: {a: 0, b: hey}
action:
  - cell: test
    path: [{string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.jsonNode.get("a").getIntValue should be (0)
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.jsonNode.get("a").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.jsonNode.get("a").getIntValue should be (5)
  }

  it must "union" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: [int, string]
    init: {int: 0}
action:
  - cell: test
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("{\"int\":0}")
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("{\"int\":3}")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("{\"int\":5}")
  }

  "Dump public cells" must "int" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: int
    init: 0
    shared: true
action:
  - cell: test
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("0")
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("3")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("5")
  }

  it must "string" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: "null"
cells:
  test:
    type: string
    init: ""
    shared: true
action:
  - cell: test
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("\"\"")
    engine.action("hey")
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("\"hey\"")
    engine.action("there")
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("\"heythere\"")
  }

  it must "array" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: {type: array, items: int}
    init: []
    shared: true
action:
  - cell: test
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("[]")
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("[3]")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("[3,2]")
  }

  it must "map" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: {type: map, values: int}
    init: {"a": 0, "b": 0}
    shared: true
action:
  - cell: test
    path: [{string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.jsonNode.get("b").getIntValue should be (0)
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.jsonNode.get("b").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.jsonNode.get("b").getIntValue should be (5)
  }

  it must "record" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
    init: {a: 0, b: hey}
    shared: true
action:
  - cell: test
    path: [{string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
  - null
""").head

    engine.snapshot.cells("test").init.jsonNode.get("a").getIntValue should be (0)
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.jsonNode.get("a").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.jsonNode.get("a").getIntValue should be (5)
  }

  it must "union" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
cells:
  test:
    type: [int, string]
    init: {int: 0}
    shared: true
action:
  - cell: test
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
  - null
""").head

    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("{\"int\":0}")
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("{\"int\":3}")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.cells("test").init.asInstanceOf[EmbeddedJsonDomCellSource].jsonDom.json should be ("{\"int\":5}")
  }

  "Dump private pools" must "int" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: int
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: 0
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroInt()))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("3")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("5")
  }

  it must "string" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: "null"
pools:
  test:
    type: string
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
    init: {string: ""}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroString()))
    engine.action("hey")
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("\"hey\"")
    engine.action("there")
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("\"heythere\"")
  }

  it must "array" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: {type: array, items: int}
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
    init: {value: [], type: {type: array, items: int}}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("[3]")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("[3,2]")
  }

  it must "map" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: {type: map, values: int}
    init: {"zzz": {"a": 0, "b": 0}}
action:
  - pool: test
    path: [{string: zzz}, {string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {}, type: {type: map, values: int}}
  - null
""").head

    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("b").getIntValue should be (0)
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("b").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("b").getIntValue should be (5)
  }

  it must "record" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
action:
  - pool: test
    path: [{string: zzz}, {string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {a: 0, b: hey}, type: MyRecord}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroRecord(List(AvroField("a", AvroInt()), AvroField("b", AvroString())), "MyRecord")))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("a").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("a").getIntValue should be (5)
  }

  it must "union" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: [int, string]
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
    init: {int: 0}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroUnion(List(AvroInt(), AvroString()))))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("{\"int\":3}")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("{\"int\":5}")
  }

  "Dump public pools" must "int" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: int
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: 0
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroInt()))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("3")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("5")
  }

  it must "string" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: "null"
pools:
  test:
    type: string
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: string}]
      ret: string
      do: {s.concat: [x, input]}
    init: {string: ""}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroString()))
    engine.action("hey")
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("\"hey\"")
    engine.action("there")
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("\"heythere\"")
  }

  it must "array" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: {type: array, items: int}
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [x, input]}
    init: {value: [], type: {type: array, items: int}}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroArray(AvroInt())))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("[3]")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("[3,2]")
  }

  it must "map" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: {type: map, values: int}
    init: {"zzz": {"a": 0, "b": 0}}
    shared: true
action:
  - pool: test
    path: [{string: zzz}, {string: b}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {}, type: {type: map, values: int}}
  - null
""").head

    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("b").getIntValue should be (0)
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("b").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("b").getIntValue should be (5)
  }

  it must "record" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type:
      type: record
      name: MyRecord
      fields:
        - {name: a, type: int}
        - {name: b, type: string}
    shared: true
action:
  - pool: test
    path: [{string: zzz}, {string: a}]
    to:
      params: [{x: int}]
      ret: int
      do: {+: [x, input]}
    init: {value: {a: 0, b: hey}, type: MyRecord}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroRecord(List(AvroField("a", AvroInt()), AvroField("b", AvroString())), "MyRecord")))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("a").getIntValue should be (3)
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.jsonNode.get("zzz").get("a").getIntValue should be (5)
  }

  it must "union" taggedAs(DumpState) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: "null"
pools:
  test:
    type: [int, string]
    shared: true
action:
  - pool: test
    path: [{string: zzz}]
    to:
      params: [{x: [int, string]}]
      ret: [int, string]
      do:
        cast: x
        cases:
          - as: int
            named: y
            do: {+: [y, input]}
          - as: string
            named: y
            do: y
    init: {int: 0}
  - null
""").head

    engine.snapshot.pools("test").init should be (EmbeddedJsonDomPoolSource(Map(), AvroUnion(List(AvroInt(), AvroString()))))
    engine.action(java.lang.Integer.valueOf(3))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("{\"int\":3}")
    engine.action(java.lang.Integer.valueOf(2))
    engine.snapshot.pools("test").init.asInstanceOf[EmbeddedJsonDomPoolSource].jsonDoms("zzz").json should be ("{\"int\":5}")
  }

}
