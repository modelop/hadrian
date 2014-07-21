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

package test.scala.lib1.model.tree

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
class Lib1ModelTreeSuite extends FlatSpec with Matchers {
  "simpleTest" must "do numerical comparisons" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.simpleTest: [datum, tree]
""").head

    engine.action("<=").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action("<").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action("!=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action("==").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(">").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(">=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    val engine2 = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: int}
        new: {field: {type: Fields, value: two}, operator: input, value: 2}
  - model.tree.simpleTest: [datum, tree]
""").head

    engine2.action("<=").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine2.action("<").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine2.action("!=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine2.action("==").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine2.action(">").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine2.action(">=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    val engine3 = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 3, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: one}, operator: input, value: 2.0}
  - model.tree.simpleTest: [datum, tree]
""").head

    engine3.action("<=").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine3.action("<").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine3.action("!=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine3.action("==").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine3.action(">").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine3.action(">=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)

  }

  it must "do string comparisons" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: string}
        new: {field: {type: Fields, value: three}, operator: input, value: ["GZK"]}
  - model.tree.simpleTest: [datum, tree]
""").head

    engine.action("<=").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action("<").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action("!=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action("==").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(">").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(">=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "do other comparisons" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: bytes}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: bytes}
        new: {field: {type: Fields, value: three}, operator: input, value: {type: bytes, value: "GZK"}}
  - model.tree.simpleTest: [datum, tree]
""").head

    engine.action("<=").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action("<").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action("!=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action("==").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
    engine.action(">").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(">=").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  "simpleTest" must "do set inclusion" taggedAs(Lib1, Lib1ModelTree) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: string}}
        value: {field: three, operator: in, value: ["IBM", "HAL", "GZK"]}
  - model.tree.simpleTest: [datum, tree]
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: string}}
        value: {field: three, operator: in, value: ["IBM", "HALLY", "GZK"]}
  - model.tree.simpleTest: [datum, tree]
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: int}}
        value: {field: one, operator: in, value: [0, 1, 2]}
  - model.tree.simpleTest: [datum, tree]
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: {type: array, items: int}}
        value: {field: one, operator: in, value: [0, 2, 3]}
  - model.tree.simpleTest: [datum, tree]
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  "missingTest" must "do pass through missing values" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: ["null", double]}, {name: three, type: string}]}
        value: {one: 1, two: null, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.missingTest: [datum, tree]
""").head
    engine.action("<=").asInstanceOf[java.lang.Boolean] should be (null)
  }

  it must "not work for simpleTest" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: ["null", double]}, {name: three, type: string}]}
        value: {one: 1, two: null, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.simpleTest: [datum, tree]
""").head
    evaluating { engine.action("<=") } should produce [PFARuntimeException]
  }

  it must "be okay for simpleTest if the types match" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: ["null", double]}, {name: three, type: string}]}
        value: {one: 1, two: null, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: ["null", double]}
        value: {field: two, operator: "<=", value: null}
  - model.tree.simpleTest: [datum, tree]
""").head
    engine.action("==").asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "not act like simpleTest when nothing is missing" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: ["null", boolean]
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
        value: {one: 1, two: 2.2, three: "HAL"}
      tree:
        type:
          type: record
          name: Tree
          fields:
            - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
            - {name: operator, type: string}
            - {name: value, type: double}
        new: {field: {type: Fields, value: two}, operator: input, value: 2.0}
  - model.tree.missingTest: [datum, tree]
""").head
    engine.action("<=").asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  "surrogateTest" must "evaluate a chain of comparisons" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: ["null", int]}, {name: two, type: ["null", double]}, {name: three, type: ["null", string]}]}
        value: {one: null, two: null, three: null}
      comparisons:
        type:
          type: array
          items:
            type: record
            name: Comparison
            fields:
              - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
              - {name: operator, type: string}
              - {name: value, type: [int, double, string]}
        value:
          - {field: one, operator: "==", value: {int: 1}}
          - {field: two, operator: "==", value: {double: 2.2}}
          - {field: three, operator: "==", value: {string: "THREE"}}
  - model.tree.surrogateTest:
      - datum
      - comparisons
      - params: [{d: Datum}, {c: Comparison}]
        ret: ["null", boolean]
        do: {model.tree.missingTest: [d, c]}
""").head
    evaluating { engine.action(null) } should produce [PFARuntimeException]

    val engine2 = PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  - let:
      datum:
        type: {type: record, name: Datum, fields: [{name: one, type: ["null", int]}, {name: two, type: ["null", double]}, {name: three, type: ["null", string]}]}
        value: {one: null, two: null, three: {string: "THREE"}}
      comparisons:
        type:
          type: array
          items:
            type: record
            name: Comparison
            fields:
              - {name: field, type: {type: enum, name: Fields, symbols: [one, two, three]}}
              - {name: operator, type: string}
              - {name: value, type: [int, double, string]}
        value:
          - {field: one, operator: "==", value: {int: 1}}
          - {field: two, operator: "==", value: {double: 2.2}}
          - {field: three, operator: "==", value: {string: "THREE"}}
  - model.tree.surrogateTest:
      - datum
      - comparisons
      - params: [{d: Datum}, {c: Comparison}]
        ret: ["null", boolean]
        do: {model.tree.missingTest: [d, c]}
""").head
    engine2.action(null)
  }

  "simpleWalk" must "work for a typical example" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - name: field
          type:
            type: enum
            name: Fields
            symbols: [one, two, three]
        - {name: operator, type: string}
        - {name: value, type: [int, double, string]}
        - {name: pass, type: [string, TreeNode]}
        - {name: fail, type: [string, TreeNode]}
    init:
      field: one
      operator: "<"
      value: {double: 12}
      pass:
        TreeNode:
          field: two
          operator: ">"
          value: {double: 3.5}
          pass: {string: yes-yes}
          fail: {string: yes-no}
      fail:
        TreeNode:
          field: three
          operator: ==
          value: {string: TEST}
          pass: {string: no-yes}
          fail: {string: no-no}
action:
  - model.tree.simpleWalk:
      - input
      - cell: tree
      - params: [{d: Datum}, {t: TreeNode}]
        ret: boolean
        do: {model.tree.simpleTest: [d, t]}
""").head

    engine.action(engine.fromJson("""{"one": 1, "two": 7, "three": "whatever"}""", engine.inputType)) should be ("yes-yes")
    engine.action(engine.fromJson("""{"one": 1, "two": 0, "three": "whatever"}""", engine.inputType)) should be ("yes-no")
    engine.action(engine.fromJson("""{"one": 15, "two": 7, "three": "TEST"}""", engine.inputType)) should be ("no-yes")
    engine.action(engine.fromJson("""{"one": 15, "two": 7, "three": "ZEST"}""", engine.inputType)) should be ("no-no")
  }

  it must "work with a completely user-defined predicate" taggedAs(Lib1, Lib1ModelTree) in {
    val engine = PFAEngine.fromYaml("""
input: {type: record, name: Datum, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
output: string
cells:
  tree:
    type:
      type: record
      name: TreeNode
      fields:
        - {name: field, type: string}
        - {name: pass, type: [string, TreeNode]}
        - {name: fail, type: [string, TreeNode]}
    init:
      field: one
      pass:
        TreeNode:
          field: two
          pass: {string: yes-yes}
          fail: {string: yes-no}
      fail:
        TreeNode:
          field: three
          pass: {string: no-yes}
          fail: {string: no-no}
action:
  - {model.tree.simpleWalk: [input, {cell: tree}, {fcnref: u.myPredicate}]}
fcns:
  myPredicate:
    params:
      - datum: Datum
      - treeNode: TreeNode
    ret: boolean
    do:
      cond:
        - {if: {"==": [treeNode.field, [one]]}, then: {"<": [datum.one, 12]}}
        - {if: {"==": [treeNode.field, [two]]}, then: {">": [datum.two, 3.5]}}
      else: {"==": [datum.three, [TEST]]}
""").head

    engine.action(engine.fromJson("""{"one": 1, "two": 7, "three": "whatever"}""", engine.inputType)) should be ("yes-yes")
    engine.action(engine.fromJson("""{"one": 1, "two": 0, "three": "whatever"}""", engine.inputType)) should be ("yes-no")
    engine.action(engine.fromJson("""{"one": 15, "two": 7, "three": "TEST"}""", engine.inputType)) should be ("no-yes")
    engine.action(engine.fromJson("""{"one": 15, "two": 7, "three": "ZEST"}""", engine.inputType)) should be ("no-no")
  }

}
