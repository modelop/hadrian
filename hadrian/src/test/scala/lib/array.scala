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

package test.scala.lib.array

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
class LibArraySuite extends FlatSpec with Matchers {
  //////////////////////////////////////////////////////////////////// basic access

  "basic access" must "get length" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action: {a.len: {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}}
""").head.action(null) should be (5)

    PFAEngine.fromYaml("""
input: "null"
output: int
action: {a.len: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}}
""").head.action(null) should be (5)

    PFAEngine.fromYaml("""
input: "null"
output: int
action: {a.len: {value: [], type: {type: array, items: string}}}
""").head.action(null) should be (0)
  }

  it must "get subseq" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 4]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, -2]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 100]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three", "four", "five"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 3]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector())

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseq: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 2]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector())
  }

  it must "do head, tail, last, init" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: string
action: {a.head: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
""").head.action(null) should be ("zero")

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.tail: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three", "four", "five"))

    PFAEngine.fromYaml("""
input: "null"
output: string
action: {a.last: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
""").head.action(null) should be ("five")

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.init: {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("zero", "one", "two", "three", "four"))
  }

  it must "set subseqto" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 4, {value: ["ACK!"], type: {type: array, items: string}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("zero", "ACK!", "four", "five"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, -2, {value: ["ACK!"], type: {type: array, items: string}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("zero", "ACK!", "four", "five"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 1, 100, {value: ["ACK!"], type: {type: array, items: string}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("zero", "ACK!"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 3, {value: ["ACK!"], type: {type: array, items: string}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("zero", "one", "two", "ACK!", "three", "four", "five"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action: {a.subseqto: [{value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}, 3, 2, {value: ["ACK!"], type: {type: array, items: string}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("zero", "one", "two", "ACK!", "three", "four", "five"))
  }

  //////////////////////////////////////////////////////////////////// searching

  "searching" must "do contains" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["two", "three", "four"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["two", "four", "three"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "two"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "two"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.contains: 
    - {value: ["zero", "one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "whatev"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "do count" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {value: ["two", "one"], type: {type: array, items: string}}
""").head.action(null) should be (2)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {value: ["one", "two"], type: {type: array, items: string}}
""").head.action(null) should be (3)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {value: ["ACK!"], type: {type: array, items: string}}
""").head.action(null) should be (0)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {string: "two"}
""").head.action(null) should be (3)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: ["one", "two", "one", "two", "one", "two"], type: {type: array, items: string}}
    - {string: "ACK!"}
""").head.action(null) should be (0)
  }

  it must "do countPredicate" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [{"%": [x, 2]}, 0]}
""").head.action(null) should be (3)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: [1, 3, 5, 7, 9], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [{"%": [x, 2]}, 0]}
""").head.action(null) should be (0)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.even}
fcns:
  even:
    params: [{x: int}]
    ret: boolean
    do: {"==": [{"%": [x, 2]}, 0]}
""").head.action(null) should be (3)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.count:
    - {value: [1, 3, 5, 7, 9], type: {type: array, items: int}}
    - {fcn: u.even}
fcns:
  even:
    params: [{x: int}]
    ret: boolean
    do: {"==": [{"%": [x, 2]}, 0]}
""").head.action(null) should be (0)
  }

  it must "do index" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["two", "three"], type: {type: array, items: string}}
""").head.action(null) should be (2)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "three"}
""").head.action(null) should be (3)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["ACK!"], type: {type: array, items: string}}
""").head.action(null) should be (-1)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.index:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "ACK!"}
""").head.action(null) should be (-1)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["two", "three"], type: {type: array, items: string}}
""").head.action(null) should be (6)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "three"}
""").head.action(null) should be (7)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {value: ["ACK!"], type: {type: array, items: string}}
""").head.action(null) should be (-1)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  a.rindex:
    - {value: ["zero", "one", "two", "three", "zero", "one", "two", "three"], type: {type: array, items: string}}
    - {string: "ACK!"}
""").head.action(null) should be (-1)
  }

  it must "do startswith" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["one", "two"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["two", "three"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "one"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.startswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "two"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "do endswith" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["four", "five"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {value: ["three", "four"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "five"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.endswith:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
    - {string: "four"}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  //////////////////////////////////////////////////////////////////// manipulation

  "manipulation" must "concat" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.concat:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - {value: ["four", "five"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three", "four", "five"))
  }

  it must "append" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.append:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - {string: "four"}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "two", "three", "four"))
  }

  it must "cycle" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.cycle:
    - {value: ["one", "two", "three", "four", "five", "six", "seven", "eight"], type: {type: array, items: string}}
    - {string: "nine"}
    - 3
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("seven", "eight", "nine"))
  }

  it must "insert" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.insert:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
    - {string: "four"}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "four", "two", "three"))
  }

  it must "replace" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.replace:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
    - {string: "four"}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "four", "three"))
  }

  it must "remove" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.remove:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "three"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.remove:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
    - 2
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("one", "three"))
  }

  it must "rotate" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.rotate:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 1
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "three", "one"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.rotate:
    - {value: ["one", "two", "three"], type: {type: array, items: string}}
    - 4
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "three", "one"))
  }

  //////////////////////////////////////////////////////////////////// reordering

  "reordering" must "sort numbers" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.sort:
    - {value: [6, 2, 4, 6, 4, 5], type: {type: array, items: int}}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(2, 4, 4, 5, 6, 6))
  }

  it must "sort objects" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.sort:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("five", "four", "one", "three", "two"))
  }

  it must "sort with a user-defined function" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.sortLT:
    - {value: [6, 2, 4, 6, 4, 5], type: {type: array, items: int}}
    - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: int}, {b: int}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 3.4]}}, {m.abs: {"-": [b, 3.4]}}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(4, 4, 2, 5, 6, 6))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.sortLT:
    - {value: [6, 2, 4, 6, 4, 5], type: {type: array, items: int}}
    - params: [{a: int}, {b: int}]
      ret: boolean
      do: {"<": [{m.abs: {"-": [a, 3.4]}}, {m.abs: {"-": [b, 3.4]}}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(4, 4, 2, 5, 6, 6))
  }

//   it must "shuffle" taggedAs(Lib, LibArray) in {
//     PFAEngine.fromYaml("""
// input: "null"
// output: {type: array, items: string}
// action:
//   a.shuffle:
//     - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
// randseed: 12345
// """).head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("three", "one", "two", "five", "four"))
//   }

  it must "reverse" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.reverse:
    - {value: ["one", "two", "three", "four", "five"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("five", "four", "three", "two", "one"))
  }

  //////////////////////////////////////////////////////////////////// extreme values

  "extreme values" must "find numerical max/min" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {a.max: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""").head.action(null) should be (7.7)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - {a.min: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""").head.action(null) should be (2.2)
  }

  it must "find object max/min" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - {a.max: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""").head.action(null) should be ("two")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - {a.min: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""").head.action(null) should be ("five")
  }

  it must "find user-defined max/min" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.maxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (2.2)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.maxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (2.2)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.minLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (6.6)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.minLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (6.6)
  }

  it must "find the top 3 numerical max/min" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - {a.maxN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector should be (Vector(7.7, 7.6, 6.6))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - {a.minN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.2, 2.2, 4.4))
  }

  it must "find the top 3 object max/min" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  - {a.maxN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("two", "three", "six"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  - {a.minN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("five", "four", "one"))
  }

  it must "find the top 3 user-defined max/min" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.maxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.2, 2.2, 4.4))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.maxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector should be (Vector(2.2, 2.2, 4.4))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.minNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector should be (Vector(6.6, 5.5, 7.6))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  - a.minNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector should be (Vector(6.6, 5.5, 7.6))
  }

  it must "find numerical argmax/argmin" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmax: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""").head.action(null) should be (2)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmin: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}]}
""").head.action(null) should be (1)
  }

  it must "find object argmax/argmin" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmax: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""").head.action(null) should be (1)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - {a.argmin: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}]}
""").head.action(null) should be (4)
  }

  it must "find user-defined argmax/argmin" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argmaxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (1)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argmaxLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (1)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argminLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (4)

    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.argminLT:
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null) should be (4)
  }

  it must "find the top 3 numerical argmax/argmin" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argmaxN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(2, 6, 4))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argminN: [{value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(1, 5, 3))
  }

  it must "find the top 3 object argmax/argmin" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argmaxN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(1, 2, 5))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - {a.argminN: [{value: ["one", "two", "three", "four", "five", "six", "seven"], type: {type: array, items: string}}, 3]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(4, 3, 0))
  }

  it must "find the top 3 user-defined argmax/argmin" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argmaxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(1, 5, 3))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argmaxNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(1, 5, 3))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argminNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - {fcn: u.mylt}
fcns:
  mylt:
    params: [{a: double}, {b: double}]
    ret: boolean
    do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(4, 0, 6))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  - a.argminNLT: 
      - {value: [5.5, 2.2, 7.7, 4.4, 6.6, 2.2, 7.6], type: {type: array, items: double}}
      - 3
      - params: [{a: double}, {b: double}]
        ret: boolean
        do: {"<": [{m.abs: {"-": [a, 6.2]}}, {m.abs: {"-": [b, 6.2]}}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(4, 0, 6))
  }

  //////////////////////////////////////////////////////////////////// numerical

  "numerical" must "sum" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.sum: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}
""").head.action(null) should be (15)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.sum: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}
""").head.action(null) should be (15.0)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.sum: {value: [], type: {type: array, items: double}}
""").head.action(null) should be (0.0)
  }

  it must "product" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.product: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}
""").head.action(null) should be (120)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.product: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}
""").head.action(null) should be (120.0)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.product: {value: [], type: {type: array, items: double}}
""").head.action(null) should be (1.0)
  }

  it must "lnsum" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [1, 2, 3, 4, 5], type: {type: array, items: int}}
""").head.action(null).asInstanceOf[java.lang.Double].doubleValue should be (4.79 +- 0.01)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [1, 2, 3, 4, 5], type: {type: array, items: double}}
""").head.action(null).asInstanceOf[java.lang.Double].doubleValue should be (4.79 +- 0.01)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [], type: {type: array, items: double}}
""").head.action(null) should be (0.0)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.lnsum: {value: [1, 2, -3, 4, 5], type: {type: array, items: double}}
""").head.action(null).toString should be ("NaN")
  }

  it must "median" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: int
action:
  - a.median: {value: [1,2,3,4], type: {type: array, items: int}}
""").head.action(null) should be (2)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.median: {value: [1,2,3,4,5], type: {type: array, items: double}}
""").head.action(null) should be (3.0)

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - a.median: {value: ["a","c","b","d"], type: {type: array, items: string}}
""").head.action(null) should be ("b")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - a.median: {value: ["e","c","d","b","a"], type: {type: array, items: string}}
""").head.action(null) should be ("c")
  }

  it must "ntile" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.ntile:
     - {value: [1,2,3,4], type: {type: array, items: double}}
     - 0.5
""").head.action(null) should be (2.5)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.ntile:
     - {value: [1,2,3,4,5], type: {type: array, items: double}}
     - 0.5
""").head.action(null) should be (3.0)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.ntile:
     - {value: [0,1,2,3,4,5,6,7,8,9,10], type: {type: array, items: double}}
     - 0.1
""").head.action(null) should be (1.0)

    PFAEngine.fromYaml("""
input: "null"
output: double
action:
  - a.ntile:
     - {value: [4,5,6,7,8,9,10], type: {type: array, items: double}}
     - -100
""").head.action(null) should be (4)

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - a.ntile:
      - {value: ["a","c","b","d"], type: {type: array, items: string}}
      - 0.5
""").head.action(null) should be ("b")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - a.ntile:
      - {value: ["a","c","b","d","e"], type: {type: array, items: string}}
      - 0.5
""").head.action(null) should be ("c")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - a.ntile:
     - {value: ["a","c","b","d","e","f","g","h","i","j","k"], type: {type: array, items: string}}
     - 0.1
""").head.action(null) should be ("b")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  - a.ntile:
     - {value: ["a","c","b","d","e","f","g","h","i","j","k"], type: {type: array, items: string}}
     - 1000
""").head.action(null) should be ("k")
  }


  //////////////////////////////////////////////////////////////////// set or set-like functions

  "set-like functions" must "distinct" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.distinct:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("hey", "there", "you", "guys"))
  }

  it must "seteq" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.seteq:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "hey", "hey", "you", "guys", "there"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.seteq:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "hey", "hey", "you", "guys"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "union" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.union:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector.toSet should be (Set("wow", "this", "is", "guys", "different", "you", "there", "hey"))
  }

  it must "intersection" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.intersection:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector.toSet should be (Set("hey", "there"))
  }

  it must "diff" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.diff:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector.toSet should be (Set("you", "guys"))
  }

  it must "symdiff" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.symdiff:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector.toSet should be (Set("different", "this", "wow", "guys", "is", "you"))
  }

  it must "subset" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.subset:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.subset:
    - {value: ["hey", "there", "guys"], type: {type: array, items: string}}
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "disjoint" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.disjoint:
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
    - {value: ["hey", "there", "wow", "this", "is", "different"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.disjoint:
    - {value: ["hey", "there", "guys"], type: {type: array, items: string}}
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.disjoint:
    - {value: ["this", "is", "entirely", "different"], type: {type: array, items: string}}
    - {value: ["hey", "there", "you", "hey", "guys", "there"], type: {type: array, items: string}}
""").head.action(null).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  //////////////////////////////////////////////////////////////////// functional programming

  it must "map" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.map:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: long
      do: {"*": [x, {long: 2}]}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0, 2, 4, 6, 8, 10))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.map:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.double}
fcns:
  double:
    params: [{x: int}]
    ret: long
    do: {"*": [x, {long: 2}]}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0, 2, 4, 6, 8, 10))
  }

  it must "mapWithIndex" taggedAs(Lib, LibArray) in {
    val x = PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: double}
action:
  a.mapWithIndex:
    - {value: [0.0, 1.1, 2.2, 3.3, 4.4, 5.5], type: {type: array, items: double}}
    - params: [{i: int}, {x: double}]
      ret: double
      do: {"-": [x, i]}
""").head.action(null).asInstanceOf[PFAArray[Double]].toVector
    x(0) should be (0.00 +- 0.01)
    x(1) should be (0.10 +- 0.01)
    x(2) should be (0.20 +- 0.01)
    x(3) should be (0.30 +- 0.01)
    x(4) should be (0.40 +- 0.01)
    x(5) should be (0.50 +- 0.01)
  }

  it must "filter" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filter:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"==": [{"%": [x, 2]}, 0]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 2, 4))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filter:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.even}
fcns:
  even:
    params: [{x: int}]
    ret: boolean
    do: {"==": [{"%": [x, 2]}, 0]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 2, 4))
  }

  it must "filterWithIndex" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.filterWithIndex:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}]
      ret: boolean
      do: {"&&": [{"==": [{"%": [x, 2]}, 0]}, {"<": [i, 3]}]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 2))
  }

  it must "filtermap" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.filterMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: [long, "null"]
      do:
        if: {"==": [{"%": [x, 2]}, 0]}
        then: null
        else: {"*": [x, {long: 10}]}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(10, 30, 50))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.filterMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.maybeten}
fcns:
  maybeten:
    params: [{x: int}]
    ret: [long, "null"]
    do:
      if: {"==": [{"%": [x, 2]}, 0]}
      then: null
      else: {"*": [x, {long: 10}]}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(10, 30, 50))
 }

  it must "filtermapwithindex" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.filterMapWithIndex:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}]
      ret: [long, "null"]
      do:
        if: {"==": [{"%": [i, 2]}, 0]}
        then: null
        else: {"*": [x, {long: 10}]}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(10, 30, 50))
  }

  it must "flatMap" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.flatMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: {type: array, items: long}
      do: {new: [x, x], type: {type: array, items: long}}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.flatMap:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - {fcn: u.stutter}
fcns:
  stutter:
    params: [{x: int}]
    ret: {type: array, items: long}
    do: {new: [x, x], type: {type: array, items: long}}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5))
  }

  it must "flatMapWithIndex" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: long}
action:
  a.flatMapWithIndex:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}]
      ret: {type: array, items: long}
      do:
        if: {"==": [{"%": [i, 2]}, 0]}
        then: {new: [x, x], type: {type: array, items: long}}
        else: {value: [], type: {type: array, items: long}}
""").head.action(null).asInstanceOf[PFAArray[Long]].toVector should be (Vector(0, 0, 2, 2, 4, 4))
  }

  it must "zipmap" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.zipmap:
    - {value: ["x", "y", "z"], type: {type: array, items: string}}
    - {value: [101, 102, 103], type: {type: array, items: int}}
    - params: [{a: string}, {b: int}]
      ret: string
      do: {s.concat: [a, {s.int: b}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("x101", "y102", "z103"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.zipmap:
    - {value: ["x", "y", "z"], type: {type: array, items: string}}
    - {value: [101, 102, 103], type: {type: array, items: int}}
    - {value: ["a", "b", "c"], type: {type: array, items: string}}
    - params: [{a: string}, {b: int}, {c: string}]
      ret: string
      do: {s.concat: [{s.concat: [a, {s.int: b}]}, c]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("x101a", "y102b", "z103c"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.zipmap:
    - {value: ["x", "y", "z"], type: {type: array, items: string}}
    - {value: [101, 102, 103], type: {type: array, items: int}}
    - {value: ["a", "b", "c"], type: {type: array, items: string}}
    - {value: [true, false, true], type: {type: array, items: boolean}}
    - params: [{a: string}, {b: int}, {c: string}, {d: boolean}]
      ret: string
      do: {s.concat: [{s.concat: [{s.concat: [a, {s.int: b}]}, c]}, {if: d, then: {string: "-up"}, else: {string: "-down"}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("x101a-up", "y102b-down", "z103c-up"))
  }

  it must "zipmapWithIndex" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.zipmapWithIndex:
    - {value: ["x", "y", "z"], type: {type: array, items: string}}
    - {value: [101, 102, 103], type: {type: array, items: int}}
    - params: [{i: int}, {a: string}, {b: int}]
      ret: string
      do: {s.concat: [{s.concat: [{s.int: i}, a]}, {s.int: b}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("0x101", "1y102", "2z103"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.zipmapWithIndex:
    - {value: ["x", "y", "z"], type: {type: array, items: string}}
    - {value: [101, 102, 103], type: {type: array, items: int}}
    - {value: ["a", "b", "c"], type: {type: array, items: string}}
    - params: [{i: int}, {a: string}, {b: int}, {c: string}]
      ret: string
      do: {s.concat: [{s.concat: [{s.concat: [{s.int: i}, a]}, {s.int: b}]}, c]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("0x101a", "1y102b", "2z103c"))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: string}
action:
  a.zipmapWithIndex:
    - {value: ["x", "y", "z"], type: {type: array, items: string}}
    - {value: [101, 102, 103], type: {type: array, items: int}}
    - {value: ["a", "b", "c"], type: {type: array, items: string}}
    - {value: [true, false, true], type: {type: array, items: boolean}}
    - params: [{i: int}, {a: string}, {b: int}, {c: string}, {d: boolean}]
      ret: string
      do: {s.concat: [{s.concat: [{s.concat: [{s.concat: [{s.int: i}, a]}, {s.int: b}]}, c]}, {if: d, then: {string: "-up"}, else: {string: "-down"}}]}
""").head.action(null).asInstanceOf[PFAArray[String]].toVector should be (Vector("0x101a-up", "1y102b-down", "2z103c-up"))
  }

  it must "reduce" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduce:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - params: [{tally: string}, {x: string}]
      ret: string
      do: {s.concat: [tally, x]}
""").head.action(null) should be ("abcde")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduce:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{tally: string}, {x: string}]
    ret: string
    do: {s.concat: [tally, x]}
""").head.action(null) should be ("abcde")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduceRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - params: [{x: string}, {tally: string}]
      ret: string
      do: {s.concat: [tally, x]}
""").head.action(null) should be ("edcba")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.reduceRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{x: string}, {tally: string}]
    ret: string
    do: {s.concat: [tally, x]}
""").head.action(null) should be ("edcba")
  }

  it must "fold" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.fold:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - params: [{tally: string}, {x: string}]
      ret: string
      do: {s.concat: [tally, x]}
""").head.action(null) should be ("abcde")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.fold:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{tally: string}, {x: string}]
    ret: string
    do: {s.concat: [tally, x]}
""").head.action(null) should be ("abcde")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.foldRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - params: [{x: string}, {tally: string}]
      ret: string
      do: {s.concat: [tally, x]}
""").head.action(null) should be ("edcba")

    PFAEngine.fromYaml("""
input: "null"
output: string
action:
  a.foldRight:
    - {value: ["a", "b", "c", "d", "e"], type: {type: array, items: string}}
    - {string: ""}
    - {fcn: u.monoid}
fcns:
  monoid:
    params: [{x: string}, {tally: string}]
    ret: string
    do: {s.concat: [tally, x]}
""").head.action(null) should be ("edcba")
  }

  it must "takeWhile" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.takeWhile:
    - {value: [0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"<": [x, 3]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0, 1, 2))
  }

  it must "dropWhile" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.dropWhile:
    - {value: [0, 1, 2, 3, 4, 5, 4, 3, 2, 1, 0], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: boolean
      do: {"<": [x, 3]}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(3, 4, 5, 4, 3, 2, 1, 0))
  }

  //////////////////////////////////////////////////////////////////// functional tests

  "functional tests" must "any" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.any:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"==": [{s.len: x}, 5]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.any:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"==": [{s.len: x}, 6]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (false)
  }

  it must "all" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.all:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"<": [{s.len: x}, 6]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.all:
    - {value: ["one", "two", "three", "four"], type: {type: array, items: string}}
    - params: [{x: string}]
      ret: boolean
      do: {"<": [{s.len: x}, 5]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (false)
  }

  it must "corresponds" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.corresponds:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 4, 6], type: {type: array, items: int}}
    - params: [{x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (true)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.corresponds:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 999, 6], type: {type: array, items: int}}
    - params: [{x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (false)

    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.corresponds:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 4], type: {type: array, items: int}}
    - params: [{x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (false)
  }

  it must "correspondsWithIndex" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: boolean
action:
  a.correspondsWithIndex:
    - {value: [0, 1, 2, 3], type: {type: array, items: int}}
    - {value: [0, 2, 4, 6], type: {type: array, items: int}}
    - params: [{i: int}, {x: int}, {y: int}]
      ret: boolean
      do: {"==": [{"*": [x, 2]}, y]}
""").head.action(null).asInstanceOf[Boolean].booleanValue should be (true)
  }

  //////////////////////////////////////////////////////////////////// restructuring

  "restructuring" must "slidingWindow" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [1, 2, 3, 4], type: {type: array, items: int}}
    - 2
    - 1
""").head.action(null).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(1, 2), Vector(2, 3), Vector(3, 4)))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [], type: {type: array, items: int}}
    - 2
    - 1
""").head.action(null).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector())

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [1, 2, 3, 4, 5, 6], type: {type: array, items: int}}
    - 3
    - 2
""").head.action(null).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(1, 2, 3), Vector(3, 4, 5)))

    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.slidingWindow:
    - {value: [1, 2, 3, 4, 5, 6], type: {type: array, items: int}}
    - 3
    - 2
""").head.action(null).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(1, 2, 3), Vector(3, 4, 5)))
  }

  it must "combinations" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  - a.combinations:
      - {value: [1, 2, 3, 4], type: {type: array, items: int}}
      - 2
""").head.action(null).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(1, 2), Vector(1, 3), Vector(1, 4), Vector(2, 3), Vector(2, 4), Vector(3, 4)))
  }

  it must "permutations" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.permutations:
    {value: [1, 2, 3, 4], type: {type: array, items: int}}
""").head.action(null).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(1, 2, 3, 4), Vector(1, 2, 4, 3), Vector(1, 3, 2, 4), Vector(1, 3, 4, 2), Vector(1, 4, 2, 3), Vector(1, 4, 3, 2), Vector(2, 1, 3, 4), Vector(2, 1, 4, 3), Vector(2, 3, 1, 4), Vector(2, 3, 4, 1), Vector(2, 4, 1, 3), Vector(2, 4, 3, 1), Vector(3, 1, 2, 4), Vector(3, 1, 4, 2), Vector(3, 2, 1, 4), Vector(3, 2, 4, 1), Vector(3, 4, 1, 2), Vector(3, 4, 2, 1), Vector(4, 1, 2, 3), Vector(4, 1, 3, 2), Vector(4, 2, 1, 3), Vector(4, 2, 3, 1), Vector(4, 3, 1, 2), Vector(4, 3, 2, 1)))

    intercept[PFATimeoutException] { PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: {type: array, items: int}}
action:
  a.permutations:
    {value: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], type: {type: array, items: int}}
options:
  timeout: 1000
""").head.action(null) }
  }

  it must "flatten" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: array, items: int}
action:
  a.flatten: {value: [[1, 2], [], [3, 4, 5]], type: {type: array, items: {type: array, items: int}}}
""").head.action(null).asInstanceOf[PFAArray[Int]].toVector should be (Vector(1, 2, 3, 4, 5))
  }

  it must "groupby" taggedAs(Lib, LibArray) in {
    PFAEngine.fromYaml("""
input: "null"
output: {type: map, values: {type: array, items: int}}
action:
  a.groupby:
    - {value: [0, 1, 2, 3, 4, 5], type: {type: array, items: int}}
    - params: [{x: int}]
      ret: string
      do:
        if: {"==": [{"%": [x, 2]}, 0]}
        then: {string: "even"}
        else: {string: "odd"}
""").head.action(null).asInstanceOf[PFAMap[PFAArray[Int]]].toMap map {case (k, v) => (k, v.toVector)} should be (Map("even" -> Vector(0, 2, 4), "odd" -> Vector(1, 3, 5)))
  }

}
