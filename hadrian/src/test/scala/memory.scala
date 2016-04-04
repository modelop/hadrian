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

package test.scala.memory

import java.lang.management.ManagementFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.map.ObjectMapper

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.memory._
import test.scala._

@RunWith(classOf[JUnitRunner])
class JVMCompilationSuite extends FlatSpec with Matchers {
//   "large memory usage" must "not crash" taggedAs(Memory) in {
//     val engineString = s"""
// input: "null"
// output: int
// cells:
//   biggie:
//     type:
//       type: array
//       items: string
//     init:
// ${Array.fill(700000)("      - alskdjfhalkdjfhalksdjfhalskdjfhalskdjfhalskdjfhasldkfjhalksdjfhalksjdhflkasjdhasdlkfjhalsdkjfhasldkfjhafalksjdfhalskdjfhaslkdjfhasldkfjh").mkString("\n")}
// action:
//   a.len:
//     cell: biggie
// """
//     val engine = PFAEngine.fromYaml(engineString).head

//     val GB_PER_BYTE = 1.0 / 1024.0 / 1024.0 / 1024.0
//     var usedMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
//     var maxMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax
//     println(s"first memory used ${usedMemBytes * GB_PER_BYTE} GB max ${maxMemBytes * GB_PER_BYTE} GB")

//     println(engine.action(null))
//     usedMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
//     maxMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax
//     println(s"second memory used ${usedMemBytes * GB_PER_BYTE} GB max ${maxMemBytes * GB_PER_BYTE} GB")

//     println(engine.action(null))
//     usedMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
//     maxMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax
//     println(s"third memory used ${usedMemBytes * GB_PER_BYTE} GB max ${maxMemBytes * GB_PER_BYTE} GB")
//   }

  "memory tracker" must "not crash" taggedAs(Memory) in {
    val engineString = """
input: int
output: "null"
cells:
  one:
    type:
      type: record
      name: SimpleRecord
      fields:
        - {name: x, type: int}
        - {name: y, type: double}
        - {name: z, type: string}
    init: {x: 0, y: 0.0, z: ""}
  two:
    type:
      type: array
      items: int
    init: []
  three:
    shared: true
    type:
      type: record
      name: SimpleRecord
      fields:
        - {name: x, type: int}
        - {name: y, type: double}
        - {name: z, type: string}
    init: {x: 0, y: 0.0, z: ""}
  four:
    shared: true
    type:
      type: array
      items: int
    init: []
pools:
  five:
    type:
      type: record
      name: SimpleRecord
      fields:
        - {name: x, type: int}
        - {name: y, type: double}
        - {name: z, type: string}
    init: {}
  six:
    type:
      type: array
      items: int
    init: {}
  seven:
    shared: true
    type:
      type: record
      name: SimpleRecord
      fields:
        - {name: x, type: int}
        - {name: y, type: double}
        - {name: z, type: string}
    init: {}
  eight:
    shared: true
    type:
      type: array
      items: int
    init: {}
action:
  - let:
      rec:
        type: SimpleRecord
        new: {x: input, y: input, z: {s.int: input}}
  - cell: one
    to: rec
  - cell: two
    to: {a.append: [{cell: two}, input]}
  - cell: three
    to: rec
  - cell: four
    to: {a.append: [{cell: two}, input]}
  - pool: five
    path: [{s.int: input}]
    to:
      params: [{old: SimpleRecord}]
      ret: SimpleRecord
      do: rec
    init: rec
  - pool: six
    path: [{s.int: input}]
    to:
      params: [{old: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [old, input]}
    init:
      type: {type: array, items: int}
      value: []
  - pool: seven
    path: [{s.int: input}]
    to:
      params: [{old: SimpleRecord}]
      ret: SimpleRecord
      do: rec
    init: rec
  - pool: eight
    path: [{s.int: input}]
    to:
      params: [{old: {type: array, items: int}}]
      ret: {type: array, items: int}
      do: {a.append: [old, input]}
    init:
      type: {type: array, items: int}
      value: []
  - null
"""

    // println()
    val engine = PFAEngine.fromYaml(engineString).head
    // println("Single engine step 1")
    // println(EngineReport(engine))
    EngineReport(engine)
    engine.action(java.lang.Integer.valueOf(1))
    // println("Single engine step 2")
    // println(EngineReport(engine))
    EngineReport(engine)
    engine.action(java.lang.Integer.valueOf(2))
    // println("Single engine step 3")
    // println(EngineReport(engine))
    EngineReport(engine)
    engine.action(java.lang.Integer.valueOf(2))
    // println("Single engine step 4")
    // println(EngineReport(engine))
    EngineReport(engine)

    // println()
    val engines = PFAEngine.fromYaml(engineString, multiplicity = 3)
    // println("Three engines step 1")
    // println(EnginesReport(engines))
    EnginesReport(engines)
    engines(0).action(java.lang.Integer.valueOf(1))
    // println("Three engines step 2")
    // println(EnginesReport(engines))
    EnginesReport(engines)
    engines(1).action(java.lang.Integer.valueOf(2))
    // println("Three engines step 3")
    // println(EnginesReport(engines))
    EnginesReport(engines)
    engines(2).action(java.lang.Integer.valueOf(2))
    // println("Three engines step 4")
    // println(EnginesReport(engines))
    EnginesReport(engines)
  }
}
