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

package test.scala.lib.time

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

// This is March 4 2015, 4:35:27
// 1425508527.52482

@RunWith(classOf[JUnitRunner])
class LibTimeSuite extends FlatSpec with Matchers {
  "Year" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.year: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (2015)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (2015)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (2015)
  }

  "MonthOfYear" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.monthOfYear: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (3)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (3)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (3)
  }

  "DayOfYear" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfYear: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (63)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (63)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (63)
  }

  "DayOfMonth" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfMonth: [input, {string: ""}]} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (4)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (4)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (4)
  }

  "DayOfWeek" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfWeek: [input, {string: ""}]} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (2)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (2)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (2)
  }

  "HourOfDay" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.hourOfDay: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Integer]                should be (0)

    val engine2 = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {time.hourOfDay: [0, input]}
""").head
    engine2.action("Europe/Paris").asInstanceOf[Integer]                            should be (1)
    engine2.action("Etc/UTC").asInstanceOf[Integer]                                 should be (0)
    engine2.action("Atlantic/Azores").asInstanceOf[Integer]                         should be (23)
  }

  "MinuteOfHour" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.minuteOfHour: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (35)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (35)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (35)
  }

  "SecondOfMinute" must "return the correct anwer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.secondOfMinute: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (27)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (27)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (27)
  }

  "makeTimestamp" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {time.makeTimestamp: [input, 3, 11, 14, 50, 6, 245, {string: ""}]}
""").head
    engine.action(java.lang.Integer.valueOf(2015)).asInstanceOf[Double] should be (1426085406.245)

    val engine2 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {time.makeTimestamp: [input, 1, 1, 0, 0, 0, 0, {string: ""}]}
""").head
    engine2.action(java.lang.Integer.valueOf(1970)).asInstanceOf[Double] should be (0)

    val engine3 = PFAEngine.fromYaml("""
input: string
output: double
action:
  - {time.makeTimestamp: [1970, 1, 1, 0, 0, 0, 0, input]}
""").head
    engine3.action("Europe/Paris").asInstanceOf[Double] should be (-3600)
    engine3.action("Etc/UTC").asInstanceOf[Double] should be (0)
    engine3.action("Atlantic/Azores").asInstanceOf[Double] should be (3600)
  }

  "isSecondOfMinute" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isSecondOfMinute: [input, {string: ""}, 25, 29]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
  }

  "isMinuteOfHour" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isMinuteOfHour: [input, {string: ""}, 34, 36]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
  }

  "isHourOfDay" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isHourOfDay: [input, {string: ""}, 20, 23]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)

    val engine1 = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isHourOfDay: [input, {string: ""}, 0, 0.01]}
""").head
    engine1.action(java.lang.Double.valueOf(0)).asInstanceOf[Boolean]               should be (true)
  }

  "isDayOfWeek" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfWeek: [input, {string: ""}, 2, 3]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
  }

  "isDayOfMonth" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfMonth: [input, {string: ""}, 3, 5]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
  }

  "isDayOfYear" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfYear: [input, {string: ""}, 1, 300]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
  }

  "isMonthOfYear" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isMonthOfYear: [input, {string: ""}, 2, 7]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
  }

  "isWeekend" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isWeekend: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (false)
    engine.action(java.lang.Double.valueOf(1427598240.0)).asInstanceOf[Boolean] should be (true)
  }

  "isWorkHours" must "return the correct answer" taggedAs(Lib, LibTime) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isWorkHours: [input, {string: ""}]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (false)
    engine.action(java.lang.Double.valueOf(1427724421.0)).asInstanceOf[Boolean] should be (true)
  }
}
