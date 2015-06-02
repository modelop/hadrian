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

package test.scala.lib1.time

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
class Lib1TimeSuite extends FlatSpec with Matchers {
  "Year" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.year: input} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (2015)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (2015)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (2015)
}

  "MonthOfYear" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.monthOfYear: input} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (3)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (3)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (3)
}

  "DayOfYear" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfYear: input} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (64)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (64)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (64)
}

  "DayOfMonth" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfMonth: input} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (4)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (4)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (4)
}

  "DayOfWeek" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.dayOfWeek: input} 
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (2)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (2)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (2)
}

  "HourOfDay" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.hourOfDay: input}
""").head
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Integer]                should be (0)

}

  "MinuteOfHour" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.minuteOfHour: input}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (35)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (35)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (35)
}

  "SecondOfMinute" must "return the correct anwer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: int
action:
  - {time.secondOfMinute: input}
""").head
    engine.action(java.lang.Double.valueOf(1425508527)).asInstanceOf[Integer]       should be (27)
    engine.action(java.lang.Double.valueOf(1.425508527E9)).asInstanceOf[Integer]    should be (27)
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Integer] should be (27)
}

  "makeTimestamp" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    // time.makeTimestamp(year:int, month:int, day:int, hour:int,
    //                    minute:int, second:int, offset:double)
    val engine = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {time.makeTimestamp: [input, 3, 11, 14, 50, 6, 245]}
""").head
    engine.action(java.lang.Integer.valueOf(2015)).asInstanceOf[Double] should be (1426085406.245)

    val engine2 = PFAEngine.fromYaml("""
input: int
output: double
action:
  - {time.makeTimestamp: [input, 1, 1, 0, 0, 0, 0]}
""").head
    engine2.action(java.lang.Integer.valueOf(1970)).asInstanceOf[Double] should be (0)
}

  "isSecondOfMinute" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isSecondOfMinute: [input, 25, 29]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
}

  "isMinuteOfHour" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isMinuteOfHour: [input, 34, 36]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
}

  "isHourOfDay" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isHourOfDay: [input, 20, 23]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)

    val engine1 = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isHourOfDay: [input, 0, 0.01]}
""").head
    engine1.action(java.lang.Double.valueOf(0)).asInstanceOf[Boolean]               should be (true)
}

  "isDayOfWeek" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfWeek: [input, 2, 3]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
}

  "isDayOfMonth" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfMonth: [input, 3, 5]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
}

  "isDayOfYear" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isDayOfYear: [input, 1, 300]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
}

  "isMonthOfYear" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isMonthOfYear: [input, 2, 7]}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
}

  "isWeekend" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isWeekend: input}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (false)
    engine.action(java.lang.Double.valueOf(1427598240.0)).asInstanceOf[Boolean] should be (true)
}

  "isWorkHours" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isWorkHours: input}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (false)
    engine.action(java.lang.Double.valueOf(1427724421.0)).asInstanceOf[Boolean] should be (true)
}

  "isNonWorkHours" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: boolean
action:
  - {time.isNonWorkHours: input}
""").head
    engine.action(java.lang.Double.valueOf(1425508527.52482)).asInstanceOf[Boolean] should be (true)
    engine.action(java.lang.Double.valueOf(1427724421.0)).asInstanceOf[Boolean] should be (false)
}

  "fromUTCToLocal" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {time.fromUTCToLocal: [input, -5]}
""").head
    engine.action(java.lang.Double.valueOf(1426018000)).asInstanceOf[Double] should be (1426000000)
    engine.action(java.lang.Double.valueOf(1400018000)).asInstanceOf[Double] should be (1400000000)
    engine.action(java.lang.Double.valueOf(1000018000)).asInstanceOf[Double] should be (1000000000)
    engine.action(java.lang.Double.valueOf(18000)).asInstanceOf[Double] should be (0)
}

  "fromLocalToUTC" must "return the correct answer" taggedAs(Lib1, Lib1Time) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: double
action:
  - {time.fromLocalToUTC: [input, -5]}
""").head
    engine.action(java.lang.Double.valueOf(1426000000)).asInstanceOf[Double] should be (1426018000)
    engine.action(java.lang.Double.valueOf(1400000000)).asInstanceOf[Double] should be (1400018000)
    engine.action(java.lang.Double.valueOf(1000000000)).asInstanceOf[Double] should be (1000018000)
    engine.action(java.lang.Double.valueOf(0)).asInstanceOf[Double] should be (18000)
}}


