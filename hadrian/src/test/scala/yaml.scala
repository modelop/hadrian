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

package test.scala.yaml

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class YamlSuite extends FlatSpec with Matchers {
  "YAML to JSON conversion" must "convert scalars" taggedAs(Yaml) in {
    yamlToJson("""~""") should be ("""null""")
    yamlToJson("""null""") should be ("""null""")
    yamlToJson("""Null""") should be ("""null""")
    yamlToJson("""NULL""") should be ("""null""")
    yamlToJson("""true""") should be ("""true""")
    yamlToJson("""True""") should be ("""true""")
    yamlToJson("""TRUE""") should be ("""true""")
    yamlToJson("""false""") should be ("""false""")
    yamlToJson("""False""") should be ("""false""")
    yamlToJson("""FALSE""") should be ("""false""")
    yamlToJson("""yes""") should be ("""true""")
    yamlToJson("""Yes""") should be ("""true""")
    yamlToJson("""YES""") should be ("""true""")
    yamlToJson("""no""") should be ("""false""")
    yamlToJson("""No""") should be ("""false""")
    yamlToJson("""NO""") should be ("""false""")
    yamlToJson("""on""") should be ("""true""")
    yamlToJson("""On""") should be ("""true""")
    yamlToJson("""ON""") should be ("""true""")
    yamlToJson("""off""") should be ("""false""")
    yamlToJson("""Off""") should be ("""false""")
    yamlToJson("""OFF""") should be ("""false""")
    yamlToJson("""0""") should be ("""0""")
    yamlToJson("""1""") should be ("""1""")
    yamlToJson("""-0""") should be ("""0""")
    yamlToJson("""-1""") should be ("""-1""")
    yamlToJson("""1_111""") should be ("""1111""")
    yamlToJson("""0xff""") should be ("""255""")
    yamlToJson("""077""") should be ("""63""")
    yamlToJson("""2.2""") should be ("""2.2""")
    yamlToJson("""-2.2""") should be ("""-2.2""")
    yamlToJson("""1_111.222_2""") should be ("""1111.2222""")
    yamlToJson("""1e10""") should be ("""1.0E10""")
    yamlToJson("""1e+10""") should be ("""1.0E10""")
    yamlToJson("""1e-10""") should be ("""1.0E-10""")
    yamlToJson("""2.2e10""") should be ("""2.2E10""")
    yamlToJson(""".2e10""") should be ("""2.0E9""")
    yamlToJson(""".inf""") should be (""""Infinity"""")
    yamlToJson(""".Inf""") should be (""""Infinity"""")
    yamlToJson(""".INF""") should be (""""Infinity"""")
    yamlToJson("""-.inf""") should be (""""-Infinity"""")
    yamlToJson("""-.Inf""") should be (""""-Infinity"""")
    yamlToJson("""-.INF""") should be (""""-Infinity"""")
    yamlToJson(""".nan""") should be (""""NaN"""")
    yamlToJson(""".NaN""") should be (""""NaN"""")
    yamlToJson(""".NAN""") should be (""""NaN"""")
    yamlToJson("""hello""") should be (""""hello"""")
    yamlToJson(""""hello"""") should be (""""hello"""")
    yamlToJson("""'hello'""") should be (""""hello"""")
  }

  it must "convert both kinds of sequences, adding \"pos\" to maps" taggedAs(Yaml) in {
    yamlToJson("""[1, 2, 3]""") should be ("""[1,2,3]""")
    yamlToJson("""[1, 2, 3,]""") should be ("""[1,2,3]""")
    yamlToJson("""[1, 2, [3, 4, 5]]""") should be ("""[1,2,[3,4,5]]""")
    yamlToJson("""
- 1
- 2
- 3
""") should be ("""[1,2,3]""")
    yamlToJson("""
- 1
- 2
- [3, 4, 5]
""") should be ("""[1,2,[3,4,5]]""")
    yamlToJson("""
- 1
- 2
-
  - 3
  - 4
  - 5
""") should be ("""[1,2,[3,4,5]]""")
    yamlToJson("""{"one": 1, "two": 2, "three": 3}""") should be ("""{"@":"YAML line 1","one":1,"two":2,"three":3}""")
    yamlToJson("""{"one": 1, "two": 2, "three": 3,}""") should be ("""{"@":"YAML line 1","one":1,"two":2,"three":3}""")
    yamlToJson("""
"one": 1
"two": 2
"three": 3
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"three":3}""")
    yamlToJson("""
one: 1
two: 2
three: 3
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"three":3}""")
    yamlToJson("""
one: 1
two: 2
three: {four: 4, five: 5}
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"three":{"@":"YAML line 4","four":4,"five":5}}""")

    yamlToJson("""
- 1
- 2
- {three: 3, four: 4}
""") should be ("""[1,2,{"@":"YAML line 4","three":3,"four":4}]""")
    yamlToJson("""
- 1
- 2
-
  three: 3
  four: 4
""") should be ("""[1,2,{"@":"YAML lines 5 to 7","three":3,"four":4}]""")
    yamlToJson("""
one: 1
two: 2
three: [3, 4]
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"three":[3,4]}""")
    yamlToJson("""
one: 1
two: 2
three:
  - 3
  - 4
""") should be ("""{"@":"YAML lines 2 to 7","one":1,"two":2,"three":[3,4]}""")
  }

  it must "simplify all fancy types to maps" taggedAs(Yaml) in {
    intercept[org.yaml.snakeyaml.constructor.ConstructorException] { yamlToJson("""!!set {one, two, three}""") }
    intercept[org.yaml.snakeyaml.constructor.ConstructorException] { yamlToJson("""!!omap [one: 1, two: 2, three: 3]""") }
    intercept[org.yaml.snakeyaml.constructor.ConstructorException] { yamlToJson("""!!pairs [one: 1, two: 2, three: 3]""") }
  }

  it must "support references" taggedAs(Yaml) in {
    yamlToJson("""
one: 1
two: &THIS
  - 2
  - 3
  - 4
three: *THIS
four: *THIS
""") should be ("""{"@":"YAML lines 2 to 9","one":1,"two":[2,3,4],"three":[2,3,4],"four":[2,3,4]}""")
  }

  it must "fail on recursive references" taggedAs(Yaml) in {
    intercept[java.lang.StackOverflowError] { yamlToJson("""
one: 1
two: &THIS
  - 2
  - 3
  - *THIS
""") }
  }

  it must "non-string key" taggedAs(Yaml) in {
    yamlToJson("""
one: 1
two: 2
3: 3
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"3":3}""")

    yamlToJson("""
one: 1
two: 2
[3, 3]: 3
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"[3, 3]":3}""")

    yamlToJson("""
one: 1
two: 2
{"three": 3}: 3
""") should be ("""{"@":"YAML lines 2 to 5","one":1,"two":2,"{@=YAML line 4, three=3}":3}""")
  }

}
