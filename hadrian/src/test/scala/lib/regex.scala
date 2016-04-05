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

package test.scala.lib.regex

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
class LibRegexSuite extends FlatSpec with Matchers {

  "the regex engine" must "not be leaking memory" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [ab(c|d)*]]}
""").head
    for (i <- 0 to 200000) {
       engine.action("abcccdc")
    }
    engine.action("abcccdc").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
  }


  "index" must "be very Posix extended" taggedAs(Lib, LibRegex) in {
   val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[hc]+at"}]}
""").head
    engine1.action("hat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine1.action("cat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine1.action("hhat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,4))
    engine1.action("chat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,4))
    engine1.action("hcat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,4))
    engine1.action("cchchat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine1.action("at").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[hc]?at"}]}
""").head
    engine2.action("hat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine2.action("cat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine2.action("at").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,2))
    engine2.action("dog").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

    val engine3 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[hc]*at"}]}
""").head
    engine3.action("hat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine3.action("cat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine3.action("hhat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,4))
    engine3.action("chat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,4))
    engine3.action("hcat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,4))
    engine3.action("cchchat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine3.action("at").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,2))

    val engine4 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "cat|dog"}]}
""").head
    engine4.action("dog").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine4.action("cat").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,3))
    engine4.action("mouse").asInstanceOf[PFAArray[Int]].toVector should be (Vector())
 
    val engine5 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "(abc){2}|(def){2}"}]}
""").head
    engine5.action("abcabc").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,6))
    engine5.action("defdef").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,6))
    engine5.action("XKASGJ8").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

// backreferences
    val engine6 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, [(the )\1]]}
""").head
    engine6.action("Paris in the the spring").asInstanceOf[PFAArray[Int]].toVector should be (Vector(9,17))

    val engine7 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[[:upper:]ab]"}]}
""").head
    engine7.action("GHab").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,1))
    engine7.action("ab").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,1))
    engine7.action("p").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

  }


  "index" must "return correct index" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "ab(c|d)*"}]}
""").head
    engine.action("abcccdc").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine.action("abddddd").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine.action("XKASGJ8").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, [dog]]}
""").head
    engine1.action("999dogggggg").asInstanceOf[PFAArray[Int]].toVector should be (Vector(3,6))
    engine1.action("cat").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

// check non ascii strings
    val engine0 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "对讲(机|p)*"}]}
""").head
    engine0.action("对讲机机机机机机").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,8))
    engine0.action("对讲pppppppppp").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,12))

// check byte input
  val engine2= PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: int}
action:
  - re.index: [input, {bytes.encodeUtf8: {string: "ab(c|d)*"}}]   
""").head
    engine2.action("abcccdc".getBytes("UTF-8")).asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine2.action("对讲机abcccdc".getBytes("UTF-8")).asInstanceOf[PFAArray[Int]].toVector should be (Vector(9,16))
    engine2.action("对讲机abcccdc讲机".getBytes("UTF-8")).asInstanceOf[PFAArray[Int]].toVector should be (Vector(9,16))
  }



  "contains" must "check if match exists" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {re.contains: [input, [ab(c|d)*]]}
""").head
    engine.action("i83736abcccdc").asInstanceOf[Boolean] should be (true)
    engine.action("938372abddddd").asInstanceOf[Boolean] should be (true)
    engine.action("938272XKASGJ8").asInstanceOf[Boolean] should be (false)

    val engine1 = PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {re.contains: [input, [dog]]}
""").head
    engine1.action("999dogggggg").asInstanceOf[Boolean] should be (true)
    engine1.action("928373cat").asInstanceOf[Boolean] should be   (false)

// check non ascii strings
    val engine2= PFAEngine.fromYaml("""
input: string
output: boolean
action:
  - {re.contains: [input, [对讲机(讲|机)*]]}   
""").head
    engine2.action("abcccdc").asInstanceOf[Boolean] should be (false)
    engine2.action("xyzzzz对讲机机abcc").asInstanceOf[Boolean] should be (true)

// check byte input
    val engine3 = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  - re.contains: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""").head
    engine3.action("abcccdc".getBytes("UTF-8")).asInstanceOf[Boolean] should be (false)
    engine3.action("xyzzzz对讲机机abcc".getBytes("UTF-8")).asInstanceOf[Boolean] should be (true)
  }

  "count" must "count occurances" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {re.count: [input, [ab(c|d)*]]}
""").head
    engine.action("938272XKASGJ8").asInstanceOf[Integer]    should be (0)
    engine.action("iabc1abc2abc2abc").asInstanceOf[Integer] should be (4)
    engine.action("938372abddddd").asInstanceOf[Integer]    should be (1)

    val engine1 = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {re.count: [input, [dog]]}
""").head
    engine1.action("999dogggggg").asInstanceOf[Integer]     should be (1)
    engine1.action("928373cat").asInstanceOf[Integer]       should be (0)
    engine1.action("dogdogdogdogdog").asInstanceOf[Integer] should be (5)
    engine1.action("dogDogdogdogdog").asInstanceOf[Integer] should be (4)
    engine1.action("dogdog \n dogdogdog").asInstanceOf[Integer] should be (5)

    val engine2 = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {re.count: [input, [a*]]}
""").head
    engine2.action("aaaaaaaaaaaaaaa").asInstanceOf[Integer] should be (1)

    val engine3 = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {re.count: [input, [ba]]}
""").head
    engine3.action("ababababababababababa").asInstanceOf[Integer] should be (10)

// check non ascii strings
    val engine4 = PFAEngine.fromYaml("""
input: string
output: int
action:
  - {re.count: [input, [对讲机(讲|机)*]]}   
""").head
    engine4.action("abcccdc").asInstanceOf[Integer] should be (0)
    engine4.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa").asInstanceOf[Integer] should be (3)

// check byte input
    val engine5 = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  - re.count: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""").head
    engine5.action("abcccdc".getBytes("UTF-8")).asInstanceOf[Integer] should be (0)
    engine5.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa".getBytes("UTF-8")).asInstanceOf[Integer] should be (3)
  }


  "rIndex" must "find indices of the last match" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [ab(c|d)*]]}
""").head
    engine.action("abcccdc").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine.action("abddddd").asInstanceOf[PFAArray[Int]].toVector should be (Vector(0,7))
    engine.action("XKASGJ8").asInstanceOf[PFAArray[Int]].toVector should be (Vector())

    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [dog]]}
""").head
    engine1.action("999dogggggg").asInstanceOf[PFAArray[Int]].toVector should be (Vector(3,6))
    engine1.action("cat").asInstanceOf[PFAArray[Int]].toVector should be (Vector())
    engine1.action("catdogpppdog").asInstanceOf[PFAArray[Int]].toVector should be (Vector(9,12))

// check non-ascii string input
    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [对讲机(讲|机)*]]}   
""").head
    engine2.action("abcccdc").asInstanceOf[PFAArray[Int]].toVector should be (Vector())
    engine2.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa").asInstanceOf[PFAArray[Int]].toVector should be (Vector(23,27))

// check byte input
    val engine3 = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: int}
action:
  - re.rindex: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""").head
    engine3.action("abcccdc".getBytes("UTF-8")).asInstanceOf[PFAArray[Int]].toVector should be (Vector())
    engine3.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa".getBytes("UTF-8")).asInstanceOf[PFAArray[Int]].toVector should be (Vector(39,51))
  }


  "groups" must "show which groups matched where" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, [(a(b)c)d]]}
""").head
    engine.action("abcd").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(0,4), Vector(0,3), Vector(1,2)))

    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, [(the )+]]}
""").head
    engine1.action("Paris in the the spring").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(9,17), Vector(13,17)))

    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, [(the )\1]]}
""").head
    engine2.action("Paris in the the spring").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(9,17), Vector(9,13)))

    val engine3 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, [()(a)bc(def)ghijk]]}
""").head
    engine3.action("abcdefghijk").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(0,11), Vector(0,0), Vector(0,1), Vector(3,6)))

// check non-ascii string input
    val engine4 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, [对讲机(讲|机)*]]}   
""").head
    engine4.action("abcccdc").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector())
    engine4.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(6,10), Vector(9,10)))

// check byte input
    val engine5 = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: int}}
action:
  - re.groups: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""").head
    engine5.action("abcccdc".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector())
    engine5.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(6,18), Vector(15,18)))
  }


  "indexAll" must "must return indices of where patterns longest group matched" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.indexall: [input, [ab]]}
""").head
    engine.action("abcabcabc").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(0,2), Vector(3,5), Vector(6,8)))
    engine.action("88cabcc").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(3,5)))

    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.indexall: [input, [(the )\1]]}
""").head
    engine2.action("Paris in the the spring, LA in the the summer").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(9,17), Vector(31,39)))

// check non-ascii string input
    val engine3 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.indexall: [input, [对讲机(讲|机)*]]}   
""").head
    engine3.action("abcccdc").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector())
    engine3.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa").asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(6,10), Vector(14,18), Vector(23,27)))

// check byte input
    val engine4 = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: int}}
action:
  - re.indexall: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""").head
    engine4.action("abcccdc".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector())
    engine4.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[Int]]].toVector.map(_.toVector) should be (Vector(Vector(6,18), Vector(22,34), Vector(39,51)))

  }


  "findAll" must "must return strings from where patterns longest group matched" taggedAs(Lib, LibRegex) in {


    val engineTEST = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findall: [input, [猫(机)+猫]]}
""").head
    engineTEST.action("猫机猫oooo猫机机猫ppp猫机机机猫bbbb猫机aaaa猫机机").asInstanceOf[PFAArray[String]].toVector should be (Vector("猫机猫" ,"猫机机猫","猫机机机猫"))


    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findall: [input, [ab]]}
""").head
    engine.action("abcabcabc").asInstanceOf[PFAArray[String]].toVector should be (Vector("ab","ab", "ab"))
    engine.action("88cabcc").asInstanceOf[PFAArray[String]].toVector should be (Vector("ab"))
    engine.action("88xyz").asInstanceOf[PFAArray[String]].toVector should be (Vector())

// check non-ascii string input
    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findall: [input, [机机+]]}
""").head
    engine1.action("abc机机+abca机机bc  asdkj 机机sd").asInstanceOf[PFAArray[String]].toVector should be (Vector("机机","机机", "机机"))

// check byte input
    val engine2= PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: bytes}
action:
  - re.findall: [input, {bytes.encodeUtf8: {string: "ab+"}}]   
""").head
    engine2.action("xyz".getBytes("UTF-8")).asInstanceOf[PFAArray[Array[Byte]]].toVector should be (Vector())
    for ((x, y) <- engine2.action("ab+c机机ab+cabc".getBytes("UTF-8")).asInstanceOf[PFAArray[Array[Byte]]].toVector zip Vector(Array[Byte](97,98) ,  Array[Byte](97,98), Array[Byte](97,98))) { x.sameElements(y) should be (true) } 

  }


  "findFirst" must "must return first substring of match" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: [string, "null"]
action:
  - {re.findfirst: [input, [ab+]]}
""").head
    engine.action("88ccc555").asInstanceOf[String] should be (null)
    engine.action("abcabcab+c").asInstanceOf[String] should be ("ab")

// check non-ascii input
    val engine1 = PFAEngine.fromYaml("""
input: string
output: [string, "null"]
action:
  - {re.findfirst: [input, [机机+]]}
""").head
    engine1.action("abc机机abca机机bc  asdkj 机机sd").asInstanceOf[String] should be ("机机") 
    engine1.action("abdefg").asInstanceOf[String] should be (null) 

// check byte input
    val engine2 = PFAEngine.fromYaml("""
input: bytes
output: [bytes, "null"]
action:
  - re.findfirst: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""").head
    engine2.action("abcde对讲机讲fgg对讲机讲h".getBytes("UTF-8")).asInstanceOf[Array[Byte]].deep should be ("对讲机讲".getBytes("UTF-8").deep)  
    engine2.action("abcdefghijk".getBytes("UTF-8")).asInstanceOf[Array[Byte]] should be (null)  
  }


  "findGroupsFirst" must "must return strings of the longest group match" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [ab]]}
""").head
    engine.action("abcabcabc").asInstanceOf[PFAArray[String]].toVector should be (Vector("ab"))
    engine.action("88ccc").asInstanceOf[PFAArray[String]].toVector should be (Vector())

    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [()(a)bc(def)ghijk]]}
""").head
    engine1.action("abcdefghijk").asInstanceOf[PFAArray[String]].toVector should be (Vector("abcdefghijk", "", "a", "def"))

    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [(the.)\1]]}
""").head
    engine2.action("Paris in the the spring").asInstanceOf[PFAArray[String]].toVector should be (Vector("the the ", "the "))

// check non-ascii input
    val engine3 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [机(机)]]}
""").head
    engine3.action("abc机机abca机机bc").asInstanceOf[PFAArray[String]].toVector should be (Vector("机机", "机")) 

// check byte input
    val engine4 = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: bytes}
action:
  - re.findgroupsfirst: [input, {bytes.encodeUtf8: {string: "机(机)"}}]   
""").head
  for ((x, y) <- engine4.action("abc机机abca机机bc".getBytes("UTF-8")).asInstanceOf[PFAArray[Array[Byte]]].toVector zip Vector(Array[Byte](-26, -100, -70, -26, -100, -70), Array[Byte](-26, -100, -70))) {x.sameElements(y) should be (true) }
  engine4.action("abcd".getBytes("UTF-8")).asInstanceOf[PFAArray[Array[Byte]]].toVector should be (Vector())  
  }


  "findGroupsAll" must "must return substrings of each group match in each location" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: string}}
action:
  - {re.findgroupsall: [input, [ab]]}
""").head
    engine.action("aabb").asInstanceOf[PFAArray[PFAArray[String]]].toVector.map(_.toVector) should be (Vector(Vector("ab")))
    engine.action("kkabkkabkkab").asInstanceOf[PFAArray[PFAArray[String]]].toVector.map(_.toVector) should be (Vector(Vector("ab"), Vector("ab"), Vector("ab")))
    engine.action("TTTTT").asInstanceOf[PFAArray[PFAArray[String]]].toVector.map(_.toVector) should be (Vector())

    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: string}}
action:
  - {re.findgroupsall: [input, [()(a)bc(def)ghijk]]}
""").head
    engine1.action("abcdefghijkMMMMMabcdefghijkMMMM").asInstanceOf[PFAArray[PFAArray[String]]].toVector.map(_.toVector) should be (Vector(Vector("abcdefghijk", "", "a", "def"), Vector("abcdefghijk","", "a", "def")))

// check non-ascii input
    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: string}}
action:
  - {re.findgroupsall: [input, [机(机)]]}
""").head
    engine2.action("abc机机abca机机bc").asInstanceOf[PFAArray[PFAArray[String]]].toVector.map(_.toVector) should be (Vector(Vector("机机", "机"), Vector("机机", "机"))) 

// check byte input
    val engine3 = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: bytes}}
action:
  - re.findgroupsall: [input, {bytes.encodeUtf8: {string: "机(机)"}}]   
""").head
    for ((x, y) <- engine3.action("abc机机abca机机bc".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[Array[Byte]]]].toVector.map(_.toVector).flatten zip Vector(Vector(Array[Byte](-26, -100, -70, -26, -100, -70), Array[Byte](-26, -100, -70)), Vector(Array[Byte](-26, -100, -70, -26, -100, -70), Array[Byte](-26, -100, -70))).flatten) {x.sameElements(y) should be (true) }
  engine3.action("abcd".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[Array[Byte]]]].toVector.map(_.toVector) should be (Vector())  
  }     


  "groupsAll" must "must return all group indices of all matches" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: {type: array, items: int}}}
action:
  - {re.groupsall: [input, [()(a)bc(def)ghijk]]}
""").head
    engine.action("abcdefghijkMMMMMabcdefghijkMMMM").asInstanceOf[PFAArray[PFAArray[PFAArray[Int]]]].toVector.map(_.toVector.map(_.toVector)) should be (Vector(Vector(Vector(0,11), Vector(0,0), Vector(0,1), Vector(3,6)), Vector(Vector(16, 27),Vector(16,16),Vector(16,17), Vector(19,22))))

// check non-ascii input
    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: {type: array, items: int}}}
action:
  - {re.groupsall: [input, [(机)机]]}
""").head
    engine1.action("abc机机abca机机bc").asInstanceOf[PFAArray[PFAArray[PFAArray[Int]]]].toVector.map(_.toVector.map(_.toVector)) should be (Vector(Vector(Vector(3,5), Vector(3,4)), Vector(Vector(9,11), Vector(9,10))))

// check byte input
    val engine2 = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: {type: array, items: int}}}
action:
  - re.groupsall: [input, {bytes.encodeUtf8: {string: "(机)机"}}]   
""").head
    engine2.action("abc机机abca机机bc".getBytes("UTF-8")).asInstanceOf[PFAArray[PFAArray[PFAArray[Int]]]].toVector.map(_.toVector.map(_.toVector)) should be (Vector(Vector(Vector(3,9), Vector(3,6)), Vector(Vector(13,19), Vector(13,16))))
  }


  "replaceFirst" must "replace first occurance" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacefirst: [input, ["ab(c|d)*"], ["person"]]}
""").head
    engine.action("abcccdcPPPP").asInstanceOf[String] should be ("personPPPP")
    engine.action("PPPPabcccdcPPPPabcccdc").asInstanceOf[String] should be ("PPPPpersonPPPPabcccdc")
    engine.action("PPPPPPPP").asInstanceOf[String] should be ("PPPPPPPP")


    val engine1 = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacefirst: [input, ["ab(c|d)*"], ["walkie talkie"]]}
""").head
    engine1.action("This abcccdc works better than that abcccdc.").asInstanceOf[String] should be ("This walkie talkie works better than that abcccdc.")

// check non-ascii input
    val engine2 = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacefirst: [input, [对讲机+], ["walkie talkie"]]}
""").head
    engine2.action("This 对讲机 works better than that 对讲机.").asInstanceOf[String] should be ("This walkie talkie works better than that 对讲机.")

// check byte input
    val engine3 = PFAEngine.fromYaml("""
input: bytes
output: bytes
action:
  - {re.replacefirst: [input, {bytes.encodeUtf8: {string: "对讲机+"}}, {bytes.encodeUtf8: {string: "walkie talkie"}}]}
""").head
    engine3.action("This 对讲机 works better than that 对讲机.".getBytes("UTF-8")).asInstanceOf[Array[Byte]].deep should be ("This walkie talkie works better than that 对讲机.".getBytes("UTF-8").deep)
  }


  "replaceLast" must "replace last instance" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacelast: [input, ["ab(c|d)*"], ["person"]]}
""").head
    engine.action("abcccdcPPPPabcccdc").asInstanceOf[String] should be ("abcccdcPPPPperson")
    engine.action("abcccdcPPPPabcccdcPPPP").asInstanceOf[String] should be ("abcccdcPPPPpersonPPPP")

// check non-ascii input
    val engine1 = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacelast: [input, [对讲机+], ["walkie talkie"]]}
""").head
    engine1.action("This 对讲机 works better than that 对讲机.").asInstanceOf[String] should be ("This 对讲机 works better than that walkie talkie.")

// check byte input
    val engine2 = PFAEngine.fromYaml("""
input: bytes
output: bytes
action:
  - {re.replacelast: [input, {bytes.encodeUtf8: {string: "对讲机+"}}, {bytes.encodeUtf8: {string: "walkie talkie"}}]}
""").head
    engine2.action("This 对讲机 works better than that 对讲机.".getBytes("UTF-8")).asInstanceOf[Array[Byte]].deep should be ("This 对讲机 works better than that walkie talkie.".getBytes("UTF-8").deep)
  }


  "replaceAll" must "make replacements correctly" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [cow], [doggy]]}
""").head
    engine.action("pcowppcowpppcow").asInstanceOf[String] should be ("pdoggyppdoggypppdoggy")
    engine.action("cowpcowppcowppp").asInstanceOf[String] should be ("doggypdoggyppdoggyppp")

    val engine2 = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [cow], [Y]]}
""").head
    engine2.action("cowpcowppcowppp").asInstanceOf[String] should be ("YpYppYppp")
    engine2.action("pcowppcowpppcow").asInstanceOf[String] should be ("pYppYpppY")

    val engine1 = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [ab(c|d)*], [cow]]}
""").head
    engine1.action("abcccdcPPPP").asInstanceOf[String] should be ("cowPPPP")
    engine1.action("PPPPabcccdc").asInstanceOf[String] should be ("PPPPcow")
    engine1.action("PPabcdddcPPabcccdcPPabcccdcPP").asInstanceOf[String] should be ("PPcowPPcowPPcowPP")

// check non-ascii input
    val engine3 = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [对讲机+], ["walkie talkie"]]}
""").head
    engine3.action("This 对讲机 works better than that 对讲机.").asInstanceOf[String] should be ("This walkie talkie works better than that walkie talkie.")

// check byte input
    val engine4 = PFAEngine.fromYaml("""
input: bytes
output: bytes
action:
  - {re.replaceall: [input, {bytes.encodeUtf8: {string: "对讲机+"}}, {bytes.encodeUtf8: {string: "walkie talkie"}}]}
""").head
    engine4.action("This 对讲机 works better than that 对讲机.".getBytes("UTF-8")).asInstanceOf[Array[Byte]].deep should be ("This walkie talkie works better than that walkie talkie.".getBytes("UTF-8").deep)
  }


  "split" must "split on the pattern" taggedAs(Lib, LibRegex) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.split: [input, [cow]]}
""").head
    engine.action("cowpcowppcowppp").asInstanceOf[PFAArray[String]].toVector should be (Vector("p","pp","ppp"))
    engine.action("pcowppcowpppcow").asInstanceOf[PFAArray[String]].toVector should be (Vector("p","pp","ppp"))

    val engine1 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.split: [input, [ab(c|d)*]]}
""").head
    engine1.action("abcccdcPPPP").asInstanceOf[PFAArray[String]].toVector should be (Vector("PPPP"))
    engine1.action("PPPPabcccdc").asInstanceOf[PFAArray[String]].toVector should be (Vector("PPPP"))
    engine1.action("PPabcccdcPPabcccdcPPabcccdcPP").asInstanceOf[PFAArray[String]].toVector should be (Vector("PP","PP","PP","PP"))

// check non-ascii string input
    val engine2 = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.split: [input, [机机+]]}
""").head
    engine2.action("abc机机abca机机bc  asdkj 机机sd").asInstanceOf[PFAArray[String]].toVector should be (Vector("abc","abca","bc  asdkj ", "sd" ))

// check byte input
    val engine3= PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: bytes}
action:
  - re.split: [input, {bytes.encodeUtf8: {string: "机机+"}}]   
""").head
    for ((x, y) <- engine3.action("xyz".getBytes("UTF-8")).asInstanceOf[PFAArray[Array[Byte]]].toVector zip (Vector(Array[Byte](120,121,122)))) {x.sameElements(y) should be (true)}
    for ((x, y) <- engine3.action("ab机机ab机机abc机机abc".getBytes("UTF-8")).asInstanceOf[PFAArray[Array[Byte]]].toVector zip Vector(Array[Byte](97,98) ,  Array[Byte](97,98), Array[Byte](97,98,99), Array[Byte](97,98,99))) { x.sameElements(y) should be (true) } 
  }
}

