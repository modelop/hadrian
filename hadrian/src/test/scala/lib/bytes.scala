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

package test.scala.lib.bytes

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class LibBytesSuite extends FlatSpec with Matchers {
  //////////////////////////////////////////////////////////////////// basic access
  
  "basic access" must "get length" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  bytes.len: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)).asInstanceOf[java.lang.Integer].intValue should be (5)
  }

  it must "get subseq" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: string
action:
  {bytes.decodeAscii: {bytes.subseq: [{bytes.encodeAscii: {string: ABCDEFGHIJKLMNOPQRSTUVWXYZ}}, 5, input]}}
""").head
    engine.action(java.lang.Integer.valueOf(10)) should be ("FGHIJ")
    engine.action(java.lang.Integer.valueOf(-10)) should be ("FGHIJKLMNOP")
    engine.action(java.lang.Integer.valueOf(0)) should be ("")
    engine.action(java.lang.Integer.valueOf(1)) should be ("")
    engine.action(java.lang.Integer.valueOf(100)) should be ("FGHIJKLMNOPQRSTUVWXYZ")
  }

  it must "set subseq" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: int
output: string
action:
  {bytes.decodeAscii: {bytes.subseqto: [{bytes.encodeAscii: {string: ABCDEFGHIJKLMNOPQRSTUVWXYZ}}, 5, input, {bytes.encodeAscii: {string: ...}}]}}
""").head
    engine.action(java.lang.Integer.valueOf(10)) should be ("ABCDE...KLMNOPQRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(-10)) should be ("ABCDE...QRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(0)) should be ("ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(1)) should be ("ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
    engine.action(java.lang.Integer.valueOf(100)) should be ("ABCDE...")
  }

  //////////////////////////////////////////////////////////////////// encoding testers

  "tests" must "check ascii" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  bytes.isAscii: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
    engine.action(Array[Byte](104, 101, 108, -127, 111)).asInstanceOf[java.lang.Boolean].booleanValue should be (false)
  }

  it must "check latin-1" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  bytes.isLatin1: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "check utf-8" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  bytes.isUtf8: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "check utf-16" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  bytes.isUtf16: input
""").head
    engine.action(Array[Byte](-1, -2, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "check utf-16be" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  bytes.isUtf16be: input
""").head
    engine.action(Array[Byte](0, 104, 0, 101, 0, 108, 0, 108, 0, 111)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  it must "check utf-16le" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  bytes.isUtf16le: input
""").head
    engine.action(Array[Byte](104, 0, 101, 0, 108, 0, 108, 0, 111, 0)).asInstanceOf[java.lang.Boolean].booleanValue should be (true)
  }

  //////////////////////////////////////////////////////////////////// decoders

  "decoders" must "decode ascii" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.decodeAscii: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)) should be ("hello")
    evaluating { engine.action(Array[Byte](104, 101, 108, -127, 111)) } should produce [PFARuntimeException]
  }

  it must "decode latin-1" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.decodeLatin1: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)) should be ("hello")
  }

  it must "decode utf-8" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.decodeUtf8: input
""").head
    engine.action(Array[Byte](104, 101, 108, 108, 111)) should be ("hello")
  }

  it must "decode utf-16" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.decodeUtf16: input
""").head
    engine.action(Array[Byte](-1, -2, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0)) should be ("hello")
  }

  it must "decode utf-16be" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.decodeUtf16be: input
""").head
    engine.action(Array[Byte](0, 104, 0, 101, 0, 108, 0, 108, 0, 111)) should be ("hello")
  }

  it must "decode utf-16le" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.decodeUtf16le: input
""").head
    engine.action(Array[Byte](104, 0, 101, 0, 108, 0, 108, 0, 111, 0)) should be ("hello")
  }

  //////////////////////////////////////////////////////////////////// encoders

  "encoders" must "encode ascii" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.encodeAscii: input
""").head
    engine.action("hello") should be (Array[Byte](104, 101, 108, 108, 111))
    evaluating { engine.action(new String(Array[Byte](104, 101, 108, -127, 111))) } should produce [PFARuntimeException]
  }

  it must "encode latin-1" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.encodeLatin1: input
""").head
    engine.action("hello") should be (Array[Byte](104, 101, 108, 108, 111))
  }

  it must "encode utf-8" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.encodeUtf8: input
""").head
    engine.action("hello") should be (Array[Byte](104, 101, 108, 108, 111))
  }

  it must "encode utf-16" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.encodeUtf16: input
""").head
    engine.action("hello") should be (Array[Byte](-2, -1, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111))
  }

  it must "encode utf-16be" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.encodeUtf16be: input
""").head
    engine.action("hello") should be (Array[Byte](0, 104, 0, 101, 0, 108, 0, 108, 0, 111))
  }

  it must "encode utf-16le" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.encodeUtf16le: input
""").head
    engine.action("hello") should be (Array[Byte](104, 0, 101, 0, 108, 0, 108, 0, 111, 0))
  }

  //////////////////////////////////////////////////////////////////// base64

  "base64" must "convert to base64" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: bytes
output: string
action:
  bytes.toBase64: input
""").head
    engine.action(Array[Byte](0, 127, 64, 38, 22)) should be ("AH9AJhY=")
  }

  it must "convert from base64" taggedAs(Lib, LibBytes) in {
    val engine = PFAEngine.fromYaml("""
input: string
output: bytes
action:
  bytes.fromBase64: input
""").head
    engine.action("AH9AJhY=") should be (Array[Byte](0, 127, 64, 38, 22))
  }

}
