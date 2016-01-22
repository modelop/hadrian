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

package com.opendatagroup.hadrian.lib

import scala.collection.immutable.ListMap

import org.codehaus.jackson.map.ObjectMapper

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAFixed

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroNull
import com.opendatagroup.hadrian.datatype.AvroBoolean
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroBytes
import com.opendatagroup.hadrian.datatype.AvroFixed
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroEnum
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion

import com.opendatagroup.hadrian.signature.Sigs
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.P

import com.opendatagroup.hadrian.lib.array.startEnd
import com.opendatagroup.hadrian.lib.array.describeIndex

package object bytes {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "bytes."

  //////////////////////////////////////////////////////////////////// basic access

  ////   len (Len)
  class Len(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], Int] {
    def name = prefix + "len"
    def sig = Sig(List("x" -> P.Bytes), P.Int)
    def doc =
      <doc>
        <desc>Return the length of byte array <p>x</p>.</desc>
      </doc>
    def errcodeBase = 16000
    def apply(x: Array[Byte]): Int = x.size
  }
  provide(new Len)

  ////   subseq (Subseq)
  class Subseq(val pos: Option[String] = None) extends LibFcn with Function3[Array[Byte], Int, Int, Array[Byte]] {
    def name = prefix + "subseq"
    def sig = Sig(List("x" -> P.Bytes, "start" -> P.Int, "end" -> P.Int), P.Bytes)
    def doc =
      <doc>
        <desc>Return the subsequence of <p>x</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive).</desc>{describeIndex}
      </doc>
    def errcodeBase = 16010
    def apply(x: Array[Byte], start: Int, end: Int): Array[Byte] = {
      val (normStart, normEnd) = startEnd(x.size, start, end)
      java.util.Arrays.copyOfRange(x, normStart, normEnd)
    }
  }
  provide(new Subseq)

  ////   subseqto (SubseqTo)
  class SubseqTo(val pos: Option[String] = None) extends LibFcn with Function4[Array[Byte], Int, Int, Array[Byte], Array[Byte]] {
    def name = prefix + "subseqto"
    def sig = Sig(List("x" -> P.Bytes, "start" -> P.Int, "end" -> P.Int, "replacement" -> P.Bytes), P.Bytes)
    def doc =
      <doc>
        <desc>Replace <p>x</p> from <p>start</p> (inclusive) until <p>end</p> (exclusive) with <p>replacement</p>.</desc>{describeIndex}
      </doc>
    def errcodeBase = 16020
    def apply(x: Array[Byte], start: Int, end: Int, replacement: Array[Byte]): Array[Byte] = {
      val (normStart, normEnd) = startEnd(x.size, start, end)

      val out = Array.fill[Byte]((normStart - 0) + replacement.size + (x.size - normEnd))(0)

      //               src            srcPos     dest   destPos                         length
      System.arraycopy(x,             0,         out,   0,                              normStart)
      System.arraycopy(replacement,   0,         out,   normStart,                      replacement.size)
      System.arraycopy(x,             normEnd,   out,   normStart + replacement.size,   x.size - normEnd)
      out
    }
  }
  provide(new SubseqTo)

  //////////////////////////////////////////////////////////////////// encoding/decoding

  case class Decoder(charset: String) {
    def decoder = java.nio.charset.Charset.forName(charset).newDecoder
    def check(x: Array[Byte]): Boolean =
      try {
        decoder.decode(java.nio.ByteBuffer.wrap(x))
        true
      }
      catch {
        case err: java.nio.charset.MalformedInputException => false
      }
    def decode(x: Array[Byte], code: Int, fcnName: String, pos: Option[String]): String =
      try {
        decoder.decode(java.nio.ByteBuffer.wrap(x)).toString
      }
      catch {
        case err: java.nio.charset.MalformedInputException => throw new PFARuntimeException("invalid bytes", code, fcnName, pos)
      }
  }

  case class Encoder(charset: String) {
    def encoder = java.nio.charset.Charset.forName(charset).newEncoder
    def check(x: String): Boolean =
      try {
        encoder.encode(java.nio.CharBuffer.wrap(x))
        true
      }
      catch {
        case err: java.nio.charset.UnmappableCharacterException => false
      }
    def encode(s: String, code: Int, fcnName: String, pos: Option[String]): Array[Byte] =
      try {
        val bb = encoder.encode(java.nio.CharBuffer.wrap(s))
        val out = Array.fill[Byte](bb.limit)(0)
        bb.get(out)
        out
      }
      catch {
        case err: java.nio.charset.UnmappableCharacterException => throw new PFARuntimeException("invalid string", code, fcnName, pos)
      }
  }

  val US_ASCII_Decoder = Decoder("US-ASCII")
  val ISO_8859_1_Decoder = Decoder("ISO-8859-1")
  val UTF8_Decoder = Decoder("UTF-8")
  val UTF16_Decoder = Decoder("UTF-16")
  val UTF16BE_Decoder = Decoder("UTF-16BE")
  val UTF16LE_Decoder = Decoder("UTF-16LE")

  val US_ASCII_Encoder = Encoder("US-ASCII")
  val ISO_8859_1_Encoder = Encoder("ISO-8859-1")
  val UTF8_Encoder = Encoder("UTF-8")
  val UTF16_Encoder = Encoder("UTF-16")
  val UTF16BE_Encoder = Encoder("UTF-16BE")
  val UTF16LE_Encoder = Encoder("UTF-16LE")

  //////////////////////////////////////////////////////////////////// testers
  
  ////   isAscii (IsAscii)
  class IsAscii(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isAscii"
    def sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    def doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid ASCII; <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 16030
    def apply(x: Array[Byte]): Boolean = US_ASCII_Decoder.check(x)
    def apply(x: String): Boolean = US_ASCII_Encoder.check(x)
  }
  provide(new IsAscii)

  ////   isLatin1 (IsLatin1)
  class IsLatin1(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isLatin1"
    def sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    def doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid latin-1 (ISO-8859-1); <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 16040
    def apply(x: Array[Byte]): Boolean = ISO_8859_1_Decoder.check(x)
    def apply(x: String): Boolean = ISO_8859_1_Encoder.check(x)
  }
  provide(new IsLatin1)

  ////   isUtf8 (IsUtf8)
  class IsUtf8(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isUtf8"
    def sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    def doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid utf-8; <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 16050
    def apply(x: Array[Byte]): Boolean = UTF8_Decoder.check(x)
    def apply(x: String): Boolean = UTF8_Encoder.check(x)
  }
  provide(new IsUtf8)

  ////   isUtf16 (IsUtf16)
  class IsUtf16(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isUtf16"
    def sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    def doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid utf-16 (byte order identified by optional byte-order mark); <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 16060
    def apply(x: Array[Byte]): Boolean = UTF16_Decoder.check(x)
    def apply(x: String): Boolean = UTF16_Encoder.check(x)
  }
  provide(new IsUtf16)

  ////   isUtf16be (IsUtf16BE)
  class IsUtf16BE(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isUtf16be"
    def sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    def doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid big endian utf-16; <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 16070
    def apply(x: Array[Byte]): Boolean = UTF16BE_Decoder.check(x)
    def apply(x: String): Boolean = UTF16BE_Encoder.check(x)
  }
  provide(new IsUtf16BE)

  ////   isUtf16le (IsUtf16LE)
  class IsUtf16LE(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "isUtf16le"
    def sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    def doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid little endian utf-16; <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 16080
    def apply(x: Array[Byte]): Boolean = UTF16LE_Decoder.check(x)
    def apply(x: String): Boolean = UTF16LE_Encoder.check(x)
  }
  provide(new IsUtf16LE)

  //////////////////////////////////////////////////////////////////// decoders

  ////   decodeAscii (DecodeAscii)
  class DecodeAscii(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "decodeAscii"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Decode a bytes object as an ASCII string.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    def errcodeBase = 16090
    def apply(x: Array[Byte]): String = US_ASCII_Decoder.decode(x, errcodeBase + 0, name, pos)
  }
  provide(new DecodeAscii)

  ////   decodeLatin1 (DecodeLatin1)
  class DecodeLatin1(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "decodeLatin1"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Decode a bytes object as a latin-1 (ISO-8859-1) string.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    def errcodeBase = 16100
    def apply(x: Array[Byte]): String = ISO_8859_1_Decoder.decode(x, errcodeBase + 0, name, pos)
  }
  provide(new DecodeLatin1)

  ////   decodeUtf8 (DecodeUtf8)
  class DecodeUtf8(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "decodeUtf8"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Decode a bytes object as a utf-8 string.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    def errcodeBase = 16110
    def apply(x: Array[Byte]): String = UTF8_Decoder.decode(x, errcodeBase + 0, name, pos)
  }
  provide(new DecodeUtf8)

  ////   decodeUtf16 (DecodeUtf16)
  class DecodeUtf16(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "decodeUtf16"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Decode a bytes object as a utf-16 (byte order identified by optional byte-order mark) string.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    def errcodeBase = 16120
    def apply(x: Array[Byte]): String = UTF16_Decoder.decode(x, errcodeBase + 0, name, pos)
  }
  provide(new DecodeUtf16)

  ////   decodeUtf16be (DecodeUtf16BE)
  class DecodeUtf16BE(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "decodeUtf16be"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Decode a bytes object as a big endian utf-16 string.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    def errcodeBase = 16130
    def apply(x: Array[Byte]): String = UTF16BE_Decoder.decode(x, errcodeBase + 0, name, pos)
  }
  provide(new DecodeUtf16BE)

  ////   decodeUtf16le (DecodeUtf16LE)
  class DecodeUtf16LE(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "decodeUtf16le"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Decode a bytes object as a little endian utf-16 string.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    def errcodeBase = 16140
    def apply(x: Array[Byte]): String = UTF16LE_Decoder.decode(x, errcodeBase + 0, name, pos)
  }
  provide(new DecodeUtf16LE)

  //////////////////////////////////////////////////////////////////// encoders

  ////   encodeAscii (EncodeAscii)
  class EncodeAscii(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "encodeAscii"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Encode a string as ASCII bytes.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    def errcodeBase = 16150
    def apply(s: String): Array[Byte] = US_ASCII_Encoder.encode(s, errcodeBase + 0, name, pos)
  }
  provide(new EncodeAscii)

  ////   encodeLatin1 (EncodeLatin1)
  class EncodeLatin1(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "encodeLatin1"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Encode a string as latin-1 (ISO-8859-1) bytes.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    def errcodeBase = 16160
    def apply(s: String): Array[Byte] = ISO_8859_1_Encoder.encode(s, errcodeBase + 0, name, pos)
  }
  provide(new EncodeLatin1)

  ////   encodeUtf8 (EncodeUtf8)
  class EncodeUtf8(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "encodeUtf8"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Encode a string as utf-8 bytes.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    def errcodeBase = 16170
    def apply(s: String): Array[Byte] = UTF8_Encoder.encode(s, errcodeBase + 0, name, pos)
  }
  provide(new EncodeUtf8)

  ////   encodeUtf16 (EncodeUtf16)
  class EncodeUtf16(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "encodeUtf16"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Encode a string as utf-16 (byte order identified by optional byte-order mark) bytes.</desc>
        <nondeterministic type="unstable" />
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    def errcodeBase = 16180
    def apply(s: String): Array[Byte] = UTF16_Encoder.encode(s, errcodeBase + 0, name, pos)
  }
  provide(new EncodeUtf16)

  ////   encodeUtf16be (EncodeUtf16BE)
  class EncodeUtf16BE(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "encodeUtf16be"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Encode a string as big endian utf-16 bytes.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    def errcodeBase = 16190
    def apply(s: String): Array[Byte] = UTF16BE_Encoder.encode(s, errcodeBase + 0, name, pos)
  }
  provide(new EncodeUtf16BE)

  ////   encodeUtf16le (EncodeUtf16LE)
  class EncodeUtf16LE(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "encodeUtf16le"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Encode a string as little endian utf-16 bytes.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    def errcodeBase = 16200
    def apply(s: String): Array[Byte] = UTF16LE_Encoder.encode(s, errcodeBase + 0, name, pos)
  }
  provide(new EncodeUtf16LE)

  //////////////////////////////////////////////////////////////////// base64

  val objectMapper = new ObjectMapper

  ////   toBase64 (ToBase64)
  class ToBase64(val pos: Option[String] = None) extends LibFcn with Function1[Array[Byte], String] {
    def name = prefix + "toBase64"
    def sig = Sig(List("x" -> P.Bytes), P.String)
    def doc =
      <doc>
        <desc>Convert an arbitrary bytes object to a base64-encoded string.</desc>
    </doc>
    def errcodeBase = 16210
    def apply(x: Array[Byte]): String = objectMapper.convertValue(x, classOf[String])
  }
  provide(new ToBase64)

  ////   fromBase64 (FromBase64)
  class FromBase64(val pos: Option[String] = None) extends LibFcn with Function1[String, Array[Byte]] {
    def name = prefix + "fromBase64"
    def sig = Sig(List("s" -> P.String), P.Bytes)
    def doc =
      <doc>
        <desc>Convert a base64-encoded string to a bytes object.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid base64" error if the string is not valid base64.</error>
    </doc>
    def errcodeBase = 16220
    def apply(s: String): Array[Byte] =
      try {
        objectMapper.convertValue(s, classOf[Array[Byte]])
      }
      catch {
        case err: java.lang.IllegalArgumentException => throw new PFARuntimeException("invalid base64", errcodeBase + 0, name, pos)
      }
  }
  provide(new FromBase64)

}
