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

package com.opendatagroup.hadrian.lib1

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

package object bytes {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "bytes."

  case class Decoder(charset: String) {
    val decoder = java.nio.charset.Charset.forName(charset).newDecoder
    def check(x: Array[Byte]): Boolean =
      try {
        decoder.decode(java.nio.ByteBuffer.wrap(x))
        true
      }
      catch {
        case err: java.nio.charset.MalformedInputException => false
      }
    def decode(x: Array[Byte]): String =
      try {
        decoder.decode(java.nio.ByteBuffer.wrap(x)).toString
      }
      catch {
        case err: java.nio.charset.MalformedInputException => throw new PFARuntimeException("invalid bytes")
      }
  }

  case class Encoder(charset: String) {
    val encoder = java.nio.charset.Charset.forName(charset).newEncoder
    def check(x: String): Boolean =
      try {
        encoder.encode(java.nio.CharBuffer.wrap(x))
        true
      }
      catch {
        case err: java.nio.charset.UnmappableCharacterException => false
      }
    def encode(s: String): Array[Byte] =
      try {
        val bb = encoder.encode(java.nio.CharBuffer.wrap(s))
        val out = Array.fill[Byte](bb.limit)(0)
        bb.get(out)
        out
      }
      catch {
        case err: java.nio.charset.UnmappableCharacterException => throw new PFARuntimeException("invalid string")
      }
  }

  //////////////////////////////////////////////////////////////////// testers

  ////   isAscii (IsAscii)
  object IsAscii extends LibFcn {
    val name = prefix + "isAscii"
    val sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    val doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid ASCII; <c>false</c> otherwise.</desc>
      </doc>
    val decoder = Decoder("US-ASCII")
    val encoder = Encoder("US-ASCII")
    def apply(x: Array[Byte]): Boolean = decoder.check(x)
    def apply(x: String): Boolean = encoder.check(x)
  }
  provide(IsAscii)

  ////   isLatin1 (IsLatin1)
  object IsLatin1 extends LibFcn {
    val name = prefix + "isLatin1"
    val sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    val doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid latin-1 (ISO-8859-1); <c>false</c> otherwise.</desc>
      </doc>
    val decoder = Decoder("ISO-8859-1")
    val encoder = Encoder("ISO-8859-1")
    def apply(x: Array[Byte]): Boolean = decoder.check(x)
    def apply(x: String): Boolean = encoder.check(x)
  }
  provide(IsLatin1)

  ////   isUtf8 (IsUtf8)
  object IsUtf8 extends LibFcn {
    val name = prefix + "isUtf8"
    val sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    val doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid utf-8; <c>false</c> otherwise.</desc>
      </doc>
    val decoder = Decoder("UTF-8")
    val encoder = Encoder("UTF-8")
    def apply(x: Array[Byte]): Boolean = decoder.check(x)
    def apply(x: String): Boolean = encoder.check(x)
  }
  provide(IsUtf8)

  ////   isUtf16 (IsUtf16)
  object IsUtf16 extends LibFcn {
    val name = prefix + "isUtf16"
    val sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    val doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid utf-16 (byte order identified by optional byte-order mark); <c>false</c> otherwise.</desc>
      </doc>
    val decoder = Decoder("UTF-16")
    val encoder = Encoder("UTF-16")
    def apply(x: Array[Byte]): Boolean = decoder.check(x)
    def apply(x: String): Boolean = encoder.check(x)
  }
  provide(IsUtf16)

  ////   isUtf16be (IsUtf16BE)
  object IsUtf16BE extends LibFcn {
    val name = prefix + "isUtf16be"
    val sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    val doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid big endian utf-16; <c>false</c> otherwise.</desc>
      </doc>
    val decoder = Decoder("UTF-16BE")
    val encoder = Encoder("UTF-16BE")
    def apply(x: Array[Byte]): Boolean = decoder.check(x)
    def apply(x: String): Boolean = encoder.check(x)
  }
  provide(IsUtf16BE)

  ////   isUtf16le (IsUtf16LE)
  object IsUtf16LE extends LibFcn {
    val name = prefix + "isUtf16le"
    val sig = Sigs(List(Sig(List("x" -> P.Bytes), P.Boolean),
                        Sig(List("x" -> P.String), P.Boolean)))
    val doc =
      <doc>
        <desc>Returns <c>true</c> if <p>x</p> is valid little endian utf-16; <c>false</c> otherwise.</desc>
      </doc>
    val decoder = Decoder("UTF-16LE")
    val encoder = Encoder("UTF-16LE")
    def apply(x: Array[Byte]): Boolean = decoder.check(x)
    def apply(x: String): Boolean = encoder.check(x)
  }
  provide(IsUtf16LE)

  //////////////////////////////////////////////////////////////////// decoders

  ////   decodeAscii (DecodeAscii)
  object DecodeAscii extends LibFcn {
    val name = prefix + "decodeAscii"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Decode a bytes object as an ASCII string.</desc>
        <error>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    val decoder = Decoder("US-ASCII")
    def apply(x: Array[Byte]): String = decoder.decode(x)
  }
  provide(DecodeAscii)

  ////   decodeLatin1 (DecodeLatin1)
  object DecodeLatin1 extends LibFcn {
    val name = prefix + "decodeLatin1"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Decode a bytes object as a latin-1 (ISO-8859-1) string.</desc>
        <error>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    val decoder = Decoder("ISO-8859-1")
    def apply(x: Array[Byte]): String = decoder.decode(x)
  }
  provide(DecodeLatin1)

  ////   decodeUtf8 (DecodeUtf8)
  object DecodeUtf8 extends LibFcn {
    val name = prefix + "decodeUtf8"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Decode a bytes object as a utf-8 string.</desc>
        <error>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    val decoder = Decoder("UTF-8")
    def apply(x: Array[Byte]): String = decoder.decode(x)
  }
  provide(DecodeUtf8)

  ////   decodeUtf16 (DecodeUtf16)
  object DecodeUtf16 extends LibFcn {
    val name = prefix + "decodeUtf16"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Decode a bytes object as a utf-16 (byte order identified by optional byte-order mark) string.</desc>
        <error>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    val decoder = Decoder("UTF-16")
    def apply(x: Array[Byte]): String = decoder.decode(x)
  }
  provide(DecodeUtf16)

  ////   decodeUtf16be (DecodeUtf16BE)
  object DecodeUtf16BE extends LibFcn {
    val name = prefix + "decodeUtf16be"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Decode a bytes object as a big endian utf-16 string.</desc>
        <error>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    val decoder = Decoder("UTF-16BE")
    def apply(x: Array[Byte]): String = decoder.decode(x)
  }
  provide(DecodeUtf16BE)

  ////   decodeUtf16le (DecodeUtf16LE)
  object DecodeUtf16LE extends LibFcn {
    val name = prefix + "decodeUtf16le"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Decode a bytes object as a little endian utf-16 string.</desc>
        <error>Raises an "invalid bytes" error if the bytes cannot be converted.</error>
      </doc>
    val decoder = Decoder("UTF-16LE")
    def apply(x: Array[Byte]): String = decoder.decode(x)
  }
  provide(DecodeUtf16LE)

  //////////////////////////////////////////////////////////////////// encoders

  ////   encodeAscii (EncodeAscii)
  object EncodeAscii extends LibFcn {
    val name = prefix + "encodeAscii"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Encode a string as ASCII bytes.</desc>
        <error>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    val encoder = Encoder("US-ASCII")
    def apply(s: String): Array[Byte] = encoder.encode(s)
  }
  provide(EncodeAscii)

  ////   encodeLatin1 (EncodeLatin1)
  object EncodeLatin1 extends LibFcn {
    val name = prefix + "encodeLatin1"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Encode a string as latin-1 (ISO-8859-1) bytes.</desc>
        <error>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    val encoder = Encoder("ISO-8859-1")
    def apply(s: String): Array[Byte] = encoder.encode(s)
  }
  provide(EncodeLatin1)

  ////   encodeUtf8 (EncodeUtf8)
  object EncodeUtf8 extends LibFcn {
    val name = prefix + "encodeUtf8"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Encode a string as utf-8 bytes.</desc>
        <error>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    val encoder = Encoder("UTF-8")
    def apply(s: String): Array[Byte] = encoder.encode(s)
  }
  provide(EncodeUtf8)

  ////   encodeUtf16 (EncodeUtf16)
  object EncodeUtf16 extends LibFcn {
    val name = prefix + "encodeUtf16"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Encode a string as utf-16 (byte order identified by optional byte-order mark) bytes.</desc>
        <error>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    val encoder = Encoder("UTF-16")
    def apply(s: String): Array[Byte] = encoder.encode(s)
  }
  provide(EncodeUtf16)

  ////   encodeUtf16be (EncodeUtf16BE)
  object EncodeUtf16BE extends LibFcn {
    val name = prefix + "encodeUtf16be"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Encode a string as big endian utf-16 bytes.</desc>
        <error>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    val encoder = Encoder("UTF-16BE")
    def apply(s: String): Array[Byte] = encoder.encode(s)
  }
  provide(EncodeUtf16BE)

  ////   encodeUtf16le (EncodeUtf16LE)
  object EncodeUtf16LE extends LibFcn {
    val name = prefix + "encodeUtf16le"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Encode a string as little endian utf-16 bytes.</desc>
        <error>Raises an "invalid string" error if the string cannot be converted.</error>
      </doc>
    val encoder = Encoder("UTF-16LE")
    def apply(s: String): Array[Byte] = encoder.encode(s)
  }
  provide(EncodeUtf16LE)

  //////////////////////////////////////////////////////////////////// base64

  ////   toBase64 (ToBase64)
  object ToBase64 extends LibFcn {
    val name = prefix + "toBase64"
    val sig = Sig(List("x" -> P.Bytes), P.String)
    val doc =
      <doc>
        <desc>Convert an arbitrary bytes object to a base64-encoded string.</desc>
    </doc>
    val objectMapper = new ObjectMapper
    def apply(x: Array[Byte]): String = objectMapper.convertValue(x, classOf[String])
  }
  provide(ToBase64)

  ////   fromBase64 (FromBase64)
  object FromBase64 extends LibFcn {
    val name = prefix + "fromBase64"
    val sig = Sig(List("s" -> P.String), P.Bytes)
    val doc =
      <doc>
        <desc>Convert a base64-encoded string to a bytes object.</desc>
        <error>Raises an "invalid base64" error if the string is not valid base64.</error>
    </doc>
    val objectMapper = new ObjectMapper
    def apply(s: String): Array[Byte] =
      try {
        objectMapper.convertValue(s, classOf[Array[Byte]])
      }
      catch {
        case err: java.lang.IllegalArgumentException => throw new PFARuntimeException("invalid base64")
      }
  }
  provide(FromBase64)

}
